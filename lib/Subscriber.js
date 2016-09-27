/*
 *    Copyright 2016 Rethink Robotics
 *
 *    Copyright 2016 Chris Smith
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

"use strict";

let NetworkUtils = require('../utils/network_utils.js');
let SerializationUtils = require('../utils/serialization_utils.js');
let DeserializeStream = SerializationUtils.DeserializeStream;
let Deserialize =  SerializationUtils.Deserialize;
let Serialize = SerializationUtils.Serialize;
let TcprosUtils = require('../utils/tcpros_utils.js');
let Socket = require('net').Socket;
let EventEmitter = require('events');
let Logging = require('./Logging.js');

let protocols = [['TCPROS']];

//-----------------------------------------------------------------------

class Subscriber extends EventEmitter {

  constructor(options, nodeHandle) {
    super();

    this._topic = options.topic;

    this._type = options.type;

    if (options.queueSize) {
      this._queueSize = options.queueSize;
    }
    else {
      this._queueSize = 1;
    }

    /**
     * throttleMs interacts with queueSize to determine when to handle callbacks
     *  < 0  : handle immediately - no interaction with queue
     *  >= 0 : place event at end of event queue to handle after minimum delay (MS)
     */
    if (options.hasOwnProperty('throttleMs')) {
      this._throttleMs = options.throttleMs;
    }
    else {
      this._throttleMs = 0;
    }

    this._msgHandleTime = null;

    this._nodeHandle = nodeHandle;
    this._nodeHandle.getSpinner().addClient(this, this._getSpinnerId(), this._queueSize, this._throttleMs);

    this._log = Logging.getLogger('ros.rosnodejs');

    this._messageHandler = options.typeClass;

    this._ready = false;

    this._msgQueue = [];

    this._pubClients = {};

    /**
     * map of node uris we requested connections from
     */
    this._nodeMap = {};

    this._register();

    this._deserializer = new DeserializeStream();
  }

  _getSpinnerId() {
    return `Subscriber://${this.getTopic()}`;
  }

  getTopic() {
    return this._topic;
  }

  getType() {
    return this._type;
  }

  getNumPublishers() {
    return Object.keys(this._pubClients).length;
  }

  shutdown() {
    this._nodeHandle.unsubscribe(this.getTopic());
  }

  /**
   * Send a topic request to each of the publishers we haven't connected to yet
   * @param pubs {Array} array of uris of nodes that are publishing this topic
   */
  requestTopicFromPubs(pubs) {
    // filter out any uris we have already connected to
    pubs = pubs.filter((pubUri) => {
      return !this._nodeMap.hasOwnProperty(pubUri.trim());
    });
    pubs.forEach((pubUri) => {
      // pull the ip address and port from the publisher uri
      pubUri = pubUri.trim();
      this._nodeMap[pubUri] = 1;
      let info = NetworkUtils.getAddressAndPortFromUri(pubUri);
      // send a topic request to the publisher's node
      this._log.debug('Sending topic request to ' + JSON.stringify(info));
      this._nodeHandle.requestTopic(info.host, info.port, this._topic, protocols)
      .then((resp) => {
        this._handleTopicRequestResponse(resp, pubUri);
      })
      .catch((err, resp) => {
        // there was an error in the topic request
        this._log.warn('Error requesting topic on %s: %s, %s', this.getTopic(), err, resp);
        delete this._nodeMap[pubUri];
      });
    });
  }

  disconnect() {
    Object.keys(this._pubClients).forEach((clientId) => {
      const client = this._pubClients[clientId];
      client.end();
      client.destroy();
    });

    // disconnect from the spinner in case we have any pending callbacks
    this._nodeHandle.getSpinner().disconnect(this._getSpinnerId());
    this._pubClients = {};
  }

  _register() {
    this._nodeHandle.registerSubscriber(this._topic, this._type)
    .then((resp) => {
      // handle response from register subscriber call
      let code = resp[0];
      let msg = resp[1];
      let pubs = resp[2];
      if ( code === 1 ) {
        if (pubs.length > 0) {
          // this means we're ok and that publishers already exist on this topic
          // we should connect to them
          this.requestTopicFromPubs(pubs);
        }
        this.emit('registered');
      }
    })
    .catch((err, resp) => {
      this._log.warn('Error during subscriber %s registration: %s', this.getTopic(), err);
    })
  }

  /**
   * @param resp {Array} xmlrpc response to a topic request
   */
  _handleTopicRequestResponse(resp, nodeUri) {
    this._log.debug('Topic request response: ' + JSON.stringify(resp));
    let info = resp[2];
    let port = info[2];
    let address = info[1];
    let client = new Socket();
    client.name = address + ':' + port;
    client.nodeUri = nodeUri;

    client.on('end', () => {
      this._log.info('Pub %s sent END', client.name, this.getTopic());
    });
    client.on('error', () => {
      this._log.warn('Pub %s error on topic %s', client.name, this.getTopic());
    });

    client.connect(port, address, () => {
      this._log.debug('Subscriber on ' + this.getTopic() + ' connected to publisher at ' + address + ':' + port);
      client.write(this._createTcprosHandshake());
    });

    client.$boundMessageHandler = this._handleMessage.bind(this, client);
    let deserializer = new DeserializeStream();
    client.pipe(deserializer);
    deserializer.on('message', client.$boundMessageHandler);
  }

  _createTcprosHandshake() {
    return TcprosUtils.createSubHeader(this._nodeHandle.getNodeName(), this._messageHandler.md5sum(), this.getTopic(), this.getType());
  }

  _handleMessage(client, msg) {
    if (!client.$initialized) {
      let header = TcprosUtils.parseTcpRosHeader(msg);
      // check if the publisher had a problem with our connection header
      if (header.error) {
        this._log.error(header.error);
        return;
      }

      // else validate publisher's header
      const error = TcprosUtils.validatePubHeader(header, this.getType(), this._messageHandler.md5sum());
      if (error) {
        this._log.error(`Unable to validate subscriber ${this.getTopic()} connection header ${JSON.stringify(header)}`);
        TcprosUtils.parsePubHeader(msg);
        client.end(Serialize(error));
        return;
      }
      this._log.debug('Subscriber ' + this.getTopic() + ' got connection header ' + JSON.stringify(header));
      this._pubClients[client.name] = client;
      client.$initialized = true;

      this.emit('connection', header, client.name);

      client.on('close', () => {
        this._log.info('Pub %s closed on topic %s', client.name, this.getTopic());
        this._log.debug('Subscriber ' + this.getTopic() + ' client ' + client.name + ' disconnected!');
        delete this._nodeMap[client.nodeUri];
        delete this._pubClients[client.name];

        this.emit('disconnect')
      });
    }
    else {
      // deserialize message
      // deserialization returns object {data: <msg>, buffer: <remaining buffer after deserialization>}
      // remaining buffer should be empty
      // data should be your entire message
      if (this._throttleMs < 0) {
        this._handleMsgQueue([msg]);
      }
      else {
        this._nodeHandle.getSpinner().ping(this._getSpinnerId(), msg);
      }
    }
  }

  _handleMsgQueue(msgQueue) {
    msgQueue.forEach((msg) => {
      try {
        this.emit('message', this._messageHandler.deserialize(msg));
      }
      catch (err) {
        this._log.warn('Error while dispatching message ' + err);
      }
    });

    // clear out queue
    this._msgQueue = [];
  }
}

//-----------------------------------------------------------------------

module.exports = Subscriber;
