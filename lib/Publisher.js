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
 *    Unless required by applicable law or agreed to in writing,
 *    software distributed under the License is distributed on an "AS
 *    IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language
 *    governing permissions and limitations under the License.
 */

"use strict";

let SerializationUtils = require('../utils/serialization_utils.js');
let Serialize = SerializationUtils.Serialize;
let TcprosUtils = require('../utils/tcpros_utils.js');
let EventEmitter = require('events');
let Logging = require('./Logging.js');

class Publisher extends EventEmitter {
  constructor(options, nodeHandle) {
    super();
    this._topic = options.topic;

    this._type = options.type;

    this._latching = !!options.latching;

    this._tcpNoDelay =  !!options.tcpNoDelay;


    if (options.queueSize) {
      this._queueSize = options.queueSize;
    }
    else {
      this._queueSize = 1;
    }

    /**
     * throttleMs interacts with queueSize to determine when to send
     * messages.
     *  < 0  : send immediately - no interaction with queue
     * >= 0 : place event at end of event queue to publish message
         after minimum delay (MS)
     */
    if (options.hasOwnProperty('throttleMs')) {
      this._throttleMs = options.throttleMs;
    }
    else {
      this._throttleMs = 0;
    }
    this._pubTimeout = null;
    this._pubTime = null;

    // OPTIONS STILL NOT HANDLED:
    //  headers: extra headers to include
    //  subscriber_listener: callback for new subscribers connect/disconnect

    this._resolve = !!options.resolve;

    this._lastSentMsg = null;

    this._nodeHandle = nodeHandle;
    this._nodeHandle.getSpinner().addClient(this, this._getSpinnerId(), this._queueSize, this._throttleMs);

    this._log = Logging.getLogger('ros.rosnodejs');

    this._ready = false;

    this._subClients = {};

    this._messageHandler = options.typeClass;

    this._register();
  }

  _getSpinnerId() {
    return `Publisher://${this.getTopic()}`;
  }

  getTopic() {
    return this._topic;
  }

  getType() {
    return this._type;
  }

  getLatching() {
    return this._latching;
  }

  getNumSubscribers() {
    return Object.keys(this._subClients).length;
  }

  shutdown() {
    this._nodeHandle.unadvertise(this.getTopic());
  }

  disconnect() {
    Object.keys(this._subClients).forEach((clientId) => {
      const client = this._subClients[clientId];
      client.end();
      client.destroy();
    });

    // disconnect from the spinner in case we have any pending callbacks
    this._nodeHandle.getSpinner().disconnect(this._getSpinnerId());
    this._subClients = {};
  }

  /**
   * Schedule the msg for publishing - or publish immediately if we're
   * supposed to
   * @param msg {object} object type matching this._type
   * @param [throttleMs] {number} optional override for publisher setting
   */
  publish(msg, throttleMs) {
    if (typeof throttleMs !== 'number') {
      throttleMs = this._throttleMs;
    }

    if (throttleMs < 0) {
      // short circuit JS event queue, publish synchronously
      this._handleMsgQueue([msg]);
    }
    else {
      this._nodeHandle.getSpinner().ping(this._getSpinnerId(), msg);
    }
  }

  /**
   * Pulls all msgs off queue, serializes, and publishes them
   */
  _handleMsgQueue(msgQueue) {
    const numClients = this.getNumSubscribers();
    if (numClients === 0) {
      this._log.debugThrottle(2000, `Publishing message on ${this.getTopic()} with no subscribers`);
    }

    this._pubTime = Date.now();
    msgQueue.forEach((msg) => {
      try {
        if (this._resolve) {
          msg = this._messageHandler.Resolve(msg);
        }

        const serializedMsg = TcprosUtils.serializeMessage(this._messageHandler, msg);

        Object.keys(this._subClients).forEach((client) => {
          this._subClients[client].write(serializedMsg);
        });

        // if this publisher is supposed to latch,
        // save the last message. Any subscribers that connect
        // before another call to publish() will receive this message
        if (this.getLatching()) {
          this._lastSentMsg = serializedMsg;
        }
      }
      catch (err) {
        this._log.error('Error when publishing message on topic %s: %s', this.getTopic(), err.stack);
      }
    });
  }

  handleSubscriberConnection(subscriber, header) {
    let error = TcprosUtils.validateSubHeader(
      header, this.getTopic(), this.getType(),
      this._messageHandler.md5sum());
    if (error !== null) {
      this._log.error('Unable to validate subscriber connection header '
                      + JSON.stringify(header));
      subscriber.end(Serialize(error));
      return;
    }
    // else
    this._log.info('Pub %s got connection header %s', this.getTopic(), JSON.stringify(header));

    // create and send response
    let respHeader =
      TcprosUtils.createPubHeader(
        this._nodeHandle.getNodeName(),
        this._messageHandler.md5sum(),
        this.getType(),
        this.getLatching());
    subscriber.write(respHeader);

    // if this publisher had the tcpNoDelay option set
    // disable the nagle algorithm
    if  (this._tcpNoDelay) {
      subscriber.setNoDelay(true);
    }

    subscriber.on('close', () => {
      this._log.info('Publisher %s client %s disconnected!',
                      this.getTopic(), subscriber.name);
      delete this._subClients[subscriber.name];
      this.emit('disconnect');
    });

    subscriber.on('end', () => {
      this._log.info('Sub %s sent END', subscriber.name);
    });

    subscriber.on('error', () => {
      this._log.warn('Sub %s had error', subscriber.name);
    });

    if (this._lastSentMsg !== null) {
      this._log.debug('Sending latched msg to new subscriber');
      subscriber.write(this._lastSentMsg);
    }

    // if handshake good, add to list, we'll start publishing to it
    this._subClients[subscriber.name] = subscriber;

    this.emit('connection', header, subscriber.name);
  }

  _register() {
    this._nodeHandle.registerPublisher(this._topic, this._type)
    .then((resp) => {
      this._log.info('Registered %s as a publisher: %j', this._topic, resp);
      let code = resp[0];
      let msg = resp[1];
      let subs = resp[2];
      if (code === 1) {
        // registration worked
        this._ready = true;
        this.emit('registered');
      }
    })
    .catch((err, resp) => {
      this._log.error('reg pub err ' + err + ' resp: '
                      + JSON.stringify(resp));
    })
  }
}

module.exports = Publisher;
