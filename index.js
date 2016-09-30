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

//------------------------------------------------------------------

const netUtils = require('./utils/network_utils.js');
const msgUtils = require('./utils/message_utils.js');
const messages = require('./utils/messageGeneration/messages.js');
const util = require('util');
const RosLogStream = require('./utils/log/RosLogStream.js');
const ConsoleLogStream = require('./utils/log/ConsoleLogStream.js');
const LogFormatter = require('./utils/log/LogFormatter.js');
const RosNode = require('./lib/RosNode.js');
const NodeHandle = require('./lib/NodeHandle.js');
const Logging = require('./lib/Logging.js');
const MsgLoader = require('./utils/messageGeneration/MessageLoader.js');

// these will be modules, they depend on logger which isn't initialized yet
// though so they'll be required later (in initNode)
// let RosNode = null;
// let NodeHandle = null;

// will be initialized through call to initNode
let log = Logging.getLogger();
let rosNode = null;

//------------------------------------------------------------------

function _checkMasterHelper(timeout=500) {
  let firstCheck = true;

  const localHelper = (resolve) => {
    setTimeout(() => {
      // also check that the slave api server is set up
      if (!rosNode.slaveApiSetupComplete()) {
        localHelper(resolve);
        return;
      }
      // else
      if (firstCheck) {
        // hook into master api connection errors.
        // api client will continue trying to connect
        rosNode._masterApi.getXmlrpcClient().once('ECONNREFUSED', (err) => {
          log.warn(`Unable to register with master node [${rosNode.getRosMasterUri()}]: master may not be running yet. Will keep trying.`);
        });
        firstCheck = false;
      }
      rosNode.getMasterUri()
      .then(() => {
        log.infoOnce(`Connected to master at ${rosNode.getRosMasterUri()}!`);
        resolve();
      })
      .catch((err, resp) => {
        log.warnThrottle(60000, 'Unable to connect to master. ' + err);
        localHelper(resolve);
      })
    }, timeout);
  };

  return new Promise((resolve, reject) => {
    localHelper(resolve);
  });
}

/**
 * Very basic validation of node name - needs to start with a '/'
 * TODO: more
 * @return {string} name of node after validation
 */
function _validateNodeName(nodeName) {
  if (!nodeName.startsWith('/')) {
    nodeName = '/' + nodeName;
  }
  return nodeName;
}

/**
 * Appends a random string of numeric characters to the end
 * of the node name. Follows rospy logic.
 * @param nodeName {string} string to anonymize
 * @return {string} anonymized nodeName
 */
function _anonymizeNodeName(nodeName) {
  return util.format('%s_%s_%s', nodeName, process.pid, Date.now());
}

let Rosnodejs = {
  /**
   * Initializes a ros node for this process. Only one ros node can exist per process
   * If called a second time with the same nodeName, returns a handle to that node.
   * @param nodeName {string} name of the node to initialize
   * @param options {object} overrides for this node
   * @return {Promise} resolved when connection to master is established
   */
  initNode(nodeName, options) {
    options = options || {};
    if (options.anonymous) {
      nodeName = _anonymizeNodeName(nodeName);
    }

    nodeName = _validateNodeName(nodeName);

    if (rosNode !== null) {
      if (nodeName === rosNode.getNodeName()) {
        return Promise.resolve(this.getNodeHandle());
      }
      // else
      throw new Error('Unable to initialize node [' + nodeName + '] - node ['
                      + rosNode.getNodeName() + '] already exists');
    }

    let rosMasterUri = process.env.ROS_MASTER_URI;
    if (options.rosMasterUri) {
      rosMasterUri = options.rosMasterUri;
    }

    Logging.initializeNodeLogger(nodeName, options.logging);

    // create the ros node. Return a promise that will
    // resolve when connection to master is established
    rosNode = new RosNode(nodeName, rosMasterUri);

    return this.use(options.messages, options.services)
      .then(_checkMasterHelper)
      .then(Logging.initializeRosOptions.bind(Logging, this, options.logging))
      .then(() => { return this.getNodeHandle(); })
      .catch((err) => {
        log.error('Error: ' + err);
      });
  },

  reset() {
    rosNode = null;
  },

  loadPackage(packageName, outputDir=null, verbose=false) {
    const msgLoader = new MsgLoader(verbose);
    if (!outputDir) {
      outputDir = msgUtils.getTopLevelMessageDirectory();
    }
    return msgLoader.buildPackage(packageName, outputDir)
    .then(() => {
      console.log('Finished building messages!');
    })
    .catch((err) => {
      console.error(err);
    });
  },

  loadAllPackages(outputDir=null, verbose=false) {
    const msgLoader = new MsgLoader(verbose);
    if (!outputDir) {
      outputDir = msgUtils.getTopLevelMessageDirectory();
    }
    return msgLoader.buildPackageTree(outputDir)
      .then(() => {
        console.log('Finished building messages!');
      })
  },

  require(msgPackage) {
    // check our registry of on-demand generate message definition
    var fromRegistry = messages.getPackageFromRegistry(msgPackage);
    if (fromRegistry) {
      return fromRegistry;
    }

    // if we can't find it in registry, check for gennodejs
    // pre-compiled versions
    let pack = msgUtils.getPackage(msgPackage);
    if (!pack) {
      try {
        msgUtils.loadMessagePackage(msgPackage);
        return msgUtils.getPackage(msgPackage);
      }
      catch(err) {
      }
    }
    // else
    return pack;
  },

  /** check that a message definition is loaded for a ros message
      type, e.g., geometry_msgs/Twist */
  checkMessage(type) {
    const parts = type.split('/');
    let rtv;
    try {
      rtv = this.require(parts[0]).msg[parts[1]];
    } catch(e) {}
    return rtv;
  },

  /** check that a service definition is loaded for a ros service
      type, e.g., turtlesim/TeleportRelative */
  checkService(type) {
    const parts = type.split('/');
    let rtv;
    try {
      rtv = this.require(parts[0]).srv[parts[1]];
    } catch(e) {}
    return rtv;
  },

  /** create message classes and services classes for all the given
   * types before calling callback */
  use(messages, services) {
    const self = this;
    // FIXME: make old on the fly work again
    return Promise.resolve();
    // return self._useMessages(messages)
    //     .then(() => { return self._useServices(services); });
  },

  shutdown() {
    return rosNode.shutdown();
  },

  /** create message classes for all the given types */
  _useMessages(types=[]) {
    const self = this;

    // make sure required types are available
    [
      // for action lib:
      'actionlib_msgs/GoalStatusArray',
      'actionlib_msgs/GoalID',
      // for logging:
      'rosgraph_msgs/Log',
    ].forEach(function(type) {
      if (!self.checkMessage(type)) {
        // required message definition not available yet, load it
        // on-demand
        types.unshift(type);
      }
    });

    if (!types || types.length == 0) {
      return Promise.resolve();
    }
    var count = types.length;
    return new Promise((resolve, reject) => {
      types.forEach(function(type) {
        messages.getMessage(type, function(error, Message) {
          if (--count == 0) {
            resolve();
          }
        });
      });
    });
  },

  /** create service classes for all the given types */
  _useServices(types=[]) {
    if (!types || types.length == 0) {
      return Promise.resolve();
    }
    var count = types.length;
    return new Promise((resolve, reject) => {
      types.forEach(function(type) {
        messages.getService(type, function() {
          if (--count == 0) {
            resolve();
          }
        });
      });
    });
  },

  /**
   * @return {NodeHandle} for initialized node
   */
  getNodeHandle() {
    return new NodeHandle(rosNode);
  },

  get nodeHandle() {
    return new NodeHandle(rosNode);
  },

  get nh() {
    return new NodeHandle(rosNode);
  },

  get log() {
    return Logging;
  },

  get logStreams() {
    return {
      console: ConsoleLogStream,
      ros:     RosLogStream
    }
  }
};

module.exports = Rosnodejs;
