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

'use strict';

const timeUtils = require('../utils/time_utils.js');
let EventEmitter = require('events');

class ActionClient extends EventEmitter {
  constructor(options) {
    super();

    this._actionType = options.type;

    this._actionServer = options.actionServer;

    const nh = options.nh;

    // FIXME: support user options for these parameters
    const goalOptions = Object.assign({ queueSize: 10, latching: true }, options.goal);
    this._goalPub = nh.advertise(this._actionServer + '/goal', 
                                 this._actionType + 'Goal',
                                 goalOptions);

    const cancelOptions = Object.assign({ queueSize: 10, latching: true }, options.cancel);
    this._cancelPub = nh.advertise(this._actionServer + '/cancel', 
                                   'actionlib_msgs/GoalID',
                                   cancelOptions);

    const statusOptions = Object.assign({queueSize: 1}, options.status);
    this._statusSub = nh.subscribe(this._actionServer + '/status', 
                                   'actionlib_msgs/GoalStatusArray',
                                   (msg) => { this._handleStatus(msg); },
                                   statusOptions );

    const feedbackOptions = Object.assign({queueSize: 1}, options.feedback);
    this._feedbackSub = nh.subscribe(this._actionServer + '/feedback', 
                                     this._actionType + 'Feedback',
                                     (msg) => { this._handleFeedback(msg); },
                                     feedbackOptions );

    const resultOptions = Object.assign({queueSize: 1}, options.result);
    this._resultSub = nh.subscribe(this._actionServer + '/result',
                                   this._actionType + 'Result',
                                   (msg) => { this._handleResult(msg); },
                                    resultOptions );

    this._goals = {};
    this._goalCallbacks = {};
    this._goalSeqNum = 0;
  }

  _handleStatus(msg) {
    this.emit('status', msg);
  }

  _handleFeedback(msg) {
    const goalId = msg.status.goal_id.id;
    if  (this._goals.hasOwnProperty(goalId)) {
      this.emit('feedback', msg);
    }
  }

  _handleResult(msg) {
    const goalId = msg.status.goal_id.id;
    if (this._goals.hasOwnProperty(goalId)) {
      delete this._goals[goalId];
      this.emit('result', msg);
    }
  }

  cancel(goalId) {
    if (this._goals.hasOwnProperty(goalId)) {
      this._cancelPub.publish({
        stamp: timeUtils.now(),
        id: goalId
      });
    }
  }

  sendGoal(goal) {
    if (!goal.goal_id) {
      goal.goal_id = {
        stamp: timeUtils.now(),
        id: this.generateGoalId()
      };
    }
    if (!goal.header) {
      goal.header = {
        seq: this._goalSeqNum++,
        stamp: goal.goal_id.stamp,
        frame_id: 'auto-generated'
      };
    }
    const goalId = goal.goal_id.id;
    this._goals[goalId] = goal;

    this._goalPub.publish(goal);
    return goal;
  }

  generateGoalId() {
    let id = this._actionType + '.';
    id += 'xxxxxxxx'.replace(/[x]/g, function(c) {
      return (Math.random()*16).toString(16);
    });
    return id;
  }
}

module.exports = ActionClient;
