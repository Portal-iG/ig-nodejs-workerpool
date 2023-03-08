var EventEmitter = require('events').EventEmitter;
var util = require('util');
var cluster = require('cluster');

/**
 * Intercepts communication between cluster master and spawned
 * workers
 *
 * @author Ricardo Massa
 */
function WorkerListener () {
	this._workers = [];
	this._listeners = {
		online: null,
		workers: []
	}
}
util.inherits(WorkerListener, EventEmitter);

/**
 * Starts listening for worker forking
 * @return {void} 
 */
WorkerListener.prototype.startListening = function () {
	var self = this;

	function handleOnline (worker) {
		WorkerListener._listeners.online.apply(self, [ worker ]);
	}

	this._listeners.online = handleOnline;
	cluster.on('online', handleOnline);
}

/**
 * Stops all worker sniffing
 * @return {void} 
 */
WorkerListener.prototype.stopListening = function () {
	cluster.removeListener('online', this._listeners.online);
	for (var i = 0; i < this._workers.length; i++) {
		this._workers[i].removeListener('message', 
				this._listeners.workers[i]);
	}
}

WorkerListener._listeners = {};

/**
 * Handles the event of a spawning of a worker
 * @param  {Worker} worker The worker just spawned
 * @return {void}        
 */
WorkerListener._listeners.online = function (worker) {
	var self = this;
	this._workers.push(worker);

	function handleMessage (msg) {
		WorkerListener._listeners.message.apply(self, [ worker, msg ]);
	}

	this._listeners.workers.push(handleMessage);
	worker.on('message', handleMessage);
}

/**
 * Handles an intercepted message between a worker and master
 * @param  {Worker} worker Worker which sent message
 * @param  {object} msg    Message the worker sent
 * @return {void}        
 */
WorkerListener._listeners.message = function (worker, msg) {
	this.emit('message', { fromWorker: worker, forwardedMessage: msg });
}

module.exports = WorkerListener;