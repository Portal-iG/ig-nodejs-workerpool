var cluster = require('cluster');
var logger = require('winston');

/**
 * @class WorkerPool
 * 
 * Manages a set of worker process for client job processing
 *
 * @param {String} workerPath Path to worker js file
 * @param {Number} maxWorkers Maximum number of worker processes to keep active
 * @param {object=} options	  Options
 * 	   @param {object=} env 			ENV variables to set on each worker at launch
 * 	   @param {Number} workerTimeout 	Timeout (ms) to disconnect and replace a worker
 *         after sending it a job 	                                
 *
 * @todo refactor to reduce size of methods
 * 
 * @author Ricardo Massa 
 */
function WorkerPool (workerPath, maxWorkers, options) {
	this._maxWorkers = maxWorkers;
	this._workerPath = workerPath;
	this._options = WorkerPool._normalizeOptions(options);

	this._workerPath = workerPath;
	this._workerMap  = {};
	this._workerPool = [];
	this._workerRequests = {};
	this._workerCount = 0;
	this._listeners = {};
	this._closed = false;

	this._setupWorkers();
}

/**
 * Fills the optional pool parameters with default values
 * 		
 * @param  {object} userOptions (Probably incomplete) user supplied options
 * @return {object}             A complete options object
 */
WorkerPool._normalizeOptions = function (userOptions) {
	var opts = userOptions || {};
	opts.env = opts.env || {};
	opts.workerTimeout = opts.workerTimeout || 60000;
	opts.workerArgs = opts.workerArgs || [];
	return opts;
}

/**
 * Initialize the worker pool (init processes)
 * 
 * @return {void} 
 */
WorkerPool.prototype._setupWorkers = function () {
	this._setupCluster();
}

/**
 * Configure cluster constants and worker behavior handling
 * 
 * @return {void} 
 */
WorkerPool.prototype._setupCluster = function () {
	var self = this;

	cluster.setupMaster({
		exec: this._workerPath,
		args: self._options.workerArgs
	});

	this._listeners.online = function (worker) {
		logger.info('Worker ' + worker.id + ' online');
	}

	cluster.on('online', this._listeners.online);

	this._listeners.disconnect = function (worker) {
		if (typeof self._workerMap[worker.id] == 'undefined') {
			logger.warn('Worker', worker.id, 'disconnected but it was not spawned by this pool',
					self._workerPath);
			return;
		}

		self._workerCount--;

		logger.info('Worker ' + worker.id + ' disconnected');
		var id = worker.id;

		self._unregisterWorker(worker);

		var request = self._workerRequests[id];
		if (request) {
			clearTimeout(request.timer);
			worker.removeListener('message', request.messageListener);
			var cb = request.callback;
			delete(request[id]);
			cb(new Error('Worker disconnected'));
		}		
	}

	cluster.on('disconnect', this._listeners.disconnect);
}

/**
 * Spawns a new worker process
 * 
 * @return {void}
 */
WorkerPool.prototype._spawnWorker = function (cb) {
	var self = this;

	this._workerCount++;

	logger.info('Spawning a worker');

	var worker = cluster.fork(this._options.env);

	/*
	 * While this worker doesn't get ready, it is not pushed
	 * into _workerPool (thus its index is -1)
	 */
	this._workerMap[worker.id] = -1;

	worker.once('message', function (msg) {
		// @todo unit tests for this
		if (msg.event == 'error') {
			worker.disconnect();
			this._workerCount--;
			cb(msg.message)
			return;
		}
		if (msg.event != 'ready') {
			logger.warn('Unknown message from worker', msg);
			return;
		}
		logger.info('Worker', worker.id, 'is ready to receive jobs');
		cb(null, worker);
	})
}

/**
 * Drops a worker (likely to be tainted by an exception or timeout)
 * from the pool
 * 
 * @param  {Worker} worker Worker to drop
 * @return {void}
 */
WorkerPool.prototype._unregisterWorker = function (worker) {
	logger.info('Unregistering worker ' + worker.id);
	var oldId = worker.id;
	var index = this._workerMap[oldId];
	delete this._workerMap[oldId]
	if (index > -1) {
		this._workerPool.splice(index, 1);
	}
}

/**
 * Shutdown idle workers and stops accepting process requests.
 * Currently busy workers are also shutdown after returning.
 * 
 * @return {void}
 */
WorkerPool.prototype.shutdown = function () {
	if (this._closed) return;
	this._closed = true;
	for (var i = 0; i < this._workerPool.length; i++)
		this._workerPool[i].disconnect();
	cluster.removeListener('online', this._listeners.online);
	/**
	 * @todo Wait for all workers to disconnect before dropping 
	 * this event listener
	 */
	cluster.removeListener('disconnect', this._listeners.disconnect);
}

/**
 * Attempts to process a client message using an available worker
 * 
 * @param  {mixed}   msg  Message to process
 * @param  {Function} cb  Callback
 * @return {void}
 */
WorkerPool.prototype.process = function (msg, cb) {
	var self = this;

	if (this._closed) {
		cb(new Error('The pool cannot process requests after shutdown'));
		return;
	}

	var worker = this._workerPool.pop();
	if (!worker) {
		if (this._workerCount == this._maxWorkers) {
			var msg = 'No worker available to process message ' + JSON.stringify(msg); 
			logger.error(msg);
			cb(new Error(msg));
			return;			
		}

		this._spawnWorker(function (err, worker) {
			if (err) {
				logger.error('Error initializing worker', err.stack || err);
				cb(err)
				return;
			}
			self._sendJob(worker, msg, cb);
		})
		return;
	}

	this._sendJob(worker, msg, cb)
}

WorkerPool.prototype._sendJob = function (worker, msg, cb) {
	var self = this;

	logger.info('Dispatching message ', msg, ' to worker ' + worker.id);

	this._workerMap[worker.id] = -1;

	var messageListener = function (message) {
		if (message.event != 'done' && message.event != 'heartbeat')
			return;

		if (message.event == 'heartbeat') {
			clearTimeout(self._workerRequests[worker.id].timer);

			self._workerRequests[worker.id].timer = self._createTimeout(worker)
			return;
		}

		worker.removeListener('message', messageListener);

		clearTimeout(self._workerRequests[worker.id].timer);
		delete self._workerRequests[worker.id];

		if (self._closed) {
			self._unregisterWorker(worker);

		} else if (self._workerMap[worker.id] === -1) {
			self._workerPool.push(worker);
			self._workerMap[worker.id] = self._workerPool.length - 1;
		}

		cb(message.error, message.result);
	}

	worker.on('message', messageListener)

	var timer = self._createTimeout(worker);

	this._workerRequests[worker.id] = {
		callback: cb,
		messageListener: messageListener,
		timer: timer
	}

	worker.send(msg);
}

/**
 * Verifies that the work pool is full
 * 
 * @return {Boolean} If it is full, it returns true
 */
WorkerPool.prototype.isFull = function () {
	return (this._workerPool.length == 0 && this._workerCount == this._maxWorkers);
}

WorkerPool.prototype._createTimeout = function(worker) {

	var self = this;
	return setTimeout(function () {
		if (self._workerMap[worker.id] === -1) {
			logger.warn('Worker ' + worker.id + ' is taking too long to respond - killing it');
			worker.kill();	
		}
	}, self._options.workerTimeout)
}

// For debugging worker pool behavior
WorkerPool.debug = {
	WorkerListener: require('./worker-listener'),
	JobReporter: require('./job-reporter')
}

module.exports = WorkerPool;