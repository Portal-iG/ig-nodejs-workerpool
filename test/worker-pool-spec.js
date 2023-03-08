var cluster = require('cluster');
var async = require('async');
var WorkerPool = require('../lib/worker-pool');
var WorkerListener = require('../lib/worker-listener');
var winston = require('winston');

/**
 * Test suite for worker-pool module
 *
 * @todo test worker suicide
 * @todo test worker initialization timeout 
 * @todo test exception on worker initialization 
 * @todo test worker responding after timeout
 * 
 * @author Ricardo Massa
 */

describe("worker-pool module test suite", function() {

var sumWorker = __dirname + '/sum-worker';
var echoWorker = __dirname + '/echo-args-worker';

var workerListener;

beforeEach(function () {
	/*
	 * Setting up a workerListener to intercept communication between
	 * the worker-pool and its child workers
	 */ 
	workerListener = new WorkerListener;
	workerListener.startListening();
});

afterEach(function () {
	workerListener.stopListening();
});

it("should process a single message", function(done) {
	var pool = new WorkerPool(sumWorker, 1);

	workerListener.on('message', function (message) {
		if (message.forwardedMessage.event == 'requested') {
			message.fromWorker.send('resume');
		}
	})

	pool.process(
			toMessage([1, 2, 3]), 
			function (err, result) {
				expect(result).toEqual(6);
				pool.shutdown();
				done();
			})
});

it("should forward custom arguments to worker", function(done) {
	var pool = new WorkerPool(echoWorker, 1, { workerArgs: [ 'foo', 'bar' ] });

	pool.process(
			toMessage('Any payload'), 
			function (err, result) {
				// Elements 0 and 1 are the node executable and the 
				// worker script file path
				expect(result[2]).toEqual('foo');
				expect(result[3]).toEqual('bar');
				pool.shutdown();
				done();
			})
});

it("should process a set messages simultaneously, up to the pool limit", function(done) {
	var pool = new WorkerPool(sumWorker, 3);
	var workers = [];

	workerListener.on('message', function (message) {
		if (message.forwardedMessage.event != 'requested') 
			return;

		workers.push(message.fromWorker);

		// The pool is full - all 3 workers are busy - now resuming them
		if (workers.length == 3) {
			for (var i = 0; i < workers.length; i++)
				workers[i].send('resume');
		}
	})

	async.map(
			toMessageSet([ [1, 2, 3], [10, 5], [30, 4] ]),
			function (it, cb) {
				// This test workers stays busy on the pool until we send them a resume message
				pool.process(it, cb);
			},
			function (err, results) {
				expect(results[0]).toEqual(6);
				expect(results[1]).toEqual(15);
				expect(results[2]).toEqual(34);
				pool.shutdown();
				done();				
			}
		);
});

it("should gracefully reject a message when pool is full", function(done) {
	var pool = new WorkerPool(sumWorker, 3);
	var workers = [];

	workerListener.on('message', function (message) {
		if (message.forwardedMessage.event != 'requested') 
			return;

		workers.push(message.fromWorker);

		// The pool is full - all 3 workers are busy
		if (workers.length == 3) {
			// An extra process request should be rejected
			pool.process(toMessage([22, 3]), function (err, result) {
				expect(err).not.toBe(null);

				// Resuming busy workers
				for (var i = 0; i < workers.length; i++)
					workers[i].send('resume');
			})		
		}
	})

	async.map(
			toMessageSet([ [1, 2, 3], [10, 5], [30, 4] ]),
			function (it, cb) {
				// This test workers stays busy on the pool until we send them a resume message
				pool.process(it, cb);
			},
			function (err, results) {
				expect(results[0]).toEqual(6);
				expect(results[1]).toEqual(15);
				expect(results[2]).toEqual(34);
				pool.shutdown();
				done();				
			}
		);
});

it("should forward error returned by worker", function(done) {
	var pool = new WorkerPool(sumWorker, 3);

	workerListener.on('message', function (message) {
		if (message.forwardedMessage.event != 'requested') 
			return;

		// Making second message return an error (not throw)
		if (message.forwardedMessage.jobMessage.Body.terms[0] == 10) {
			message.fromWorker.send('fail');
		} else {
			message.fromWorker.send('resume');
		}
	})

	async.map(
			toMessageSet([ [1, 2, 3], [10, 5], [30, 4] ]),
			function (it, cb) {
				pool.process(it, function (err, result) {
					cb(null, { err: err, result: result});
				});
			},
			function (err, results) {
				expect(results[0].result).toEqual(6);
				expect(results[1].err).not.toBe(null);
				expect(results[2].result).toEqual(34);
				pool.shutdown();
				done();				
			}
		);
});

it("should recycle its workers", function(done) {
	var pool = new WorkerPool(sumWorker, 3);

	function fillPool (msgs, cb) {
		var workers = [];

		var messageListener = function (message) {
			if (message.forwardedMessage.event != 'requested') 
				return;

			workers.push(message.fromWorker);

			// The pool is full - all 3 workers are busy - now resuming them
			if (workers.length == 3) {
				// Resuming busy workers
				for (var i = 0; i < workers.length; i++)
					workers[i].send('resume');

				// Unregistering listener so it doesn't interfer on next fillPool call
				workerListener.removeListener('message', messageListener)
			}
		}

		workerListener.on('message', messageListener)

		async.map(
				msgs,
				function (it, cb) {
					// This test workers stays busy on the pool until we send them a resume message
					pool.process(it, cb);
				},
				cb);
	}

	async.waterfall([
		function (cb) {
			fillPool(toMessageSet([ [1, 2, 3], [10, 5], [30, 4] ]), cb);
		},
		function (results, cb) {
			expect(results[0]).toEqual(6);
			expect(results[1]).toEqual(15);
			expect(results[2]).toEqual(34);
			fillPool(toMessageSet([ [10, 1], [0, 1, 3], [9, 8, 7] ]), cb);
		},
		function (results, cb) {
			expect(results[0]).toEqual(11);
			expect(results[1]).toEqual(4);
			expect(results[2]).toEqual(24);
			pool.shutdown();
			done();				
		}
	]);
});

it("should replace a crashed worker with a new worker", function(done) {
	var pool = new WorkerPool(sumWorker, 3);
	var workers = [];
	var badWorker = -1;

	workerListener.on('message', function (message) {
		if (message.forwardedMessage.event != 'requested') 
			return;

		workers.push(message.fromWorker);

		// Remembering which worker we'll make throw an exception later
		if (message.forwardedMessage.jobMessage.Body.terms[0] == 10) {
			badWorker = message.fromWorker;
		}

		// The pool is full - all 3 workers are busy
		if (workers.length == 3) {
			/*
			 * A crashed worker has already been replaced 
			 * when it fires the 'exit' event,
			 * since we know the pool replaces a worker 
			 * during its 'disconnect' event
			 */
			/**
			 * @todo Not depend on this event
			 */ 
			badWorker.on('exit', function () {
				/*
				 * An extra process request should be accepted, since the 
				 * pool is expected to respawn a worker after 
				 * crashing of worker 2
				 */ 
				pool.process(
						toMessage([22, 3]),
						function (err, result) {
							expect(err).toBe(null);

							// Resuming other workers
							for (var i = 0; i < workers.length; i++) {
								if (workers[i] != badWorker)
									workers[i].send('resume');
							}
						});
			});

			// Making worker 2 to crash
			badWorker.send('throw');	

		// After extra request
		} else if (workers.length == 4) {
			// Resuming extra worker
			message.fromWorker.send('resume');
		}
	})

	async.map(
			toMessageSet([ [1, 2, 3], [10, 5], [30, 4] ]),
			function (it, cb) {
				// This test workers stays busy on the pool until we send them a resume message
				pool.process(it, function (err, result) {
					/*
					 * We never send an error to the map callback, since we want
					 * to know later which request failed and which succeded
					 */ 
					cb(null, {error: err, result: result});
				});
			},
			function (err, results) {
				expect(results[0].result).toEqual(6);
				expect(results[1].error).not.toBe(null);
				expect(results[2].result).toEqual(34);
				pool.shutdown();
				done();				
			}
		);
});

it("should timeout a worker and replace it with a new worker", function(done) {
	jasmine.Clock.useMock();

	var workers = [];
	var badWorker;

	var messageListener = function (message) {
		if (message.forwardedMessage.event != 'requested') 
			return;

		if (message.forwardedMessage.jobMessage.Body.terms[0] == 1) {
			badWorker = message.fromWorker;

			/*
			 * We know the 'exit' event will be fired after timeout and respawn of worker
			 */
			/**
			 * @todo Not depend on this event
			 */
			badWorker.on('exit', function () {
					/*
					 * An extra process request should be accepted, since the 
					 * pool is expected to respawn a worker after timeout.
					 * Filling the pool to the limit to be sure the respawned worker
					 * gets a job
					 */
					async.map(
							toMessageSet([ [5, 2, 3], [10, 5], [30, 4] ]),
							function (it, cb) {
								/*
								 * This test workers stays busy on the pool until 
								 * we send them a resume message
								 */ 
								pool.process(it, cb);
							},
							function (err, results) {
								expect(results[0]).toEqual(10);
								expect(results[1]).toEqual(15);
								expect(results[2]).toEqual(34);
								pool.shutdown();
								done();				
							});					
				});		
			jasmine.Clock.tick(10010);	
			return;		

		} else {
			workers.push(message.fromWorker);
		}

		if (workers.length == 3) {
			// Resuming other workers
			for (var i = 0; i < workers.length; i++) {
				workers[i].send('resume');
			}
		}
	}

	workerListener.on('message', messageListener);	

	var pool = new WorkerPool(sumWorker, 3, { workerTimeout: 10000 });
	pool.process(
			toMessage([1, 2, 3]), 
			function (err, result) {
				expect(err).not.toBe(null);		
			})
});

it("should timeout a worker lacking a message event handler", function(done) {
	var pool = new WorkerPool(sumWorker, 1, { env: {unreachable: 'true'}, workerTimeout: 100 });
	pool.process(
			toMessage([1, 2, 3]), 
			function (err, result) {
				expect(err).not.toBe(null);
				pool.shutdown();
				done();
			});
});

it("should timeout a worker which does not emit heartbeats", function(done) {
	var pool = new WorkerPool(sumWorker, 1, { workerTimeout: 100 });

	pool.process(
			toMessage([1]), 
			function (err, result) {
				expect(err).not.toBe(null);
				pool.shutdown();
				done();
			});
});

it("should wait for a worker which does emit heartbeats", function(done) {
	var startedHeartBeat = false;
	var pool = new WorkerPool(sumWorker, 1, { workerTimeout: 100 });

	workerListener.on('message', function (message) {
		setTimeout(function () {
			message.fromWorker.send('resume');
		}, 150)

		if (startedHeartBeat) {
			return;
		}

		message.fromWorker.send('start_heartbeat');
		startedHeartBeat = true;
	})

	async.series([
		function (cb) {
			pool.process(
				toMessage([1, 2, 3]), 
				function (err, result) {
					expect(err).toBe(null);
					//pool.shutdown();
					cb();
				});
		}, function (cb) {

			pool.process(
				toMessage([1, 2, 3]), 
				function (err, result) {
					expect(err).toBe(null);
					pool.shutdown();
					cb();
				});
		}], done);
	
});

/**
 * Creates a set of messages from a set of term groups to sum
 * @param  {number[][]} termSet Term groups to sum
 * @return {object[]}         Array of ready-to-send messages
 */
function toMessageSet (termSet) {
	var set = [];
	for (var i = 0; i < termSet.length; i++) {
		set.push(toMessage(termSet[i]));
	}
	return set;
}

/**
 * Wraps an array of terms to sum in a common message envelope
 * @param  {number[]} terms terms to sum
 * @return {object}       A ready-to-send message
 */
function toMessage (terms) {
	return { Body: { terms: terms } };
}

});