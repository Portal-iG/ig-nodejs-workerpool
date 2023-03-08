/**
 * A worker which calculates the sum of a set of terms
 *
 * @param {object} env Environment vars set on this worker
 *     @param {String} [unreachable=false] If this worker should not estabilish
 *         a communication channel with master (for testing purposes)
 *     @param {string} [immediate=false] If this worker should execute a calculation
 *         job right after receiving it, instead of delaying execution to when
 *         it receives a 'resume' command
 *
 * @author Ricardo Massa
 */

var env = normalizeEnvOptions();
var terms = null;
var message = null;

if (!env.unreachable) {
	process.on('message', function (msg) {
		if (msg == 'resume') {
			/*
			 * Instructs this worker to process the received message
			 */ 
			run();

		} else if (msg == 'return') {
			/*
			 * Instructs this worker to return the received message to the queue
			 */ 
			returnToQueue();

		} else if (msg == 'fail') {
			/*
			 * Instructs this worker to return the received message to the queue
			 */ 
			fail();

		} else if (msg == 'throw') {
			/* 
			 * Instructs this worker to crash
			 */
			throw new Error('Worker throw test')

		} else if (msg == 'start_heartbeat') {
			startHeartbeat();

		} else {
			/* 
			 * The actual job payload
			 */
			terms = msg.Body.terms;
			message = msg;
			if (env.immediate) {
				// Processes and returns immediately
				run();
			} else {
				// Delays job processing to when it receives a 'resume' message
				process.send({event: 'requested', jobMessage: msg});
			}
		}
	})	
}

process.send({event: 'ready'});

var HEARTBEAT_INTERVAL = 25;
var finished = false;

function startHeartbeat () {
	var heartbeat = setInterval(function () {

		if (finished) {
			clearInterval(heartbeat);
			return;
		}

		process.send({
			event: 'heartbeat', 
		});		
	}, HEARTBEAT_INTERVAL)
}

function run () {
	finished = true;
	process.send({
		event: 'done', 
		error: null, 
		original: message, 
		result: sum(terms)
	});
}

function fail () {
	finished = true;
	process.send({
		event: 'done', 
		error: { message: 'Worker error test' }, 
		original: message
	});
}

function returnToQueue () {
	finished = true;
	process.send({
		event: 'done', 
		error: null, 
		original: message, 
		result: { $return: true }
	});
}

/**
 * Fills the optional env parameters with default values
 * @return {object} A complete options object
 */
function normalizeEnvOptions () {
	var env = {};
	env.unreachable = (process.env.unreachable == 'true');
	env.immediate = (process.env.immediate == 'true');
	return env;	
}

/**
 * Sums a set of terms
 * @param  {number[]} terms Array of terms
 * @return {number}       The sum of the given terms
 */
function sum (terms) {
	var sum = 0;

	for (var i = 0; i < terms.length; i++) {
		sum += terms[i];
	}		

	return sum;
}
