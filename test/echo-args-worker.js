/**
 * A worker which gives back the arguments passed to it
 *
 * @author Ricardo Massa
 */

process.on('message', function (msg) {
	process.send({
		event: 'done', 
		error: null, 
		result: process.argv
	});
})	

process.send({event: 'ready'});

