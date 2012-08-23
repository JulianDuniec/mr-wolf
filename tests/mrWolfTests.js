var mrWolf = require('../');

module.exports = {
	//When setting job-directory, the available jobs should match
	findJobsInDirectory : function(test) {
		var dir = __dirname + "/mock/findJobsInDirectory";
		//The two files present in the directory
		var jobNames = ['test1', 'test2'];

		mrWolf.setJobDirectory(dir);
		mrWolf.getJobs(function(err, jobs) {
			for (var i = jobNames.length - 1; i >= 0; i--) {
				test.ok(!!~jobs.indexOf(jobNames[i]), jobNames[i] + " was not present");
			};
			test.done();
		});
		
	},

	
	//When pushing a job, the  queue-size should be 1
	enqueueJob : function(test) {
		var dir = __dirname + "/mock/enqueueJob";
		mrWolf.setJobDirectory(dir);
		mrWolf.enqueue("job", {});
		mrWolf.stats(function(err, stats) {
			test.equal(stats.queueSize, 1);
			test.done();
		});
	},

	//When processing a single job, the queue-size should be 0 and the in-progress-count should be 1
	//When processing a job that timeouts, the job-queue-size should revert back to 1.
	//When processing a job that is finished, the job-queue-size should be zero, the in-progress-count should be zero and the finished-count should be 1
	//When adding a job in the directory that does contain the receive-method, an error should occur.
};