var mrWolf = require('../');

module.exports = {
	setUp : function(cb) {
		mrWolf.init(function() {
			mrWolf.drop(cb);
		});
	},

	tearDown : function(cb) {
		mrWolf.destruct();
		cb();
	},

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
		mrWolf.enqueue("job", {}, function(err, job) {
			mrWolf.stats(function(err, stats) {
				test.equal(stats.queueCount, 1, "Job wasn't queued correctly");
				test.equal(stats.inProgressCount, 0);
				test.equal(stats.finishedCount, 0);
				mrWolf.processNext(function(err) {
					mrWolf.stats(function(err, stats) {
						test.equal(stats.queueCount, 0, "Job wasn't pulled from queue, queueCount:" + stats.queueCount);
						test.equal(stats.inProgressCount, 0, "Job still marked as inProgress, inProgressCount:" + stats.inProgressCount);
						test.equal(stats.finishedCount, 1, "Job wasn't marked as finished: " + stats.finishedCount);
						test.done();
					});
				});
				mrWolf.stats(function(err, stats) {
					test.equal(stats.queueCount, 0, "Job wasn't pulled from queue, queueCount:" + stats.queueCount);
					test.equal(stats.inProgressCount, 1, "Job wasn't pushed into working, inProgressCount:" + stats.inProgressCount);
					test.equal(stats.finishedCount, 0, "Job was marked as finished, when should be in progress: " + stats.finishedCount);
				});
			});
		});
		
	},

	//When processing a job that timeouts, the job-queue-size should revert back to 1.
	//When processing a job that is finished, the job-queue-size should be zero, the in-progress-count should be zero and the finished-count should be 1
	//When adding a job in the directory that does contain the receive-method, an error should occur.
};