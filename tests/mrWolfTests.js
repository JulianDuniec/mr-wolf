var mrWolf = require('../');

exports.unittests = {
	setUp : function(cb) {
		mrWolf.init({}, function() {
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


	erraneousJob : function(test) {
		var dir = __dirname + "/mock/erraneousJob";
		mrWolf.setJobDirectory(dir);
		//Job throws error
		mrWolf.enqueue("job", {}, function(err, job) {
			mrWolf.processNext(function(err) {
				mrWolf.stats(function(err, stats) {
					test.equal(stats.queueCount, 0, "Job wasn't pulled from queue, queueCount:" + stats.queueCount);
					test.equal(stats.inProgressCount, 0, "Job still marked as inProgress, inProgressCount:" + stats.inProgressCount);
					test.equal(stats.finishedCount, 0, "Job marked as finished " + stats.finishedCount);
					test.equal(stats.errorCount, 1, "Job wasn't marked as erraneous: " + stats.errorCount);
					
					//Job throws error
					mrWolf.enqueue("job2", {}, function(err, job) {
						mrWolf.processNext(function(err) {
							mrWolf.stats(function(err, stats) {
								test.equal(stats.queueCount, 0, "Job2 wasn't pulled from queue, queueCount:" + stats.queueCount);
								test.equal(stats.inProgressCount, 0, "Job2 still marked as inProgress, inProgressCount:" + stats.inProgressCount);
								test.equal(stats.finishedCount, 0, "Job2 marked as finished " + stats.finishedCount);
								test.equal(stats.errorCount, 2, "Job2 wasn't marked as erraneous: " + stats.errorCount);
								test.done();
							});
						});
					});
				});
			});
		});
	},

	enqueueListener : function(test) {
		var dir = __dirname + "/mock/startingAndListening";

		var jobName = "job";
		mrWolf.setJobDirectory(dir);
		mrWolf.start({
			onEnqueue : function(job) {
				test.equal(job.name, jobName);
				mrWolf.stop();
				test.done();
			}
		});

		mrWolf.enqueue(jobName, {}, function(err, job) {
			test.equal(err, null);
		});

	},

	errorListener : function(test) {
		var dir = __dirname + "/mock/erraneousJob";
		var jobName = "job";
		mrWolf.setJobDirectory(dir);
		mrWolf.start({
			onError : function(job, error) {
				test.notEqual(error, null);
				test.equal(job.name, jobName);
				mrWolf.stop();
				test.done();
			}
		});

		mrWolf.enqueue(jobName, {}, function(err, job) {
			test.equal(err, null);
		});
	},

	startAndCompleteListeners : function(test) {
		var dir = __dirname + "/mock/startingAndListening";
		var jobName = "job";
		mrWolf.setJobDirectory(dir);
		mrWolf.start({
			//The job should start after enqueue
			onStart : function(job) {
				test.equal(job.name, jobName);
				
			},
			//The job should start after enqueue
			onComplete : function(job) {
				test.equal(job.name, jobName);
				mrWolf.stop();
				
				test.done();
			}
		});

		mrWolf.enqueue(jobName, {}, function(err, job) {
			test.equal(err, null);
		});
	},

	enqueueFutureJob : function(test) {
		var dir = __dirname + "/mock/startingAndListening";
		var jobName = "job";
		var seconds = 1;
		var enqueueDate = new Date();
		var startAtDate = new Date();
		var expectedDiff = seconds*1000;
		startAtDate.setSeconds(startAtDate.getSeconds()+seconds)

		mrWolf.setJobDirectory(dir);
		mrWolf.start({
			//The job should start after enqueue
			onStart : function(job) {
				test.equal(job.name, jobName);
				var executionDate = new Date();
				var diff = executionDate.getTime()-enqueueDate.getTime();
				test.ok(diff >= expectedDiff, "The diff was too small: " + diff + ", expected " + expectedDiff);
				test.done();
			}
		});


		mrWolf.enqueue(jobName, {
			startAtDate : startAtDate
		}, function(err, job) {
			test.equal(err, null);
		});
	},

	enqueuePeriodical : function(test) {
		var dir = __dirname + "/mock/startingAndListening";
		var jobName = "job";
		var seconds = 1;
		var count = 0;
		var maxCount = 3;
		var enqueueDate = new Date();
		var startAtDate = new Date();
		startAtDate.setSeconds(startAtDate.getSeconds()+seconds)

		mrWolf.setJobDirectory(dir);
		mrWolf.start({
			//The job should start after enqueue
			onStart : function(job) {
				console.log("Start", count);
				if(count <= maxCount) {
					test.equal(job.name, jobName);
					var executionDate = new Date();
					var expectedDiff = seconds * ++count * 1000;
					var diff = executionDate.getTime()-enqueueDate.getTime();
					test.ok(diff >= expectedDiff, "The diff was too small: " + diff + ", expected " + expectedDiff);
					if(count == maxCount)
						test.done();
				}
				
			}
		});


		mrWolf.enqueuePeriodical(jobName, {}, seconds, function(err, job) {
			test.equal(err, null);
		});
	},

	/*
		Fix for bug: When enqueue periodial two times, job is queued twice.
		It should only be enqueued once.
	*/
	enqueuePeriodicalDuplicate : function(test) {
		var dir = __dirname + "/mock/startingAndListening";
		var jobName = "job";
		var seconds = 1;
		
		mrWolf.setJobDirectory(dir);
		
		//Enqueue two times (simulate multiple servers starting up and enqueuing the job)
		mrWolf.enqueuePeriodical(jobName, {}, seconds, function(err, job) {
			test.equal(err, null);
			mrWolf.enqueuePeriodical(jobName, {}, seconds, function(err, job) {
				test.equal(err, null);
				mrWolf.stats(function(err, stats) {
					test.equal(stats.queueCount, 1);
					//Wait until periodical is done
					setTimeout(function() {
						test.done();
					}, seconds * 1000);
				});
			});
		});
	},

	cleanJobTable : function(test) {
		var dir = __dirname + "/mock/startingAndListening";
		var jobName = "job";
		var cleanupInterval = 1000;
		mrWolf
			.setJobDirectory(dir)
			.setCleanupInterval(cleanupInterval);
		mrWolf.enqueue("job", {}, function() {

		});
		mrWolf.start({
			onComplete : function(job) {
				setTimeout(function() {
					mrWolf.stats(function(err, stats) {
						test.equal(stats.queueCount, 0, "Job wasn't pulled from queue, queueCount:" + stats.queueCount);
						test.equal(stats.inProgressCount, 0, "Job still marked as inProgress, inProgressCount:" + stats.inProgressCount);
						test.equal(stats.finishedCount, 0, "Job wasn't removed from finished: " + stats.finishedCount);
						test.done();
					});
				}, cleanupInterval*2);
				
			}
		});
		

	}

	//When processing a job that timeouts, the job-queue-size should revert back to 1.

	//When adding a job in the directory that does contain the receive-method, an error should occur.
};