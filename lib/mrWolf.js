var fs = require('fs'),
	mongo = require('mongodb'),
  	Server = mongo.Server,
  	Db = mongo.Db;

module.exports = {
	
	/*
		Points to the default directory where to look for jobs
	*/
	jobDirectory : __dirname + './jobs',
	
	/*
		The mongodb server
	*/
	server : null,
	
	/*
		The mongodb database
	*/
	db : null,

	/*
		Closes db-connection
	*/
	destruct : function() {
		this.running = false;
		this.db.close();
	},

	/*
		Starts the queue-processing loop
	*/
	start : function(params) {
		this.running = true;
		this.onEnqueue = params.onEnqueue;
		this.onError = params.onError;
		this.onStart = params.onStart;
		this.onComplete = params.onComplete;
		this.loop();
	},

	/*
		processes each job in queue while this.running = true
	*/
	loop : function() {
		var me = this;
		if(this.running == true) {
			this.processNext(function() {
				setTimeout(function() {
					me.loop();
				}, me.loopDelayMs);
			});
		}
	},

	/*
		Sets this.running to false (this stopping the processing)
	*/
	stop : function() {
		this.running = false;
	},

	/*
		Initializes db-connection
	*/
	init : function(options, cb) {
		this.loopDelayMs = options.loopDelayMs || 10,
		this.dbName = options.dbName || 'mr-wolf',
		this.host = options.host || 'localhost',
		this.port = options.port || 27017,

		this.server = new Server(this.host, this.port, {auto_reconnect : true});
		var me = this;
		new Db(me.dbName, this.server, {safe : true}).open(function(err, db) {
			//No db, no fun
			if(err) throw err;
			me.db = db;
			cb();
		});
	},

	/*
		Sets the directory in which to find the job-runners
	*/
	setJobDirectory : function(directory) {
		this.jobDirectory = directory;
		return this;
	},

	/*
		Returns a list of all available jobs in the job-directory
	*/
	getJobs : function(cb) {
		fs.readdir(this.jobDirectory, function(err, files) {
			if(err) cb(err);
			else {
				var res = [];
				files.forEach(function(file) {
					res.push(file.substring(0, file.lastIndexOf(".")));
				});
				cb(err, res);
			}
		});
	},

	/*
		Drops the message-collection (for testing purposes only!)
	*/
	drop : function(cb) {
		this.db.collection('messages', function(err, collection) {
			collection.drop(function() {
				cb();
			});
			
		});
	},

	enqueuePeriodical : function(name, meta, seconds, cb) {
		var me = this;
		this.db.collection('messages', function(err, collection) {
			collection.findOne({
				name : name,
				"meta.periodicalSeconds" : seconds
			}, 
			{},
			function(err, job) {
				if(job == null ) {
					meta.periodicalSeconds = seconds;
					me.enqueue(name, meta, cb);
				}
				else {
					cb(null, job);
				}
			})
		});
		
	},

	/*
		Enqueues a work item
	*/
	enqueue : function(name, meta, cb) {
		var me = this;
		//we need to figure out when to start.
		//Default mode is asap.
		var startAtDate = new Date();
		//But if we pass periodicalSeconds, we will append it to the startAtDate
		if(meta.periodicalSeconds)
			startAtDate.setSeconds(startAtDate.getSeconds()+meta.periodicalSeconds);
		//Otherwise, if we specify startAtDate, use this. But
		//remember that the periodicalSeconds will always be prioritized.
		else if(meta.startAtDate)
			startAtDate = meta.startAtDate;

		this.db.collection('messages', function(err, collection) {
			collection.insert({
					name : name,
					meta : meta, 
					enqueued : new Date(), 
					started : null, 
					finished : null, 
					error : null,
					startAtDate : startAtDate
				}, {
					safe : true
				}, 
					function(err, result) {
						var res = result.length >= 1 ? result[0] : null;
						if(me.onEnqueue)
							me.onEnqueue(res);
						cb(err, res);
					}
			);
		});
	},


	/*
		Counts the current amount of items in queue, in progress and finished.
	*/
	stats : function(cb) {
		this.db.collection('messages', function(err, collection) {
			collection.count({started : null}, function(err, queueCount) {
			collection.count({started : {$ne : null}, finished : null, error : null}, function(err, inProgressCount) {
			collection.count({finished : {$ne : null}, error : null}, function(err, finishedCount) {
			collection.count({error : {$ne : null}}, function(err, errorCount) {
				cb(err, {
					queueCount : queueCount,
					inProgressCount : inProgressCount,
					finishedCount : finishedCount,
					errorCount : errorCount
				});
			});
			});
			});
			});
		});
	},

	/*
		Dequeues one item and sends it to run();
	*/
	processNext : function(cb) {
		var me = this;
		this.db.collection('messages', function(err, collection) {
			//Mark the item as started to pull it from the queue
			collection.findAndModify(
				{
					started : null,
					startAtDate : { $lte : new Date()}
				},
				[['enqueued', 1]],
				{$set : {started : new Date()}},
				{},
				function(err, message) {
					if(message != null)
						me.run(message, cb);
					else
						cb();
				}
			);
		});
	},

	/*
		Executes a single method and then marks it as finished
	*/
	run : function(message, cb) {
		var me = this;
		//Locate the runner for this job and execute it
		try {
			var runner = require(this.jobDirectory + "/" + message.name);
			if(me.onStart)
				me.onStart(message);
			runner.receive(message.meta, function(err) {
				if(err) {
					me.markAsErraneous(message._id, err, cb);
					if(me.onError) {
						me.onError(message, err);
					}
				}
				else
					me.markAsFinished(message._id, function(err) {
						if(me.onComplete)
							me.onComplete(message);
						cb();
					});
				//If we have a periodical job, we re-enqueue-it to the database.
				if(message.meta.periodicalSeconds) {
					me.reEnqueuePeriodical(message);
				}
			});
		}
		catch(err) {
			me.markAsErraneous(message._id, err, cb);
			if(me.onError) {
				me.onError(message, err);
			}
		}
	},

	reEnqueuePeriodical : function(message) {
		this.enqueue(message.name, message.meta, function(err, job) {

		});
	},

	/*	
		Marks a job as erraneous in the database.
	*/
	markAsErraneous : function(id, error, cb) {
		this.db.collection('messages', function(err, collection) {
			collection.update({_id : id}, {$set : {error : error}}, {safe:true}, function(err, result) {
				cb(err);
			});
		});
	},

	/*
		Sets the finished-date to mark the item as finished.
	*/
	markAsFinished : function(id, cb) {
		this.db.collection('messages', function(err, collection) {
			collection.update({_id : id}, {$set : {finished : new Date()}}, {safe:true}, function(err, result) {
				cb(err);
			});
		});
	}
};
