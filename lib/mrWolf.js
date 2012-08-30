var fs = require('fs'),
	mongo = require('mongodb'),
  	Server = mongo.Server,
  	Db = mongo.Db;

module.exports = {
	
	jobDirectory : __dirname + './jobs',
	
	server : null,
	
	db : null,

	onEnqueue : null,

	loopDelayMs : 10,

	dbName : 'mr-wolf',

	
	/*
		Closes db-connection
	*/
	destruct : function() {
		this.db.close();
	},

	start : function(params) {
		this.running = true;
		this.onEnqueue = params.onEnqueue;
		this.onError = params.onError;
		this.loop();
	},

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

	stop : function() {
		this.running = false;
	},

	/*
		Initializes db-connection
	*/
	init : function(cb) {
		this.server = new Server('localhost', 27017, {auto_reconnect : true});
		var me = this;
		new Db(me.dbName, this.server).open(function(err, db) {
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

	/*
		Enqueues a work item
	*/
	enqueue : function(name, meta, cb) {
		var me = this;
		this.db.collection('messages', function(err, collection) {
			collection.insert({
					name : name,
					meta : meta, 
					enqueued : new Date(), 
					started : null, 
					finished : null, 
					error : null
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
				{started : null},
				[['enqueued', -1]],
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
			runner.receive(message.meta, function(err) {
				if(err)
					me.markAsErraneous(message._id, err, cb);
				else
					me.markAsFinished(message._id, function(err) {
						cb();
					});
			});
		}
		catch(err) {
			me.markAsErraneous(message._id, err, cb);
			if(me.onError) {
				me.onError(message, err);

			}
		}
	},

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
