var fs = require('fs'),
	mongo = require('mongodb'),
  	Server = mongo.Server,
  	Db = mongo.Db;

module.exports = {
	
	jobDirectory : __dirname + './jobs',
	
	server : null,
	
	db : null,
	
	/*
		Closes db-connection
	*/
	destruct : function() {
		this.db.close();
	},

	/*
		Initializes db-connection
	*/
	init : function(cb) {
		this.server = new Server('localhost', 27017, {auto_reconnect : true});
		var me = this;
		new Db('mr-wolf', this.server).open(function(err, db) {
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
			collection.drop();
			cb();
		});
	},

	/*
		Enqueues a work item
	*/
	enqueue : function(name, meta, cb) {
		this.db.collection('messages', function(err, collection) {
			collection.insert({name : name, meta : meta, enqueued : new Date(), started : null, finished : null, error : null}, {safe : true}, function(err, result) {
				cb(err, result);
			});
		});
	},


	/*
		Counts the current amount of items in queue, in progress and finished.
	*/
	stats : function(cb) {
		this.db.collection('messages', function(err, collection) {
			collection.count({started : null}, function(err, queueCount) {
				collection.count({started : {$ne : null}, finished : null}, function(err, inProgressCount) {
					collection.count({finished : {$ne : null}}, function(err, finishedCount) {
						cb(err, {
							queueCount : queueCount,
							inProgressCount : inProgressCount,
							finishedCount : finishedCount
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
				{started : null}
				[['enqueued', -1]],
				{},
				{$set : {started : new Date()}},
				function(err, message) {
					me.run(message, cb);
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
		var runner = require(this.jobDirectory + "/" + message.name);
		runner.receive(message.meta, function(err) {
			me.markAsFinished(message._id, function(err) {
				cb();
			})
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
