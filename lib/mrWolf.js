var fs = require('fs'),
	mongo = require('mongodb'),
  	Server = mongo.Server,
  	Db = mongo.Db;

module.exports = {
	
	jobDirectory : __dirname + './jobs',
	
	server : null,
	
	db : null,
	
	destruct : function() {
		this.db.close();
	},

	init : function(cb) {
		this.server = new Server('localhost', 27017, {auto_reconnect : true});
		var me = this;
		new Db('mr-wolf', this.server).open(function(err, db) {
			me.db = db;
			cb();
		});
	},

	setJobDirectory : function(directory) {
		this.jobDirectory = directory;
		return this;
	},

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

	drop : function(cb) {
		this.db.collection('messages', function(err, collection) {
			collection.drop();
			cb();
		});
	},

	enqueue : function(name, meta, cb) {
		this.db.collection('messages', function(err, collection) {
			collection.insert({name : name, meta : meta, enqueued : new Date(), started : null, finished : null, error : null}, {safe : true}, function(err, result) {
				cb(err, result);
			});
		});
	},

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

	processNext : function(cb) {
		var me = this;
		this.db.collection('messages', function(err, collection) {
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

	run : function(message, cb) {
		var me = this;
		var runner = require(this.jobDirectory + "/" + message.name);
		runner.receive(message.meta, function(err) {
			me.markAsFinished(message._id, function(err) {
				cb();
			})
		});
	},

	markAsFinished : function(id, cb) {
		this.db.collection('messages', function(err, collection) {
			collection.update({_id : id}, {$set : {finished : new Date()}}, {safe:true}, function(err, result) {
				cb(err);
			});
		});
	}
};
