var fs = require('fs'),
	mongoose = require('mongoose'),
	Schema = mongoose.Schema;

module.exports = {
	
	jobDirectory : __dirname + './jobs',

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

	enqueue : function(name, meta) {

	},

	stats : function() {

	}
};

var MessageSchema = new Schema({
	name		: String,
	meta		: {},
	enqueued	: Date,
	started 	: Date,
	finished 	: Date,
	error : {}
});

MessageSchema.statics.insert = function(name, meta, cb) {
	var msg = new Message();
	msg.name = name;
	msg.meta = meta;
	msg.enqueued = new Date();
	msg.save(cb);
};

mongoose.connect("mongodb://localhost/mrwolf");

Message = mongoose.model('Message', MessageSchema);