var mongoose = require('mongoose'),
	Schema = mongoose.Schema,
	RegexHelpers = require("../helpers/text/RegexHelpers")
	config = require('config');

var CompanySchema = new Schema({
	name	: String,
	urlName	: String	
});

CompanySchema.statics.create = function(name, cb) {
	var company = new Company();
	company.name = name;
	company.urlName = RegexHelpers.makeUrlName(name);
	company.save(cb);
};

mongoose.connect(config.mrWolf.database.host);

module.exports = Company = mongoose.model('Company', CompanySchema);