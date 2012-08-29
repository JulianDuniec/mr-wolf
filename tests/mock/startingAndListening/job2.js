module.exports = {
	receive : function(meta, callback) {
		setTimeout(function() {
			callback(null);
		}, 200);
	}
}