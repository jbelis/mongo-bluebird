
var Database = require('mongodb').Db,
	Connection = require('mongodb').Connection,
	Server = require('mongodb').Server;

var Promise = require("bluebird");

exports.Mongo = function(settings) {
	this.db = new Database(settings.db,
		new Server(settings.host, settings.port || Connection.DEFAULT_PORT, {auto_reconnect: true}),
		{safe: true});
	this.collections = {};
};

var Collection = function(col) {
	this.inner = col;
}

instance.prototype.collection = function(collectionName) {
	return new Promise(function(resolve, reject) {
		var col = this.collections[collectionName];
		if (col) {
			resolve(col);
		} else {
			this.db.open(function(err, db) {
				if (err) {
					reject(err);
				} else {
					col = db.collection(collectionName);
					if (!col) {
						reject("missing collection " + collectionName);
					} else {
						this.collections[collectionName] = col;
						resolve(col);
					}
				}
			}.bind(this));
		}
	}.bind(this));
};

Collection.prototype.remove = function(query) {
	//return;//be careful to use remove
	if (query) {
		return new Promise(function (resolve, reject) {
			this.inner.remove(query, function (err, items) {
				if (err) {
					reject(err);
				} else {
					resolve(items);
				}
			});
		}.bind(this));
	} else {
		return Promise.reject("Programming error: null query");
	}
};

Collection.prototype.insert = function(data) {
	if (data) {
		return new Promise(function (resolve, reject) {
			this.inner.insert(data, function (err, docs) {
				if (err) {
					reject(err);
				} else {
					resolve(docs);
				}
			});
		}.bind(this));
	} else {
		return Promise.reject("Programming error: null data to insert");
	}
};

Collection.prototype.update = function(query, data) {
	if (data) {
		return new Promise(function (resolve, reject) {
			this.inner.update(query, data, {upsert: true}, function (err, docs) {
				if (err) {
					reject(err);
				} else {
					resolve(docs);
				}
			});
		}.bind(this));
	} else {
		return Promise.reject("Programming error: null data to update");
	}
};

Collection.prototype.find = function(collectionName, query) {
	query = query || {};
	return new Promise(function (resolve, reject) {
		this.inner.find(query).toArray(function (err, items) {
			if (err) {
				reject(err);
			} else {
				resolve(items);
			}
		});
	}.bind(this));
};

