
var Database = require('mongodb').Db,
	Connection = require('mongodb').Connection,
	Server = require('mongodb').Server;

var Promise = require("bluebird");

var _instance = function(settings) {
	this.db = new Database(settings.db,
		new Server(settings.host, settings.port || Connection.DEFAULT_PORT, {auto_reconnect: true}),
		{safe: true});
	this.collections = {};
	this.promise = new Promise(function(resolve, reject) {
		return this.db.open(function (err, db) {
			if (err) {
				reject(err);
			} else {
				resolve(db);
			}
		});
	}.bind(this));
};

var Collection = function(db, name) {
	this.promise = new Promise(function(resolve, reject) {
		return db.promise.then(function(db) {
			resolve(db.collection(name));
		});
	});
};

_instance.prototype.collection = function(name) {
	var col = this.collections[name];
	if (!col) {
		col = new Collection(this, name)
		this.collections[name] = col;
	}
	return col;
};

Collection.prototype.remove = function(query) {
	if (query) {
		return this.promise.then(function(inner) {
			return new Promise(function (resolve, reject) {
				inner.remove(query, function (err, items) {
					if (err) {
						reject(err);
					} else {
						resolve(items);
					}
				});
			});
		});
	} else {
		return Promise.reject("Programming error: null query");
	}
};

Collection.prototype.insert = function(data) {
	if (data && (data.length || !Array.isArray(data))) {
		return this.promise.then(function (inner) {
			return new Promise(function (resolve, reject) {
				inner.insert(data, function (err, docs) {
					if (err) {
						reject(err);
					} else {
						resolve(docs);
					}
				});
			});
		});
	} else {
		return Promise.reject("Programming error: null or empty data to insert");
	}
};

Collection.prototype.update = function(query, data) {
	if (data && (data.length || !Array.isArray(data))) {
		return this.promise.then(function (inner) {
			return new Promise(function (resolve, reject) {
				inner.update(query, data, {upsert: true}, function (err, docs) {
					if (err) {
						reject(err);
					} else {
						resolve(docs);
					}
				});
			});
		});
	} else {
		return Promise.reject("Programming error: null or empty data to update");
	}
};

Collection.prototype.find = function(collectionName, query) {
	query = query || {};
	return this.promise.then(function (inner) {
		return new Promise(function (resolve, reject) {
			inner.find(query).toArray(function (err, items) {
				if (err) {
					reject(err);
				} else {
					resolve(items);
				}
			});
		});
	});
};

exports.create = function(settings) {
	return new _instance(settings);
};

