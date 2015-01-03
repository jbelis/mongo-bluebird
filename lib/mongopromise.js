var extend = require("util-extend");

var mongodb = require('mongodb'),
	Database = mongodb.Db,
	Connection = mongodb.Connection,
	Server = mongodb.Server;

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
	this.promise = db.promise.then(function(db) {
		return db.collection(name);
	});
};

_instance.prototype.collection = function(name) {
	var col = this.collections[name];
	if (!col) {
		col = new Collection(this, name);
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

Collection.prototype.upsert = function(query, data, options) {
	return this.update(query, data, extend(options || {}, {upsert:true}));
};

Collection.prototype.update = function(query, data, options) {
	if (data && (data.length || !Array.isArray(data))) {
		return this.promise.then(function (inner) {
			return new Promise(function (resolve, reject) {
				inner.update(query, data, options || {}, function (err, docs) {
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

Collection.prototype.find = function(query, fields, sort, limit) {
	query = query || {};
	return this.promise.then(function (inner) {
		return new Promise(function (resolve, reject) {
			var cursor = fields ? inner.find(query, fields) : inner.find(query);
			if (sort) cursor = cursor.sort(sort);
			if (limit) cursor = cursor.limit(limit);
			return cursor.toArray(function (err, items) {
				if (err) {
					reject(err);
				} else {
					resolve(items);
				}
			});
		});
	});
};

Collection.prototype.findOne = function(query, fields) {
	query = query || {};
	return this.promise.then(function (inner) {
		return new Promise(function (resolve, reject) {
			inner.findOne(query, {fields: fields}, function(err, doc) {
				if (err) {
					reject(err);
				} else {
					resolve(doc);
				}
			});
		});
	});
};

Collection.prototype.ensureIndex = function(query, options) {
	query = query || {};
	return this.promise.then(function (inner) {
		return new Promise(function (resolve, reject) {
			inner.ensureIndex(query, options, function(err, name) {
				if (err) {
					reject(err);
				} else {
					resolve(name);
				}
			});
		});
	});
};

Collection.prototype.findById = function (mongoId) {
	return this.promise.then(function (inner) {
		return new Promise(function (resolve, reject) {
			var cursor = inner.find({ _id: new mongodb.ObjectID(mongoId) });

			cursor.next(function (error, result) {
				if (error) {
					reject(error);
				} else {
					resolve(result);
				}
			});
		});
	});
};

Collection.prototype.aggregate = function(query) {
	query = query || [];
	return this.promise.then(function (inner) {
		return new Promise(function (resolve, reject) {
			return inner.aggregate(query, function (err, result) {
				if (err) {
					reject(err);
				} else {
					resolve(result);
				}
			});
		});
	});
};

exports.create = function(settings) {
	return new _instance(settings);
};

