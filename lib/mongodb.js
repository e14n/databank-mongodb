// mongodatabank.js
//
// Implementation of Databank interface for MongoDB
//
// Copyright 2011,2012 StatusNet Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var databank = require('databank'),
    Step = require('step'),
    Databank = databank.Databank,
    DatabankError = databank.DatabankError,
    AlreadyExistsError = databank.AlreadyExistsError,
    NoSuchThingError = databank.NoSuchThingError,
    NotImplementedError = databank.NotImplementedError,
    AlreadyConnectedError = databank.AlreadyConnectedError,
    NotConnectedError = databank.NotConnectedError;

var mongodb = require('mongodb'),
    Db = mongodb.Db,
    Server = mongodb.Server;

var MongoDatabank = function(params) {

    this.db = null;
    this.host = params.host || 'localhost';
    this.port = params.port || 27017;
    this.dbname = params.dbname || 'test';
    this.checkSchema = params.checkSchema || true;
    
    this.schema = params.schema || {};
    this.collections = {};
};

MongoDatabank.prototype = new Databank();
MongoDatabank.prototype.constructor = MongoDatabank;

// keep a cache of collections

MongoDatabank.prototype.getCollection = function(name, callback) {
    var bank = this;

    if (bank.collections[name]) {
        callback(null, bank.collections[name]);
    } else {
        bank.db.collection(name, function(err, coll) {
            if (err) {
                callback(err, null);
            } else {
                bank.collections[name] = coll;
                callback(null, coll);
            }
        });
    }
};

MongoDatabank.prototype.connect = function(params, callback) {

    var bank = this,
        server = new Server(bank.host, bank.port, {});

    if (bank.db) {
        callback(new AlreadyConnectedError());
        return;
    }

    bank.db = new Db(bank.dbname, server);

    bank.db.open(function(err, newDb) {
        if (err) {
            callback(err);
        } else {
            if (bank.checkSchema) {
                bank.checkBankSchema(callback);
            } else {
                callback(null);
            }
        }
    });
};

// Disconnect yourself.
// callback(err): function to call on completion

MongoDatabank.prototype.disconnect = function(callback) {
    var bank = this;

    if (!bank.db) {
        callback(new NotConnectedError());
        return;
    }
    bank.db.close(function() {
        bank.db     = null;
        bank.server = null;
        callback(null);
    });
};

MongoDatabank.prototype._valueToRec = function(type, id, value) {

    var pkey = this.getPrimaryKey(type);

    if (typeof value === 'object' && !(value instanceof Array)) {
        value._id = id;
        if (value.hasOwnProperty(pkey)) {
            delete value[pkey];
        }
    } else {
        value = {_v: value, _s: true, _id: id};
    }

    return value;
};

MongoDatabank.prototype._recToValue = function(type, rec) {

    var pkey = this.getPrimaryKey(type), value;

    if (rec._s) {
        value = rec._v;
    } else {
        value = rec;
        if (pkey !== '_id') {
            value[pkey] = rec._id;
            delete value._id;
        }
    }

    return value;
};

// Create a new thing
// type: string, type of thing, usually 'user' or 'activity'
// id: a unique ID, like a nickname or a UUID
// value: JavaScript value; will be JSONified
// callback(err, value): function to call on completion

MongoDatabank.prototype.create = function(type, id, value, callback) {
    
    var bank = this;

    if (!this.db) {
        callback(new NotConnectedError());
        return;
    }

    value = this._valueToRec(type, id, value);

    bank.getCollection(type, function(err, coll) {
        if (err) {
            callback(err, null);
        }
        coll.insert(value, {safe: true}, function(err, newValues) {
            if (err) {
                if (err.name && err.name == 'MongoError' && err.code && err.code == 11000) {
                    callback(new AlreadyExistsError(type, id), null);
                } else {
                    callback(err, null);
                }
            } else {
                // Mongo returns an array of values
                value = bank._recToValue(type, newValues[0]);
                callback(null, value);
            }
        });
    });
};

// Read an existing thing
// type: the type of thing; 'user', 'activity'
// id: a unique ID -- nickname or UUID or URI
// callback(err, value): function to call on completion

MongoDatabank.prototype.read = function(type, id, callback) {

    var bank = this;

    if (!this.db) {
        callback(new NotConnectedError());
        return;
    }

    bank.getCollection(type, function(err, coll) {
        var sel = {};
        if (err) {
            callback(err, null);
        }
        sel._id = id;
        coll.findOne(sel, function(err, value) {
            if (err) {
                // FIXME: find key-miss errors and return a NotExistsError
                callback(err, null);
            } else if (!value) {
                callback(new NoSuchThingError(type, id), null);
            } else {
                value = bank._recToValue(type, value);
                callback(null, value);
            }
        });
    });
};

// Update an existing thing
// type: the type of thing; 'user', 'activity'
// id: a unique ID -- nickname or UUID or URI
// value: the new value of the thing
// callback(err, value): function to call on completion

MongoDatabank.prototype.update = function(type, id, value, callback) {

    var bank = this;

    if (!this.db) {
        callback(new NotConnectedError());
        return;
    }

    value = this._valueToRec(type, id, value);

    bank.getCollection(type, function(err, coll) {
        var sel = {};
        if (err) {
            callback(err, null);
        }
        sel._id = id;
        coll.findAndModify(sel, [['_id', 'ascending']], value, {safe: true, 'new': true}, function(err, result) {
            if (err) {
                // FIXME: find key-miss errors and return a NotExistsError
                callback(err, null);
            } else {
                result = bank._recToValue(type, result);
                callback(null, result);
            }
        });
    });
};

MongoDatabank.prototype.save = function(type, id, value, callback) {

    var bank = this;

    if (!this.db) {
        callback(new NotConnectedError());
        return;
    }

    value = this._valueToRec(type, id, value);

    bank.getCollection(type, function(err, coll) {
        var sel = {};
        if (err) {
            callback(err, null);
        }
        sel._id = id;
        coll.update(sel, value, {upsert: true}, function(err) {
            if (err) {
                // FIXME: find key-miss errors and return a NotExistsError
                callback(err, null);
            } else {
                value = bank._recToValue(type, value);
                callback(null, value);
            }
        });
    });
};

// Delete an existing thing
// type: the type of thing; 'user', 'activity'
// id: a unique ID -- nickname or UUID or URI
// value: the new value of the thing
// callback(err): function to call on completion

MongoDatabank.prototype.del = function(type, id, callback) {

    var bank = this;

    if (!bank.db) {
        callback(new NotConnectedError());
        return;
    }

    bank.getCollection(type, function(err, coll) {

        var sel = {};

        if (err) {
            callback(err, null);
        }

        sel._id = id;
        coll.remove(sel, {}, callback);
    });
};

// Search for things
// type: type of thing
// criteria: map of criteria, with exact matches, like {'subject.id':'tag:example.org,2011:evan' }
// onResult(value): called once per result found
// callback(err): called once at the end of results

MongoDatabank.prototype.search = function(type, criteria, onResult, callback) {

    var pkey = this.getPrimaryKey(type), bank = this;

    if (!this.db) {
        callback(new NotConnectedError());
        return;
    }

    if (criteria.hasOwnProperty(pkey)) {
        criteria._id = criteria[pkey];
        delete criteria[pkey];
    }

    bank.getCollection(type, function(err, coll) {
        if (err) {
            callback(err, null);
        } else {
            coll.find(criteria, function(err, cursor) {
                if (err) {
                    callback(err);
                } else {
                    var lastErr = null;

                    cursor.each(function(err, value) {
                        if (err) {
                            lastErr = err;
                        } else if (value && !lastErr) {
                            value = bank._recToValue(type, value);
                            onResult(value);
                        } else if (value === null) { // called after last value
                            callback(lastErr);
                        }
                    });
                }
            });
        }
    });
};

MongoDatabank.prototype.incr = function(type, id, callback) {

    var bank = this;

    if (!this.db) {
        callback(new NotConnectedError());
        return;
    }

    bank.getCollection(type, function(err, coll) {
        if (err) {
            callback(err, null);
        } else {
            coll.update({_id: id}, {"$inc": {"_v": 1}, "$set": {"_s": true}}, {upsert: true, multi: false}, function(err) {
                if (err) {
                    callback(err, null);
                } else {
                    bank.read(type, id, callback);
                }
            });
        }
    });
};

MongoDatabank.prototype.decr = function(type, id, callback) {

    var bank = this;

    if (!this.db) {
        callback(new NotConnectedError());
        return;
    }

    bank.getCollection(type, function(err, coll) {
        if (err) {
            callback(err, null);
        } else {
            coll.update({_id: id}, {"$inc": {"_v": -1}, "$set": {"_s": true}}, {upsert: true, multi: false}, function(err) {
                if (err) {
                    callback(err, null);
                } else {
                    bank.read(type, id, callback);
                }
            });
        }
    });
};

MongoDatabank.prototype.append = function(type, id, toAppend, callback) {
    var bank = this;

    if (!this.db) {
        callback(new NotConnectedError());
        return;
    }

    bank.getCollection(type, function(err, coll) {
        if (err) {
            callback(err, null);
        } else {
            coll.update({_id: id}, {"$push": {"_v": toAppend}, "$set": {"_s": true}}, {upsert: true, multi: false}, function(err) {
                if (err) {
                    callback(err, null);
                } else {
                    bank.read(type, id, callback);
                }
            });
        }
    });
};

MongoDatabank.prototype.readAll = function(type, ids, callback) {

    var pkey = this.getPrimaryKey(type), bank = this;

    if (!this.db) {
        callback(new NotConnectedError());
        return;
    }

    bank.getCollection(type, function(err, coll) {
        if (err) {
            callback(err, null);
        } else {
            coll.find({'_id': {'$in': ids}}, function(err, cursor) {
                if (err) {
                    callback(err);
                } else {
                    var lastErr = null,
                        results = {}, i, id;

                    // Initialize with nulls

                    for (i in ids) {
                        results[ids[i]] = null;
                    }

                    cursor.each(function(err, value) {
                        if (err) {
                            callback(err, null);
                        } else if (value === null) {
                            callback(null, results);
                        } else {
                            id = value._id;
                            value = bank._recToValue(type, value);
                            results[id] = value;
                        }
                    });
                }
            });
        }
    });
};

MongoDatabank.prototype.getPrimaryKey = function(type) {
    return (this.schema && this.schema[type]) ? this.schema[type].pkey : '_id';
};

// XXX: this got weird. Not sure why.

MongoDatabank.prototype.checkBankSchema = function(callback) {

    var bank = this;

    Step(
        function() {
            var type, group = this.group();
            for (type in bank.schema) {
                bank.getCollection(type, group());
            }
        },
        function(err, colls) {
            var i, j, coll, type, field, keys, group = this.group();
            if (err) throw err;

            for (i = 0; i < colls.length; i++) {
                coll = colls[i];
                type = coll.collectionName;
                if (bank.schema[type].indices) {
                    for (j = 0; j < bank.schema[type].indices; j++) {
                        keys = {};
                        field = bank.schema[type].indices[j];
                        keys[field] = 1;
                        coll.ensureIndex(keys, {}, group());
                    }
                }
            }
        },
        callback
    );
};

module.exports = MongoDatabank;

