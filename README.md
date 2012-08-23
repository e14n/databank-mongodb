[![build status](https://secure.travis-ci.org/evanp/databank-mongodb.png)](http://travis-ci.org/evanp/databank-mongodb)
databank-mongodb
----------------

This is the MongoDB driver for Databank.

License
=======

Copyright 2011, 2012, StatusNet Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

> http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Usage
=====

To create a MongoDB databank, use the `Databank.get()` method:

    var Databank = require('databank').Databank;
    
    var db = Databank.get('mongodb', {});
    
The driver takes the following parameters:

* `host`: the host to connect to. Defaults to `localhost`.
* `port`: the port to connect to. Defaults to `27017`.
* `dbname`: the database to use. Defaults to `test`.
* `schema`: the database schema, as described in the Databank README.
* `checkSchema`: whether to synchronize the database with the schema
  argument at connection time. Defaults to `true`.

Database structures
===================

Databank "types" map to MongoDB collections.

The "pkey" of a schema will be mapped to the _id value in the
database. For example, consider a schema with a single type, like the
following:

    {"person": {"pkey": "username"}}
    
An object like the following:

    {"username": "evanp", "age": 43}
    
...would be stored in the database as:

    {"_id": "evanp", "age": 43}
        
The "indices" in a schema will be used to create indices in the
database.

Simple types
============

Arrays and integers are stored in wrapped objects. The following call
creates a new array:

    var db = Databank.get("mongo", {});
    
    db.connect({}, function(err) {
        db.create("friends", "frodo", ["sam", "merry", "pippin"], function(err, friendsList) {
           // done
        });
    });

The stored object in the "friends" collection in MongoDB looks like:

    {"_id": "frodo", "_v": ["sam", "merry", "pippin"], "_s": true}
    
Here, the value is stored in the "_v" field, and the "_s" field stores
a flag indicating that we've made a little shim.

Similarly for integer types:

    db.incr("age", "evanp", function(err, newAge) { });
    
The stored object will be:

    {"_id": "evanp", "_v": 43, "_s": true}

Atomic changes are made for most of the methods.
    
TODO
----

See https://github.com/evanp/databank-mongodb/issues

