// mongodb-driver-test.js
//
// Testing the mongodb driver
//
// Copyright 2012, E14N https://e14n.com/
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

var _ = require('underscore'),
    assert = require('assert'),
    vows = require('vows'),
    databank = require('databank'),
    Databank = databank.Databank,
    MongoDatabank = require('../lib/mongodb');

Databank.register('mongodb', MongoDatabank);

hostBase = {host: '172.23.42.5',
            port: 27017,
            dbname: 'test'};

rsBase = {rs_name: 'databank0',
          hosts: ['172.23.42.2', '172.23.42.3', '172.23.42.4'],
          dbname: 'test'};

var suite = vows.describe("checkSchema param is respected");

suite.addBatch({
   "When we get a one-host databank with checkSchema set to false": {
      topic: function() {
          var params = _.extend({checkSchema: false, dbname: 'test-checkschema-false-' + Date.now()}, hostBase),
          db = Databank.get("mongodb", params);
          return db;
      },
      "db.checkSchema is false": function(db) {
        assert.isFalse(db.checkSchema);
      }
    },
    "When we get a one-host databank with checkSchema set to true": {
      topic: function() {
        var params = _.extend({checkSchema: true, dbname: 'test-checkschema-true-' + Date.now()}, hostBase),
          db = Databank.get("mongodb", params);
          return db;
      },
      "db.checkSchema is true": function(db) {
        assert.isTrue(db.checkSchema);
      }
    },
    "When we get a one-host databank with checkSchema undefined": {
      topic: function() {
          var params = _.extend({dbname: 'test-checkschema-undefined-' + Date.now()}, hostBase),
                  db = Databank.get("mongodb", params);
          return db;
      },
      "db.checkSchema is true": function(db) {
        assert.isTrue(db.checkSchema);
      }
    },
  "When we get a replicated databank with checkSchema set to false": {
    topic: function() {
        var params = _.extend({checkSchema: false, dbname: 'test-checkschema-false-' + Date.now()}, rsBase),
        db = Databank.get("mongodb", params);
        return db;
    },
    "db.checkSchema is false": function(db) {
      assert.isFalse(db.checkSchema);
    }
  },
  "When we get a replicated databank with checkSchema set to true": {
    topic: function() {
      var params = _.extend({checkSchema: true, dbname: 'test-checkschema-true-' + Date.now()}, rsBase),
        db = Databank.get("mongodb", params);
        return db;
    },
    "db.checkSchema is true": function(db) {
      assert.isTrue(db.checkSchema);
    }
  },
  "When we get a replicated databank with checkSchema undefined": {
    topic: function() {
        var params = _.extend({dbname: 'test-checkschema-undefined-' + Date.now()}, rsBase),
                db = Databank.get("mongodb", params);
        return db;
    },
    "db.checkSchema is true": function(db) {
      assert.isTrue(db.checkSchema);
    }
  }
});

suite['export'](module);
