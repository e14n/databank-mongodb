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

var assert = require('assert'),
    vows = require('vows'),
    databank = require('databank'),
    Databank = databank.Databank,
    MongoDatabank = require('../lib/mongodb');

process.on('uncaughtException', function(err) {
    console.error(err);
});

Databank.register('mongodb', MongoDatabank);

var params = {host: '172.23.42.5',
              port: 27017,
              dbname: 'test-standalone-' + Date.now(),
              checkSchema: true};

var suite = databank.DriverTest('mongodb', params);

suite['export'](module);
