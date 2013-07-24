// mongodb-driver-test.js
//
// Testing special characters in object keys
//
// Copyright 2013, E14N https://e14n.com/
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

Databank.register('mongodb', MongoDatabank);

var params = {host: 'localhost',
              port: 27017,
              dbname: 'test',
              checkSchema: true};

var suite = vows.describe("special characters in mongodb objects");

suite.addBatch({
    "When we connect to a databank": {
        topic: function() {
            var callback = this.callback,
                db = Databank.get("mongodb", params);

            db.connect(params, function(err) {
                if (err) {
                    callback(err, null);
                } else {
                    callback(null, db);
                }
            });
        },
        teardown: function(db) {
            if (db && db.disconnect) {
                db.disconnect(function(err) {});
            }
        },
        "it works": function(err, db) {
            assert.ifError(err);
            assert.isObject(db);
        },
        "and we add an object with a first-level key with a period (.)": {
            topic: function(db) {
                var callback = this.callback;
                db.create("hat", 8, {"outer.brim": "orange", "crown": "black"}, callback);
            },
            "it works": function(err, hat) {
                assert.ifError(err);
                assert.isObject(hat);
                assert.include(hat, "outer.brim");
                assert.equal(hat["outer.brim"], "orange");
                assert.equal(hat.crown, "black");
            },
            "and we read it": {
                topic: function(hat, db) {
                    db.read("hat", 8, this.callback);
                },
                "it works": function(err, hat) {
                    assert.ifError(err);
                    assert.isObject(hat);
                    assert.include(hat, "outer.brim");
                    assert.equal(hat["outer.brim"], "orange");
                    assert.equal(hat.crown, "black");
                },
                "and we update it": {
                    topic: function(read, created, db) {
                        db.update("hat", 8, {"outer.brim": "green", "crown": "black"}, this.callback);
                    },
                    "it works": function(err, hat) {
                        assert.ifError(err);
                        assert.isObject(hat);
                        assert.include(hat, "outer.brim");
                        assert.equal(hat["outer.brim"], "green");
                        assert.equal(hat.crown, "black");
                    },
                    "and we delete it": {
                        topic: function(updated, read, created, db) {
                            db.del("hat", 8, this.callback);
                        },
                        "it works": function(err) {
                            assert.ifError(err);
                        }
                    }
                }
            }
        },
        "and we add an object with a first-level key with a dollar sign ($)": {
            topic: function(db) {
                var callback = this.callback;
                db.create("shoe", 15, {"$cost": 16, "material": "leather"}, callback);
            },
            "it works": function(err, shoe) {
                assert.ifError(err);
                assert.isObject(shoe);
                assert.include(shoe, "$cost");
                assert.equal(shoe["$cost"], 16);
                assert.equal(shoe.material, "leather");
            },
            "and we read it": {
                topic: function(shoe, db) {
                    db.read("shoe", 15, this.callback);
                },
                "it works": function(err, shoe) {
                    assert.ifError(err);
                    assert.isObject(shoe);
                    assert.include(shoe, "$cost");
                    assert.equal(shoe["$cost"], 16);
                    assert.equal(shoe.material, "leather");
                },
                "and we update it": {
                    topic: function(read, created, db) {
                        db.update("shoe", 15, {"$cost": 4, "material": "leather"}, this.callback);
                    },
                    "it works": function(err, shoe) {
                        assert.ifError(err);
                        assert.isObject(shoe);
                        assert.include(shoe, "$cost");
                        assert.equal(shoe["$cost"], 4);
                        assert.equal(shoe.material, "leather");
                    },
                    "and we delete it": {
                        topic: function(updated, read, created, db) {
                            db.del("shoe", 15, this.callback);
                        },
                        "it works": function(err) {
                            assert.ifError(err);
                        }
                    }
                }
            }
        },
        "and we add an object with a second-level key with a period (.)": {
            topic: function(db) {
                var callback = this.callback;
                db.create("tech", 23, {"web": {"2.0": true}}, callback);
            },
            "it works": function(err, tech) {
                assert.ifError(err);
                assert.isObject(tech);
                assert.include(tech, "web");
                assert.include(tech.web, "2.0");
                assert.isTrue(tech.web["2.0"]);
            },
            "and we read it": {
                topic: function(tech, db) {
                    db.read("tech", 23, this.callback);
                },
                "it works": function(err, tech) {
                    assert.ifError(err);
                    assert.isObject(tech);
                    assert.include(tech, "web");
                    assert.include(tech.web, "2.0");
                    assert.isTrue(tech.web["2.0"]);
                },
                "and we update it": {
                    topic: function(read, created, db) {
                        db.update("tech", 23, {"web": {"2.0": false}}, this.callback);
                    },
                    "it works": function(err, tech) {
                        assert.ifError(err);
                        assert.isObject(tech);
                        assert.include(tech, "web");
                        assert.include(tech.web, "2.0");
                        assert.isFalse(tech.web["2.0"]);
                    },
                    "and we delete it": {
                        topic: function(updated, read, created, db) {
                            db.del("tech", 23, this.callback);
                        },
                        "it works": function(err) {
                            assert.ifError(err);
                        }
                    }
                }
            }
        },
        "and we add an object with a second-level key with a dollar sign ($)": {
            topic: function(db) {
                var callback = this.callback;
                db.create("show", 42, {"entertainer": {"ke$ha": false}}, callback);
            },
            "it works": function(err, show) {
                assert.ifError(err);
                assert.isObject(show);
                assert.include(show, "entertainer");
                assert.include(show.entertainer, "ke$ha");
                assert.isFalse(show.entertainer["ke$ha"]);
            },
            "and we read it": {
                topic: function(show, db) {
                    db.read("show", 42, this.callback);
                },
                "it works": function(err, show) {
                    assert.ifError(err);
                    assert.isObject(show);
                    assert.include(show, "entertainer");
                    assert.include(show.entertainer, "ke$ha");
                    assert.isFalse(show.entertainer["ke$ha"]);
                },
                "and we update it": {
                    topic: function(read, created, db) {
                        db.update("show", 42, {"entertainer": {"ke$ha": true}}, this.callback);
                    },
                    "it works": function(err, show) {
                        assert.ifError(err);
                        assert.isObject(show);
                        assert.include(show, "entertainer");
                        assert.include(show.entertainer, "ke$ha");
                        assert.isTrue(show.entertainer["ke$ha"]);
                    },
                    "and we delete it": {
                        topic: function(updated, read, created, db) {
                            db.del("show", 42, this.callback);
                        },
                        "it works": function(err) {
                            assert.ifError(err);
                        }
                    }
                }
            }
        }
    }
});

suite["export"](module);
