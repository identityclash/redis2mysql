/*
 * Copyright (c) 2015.
 */
'use strict';

var chai = require('chai'),
  expect = chai.expect,
  is = require('is_js'),
  async = require('async'),
  Redis2MySql = require('../lib/Redis2MySql'),
  Redis = require('ioredis'),
  mysql = require('mysql'),
  OkPacket = require('../node_modules/mysql/lib/protocol/packets/OkPacket'),
  COLUMNS = {
    SEQ: 'time_sequence',
    KEY: 'key',
    FIELD: 'field',
    VALUE: 'value',
    MEMBER: 'member',
    SCORE: 'score',
    EXPIRY_DT: 'expiry_date',
    CREATION_DT: 'creation_date',
    LAST_UPDT_DT: 'last_update_date'
  },
  ERRORS = {
    MISSING_PREFIX: 'All database table prefixes should be defined by the ' +
    'user.',
    DUPLICATE_PREFIX: 'There are duplicate user-defined database ' +
    'prefixes. Please make all prefixes unique.',
    MISSING_USER: 'Please specify the username',
    MISSING_DB: 'Please specify the database',
    INVALID_INDEX: 'ERR index out of range'
  },
  testKeys = [
    'str:sometype:testkey',
    'str:sometype:testincrkey',
    'str:sometype:xx',
    'str:sometype:yyy',
    'str:some_new_type:zz',
    'str:some_new_type:aa',
    'str:somenumtype:testcounter',
    'str:another_type:some_old_key',
    'str:another_type:some_new_key',
    'str:sometype:alpha',
    'str:sometype:bravo',
    'str:sometype:charlie',
    'str:sometype:delta',
    'str:sometype:echo',
    'str:sometype:foxtrot',
    'str:sometype:golf',
    'str:sometype:hotel',
    'str:sometype:india',
    'str:sometype:juliet',
    'str:sometype:kilo',
    'str:sometype:lima',
    'str:sometype:mike',
    'str:sometype:november',
    'str:sometype:oscar',
    'str:sometype:papa',
    'str:sometype:quebec',
    'str:sometype:romeo',
    'str:sometype:sierra',
    'str:sometype:tango',
    'str:sometype:uniform',
    'str:sometype:victor',
    'str:sometype:whiskey',
    'str:sometype:xray',
    'str:sometype:yankee',
    'str:sometype:zulu',
    'str:sometype:twenty-seven',
    'str:sometype:twenty-eight',
    'str:sometype:twenty-nine',
    'str:sometype:thirty',
    'str:sometype:thirty-one',
    'lst:some_data',
    'lst:another_old_key',
    'lst:another_new_key',
    'set:somenumber',
    'set:some_data',
    'set:another_old_key',
    'set:another_new_key',
    'zset:ssgrade',
    'zset:another_old_key',
    'zset:another_new_key',
    'map:somename',
    'map:another_old_key',
    'map:another_new_key'
  ],
  connection = {
    mysql: {
      user: 'root',
      database: 'mytestxxx',
      charset: 'utf8'
    }
  };

describe('Redis2MySQL', function () {

  var extrnRedis, extrnMySql;

  /* Database and Connection Setup */
  before(function (done) {
    /* connections independent of the object being tested */
    async.series([
      function (firstCb) {
        extrnRedis = new Redis();
        extrnRedis.del(testKeys, function (err) {
          if (err) {
            firstCb(err);
          } else {
            firstCb();
          }
        });
      },
      function (secondCb) {
        extrnMySql = mysql.createConnection({
          user: connection.mysql.user,
          multipleStatements: 'true'
        });
        extrnMySql.connect();
        extrnMySql.query(
          'DROP DATABASE IF EXISTS mytestxxx; ' +
          'CREATE DATABASE IF NOT EXISTS mytestxxx CHARACTER SET = ?; ' +
          'USE mytestxxx; ',
          'utf8',
          function (err) {
            if (err) {
              secondCb(err);
            } else {
              secondCb();
            }
          });
      }
    ], function (err) {
      if (err) {
        done(err);
      } else {
        done();
      }
    });
  });
  /* End Database and Connection Setup */

  /* Object Instantiation Test */
  describe('object instantiation test', function () {

    context('positive test', function () {

      var instance;

      before(function (done) {
        instance = new Redis2MySql({
          redis: {
            showFriendlyErrorStack: true
          },
          mysql: {
            user: 'root',
            database: connection.mysql.database,
            charset: connection.mysql.charset
          },
          custom: {
            datatypePrefix: {
              string: 'str',
              list: 'lst',
              set: 'set',
              sortedSet: 'zset',
              hash: 'map'
            }
          }
        });
        done();
      });

      it('must create a Redis2MySql instance', function (done) {
        expect(instance).to.be.an.instanceOf(Redis2MySql);
        done();
      });

      it('should possess `redisConn` which will make a connection',
        function (done) {
          instance.redisConn.ping(function (err, result) {
            expect(result).equals('PONG');
            done();
          });
        });

      it('should possess `mysqlConn` which will make a connection',
        function (done) {
          instance.mysqlReadConn.ping(function (err, result) {
            expect(result).to.be.an.instanceOf(OkPacket);
            done();
          });
        });

      after(function (done) {
        if (instance) {
          instance.quit();
        }
        done();
      });
    });

    context('missing MySQL database', function () {

      var instance;

      it('should throw error', function (done) {

        expect(function () {
          instance = new Redis2MySql({
              redis: {
                showFriendlyErrorStack: true
              },
              mysql: {
                user: connection.mysql.user
              },
              custom: {
                datatypePrefix: {
                  string: 'str',
                  list: 'lst',
                  set: 'set',
                  sortedSet: 'zset',
                  hash: 'map'
                }
              }
            }
          );
        }).throws(ERRORS.MISSING_DB);
        done();
      });

      after(function (done) {
        if (instance) {
          instance.quit();
        }
        done();
      });
    });

    context('missing username', function () {

      var instance;

      it('should throw an error', function (done) {

        expect(function () {
          instance = new Redis2MySql({
              redis: {
                showFriendlyErrorStack: true
              },
              mysql: {
                database: connection.mysql.database,
                charset: connection.mysql.charset
              },
              custom: {
                datatypePrefix: {
                  string: 'str',
                  list: 'lst',
                  set: 'set',
                  sortedSet: 'zset',
                  hash: 'map'
                }
              }
            }
          );
        }).throws(ERRORS.MISSING_USER);
        done();
      });

      after(function (done) {
        if (instance) {
          instance.quit();
        }
        done();
      });
    });

    context('missing prefixes', function () {

      var instance;

      it('should throw an error', function (done) {

        expect(function () {
          instance = new Redis2MySql({
              redis: {
                showFriendlyErrorStack: true
              },
              mysql: {
                user: connection.mysql.user,
                database: connection.mysql.database,
                charset: connection.mysql.charset
              },
              custom: {
                datatypePrefix: {
                  string: 'str',
                  sortedSet: 'zset',
                  hash: 'map'
                }
              }
            }
          );
        }).throws(ERRORS.MISSING_PREFIX);
        done();
      });

      after(function (done) {
        if (instance) {
          instance.quit();
        }
        done();
      });
    });

    context('duplicate user-defined prefixes', function () {

      var instance;

      it('set and sortedSet values are the same', function (done) {
        expect(function () {
          instance = new Redis2MySql({
              redis: {
                showFriendlyErrorStack: true
              },
              mysql: {
                user: connection.mysql.user,
                database: connection.mysql.database,
                charset: connection.mysql.charset
              },
              custom: {
                datatypePrefix: {
                  string: 'str',
                  list: 'lst',
                  set: 'set',
                  sortedSet: 'set',
                  hash: 'map'
                }
              }
            }
          );
        }).throws(ERRORS.DUPLICATE_PREFIX);
        done();
      });

      it('string, set, and hash values are the same', function (done) {
        expect(function () {
          instance = new Redis2MySql({
              redis: {
                showFriendlyErrorStack: true
              },
              mysql: {
                user: 'root',
                database: 'mytest',
                charset: 'utf8'
              },
              custom: {
                datatypePrefix: {
                  string: 'str',
                  list: 'lst',
                  set: 'str',
                  sortedSet: 'zset',
                  hash: 'str'
                }
              }
            }
          );
        }).throws(ERRORS.DUPLICATE_PREFIX);
        done();
      });

      after(function (done) {
        if (instance) {
          instance.quit();
        }
        done();
      });
    });
  });
  /* End Object Instantiation Test */

  /* Method Test */
  describe('method test', function methodTest() {

    var instance;

    before(function (done) {
      /* actual object being tested */
      instance = new Redis2MySql({
          redis: {
            showFriendlyErrorStack: true
          },
          mysql: {
            user: connection.mysql.user,
            database: connection.mysql.database,
            charset: connection.mysql.charset,
            multipleStatements: 'true'
          },
          custom: {
            datatypePrefix: {
              string: 'str',
              list: 'lst',
              set: 'set',
              sortedSet: 'zset',
              hash: 'map'
            }
          }
        }
      );
      instance.on('error', function (err) {
        throw new Error('Error from listener: ' + err.error + ' ' + err.message +
          ' ' + err.redisKey);
      });

      done();
    });

    describe('#incr()', function () {
      before(function (done) {
        extrnRedis.set('str:sometype:testincrkey', '2', function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });

      it('should increment existing key `testincrkey`, return 3, and insert ' +
        'in MySQL the value of 3',
        function (done) {
          instance.incr('sometype', 'testincrkey', function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals(3); // converts from string to numeric
              extrnRedis.get('str:sometype:testincrkey', function (err, result) {
                if (err) {
                  done(err);
                } else {
                  expect(result).to.be.equals('3'); // get returns a string
                  setTimeout(
                    function () {
                      extrnMySql.query(
                        'SELECT `value` FROM `str_sometype` WHERE `key` = ? ',
                        'testincrkey',
                        function (err, result) {
                          if (err) {
                            done(err);
                          } else {
                            expect(result[0].value).to.be.equals('3');
                            done();
                          }
                        }
                      );
                    }, 400);
                }
              });
            }
          });
        });

      it('should increment the non-existent key `testcounter`, return 1, and ' +
        'insert in MySQL the value of 1',
        function (done) {
          instance.incr('somenumtype', 'testcounter', function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals(1);
              extrnRedis.get('str:somenumtype:testcounter', function (err, result) {
                if (err) {
                  done(err);
                } else {
                  expect(result).to.be.equals('1'); // get returns a string
                  setTimeout(
                    function () {
                      extrnMySql.query(
                        'SELECT `value` FROM `str_somenumtype` WHERE `key` = ? ',
                        'testcounter',
                        function (err, result) {
                          if (err) {
                            done(err);
                          } else {
                            expect(result[0].value).to.be.equals('1');
                            done();
                          }
                        }
                      );
                    }, 400);
                }
              });
            }
          });
        });

      after(function (done) {
        async.series([
          function (firstCb) {
            var keys = [
              'str:sometype:testincrkey',
              'str:sometype:testcounter'
            ];
            extrnRedis.del(keys, function (err) {
              if (err) {
                firstCb(err);
              } else {
                firstCb();
              }
            });
          },
          function (secondCb) {
            var tables = [
              'str_sometype'
            ];
            extrnMySql.query('DROP TABLE IF EXISTS ?? ',
              tables,
              function (err) {
                if (err) {
                  secondCb(err);
                } else {
                  secondCb();
                }
              }
            );
          }
        ], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#set()', function () {
      before(function (done) {
        _deleteData(['str:sometype:testkey'], ['str_sometype'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });

      it('should set the key `str:sometype:testkey`, return `OK`, and insert ' +
        'in MySQL the value of `the fox jumped`; then replace the value with ' +
        '`the wolf smiled`', function (done) {
        instance.set('sometype', 'testkey', 'the fox jumped',
          function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals('OK');
              extrnRedis.get('str:sometype:testkey', function (err, result) {
                if (err) {
                  done(err);
                } else {
                  expect(result).to.be.equals('the fox jumped');
                  setTimeout(function () {
                    extrnMySql.query(
                      'SELECT `value` FROM `str_sometype` WHERE `key` = ? ',
                      'testkey',
                      function (err, result) {
                        if (err) {
                          done(err);
                        } else {
                          expect(result[0].value).to.be.equals('the fox jumped');
                          instance.set('sometype', 'testkey', 'the wolf smiled',
                            function (err, result) {
                              if (err) {
                                done(err);
                              } else {
                                expect(result).to.be.equals('OK');
                                extrnRedis.get('str:sometype:testkey',
                                  function (err, result) {
                                    if (err) {
                                      done(err);
                                    } else {
                                      expect(result).to.be
                                        .equals('the wolf smiled');
                                      setTimeout(function () {
                                        extrnMySql.query(
                                          'SELECT `value` FROM `str_sometype` ' +
                                          'WHERE `key` = ? ',
                                          'testkey',
                                          function (err, result) {
                                            if (err) {
                                              done(err);
                                            } else {
                                              expect(result[0].value).to.be
                                                .equals('the wolf smiled');
                                              done();
                                            }
                                          });
                                      }, 600);
                                    }
                                  });
                              }
                            });
                        }
                      });
                  }, 400);
                }
              });
            }
          });
      });

      after(function (done) {
        _deleteData(['str:sometype:testkey'], ['str_sometype'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#get()', function () {
      before(function (done) {
        async.series([
          function (firstCb) {
            extrnRedis.set('str:sometype:xx', 'this is a value',
              function (err) {
                if (err) {
                  firstCb(err);
                } else {
                  firstCb();
                }
              });
          },
          function (secondCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS str_sometype ' +
              '(' +
              '`key` VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.VALUE + ' VARCHAR(255), ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              function (err) {
                if (err) {
                  secondCb(err);
                } else {
                  secondCb();
                }
              });
          }, function (thirdCb) {
            extrnMySql.query(
              'INSERT IGNORE INTO str_sometype (`key`, `value`) VALUES (?, ?)',
              [
                'xx',
                'this is a value'
              ],
              function (err) {
                if (err) {
                  thirdCb(err);
                } else {
                  thirdCb();
                }
              });
          }
        ], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });

      it('should get the value of the key `str:sometype:xx`',
        function (done) {
          instance.get('sometype', 'xx', function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals('this is a value');
              extrnRedis.del('str:sometype:xx', function (err) {
                if (err) {
                  throw err;
                } else {
                  instance.get('sometype', 'xx', function (err, result) {
                    if (err) {
                      done(err);
                    } else {
                      expect(result).to.be.equals('this is a value');
                      done();
                    }
                  });
                }
              });
            }
          });
        });

      after(function (done) {
        async.series([
          function (firstCb) {
            var keys = [
              'str:sometype:xx'
            ];
            extrnRedis.del(keys, function (err) {
              if (err) {
                firstCb(err);
              } else {
                firstCb();
              }
            });
          },
          function (secondCb) {
            var tables = [
              'str_sometype'
            ];
            extrnMySql.query('DROP TABLE IF EXISTS ??',
              tables,
              function (err) {
                if (err) {
                  secondCb(err);
                } else {
                  secondCb();
                }
              }
            );
          }
        ], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#exists()', function () {
      before(function (done) {
        async.series([
          function (firstCb) {
            extrnRedis.set('str:sometype:yyy', 3003,
              function (err) {
                if (err) {
                  firstCb(err);
                } else {
                  firstCb();
                }
              });
          },
          function (secondCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS str_sometype ' +
              '(' +
              '`key` VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.VALUE + ' VARCHAR(255), ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              function (err) {
                if (err) {
                  secondCb(err);
                } else {
                  secondCb();
                }
              });
          }, function (thirdCb) {
            extrnMySql.query(
              'INSERT INTO str_sometype (`key` , value ) VALUES (?, ?) ' +
              'ON DUPLICATE KEY UPDATE `key` = ?, value = ? ',
              [
                'yyy',
                3003,
                'yyy',
                3003
              ],
              function (err) {
                if (err) {
                  thirdCb(err);
                } else {
                  thirdCb();
                }
              });
          }, function (fourthCb) {
            extrnRedis.sadd('set:somenumber', 4003,
              function (err) {
                if (err) {
                  fourthCb(err);
                } else {
                  fourthCb();
                }
              });
          }, function (fifthCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS set_somenumber ' +
              '(' +
              COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              function (err) {
                if (err) {
                  fifthCb(err);
                } else {
                  fifthCb();
                }
              });
          }, function (sixthCb) {
            extrnMySql.query(
              'INSERT IGNORE INTO set_somenumber (`member`) VALUES (?)',
              [
                5003
              ],
              function (err) {
                if (err) {
                  sixthCb(err);
                } else {
                  sixthCb();
                }
              });
          }, function (seventhCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS map_somename ' +
              '(' +
              '`field` VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.VALUE + ' VARCHAR(255), ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              function (err) {
                if (err) {
                  seventhCb(err);
                } else {
                  seventhCb();
                }
              });
          }
        ], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });

      it('should return 1 for key `str:sometype:yyy`',
        function (done) {
          instance.exists('str:sometype:yyy', function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals(1);
              extrnRedis.del('str:sometype:yyy', function (err) {
                if (err) {
                  done(err);
                } else {
                  instance.exists('str:sometype:yyy', function (err, result) {
                    if (err) {
                      done(err);
                    } else {
                      expect(result).to.be.equals(1);
                      done();
                    }
                  });
                }
              });
            }
          });
        });

      it('should return 1 for key `set:somenumber`',
        function (done) {
          instance.exists('set:somenumber', function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals(1);
              extrnRedis.del('set:somenumber', function (err) {
                if (err) {
                  done(err);
                } else {
                  instance.exists('set:somenumber', function (err, result) {
                    if (err) {
                      done(err);
                    } else {
                      expect(result).to.be.equals(1);
                      done();
                    }
                  });
                }
              });
            }
          });
        });

      it('should return 0 for key `map:somename`',
        function (done) {
          instance.exists('map:somename', function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals(0);
              done();
            }
          });
        });

      after(function (done) {
        async.series([
          function (firstCb) {
            var keys = [
              'str:sometype:yyy',
              'set:somenumber',
              'map:somename'
            ];
            extrnRedis.del(keys, function (err) {
              if (err) {
                firstCb(err);
              } else {
                firstCb();
              }
            });
          },
          function (secondCb) {
            var tables = [
              'str_sometype',
              'set_somenumber',
              'map_somename'
            ];
            extrnMySql.query('DROP TABLE IF EXISTS ??, ??, ??',
              tables,
              function (err) {
                if (err) {
                  secondCb(err);
                } else {
                  secondCb();
                }
              }
            );
          }
        ], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#del()', function () {
      beforeEach(function (done) {
        async.series([
          function (firstCb) {
            extrnRedis.mset('str:sometype:yyy', 3003, 'str:sometype:xx',
              'this is a value',
              function (err) {
                if (err) {
                  firstCb(err);
                } else {
                  firstCb();
                }
              });
          },
          function (secondCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS str_sometype ' +
              '(' +
              '`key` VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.VALUE + ' VARCHAR(255), ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              function (err) {
                if (err) {
                  secondCb(err);
                } else {
                  secondCb();
                }
              });
          }, function (thirdCb) {
            extrnMySql.query(
              'INSERT INTO str_sometype (`key` , value ) VALUES (?, ?), (?, ?) ',
              [
                'yyy',
                3003,
                'xx',
                'this is a value'
              ],
              function (err) {
                if (err) {
                  thirdCb(err);
                } else {
                  thirdCb();
                }
              });
          }, function (fourthCb) {
            extrnRedis.sadd('set:somenumber', 4003,
              function (err) {
                if (err) {
                  fourthCb(err);
                } else {
                  fourthCb();
                }
              });
          }, function (fifthCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS set_somenumber ' +
              '(' +
              COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              function (err) {
                if (err) {
                  fifthCb(err);
                } else {
                  fifthCb();
                }
              });
          }, function (sixthCb) {
            extrnMySql.query(
              'INSERT IGNORE INTO set_somenumber (`member`) VALUES (?)',
              [
                5003
              ],
              function (err) {
                if (err) {
                  sixthCb(err);
                } else {
                  sixthCb();
                }
              });
          }, function (seventhCb) {
            extrnRedis.mset('str:some_new_type:zz', 'hello',
              'str:some_new_type:aa', 1000,
              function (err) {
                if (err) {
                  seventhCb(err);
                } else {
                  seventhCb();
                }
              });
          }, function (eighthCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS str_some_new_type ' +
              '(' +
              '`key` VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.VALUE + ' VARCHAR(255), ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              function (err) {
                if (err) {
                  eighthCb(err);
                } else {
                  eighthCb();
                }
              });
          }, function (ninthCb) {
            extrnMySql.query(
              'INSERT INTO str_some_new_type (`key` , value ) VALUES (?, ?), (?, ?) ',
              [
                'zz',
                'hello',
                'aa',
                1000
              ],
              function (err) {
                if (err) {
                  ninthCb(err);
                } else {
                  ninthCb();
                }
              });
          }
        ], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });

      it('should delete `str:sometype:xx` and `str:sometype:yyy` key ' +
        'in Redis and MySQL', function (done) {
        instance.del('str:sometype:xx', function (err, result) {
          if (err) {
            done(err);
          } else {
            expect(result).to.be.equals(1);
            extrnRedis.exists('str:sometype:xx', function (err, result) {
              if (err) {
                done(err);
              } else {
                expect(result).to.be.equals(0);
              }
            });
            setTimeout(function () {
              extrnMySql.query(
                'SELECT COUNT(1) AS cnt FROM str_sometype WHERE `key` = ? ',
                'xx',
                function (err, result) {
                  if (err) {
                    done(err);
                  } else {
                    expect(result[0].cnt).to.be.equals(0);
                    extrnMySql.query(
                      'SELECT COUNT(1) AS exist_cnt FROM str_sometype',
                      function (err, result) {
                        if (err) {
                          done(err);
                        } else {
                          expect(result[0].exist_cnt).to.be.equals(1);
                          instance.del('str:sometype:yyy', function (err, result) {
                            if (err) {
                              done(err);
                            } else {
                              expect(result).to.be.equals(1);
                              extrnRedis.exists('str:sometype:yyy',
                                function (err, result) {
                                  if (err) {
                                    done(err);
                                  } else {
                                    expect(result).to.be.equals(0);
                                  }
                                });
                              setTimeout(function () {
                                extrnMySql.query(
                                  'SELECT COUNT(1) AS tbl_exist_cnt ' +
                                  'FROM information_schema.TABLES ' +
                                  'WHERE TABLE_NAME = ? ',
                                  'str_sometype',
                                  function (err, result) {
                                    if (err) {
                                      done(err);
                                    } else {
                                      expect(result[0].tbl_exist_cnt).to.be
                                        .equals(0);
                                      done();
                                    }
                                  });
                              }, 400);
                            }
                          });
                        }
                      });
                  }
                });
            }, 400);
          }
        });
      });

      it('should delete `str:some_new_type:zz`, `str:some_new_type:aa`, ' +
        'and set:somenumber', function (done) {
        instance.del(
          [
            'str:some_new_type:zz',
            'str:some_new_type:aa',
            'set:somenumber'
          ],
          function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals(3);
              extrnRedis.exists('str:some_new_type:zz', function (err, result) {
                if (err) {
                  done(err);
                } else {
                  expect(result).to.be.equals(0);
                }
              });
              extrnRedis.exists('str:some_new_type:aa', function (err, result) {
                if (err) {
                  done(err);
                } else {
                  expect(result).to.be.equals(0);
                }
              });
              extrnRedis.exists('set:somenumber', function (err, result) {
                if (err) {
                  done(err);
                } else {
                  expect(result).to.be.equals(0);
                }
              });
              setTimeout(function () {
                extrnMySql.query(
                  'SELECT COUNT(1) AS tbl_exist_cnt ' +
                  'FROM information_schema.TABLES ' +
                  'WHERE TABLE_NAME IN (?, ?) ',
                  ['str_some_new_type', 'set_somenumber'],
                  function (err, result) {
                    if (err) {
                      done(err);
                    } else {
                      expect(result[0].tbl_exist_cnt).to.be.equals(0);
                      done();
                    }
                  });
              }, 400);
            }
          });
      });

      afterEach(function (done) {
        async.series([
          function (firstCb) {
            var keys = [
              'str:sometype:xx',
              'str:sometype:yyy',
              'str:some_new_type:zz',
              'str:some_new_type:aa',
              'set:somenumber',
              'map:somename'
            ];
            extrnRedis.del(keys, function (err) {
              if (err) {
                firstCb(err);
              } else {
                firstCb();
              }
            });
          },
          function (secondCb) {
            var tables = [
              'str_sometype',
              'str_some_new_type',
              'set_somenumber',
              'map_somename'
            ];
            extrnMySql.query('DROP TABLE IF EXISTS ??, ??, ??',
              tables,
              function (err) {
                if (err) {
                  secondCb(err);
                } else {
                  secondCb();
                }
              }
            );
          }
        ], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });

    });

    describe('#lpush()', function () {
      it('should insert all values into the list `lst:some_data`',
        function (done) {
          instance.lpush('some_data',
            [
              300,
              'name1',
              'name2',
              400
            ], function (err, result) {
              if (err) {
                return done(err);
              }
              expect(result).to.be.equals(4);
              async.map([0, 1, 2, 3],
                function (item, callback) {
                  extrnRedis.lindex('lst:some_data', item,
                    function (err, transformed) {
                      if (err) {
                        callback(err);
                      } else {
                        callback(null, transformed);
                      }
                    });
                }, function (err, result) {
                  if (err) {
                    return done(err);
                  }
                  expect(result).to.deep.equals(
                    ['400', 'name2', 'name1', '300']
                  );
                  setTimeout(function () {
                    extrnMySql.query(
                      'SELECT value ' +
                      'FROM lst_some_data ' +
                      'WHERE `value` IN (?, ?, ?, ?) ' +
                      'ORDER BY time_sequence DESC',
                      [
                        300,
                        'name1',
                        'name2',
                        400
                      ],
                      function (err, result) {
                        if (err) {
                          return done(err);
                        }
                        expect(result[0].value).to.be.equals('400');
                        expect(result[1].value).to.be.equals('name2');
                        expect(result[2].value).to.be.equals('name1');
                        expect(result[3].value).to.be.equals('300');
                        done();
                      });
                  }, 400);
                });
            });
        });

      after(function (done) {
        async.series([
          function (firstCb) {
            extrnRedis.del('lst:some_data', function (err) {
              if (err) {
                return firstCb(err);
              }
              firstCb();
            });
          },
          function (secondCb) {
            extrnMySql.query('DROP TABLE IF EXISTS ?? ',
              'lst_some_data',
              function (err) {
                if (err) {
                  return secondCb(err);
                }
                secondCb();
              }
            );
          }
        ], function (err) {
          if (err) {
            return done(err);
          }
          done();
        });
      });
    });

    describe('#lindex()', function () {
      beforeEach(function (done) {
        extrnRedis.multi().time().lpush('lst:some_data',
          [
            300,
            'name1',
            'name2',
            '400'
          ]).exec(function (err, result) {
            if (err) {
              done(err);
            } else {
              extrnMySql.query(
                'CREATE TABLE IF NOT EXISTS lst_some_data ' +
                ' (' +
                COLUMNS.SEQ + ' DOUBLE PRIMARY KEY, ' +
                COLUMNS.VALUE + ' VARCHAR(255), ' +
                COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
                COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
                ') ',
                function (err) {
                  if (err) {
                    done(err);
                  } else {
                    var time, sqlParams = [];

                    if (result[0][1][1].length > 0) { // result from Redis TIME command
                      /* UNIX time in sec + microseconds converted to seconds */
                      time = result[0][1][0] + (result[0][1][1] / 1000000);
                    }

                    sqlParams.push(time);
                    sqlParams.push(300);
                    sqlParams.push((parseFloat(time) + 0.00001));
                    sqlParams.push('name1');
                    sqlParams.push((parseFloat(time) + 0.00002));
                    sqlParams.push('name2');
                    sqlParams.push((parseFloat(time) + 0.00003));
                    sqlParams.push('400');

                    extrnMySql.query(
                      'INSERT INTO lst_some_data (`time_sequence` , `value` ) ' +
                      'VALUES (?, ?), (?, ?), (?, ?), (?, ?) ',
                      sqlParams,
                      function (err) {
                        if (err) {
                          done(err);
                        } else {
                          done();
                        }
                      });
                  }
                });
            }
          });
      });

      it('should get the values in the list in the correct order',
        function (done) {
          async.map([0, 1, 2, 3, 4, -1, -2, -3, -4, -5],
            function (item, callback) {
              instance.lindex('some_data', item,
                function (err, transformed) {
                  if (err) {
                    callback(err);
                  } else {
                    callback(null, transformed);
                  }
                });
            }, function (err, result) {
              if (err) {
                done(err);
              } else {
                expect(result[0]).to.be.equals('400');
                expect(result[1]).to.be.equals('name2');
                expect(result[2]).to.be.equals('name1');
                expect(result[3]).to.be.equals('300');
                expect(result[4]).to.be.equals(null);
                expect(result[5]).to.be.equals('300');
                expect(result[6]).to.be.equals('name1');
                expect(result[7]).to.be.equals('name2');
                expect(result[8]).to.be.equals('400');
                expect(result[9]).to.be.equals(null);
                extrnRedis.del('lst:some_data', function (err) {
                  if (err) {
                    done(err);
                  } else {
                    async.map([0, 1, 2, 3, 4, -1, -2, -3, -4, -5],
                      function (item, callback) {
                        instance.lindex('some_data', item,
                          function (err, transformed) {
                            if (err) {
                              callback(err);
                            } else {
                              callback(null, transformed);
                            }
                          });
                      }, function (err, result) {
                        if (err) {
                          done(err);
                        } else {
                          expect(result[0]).to.be.equals('400');
                          expect(result[1]).to.be.equals('name2');
                          expect(result[2]).to.be.equals('name1');
                          expect(result[3]).to.be.equals('300');
                          expect(result[4]).to.be.equals(null);
                          expect(result[5]).to.be.equals('300');
                          expect(result[6]).to.be.equals('name1');
                          expect(result[7]).to.be.equals('name2');
                          expect(result[8]).to.be.equals('400');
                          expect(result[9]).to.be.equals(null);
                          done();
                        }
                      });
                  }
                });
              }
            });
        });

      afterEach(function (done) {
        _deleteData(['lst:some_data'], ['lst_some_data'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#lset()', function () {
      beforeEach(function (done) {
        extrnRedis.multi().time().lpush('lst:some_data',
          [
            300,
            'name1',
            'name2',
            '400'
          ]).exec(function (err, result) {
            if (err) {
              done(err);
            } else {
              extrnMySql.query(
                'CREATE TABLE IF NOT EXISTS lst_some_data ' +
                ' (' +
                COLUMNS.SEQ + ' DOUBLE PRIMARY KEY, ' +
                COLUMNS.VALUE + ' VARCHAR(255), ' +
                COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
                COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
                ') ',
                function (err) {
                  if (err) {
                    done(err);
                  } else {
                    var time, sqlParams = [];

                    if (result[0][1][1].length > 0) { // result from Redis TIME command
                      /* UNIX time in sec + microseconds */
                      time = result[0][1][0] + (result[0][1][1] / 1000000);
                    }

                    sqlParams.push(time);
                    sqlParams.push(300);
                    sqlParams.push((parseFloat(time) + 0.00001));
                    sqlParams.push('name1');
                    sqlParams.push((parseFloat(time) + 0.00002));
                    sqlParams.push('name2');
                    sqlParams.push((parseFloat(time) + 0.00003));
                    sqlParams.push('400');

                    extrnMySql.query(
                      'INSERT INTO lst_some_data (`time_sequence` , `value` ) ' +
                      'VALUES (?, ?), (?, ?), (?, ?), (?, ?) ',
                      sqlParams,
                      function (err) {
                        if (err) {
                          done(err);
                        } else {
                          done();
                        }
                      });
                  }
                });
            }
          });
      });

      it('should allow modification of the value of specified indices',
        function (done) {
          async.map([-1, -2, -3, -4],
            function (item, callback) {
              instance.lset('some_data', item, ((100 * ++item).toString()),
                function (err, transformed) {
                  if (err) {
                    callback(err);
                  } else {
                    callback(null, transformed);
                  }
                });
            }, function (err, result) {
              if (err) {
                done(err);
              } else {
                expect(result[0]).to.be.equals('OK');
                expect(result[1]).to.be.equals('OK');
                expect(result[2]).to.be.equals('OK');
                expect(result[3]).to.be.equals('OK');

                /**
                 * 'setTimeout()' is needed before the deletion of tables
                 * in the 'afterEach()' block.  Without the 'setTimeout()',
                 * deletion of tables in the 'afterEach()' will execute
                 * immediately after the 'it()' is done.
                 */
                setTimeout(
                  function () {
                    extrnMySql.query(
                      'SELECT value ' +
                      'FROM lst_some_data ' +
                      'WHERE 1 = 1 ' +
                      'ORDER BY time_sequence ASC',
                      function (err, result) {
                        if (err) {
                          done(err);
                        } else {
                          expect(result[0].value).to.be.equals('0');
                          expect(result[1].value).to.be.equals('-100');
                          expect(result[2].value).to.be.equals('-200');
                          expect(result[3].value).to.be.equals('-300');
                          done();
                        }
                      });
                  }, 400);
              }
            });
        });

      it('should throw an error when replacing a non-existing index',
        function (done) {

          instance.lset('some_data', -7, 'blabla', function (err) {
            expect(function () {
              throw err;
            }).throws(ERRORS.INVALID_INDEX);
            done();
          });
        });

      afterEach(function (done) {
        _deleteData(['lst:some_data'], ['lst_some_data'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#rpop()', function () {
      before(function (done) {
        extrnRedis.multi().time().lpush('lst:some_data',
          [
            300,
            'name1',
            'name2',
            '400'
          ]).exec(function (err, result) {
            if (err) {
              done(err);
            } else {
              extrnMySql.query(
                'CREATE TABLE IF NOT EXISTS lst_some_data ' +
                ' (' +
                COLUMNS.SEQ + ' DOUBLE PRIMARY KEY, ' +
                COLUMNS.VALUE + ' VARCHAR(255), ' +
                COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
                COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
                ') ',
                function (err) {
                  if (err) {
                    done(err);
                  } else {
                    var time, sqlParams = [];

                    if (result[0][1][1].length > 0) { // result from Redis TIME command
                      /* UNIX time in sec + microseconds */
                      time = result[0][1][0] + (result[0][1][1] / 1000000);
                    }

                    sqlParams.push(time);
                    sqlParams.push(300);
                    sqlParams.push((parseFloat(time) + 0.00001));
                    sqlParams.push('name1');
                    sqlParams.push((parseFloat(time) + 0.00002));
                    sqlParams.push('name2');
                    sqlParams.push((parseFloat(time) + 0.00003));
                    sqlParams.push('400');

                    extrnMySql.query(
                      'INSERT INTO lst_some_data (`time_sequence` , `value` ) ' +
                      'VALUES (?, ?), (?, ?), (?, ?), (?, ?) ',
                      sqlParams,
                      function (err) {
                        if (err) {
                          done(err);
                        } else {
                          done();
                        }
                      });
                  }
                });
            }
          });
      });

      it('should remove the innermost (oldest) item inserted in a ' +
        'stack and return that value', function (done) {
        async.series([
            function (firstCb) {
              instance.rpop('some_data', function (err, result) {
                if (err) {
                  firstCb(err);
                } else {
                  expect(result).to.be.equals('300');
                  firstCb();
                }
              });
            },
            function (secondCb) {
              setTimeout(
                function () {
                  extrnMySql.query(
                    'SELECT COUNT(1) AS cnt ' +
                    'FROM lst_some_data ' +
                    'WHERE value = ?',
                    '300',
                    function (err, result) {
                      if (err) {
                        secondCb(err);
                      } else {
                        expect(result[0].cnt).to.be.equals(0);
                        secondCb();
                      }
                    });
                }, 400);
            },
            function (thirdCb) {
              instance.rpop('some_data', function (err, result) {
                if (err) {
                  thirdCb(err);
                } else {
                  expect(result).to.be.equals('name1');
                  thirdCb();
                }
              });
            },
            function (fourthCb) {
              setTimeout(
                function () {
                  extrnMySql.query(
                    'SELECT COUNT(1) AS cnt ' +
                    'FROM lst_some_data ' +
                    'WHERE value = ?',
                    'name1',
                    function (err, result) {
                      if (err) {
                        fourthCb(err);
                      } else {
                        expect(result[0].cnt).to.be.equals(0);
                        fourthCb();
                      }
                    });
                }, 400);
            },
            function (fifthCb) {
              instance.rpop('some_data', function (err, result) {
                if (err) {
                  fifthCb(err);
                } else {
                  expect(result).to.be.equals('name2');
                  fifthCb();
                }
              });
            },
            function (sixthCb) {
              setTimeout(
                function () {
                  extrnMySql.query(
                    'SELECT COUNT(1) AS cnt ' +
                    'FROM lst_some_data ' +
                    'WHERE value = ?',
                    'name2',
                    function (err, result) {
                      if (err) {
                        sixthCb(err);
                      } else {
                        expect(result[0].cnt).to.be.equals(0);
                        sixthCb();
                      }
                    });
                }, 400);
            },
            function (seventhCb) {
              instance.rpop('some_data', function (err, result) {
                if (err) {
                  seventhCb(err);
                } else {
                  expect(result).to.be.equals('400');
                  seventhCb();
                }
              });
            },
            function (eighthCb) {
              setTimeout(
                function () {
                  extrnMySql.query(
                    'SELECT COUNT(1) AS cnt ' +
                    'FROM lst_some_data ' +
                    'WHERE value = ?',
                    '400',
                    function (err, result) {
                      if (err) {
                        eighthCb(err);
                      } else {
                        expect(result[0].cnt).to.be.equals(0);
                        eighthCb();
                      }
                    });
                }, 400);
            },
            function (ninthCb) {
              instance.rpop('some_data', function (err, result) {
                if (err) {
                  ninthCb(err);
                } else {
                  expect(result).to.be.equals(null);
                  ninthCb();
                }
              });
            }
          ],
          function (err) {
            if (err) {
              done(err);
            } else {
              done();
            }
          });
      });

      after(function (done) {
        _deleteData(['lst:some_data'], ['lst_some_data'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#sadd()', function () {
      it('should add the specified values in a set in Redis and MySQL ',
        function (done) {
          instance.sadd('some_data',
            ['hello', 1000, 'world', 2000],
            function (err, result) {
              if (err) {
                done(err);
              } else {
                expect(result).to.be.equals(4);
                extrnRedis.smembers('set:some_data', function (err, result) {
                  if (err) {
                    done(err);
                  } else {
                    expect(result).contains('hello').and.contains('1000')
                      .and.contains('world').and.contains('2000');
                    setTimeout(
                      function () {
                        extrnMySql.query(
                          'SELECT COUNT(1) AS cnt FROM set_some_data ' +
                          'WHERE member IN (?, ?, ?, ?) ',
                          [
                            'hello',
                            1000,
                            'world',
                            2000
                          ],
                          function (err, result) {
                            if (err) {
                              done(err);
                            } else {
                              expect(result[0].cnt).to.be.equals(4);
                              instance.sadd('some_data',
                                ['hello', 'new_world'],
                                function (err, result) {
                                  if (err) {
                                    done(err);
                                  } else {
                                    expect(result).to.be.equals(1);
                                    extrnRedis.smembers('set:some_data',
                                      function (err, result) {
                                        if (err) {
                                          done(err);
                                        } else {
                                          expect(result).contains('hello')
                                            .and.contains('1000')
                                            .and.contains('world')
                                            .and.contains('2000')
                                            .and.contains('new_world');
                                          setTimeout(
                                            function () {
                                              extrnMySql.query(
                                                'SELECT COUNT(1) AS cnt ' +
                                                'FROM set_some_data ' +
                                                'WHERE member IN (?, ?, ?, ?, ?) ',
                                                [
                                                  'hello',
                                                  1000,
                                                  'world',
                                                  2000,
                                                  'new_world'
                                                ],
                                                function (err, result) {
                                                  if (err) {
                                                    done(err);
                                                  } else {
                                                    /* no duplicates */
                                                    expect(result[0].cnt)
                                                      .to.be.equals(5);
                                                    done();
                                                  }
                                                }
                                              );
                                            }, 400);
                                        }
                                      });
                                  }
                                });
                            }
                          }
                        );
                      }, 400);
                  }
                });
              }
            });
        });

      after(function (done) {
        _deleteData(['set:some_data'], ['set_some_data'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#srem()', function () {
      before(function (done) {
        extrnRedis.sadd('set:some_data',
          [
            'matthew',
            'mark',
            20,
            'luke',
            60
          ],
          function (err) {
            if (err) {
              done(err);
            } else {
              extrnMySql.query(
                'CREATE TABLE IF NOT EXISTS set_some_data ' +
                '(' +
                COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
                COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
                COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
                ') ',
                function (err) {
                  if (err) {
                    done(err);
                  } else {
                    extrnMySql.query(
                      'INSERT INTO set_some_data (member) ' +
                      'VALUES (?), (?), (?), (?), (?)',
                      [
                        'matthew',
                        'mark',
                        20,
                        'luke',
                        60
                      ],
                      function (err) {
                        if (err) {
                          done(err);
                        } else {
                          done();
                        }
                      });
                  }
                });
            }
          });
      });

      it('should be able to remove specified items from Redis and MySQL',
        function (done) {
          instance.srem('some_data', ['mark', 'luke', 60, 808],
            function (err, result) {
              if (err) {
                done(err);
              } else {
                expect(result).to.be.equals(3);
                extrnRedis.smembers('set:some_data', function (err, result) {
                  if (err) {
                    done(err);
                  } else {
                    expect(result).contains('20').and.contains('matthew');
                    extrnMySql.query(
                      'SELECT member ' +
                      'FROM set_some_data ',
                      function (err, result) {
                        if (err) {
                          done(err);
                        } else {
                          var members = [], i;
                          for (i = 0; i < result.length; i++) {
                            members.push(result[i].member);
                          }
                          expect(members).contains('matthew')
                            .and.contains('20');
                          done();
                        }
                      });
                  }
                });
              }
            });
        });

      after(function (done) {
        _deleteData(['set:some_data'], ['set_some_data'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#smembers()', function () {
      before(function (done) {
        extrnRedis.sadd('set:some_data',
          [
            'matthew',
            'mark',
            20,
            'luke',
            60
          ],
          function (err) {
            if (err) {
              done(err);
            } else {
              extrnMySql.query(
                'CREATE TABLE IF NOT EXISTS set_some_data ' +
                '(' +
                COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
                COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
                COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
                ') ',
                function (err) {
                  if (err) {
                    done(err);
                  } else {
                    extrnMySql.query(
                      'INSERT INTO set_some_data (member) ' +
                      'VALUES (?), (?), (?), (?), (?)',
                      [
                        'matthew',
                        'mark',
                        20,
                        'luke',
                        60
                      ],
                      function (err) {
                        if (err) {
                          done(err);
                        } else {
                          done();
                        }
                      });
                  }
                });
            }
          });
      });

      it('should get all the members from both Redis and MySQL',
        function (done) {
          instance.smembers('some_data', function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).contains('matthew').and.contains('20')
                .and.contains('mark').and.contains('luke').and.contains('60');
              extrnRedis.del('set:some_data', function (err) {
                if (err) {
                  done(err);
                } else {
                  instance.smembers('some_data', function (err, result) {
                    if (err) {
                      done(err);
                    } else {
                      expect(result).contains('matthew').and.contains('20')
                        .and.contains('mark').and.contains('luke')
                        .and.contains('60');
                      done();
                    }
                  });
                }
              });
            }
          });
        });

      after(function (done) {
        _deleteData(['set:some_data'], ['set_some_data'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#sismember()', function () {
      before(function (done) {
        extrnRedis.sadd('set:some_data',
          [
            'matthew',
            'mark',
            20,
            'luke',
            60
          ],
          function (err) {
            if (err) {
              done(err);
            } else {
              extrnMySql.query(
                'CREATE TABLE IF NOT EXISTS set_some_data ' +
                '(' +
                COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
                COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
                COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
                ') ',
                function (err) {
                  if (err) {
                    done(err);
                  } else {
                    extrnMySql.query(
                      'INSERT INTO set_some_data (member) ' +
                      'VALUES (?), (?), (?), (?), (?)',
                      [
                        'matthew',
                        'mark',
                        20,
                        'luke',
                        60
                      ],
                      function (err) {
                        if (err) {
                          done(err);
                        } else {
                          done();
                        }
                      });
                  }
                });
            }
          });
      });

      it('should determine whether an item is a member by checking ' +
        'from Redis or MySQL', function (done) {
        async.parallel([
          function (callback) {
            instance.sismember('some_data', 'mark', function (err, result) {
              if (err) {
                callback(err);
              } else {
                expect(result).to.be.equals(1);
                callback();
              }
            });
          },
          function (callback) {
            instance.sismember('some_data', '20', function (err, result) {
              if (err) {
                callback(err);
              } else {
                expect(result).to.be.equals(1);
                callback();
              }
            });
          },
          function (callback) {
            instance.sismember('some_data', 'luke', function (err, result) {
              if (err) {
                callback(err);
              } else {
                expect(result).to.be.equals(1);
                callback();
              }
            });
          },
          function (callback) {
            instance.sismember('some_data', 'hello_w', function (err, result) {
              if (err) {
                callback(err);
              } else {
                expect(result).to.be.equals(0);
                callback();
              }
            });
          }
        ], function (err) {
          if (err) {
            done(err);
          } else {
            extrnRedis.del('set:some_data', function (err) {
              if (err) {
                done(err);
              } else {
                async.parallel([
                  function (callback) {
                    instance.sismember('some_data', 'mark', function (err, result) {
                      if (err) {
                        callback(err);
                      } else {
                        expect(result).to.be.equals(1);
                        callback();
                      }
                    });
                  },
                  function (callback) {
                    instance.sismember('some_data', '20', function (err, result) {
                      if (err) {
                        callback(err);
                      } else {
                        expect(result).to.be.equals(1);
                        callback();
                      }
                    });
                  },
                  function (callback) {
                    instance.sismember('some_data', 'luke', function (err, result) {
                      if (err) {
                        callback(err);
                      } else {
                        expect(result).to.be.equals(1);
                        callback();
                      }
                    });
                  },
                  function (callback) {
                    instance.sismember('some_data', 'hello_w', function (err, result) {
                      if (err) {
                        callback(err);
                      } else {
                        expect(result).to.be.equals(0);
                        callback();
                      }
                    });
                  }
                ], function (err) {
                  if (err) {
                    done(err);
                  } else {
                    done();
                  }
                });
              }
            });
          }
        });
      });

      after(function (done) {
        _deleteData(['set:some_data'], ['set_some_data'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#scard()', function () {
      before(function (done) {
        extrnRedis.sadd('set:some_data',
          [
            'matthew',
            'mark',
            20,
            'luke',
            60
          ],
          function (err) {
            if (err) {
              done(err);
            } else {
              extrnMySql.query(
                'CREATE TABLE IF NOT EXISTS set_some_data ' +
                '(' +
                COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
                COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
                COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
                ') ',
                function (err) {
                  if (err) {
                    done(err);
                  } else {
                    extrnMySql.query(
                      'INSERT INTO set_some_data (member) ' +
                      'VALUES (?), (?), (?), (?), (?)',
                      [
                        'matthew',
                        'mark',
                        20,
                        'luke',
                        60
                      ],
                      function (err) {
                        if (err) {
                          done(err);
                        } else {
                          done();
                        }
                      });
                  }
                });
            }
          });
      });

      it('should return the number of members in the set from ' +
        'both Redis and MySQL', function (done) {
        instance.scard('some_data', function (err, result) {
          if (err) {
            done(err);
          } else {
            expect(result).to.be.equals(5);
            extrnRedis.del('set:some_data', function (err) {
              if (err) {
                done(err);
              } else {
                instance.scard('some_data', function (err, result) {
                  if (err) {
                    done(err);
                  } else {
                    expect(result).to.be.equals(5);
                    done();
                  }
                });
              }
            });
          }
        });
      });

      after(function (done) {
        _deleteData(['set:some_data'], ['set_some_data'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#zadd()', function () {
      it('should add the elements into the database for sorted set ' +
        'in Redis and MySQL', function (done) {
        instance.zadd('ssgrade',
          [
            98.75, 'myra',
            70.00, 'sal',
            81.50, 'john',
            81.50, 'krull',
            75.00, 'ren'
          ],
          function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals(5);
              extrnRedis.zrangebyscore('zset:ssgrade', 0, 100,
                ['withscores'],
                function (err, result) {
                  if (err) {
                    done(err);
                  } else {
                    expect(result).to.deep.equal(
                      [
                        'sal',
                        '70',
                        'ren',
                        '75',
                        'john',
                        '81.5',
                        'krull',
                        '81.5',
                        'myra',
                        '98.75'
                      ]);
                    setTimeout(function () {
                      extrnMySql.query(
                        'SELECT member, score FROM zset_ssgrade ' +
                        'ORDER BY score ASC',
                        function (err, result) {
                          if (err) {
                            done(err);
                          } else {
                            expect(result[0].member).to.be.equals('sal');
                            expect(result[0].score).to.be.equals(70);
                            expect(result[1].member).to.be.equals('ren');
                            expect(result[1].score).to.be.equals(75);
                            expect(result[2].member).to.be.equals('john');
                            expect(result[2].score).to.be.equals(81.5);
                            expect(result[3].member).to.be.equals('krull');
                            expect(result[3].score).to.be.equals(81.5);
                            expect(result[4].member).to.be.equals('myra');
                            expect(result[4].score).to.be.equals(98.75);
                            done();
                          }
                        });
                    }, 400);
                  }
                });
            }
          });
      });

      after(function (done) {
        _deleteData(['zset:ssgrade'], ['zset_ssgrade'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#zincrby()', function () {
      before(function (done) {
        var scoreMembers = [
          98.75, 'myra',
          70.00, 'sal',
          81.50, 'john',
          81.50, 'krull',
          75.00, 'ren'
        ];
        async.series([
          function (firstCb) {
            extrnRedis.zadd('zset:ssgrade',
              scoreMembers, function (err) {
                if (err) {
                  firstCb(err);
                } else {
                  firstCb();
                }
              });
          },
          function (secondCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS zset_ssgrade ' +
              '(' +
              COLUMNS.SCORE + ' DOUBLE, ' +
              COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              scoreMembers,
              function (err) {
                if (err) {
                  secondCb(err);
                } else {
                  secondCb();
                }
              }
            );
          },
          function (thirdCb) {
            extrnMySql.query(
              'INSERT IGNORE INTO zset_ssgrade (score, member) ' +
              'VALUES (?, ?) , (?, ?), (?, ?), (?, ?), (?, ?)',
              scoreMembers,
              function (err) {
                if (err) {
                  thirdCb(err);
                } else {
                  thirdCb();
                }
              }
            );
          }
        ], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });

      it('should be able to increment a member in the sorted set',
        function (done) {
          async.parallel([
            function (firstCb) {
              instance.zincrby('ssgrade', -5.5, 'john', function (err, result) {
                if (err) {
                  firstCb(err);
                } else {
                  expect(result).to.be.equals('76');
                  setTimeout(
                    function () {
                      extrnMySql.query(
                        'SELECT score ' +
                        'FROM zset_ssgrade ' +
                        'WHERE member = ? ',
                        'john',
                        function (err, result) {
                          if (err) {
                            firstCb(err);
                          } else {
                            expect(result[0].score).to.be.equals(76);
                            firstCb();
                          }
                        });
                    }, 400);
                }
              });
            },
            function (secondCb) {
              instance.zincrby('ssgrade', 10.533, 'myra', function (err, result) {
                if (err) {
                  secondCb(err);
                } else {
                  expect(result).to.be.equals('109.283');
                  setTimeout(
                    function () {
                      extrnMySql.query(
                        'SELECT score ' +
                        'FROM zset_ssgrade ' +
                        'WHERE member = ? ',
                        'myra',
                        function (err, result) {
                          if (err) {
                            secondCb(err);
                          } else {
                            expect(result[0].score).to.be.equals(109.283);
                            secondCb();
                          }
                        });
                    }, 400);
                }
              });
            }
          ], function (err) {
            if (err) {
              done(err);
            } else {
              done();
            }
          });
        });

      after(function (done) {
        _deleteData(['zset:ssgrade'], ['zset_ssgrade'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#zscore()', function () {
      beforeEach(function (done) {
        var scoreMembers = [
          98.75, 'myra',
          70.00, 'sal',
          81.50, 'john',
          81.50, 'krull',
          75.00, 'ren'
        ];
        async.series([
          function (firstCb) {
            extrnRedis.zadd('zset:ssgrade',
              scoreMembers, function (err) {
                if (err) {
                  firstCb(err);
                } else {
                  firstCb();
                }
              });
          },
          function (secondCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS zset_ssgrade ' +
              '(' +
              COLUMNS.SCORE + ' DOUBLE, ' +
              COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              scoreMembers,
              function (err) {
                if (err) {
                  secondCb(err);
                } else {
                  secondCb();
                }
              }
            );
          },
          function (thirdCb) {
            extrnMySql.query(
              'INSERT IGNORE INTO zset_ssgrade (score, member) ' +
              'VALUES (?, ?) , (?, ?), (?, ?), (?, ?), (?, ?)',
              scoreMembers,
              function (err) {
                if (err) {
                  thirdCb(err);
                } else {
                  thirdCb();
                }
              }
            );
          }
        ], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });

      it('should retrieve the correct score given the specified member',
        function (done) {
          instance.zscore('ssgrade', 'sal', function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals('70');
              async.series(
                [
                  function (firstCb) {
                    extrnRedis.zrem('zset:ssgrade', 'sal', function (err) {
                      if (err) {
                        firstCb(err);
                      } else {
                        firstCb();
                      }
                    });
                  },
                  function (secondCb) {
                    setTimeout(
                      function () {
                        extrnMySql.query(
                          'SELECT score ' +
                          'FROM zset_ssgrade ' +
                          'WHERE member = ?',
                          'sal',
                          function (err, result) {
                            if (err) {
                              secondCb(err);
                            } else {
                              expect(result[0].score).to.be.equals(70);
                              secondCb();
                            }
                          }
                        );
                      }, 400);
                  }
                ], function (err) {
                  if (err) {
                    done(err);
                  } else {
                    done();
                  }
                });
            }
          });
        });

      it('should return null when the member is non-existent in the sorted ' +
        'set', function (done) {
        instance.zscore('ssgrade', 'falcon', function (err, result) {
          if (err) {
            done(err);
          } else {
            expect(result).to.be.equals(null);
            done();
          }
        });
      });

      afterEach(function (done) {
        _deleteData(['zset:ssgrade'], ['zset_ssgrade'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#zrank()', function () {
      beforeEach(function (done) {
        var scoreMembers = [
          98.75, 'myra',
          70.00, 'sal',
          81.50, 'john',
          81.50, 'krull',
          75.00, 'ren'
        ];
        async.series([
          function (firstCb) {
            extrnRedis.zadd('zset:ssgrade',
              scoreMembers, function (err) {
                if (err) {
                  firstCb(err);
                } else {
                  firstCb();
                }
              });
          },
          function (secondCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS zset_ssgrade ' +
              '(' +
              COLUMNS.SCORE + ' DOUBLE, ' +
              COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              scoreMembers,
              function (err) {
                if (err) {
                  secondCb(err);
                } else {
                  secondCb();
                }
              }
            );
          },
          function (thirdCb) {
            extrnMySql.query(
              'INSERT IGNORE INTO zset_ssgrade (score, member) ' +
              'VALUES (?, ?) , (?, ?), (?, ?), (?, ?), (?, ?)',
              scoreMembers,
              function (err) {
                if (err) {
                  thirdCb(err);
                } else {
                  thirdCb();
                }
              }
            );
          }
        ], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });

      it('should be able to get the index of the member in a sorted set',
        function (done) {
          instance.zrank('ssgrade', 'krull', function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals(3);
              async.series([
                function (firstCb) {
                  extrnRedis.zrem('zset:ssgrade', 'krull',
                    function (err) {
                      if (err) {
                        firstCb(err);
                      } else {
                        firstCb();
                      }
                    });
                },
                function (secondCb) {
                  instance.zrank('ssgrade', 'krull', function (err, result) {
                    if (err) {
                      secondCb(err);
                    } else {
                      expect(result).to.be.equals(3);
                      secondCb();
                    }
                  });
                }
              ], function (err) {
                if (err) {
                  done(err);
                } else {
                  done();
                }
              });
            }
          });
        });

      it('should be able to return null if the member does not exist',
        function (done) {
          instance.zrank('ssgrade', 'morlock', function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals(null);
              done();
            }
          });
        });

      afterEach(function (done) {
        _deleteData(['zset:ssgrade'], ['zset_ssgrade'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#zrangebyscore()', function () {
      beforeEach(function (done) {
        var scoreMembers = [
          98.75, 'myra',
          70.00, 'sal',
          81.50, 'krull',
          81.50, 'john',
          75.00, 'ren'
        ];
        async.series([
          function (firstCb) {
            extrnRedis.zadd('zset:ssgrade',
              scoreMembers, function (err) {
                if (err) {
                  firstCb(err);
                } else {
                  firstCb();
                }
              });
          },
          function (secondCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS zset_ssgrade ' +
              '(' +
              COLUMNS.SCORE + ' DOUBLE, ' +
              COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              scoreMembers,
              function (err) {
                if (err) {
                  secondCb(err);
                } else {
                  secondCb();
                }
              }
            );
          },
          function (thirdCb) {
            extrnMySql.query(
              'INSERT IGNORE INTO zset_ssgrade (score, member) ' +
              'VALUES (?, ?) , (?, ?), (?, ?), (?, ?), (?, ?)',
              scoreMembers,
              function (err) {
                if (err) {
                  thirdCb(err);
                } else {
                  thirdCb();
                }
              }
            );
          }
        ], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });

      it('should be able to return range of members in order of their scores' +
        'with value(s) excluded',
        function (done) {
          instance.zrangebyscore('ssgrade', '61', '(81.5', true, null,
            null, null,
            function (err, result) {
              if (err) {
                done(err);
              } else {
                expect(result).to.be.deep.equals(
                  [
                    'sal', '70',
                    'ren', '75'
                  ]
                );
                extrnRedis.zrem('zset:ssgrade',
                  'sal',
                  'ren',
                  'john',
                  'krull',
                  'myra',
                  function (err) {
                    if (err) {
                      done(err);
                    } else {
                      instance.zrangebyscore('ssgrade', '61', '(81.5', true, null,
                        null, null,
                        function (err, result) {
                          if (err) {
                            done(err);
                          } else {
                            expect(result).to.be.deep.equals(
                              [
                                'sal', 70,
                                'ren', 75
                              ]
                            );
                            done();
                          }
                        });
                    }
                  });
              }
            });
        });

      it('should be able to return range of members in order of their scores' +
        'with value of infinity',
        function (done) {
          instance.zrangebyscore('ssgrade', '-inf', '81.5', true, null,
            null, null,
            function (err, result) {
              if (err) {
                done(err);
              } else {
                expect(result).to.be.deep.equals(
                  [
                    'sal', '70',
                    'ren', '75',
                    'john', '81.5',
                    'krull', '81.5'
                  ]
                );
                extrnRedis.zrem('zset:ssgrade',
                  'sal',
                  'ren',
                  'john',
                  'krull',
                  'myra',
                  function (err) {
                    if (err) {
                      done(err);
                    } else {
                      instance.zrangebyscore('ssgrade', '-inf', '81.5', true, null,
                        null, null,
                        function (err, result) {
                          if (err) {
                            done(err);
                          } else {
                            expect(result).to.be.deep.equals(
                              [
                                'sal', 70,
                                'ren', 75,
                                'john', 81.5,
                                'krull', 81.5
                              ]
                            );
                            done();
                          }
                        });
                    }
                  });
              }
            });
        });

      it('should be able to return range of members in order of their scores,' +
        'supporting limit feature',
        function (done) {
          instance.zrangebyscore('ssgrade', '61', 'inf', true, true,
            2, 4,
            function (err, result) {
              if (err) {
                done(err);
              } else {
                expect(result).to.be.deep.equals(
                  [
                    'john', '81.5',
                    'krull', '81.5',
                    'myra', '98.75'
                  ]
                );
                extrnRedis.zrem('zset:ssgrade',
                  'sal',
                  'ren',
                  'john',
                  'krull',
                  'myra',
                  function (err) {
                    if (err) {
                      done(err);
                    } else {
                      instance.zrangebyscore('ssgrade', '61', '100', true, true,
                        2, 4,
                        function (err, result) {
                          if (err) {
                            done(err);
                          } else {
                            expect(result).to.be.deep.equals(
                              [
                                'john', 81.5,
                                'krull', 81.5,
                                'myra', 98.75
                              ]
                            );
                            done();
                          }
                        });
                    }
                  });
              }
            });
        });

      it('should be able to return null if the scores are not within the ' +
        'ranges which were defined',
        function (done) {
          instance.zrangebyscore('ssgrade', '100', 'inf', true, true,
            2, 4,
            function (err, result) {
              if (err) {
                done(err);
              } else {
                expect(result).to.be.deep.equals([]);
                done();
              }
            });
        });

      afterEach(function (done) {
        _deleteData(['zset:ssgrade'], ['zset_ssgrade'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#zrevrangebyscore()', function () {
      beforeEach(function (done) {
        var scoreMembers = [
          98.75, 'myra',
          70.00, 'sal',
          81.50, 'krull',
          81.50, 'john',
          75.00, 'ren'
        ];
        async.series([
          function (firstCb) {
            extrnRedis.zadd('zset:ssgrade',
              scoreMembers, function (err) {
                if (err) {
                  firstCb(err);
                } else {
                  firstCb();
                }
              });
          },
          function (secondCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS zset_ssgrade ' +
              '(' +
              COLUMNS.SCORE + ' DOUBLE, ' +
              COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              scoreMembers,
              function (err) {
                if (err) {
                  secondCb(err);
                } else {
                  secondCb();
                }
              }
            );
          },
          function (thirdCb) {
            extrnMySql.query(
              'INSERT IGNORE INTO zset_ssgrade (score, member) ' +
              'VALUES (?, ?) , (?, ?), (?, ?), (?, ?), (?, ?)',
              scoreMembers,
              function (err) {
                if (err) {
                  thirdCb(err);
                } else {
                  thirdCb();
                }
              }
            );
          }
        ], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });

      it('should be able to return range of members in order of their scores' +
        'with value(s) excluded',
        function (done) {
          instance.zrevrangebyscore('ssgrade', '(81.5', '61', true, null,
            null, null,
            function (err, result) {
              if (err) {
                done(err);
              } else {
                expect(result).to.be.deep.equals(
                  [
                    'ren', '75',
                    'sal', '70'
                  ]
                );
                extrnRedis.zrem('zset:ssgrade',
                  'sal',
                  'ren',
                  'john',
                  'krull',
                  'myra',
                  function (err) {
                    if (err) {
                      done(err);
                    } else {
                      instance.zrevrangebyscore('ssgrade', '(81.5', '61', true,
                        null, null, null,
                        function (err, result) {
                          if (err) {
                            done(err);
                          } else {
                            expect(result).to.be.deep.equals(
                              [
                                'ren', 75,
                                'sal', 70
                              ]);
                            done();
                          }
                        });
                    }
                  });
              }
            });
        });

      it('should be able to return range of members in order of their scores' +
        'with value of infinity',
        function (done) {
          instance.zrevrangebyscore('ssgrade', '81.5', '-inf', true, null,
            null, null,
            function (err, result) {
              if (err) {
                done(err);
              } else {
                expect(result).to.be.deep.equals(
                  [
                    'krull', '81.5',
                    'john', '81.5',
                    'ren', '75',
                    'sal', '70'
                  ]
                );
                extrnRedis.zrem('zset:ssgrade',
                  'sal',
                  'ren',
                  'john',
                  'krull',
                  'myra',
                  function (err) {
                    if (err) {
                      done(err);
                    } else {
                      instance.zrevrangebyscore('ssgrade', '81.5', '-inf', true,
                        null, null, null,
                        function (err, result) {
                          if (err) {
                            done(err);
                          } else {
                            expect(result).to.be.deep.equals(
                              [
                                'krull', 81.5,
                                'john', 81.5,
                                'ren', 75,
                                'sal', 70
                              ]);
                            done();
                          }
                        });
                    }
                  });
              }
            });
        });

      it('should be able to return range of members in order of their scores,' +
        'supporting limit feature',
        function (done) {
          instance.zrevrangebyscore('ssgrade', 'inf', '61', true, true,
            1, 3,
            function (err, result) {
              if (err) {
                done(err);
              } else {
                expect(result).to.be.deep.equals(
                  [
                    'krull', '81.5',
                    'john', '81.5',
                    'ren', '75'
                  ]
                );
                extrnRedis.zrem('zset:ssgrade',
                  'sal',
                  'ren',
                  'john',
                  'krull',
                  'myra',
                  function (err) {
                    if (err) {
                      done(err);
                    } else {
                      instance.zrevrangebyscore('ssgrade', 'inf', '61', true, true,
                        1, 3,
                        function (err, result) {
                          if (err) {
                            done(err);
                          } else {
                            expect(result).to.be.deep.equals(
                              [
                                'krull', 81.5,
                                'john', 81.5,
                                'ren', 75
                              ]
                            );
                            done();
                          }
                        });
                    }
                  });
              }
            });
        });

      it('should be able to return null if the scores are not within the ' +
        'ranges which were defined',
        function (done) {
          instance.zrangebyscore('ssgrade', 'inf', '100', true, true,
            2, 4,
            function (err, result) {
              if (err) {
                done(err);
              } else {
                expect(result).to.be.deep.equals([]);
                done();
              }
            });
        });

      afterEach(function (done) {
        _deleteData(['zset:ssgrade'], ['zset_ssgrade'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#hset()', function () {
      it('should be able to add a key-value in a hash', function (done) {
        instance.hset('somename', 'hello', 'world', function (err, result) {
          if (err) {
            done(err);
          } else {
            expect(result).to.be.equals(1);
            extrnRedis.hget('map:somename', 'hello', function (err, result) {
              if (err) {
                done(err);
              } else {
                expect(result).to.be.equals('world');
                setTimeout(
                  function () {
                    extrnMySql.query(
                      'SELECT value ' +
                      'FROM map_somename ' +
                      'WHERE field = ? ',
                      'hello',
                      function (err, result) {
                        if (err) {
                          done(err);
                        } else {
                          expect(result[0].value).to.be.equals('world');
                          instance.hset('somename', 'hello', 'a new value',
                            function (err, result) {
                              if (err) {
                                done(err);
                              } else {
                                expect(result).to.be.equals(0);
                                extrnRedis.hget('map:somename', 'hello',
                                  function (err, result) {
                                    if (err) {
                                      done(err);
                                    } else {
                                      expect(result).to.be.equals('a new value');
                                      setTimeout(
                                        function () {
                                          extrnMySql.query(
                                            'SELECT value ' +
                                            'FROM map_somename ' +
                                            'WHERE field = ? ',
                                            'hello',
                                            function (err, result) {
                                              if (err) {
                                                done(err);
                                              } else {
                                                expect(result[0].value).to.be
                                                  .equals('a new value');
                                                done();
                                              }
                                            });
                                        }, 400);
                                    }
                                  });
                              }
                            });
                        }
                      });
                  }, 400);
              }
            });
          }
        });
      });

      afterEach(function (done) {
        _deleteData(['map:somename'], ['map_somename'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#hmset()', function () {
      it('should be able to add multiple key-values in a hash',
        function (done) {
          instance.hmset('somename',
            [
              'oneField', 'oneValue',
              'twoField', 'twoValue',
              'threeField', 'threeValue',
              'fourField', 'fourValue'
            ],
            function (err, result) {
              if (err) {
                done(err);
              } else {
                expect(result).to.be.equals('OK');
                extrnRedis.hmget('map:somename',
                  [
                    'fourField',
                    'twoField',
                    'oneField',
                    'threeField'
                  ],
                  function (err, result) {
                    if (err) {
                      done(err);
                    } else {
                      expect(result).to.be.deep.equals(
                        [
                          'fourValue',
                          'twoValue',
                          'oneValue',
                          'threeValue'
                        ]);
                      setTimeout(
                        function () {
                          extrnMySql.query(
                            'SELECT field, value ' +
                            'FROM map_somename ' +
                            'WHERE field IN (?, ?, ?, ?) ' +
                            'ORDER BY field ASC',
                            [
                              'oneField',
                              'twoField',
                              'fourField',
                              'threeField'
                            ],
                            function (err, result) {
                              if (err) {
                                done(err);
                              } else {
                                expect(result[0].field).to.be.equals('fourField');
                                expect(result[0].value).to.be.equals('fourValue');
                                expect(result[1].field).to.be.equals('oneField');
                                expect(result[1].value).to.be.equals('oneValue');
                                expect(result[2].field).to.be.equals('threeField');
                                expect(result[2].value).to.be.equals('threeValue');
                                expect(result[3].field).to.be.equals('twoField');
                                expect(result[3].value).to.be.equals('twoValue');
                                instance.hmset('somename',
                                  [
                                    'twoField', 'a two value',
                                    'fourField', 'a four value'
                                  ],
                                  function (err, result) {
                                    if (err) {
                                      done(err);
                                    } else {
                                      expect(result).to.be.equals('OK');
                                      extrnRedis.hmget('map:somename',
                                        [
                                          'oneField',
                                          'twoField',
                                          'threeField',
                                          'fourField'
                                        ],
                                        function (err, result) {
                                          if (err) {
                                            done(err);
                                          } else {
                                            expect(result).to.be.deep.equals(
                                              [
                                                'oneValue',
                                                'a two value',
                                                'threeValue',
                                                'a four value'
                                              ]);
                                            setTimeout(
                                              function () {
                                                extrnMySql.query(
                                                  'SELECT field, value ' +
                                                  'FROM map_somename ' +
                                                  'WHERE field IN (?, ?, ?, ?) ' +
                                                  'ORDER BY field ASC',
                                                  [
                                                    'fourField',
                                                    'twoField',
                                                    'oneField',
                                                    'threeField'
                                                  ],
                                                  function (err, result) {
                                                    if (err) {
                                                      done(err);
                                                    } else {
                                                      expect(result[0].field).to
                                                        .be.equals('fourField');
                                                      expect(result[0].value).to
                                                        .be.equals('a four value');
                                                      expect(result[1].field).to
                                                        .be.equals('oneField');
                                                      expect(result[1].value).to
                                                        .be.equals('oneValue');
                                                      expect(result[2].field).to
                                                        .be.equals('threeField');
                                                      expect(result[2].value).to
                                                        .be.equals('threeValue');
                                                      expect(result[3].field).to
                                                        .be.equals('twoField');
                                                      expect(result[3].value).to
                                                        .be.equals('a two value');
                                                      done();
                                                    }
                                                  });
                                              }, 400);
                                          }
                                        });
                                    }
                                  });
                              }
                            });
                        }, 400);
                    }
                  });
              }
            });
        });

      it('should be able to add one key-value in a hash',
        function (done) {
          instance.hmset('somename',
            'oneField', 'oneValue',
            function (err, result) {
              if (err) {
                return done(err);
              }
              expect(result).to.be.equals('OK');
              extrnRedis.hget('map:somename',
                'oneField',
                function (err, result) {
                  if (err) {
                    return done(err);
                  }
                  expect(result).equals('oneValue');
                  setTimeout(
                    function () {
                      extrnMySql.query(
                        'SELECT field, value ' +
                        'FROM map_somename ' +
                        'WHERE field = ? ' +
                        'ORDER BY field ASC',
                        'oneField',
                        function (err, result) {
                          if (err) {
                            return done(err);
                          }
                          expect(result[0].field).to.be.equals('oneField');
                          expect(result[0].value).to.be.equals('oneValue');
                          done();
                        });
                    }, 400);
                });
            });
        });

      afterEach(function (done) {
        _deleteData(['map:somename'], ['map_somename'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#hget()', function () {
      before(function (done) {
        extrnRedis.hset('map:somename', 'hello', 'world',
          function (err) {
            if (err) {
              done(err);
            } else {
              async.series(
                [
                  function (firstCb) {
                    extrnMySql.query(
                      'CREATE TABLE IF NOT EXISTS map_somename' +
                      '(' +
                      COLUMNS.FIELD + ' VARCHAR(255) PRIMARY KEY, ' +
                      COLUMNS.VALUE + ' VARCHAR(255), ' +
                      COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
                      COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
                      ') ',
                      function (err) {
                        if (err) {
                          firstCb(err);
                        } else {
                          firstCb();
                        }
                      });
                  },
                  function (secondCb) {
                    extrnMySql.query(
                      'INSERT INTO map_somename (field, value) VALUES (?, ?) ',
                      [
                        'hello',
                        'world'
                      ],
                      function (err) {
                        if (err) {
                          secondCb(err);
                        } else {
                          secondCb();
                        }
                      });
                  }
                ], function (err) {
                  if (err) {
                    done(err);
                  } else {
                    done();
                  }
                });
            }
          });
      });

      it('should get the value of the has from Redis and MySQL given the key',
        function (done) {
          instance.hget('somename', 'hello', function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals('world');
              extrnRedis.hdel('map:somename', 'hello', function (err) {
                if (err) {
                  done(err);
                } else {
                  instance.hget('somename', 'hello', function (err, result) {
                    if (err) {
                      done(err);
                    } else {
                      expect(result).to.be.equals('world');
                      done();
                    }
                  });
                }
              });
            }
          });
        });

      after(function (done) {
        _deleteData(['map:somename'], ['map_somename'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#hmget()', function () {
      beforeEach(function (done) {
        extrnRedis.hmset('map:somename',
          'hello', 'world',
          'this is', 'sparta',
          'goodness', 'gracious',
          'madame im', 'adam',
          function (err) {
            if (err) {
              done(err);
            } else {
              async.series(
                [
                  function (firstCb) {
                    extrnMySql.query(
                      'CREATE TABLE IF NOT EXISTS map_somename' +
                      '(' +
                      COLUMNS.FIELD + ' VARCHAR(255) PRIMARY KEY, ' +
                      COLUMNS.VALUE + ' VARCHAR(255), ' +
                      COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
                      COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
                      ') ',
                      function (err) {
                        if (err) {
                          firstCb(err);
                        } else {
                          firstCb();
                        }
                      });
                  },
                  function (secondCb) {
                    extrnMySql.query(
                      'INSERT INTO map_somename (field, value) ' +
                      'VALUES (?, ?) , (?, ?) , (?, ?) , (?, ?)',
                      [
                        'hello', 'world',
                        'this is', 'sparta',
                        'goodness', 'gracious',
                        'madame im', 'adam'
                      ],
                      function (err) {
                        if (err) {
                          secondCb(err);
                        } else {
                          secondCb();
                        }
                      });
                  }
                ], function (err) {
                  if (err) {
                    done(err);
                  } else {
                    done();
                  }
                });
            }
          });
      });

      it('should be able to retrieve multiple values of fields from Redis ' +
        'and MySQL', function (done) {
        instance.hmget('somename',
          [
            'madame im',
            'goodness',
            'hello',
            'this is'
          ],
          function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.deep.equals(
                [
                  'adam',
                  'gracious',
                  'world',
                  'sparta'
                ]
              );
              extrnRedis.hdel('map:somename',
                'madame im',
                'goodness',
                'hello',
                'this is',
                function (err) {
                  if (err) {
                    done(err);
                  } else {
                    instance.hmget('somename',
                      [
                        'this is',
                        'goodness',
                        'madame im',
                        'hello'
                      ],
                      function (err, result) {
                        if (err) {
                          done(err);
                        } else {
                          expect(result[0]).to.be.equals('sparta');
                          expect(result[1]).to.be.equals('gracious');
                          expect(result[2]).to.be.equals('adam');
                          expect(result[3]).to.be.equals('world');
                          done();
                        }
                      });
                  }
                });
            }
          });
      });

      it('should be able to return an array of null values for fields ' +
        'which do not exist', function (done) {
        instance.hmget('somename',
          [
            'bow',
            'samurai',
            'gelatto',
            'star'
          ],
          function (err, result) {
            if (err) {
              done(err);
            } else {
              for (var i = 0; i < result.length; i++) {
                expect(result[i]).to.be.equals(null);
              }
              done();
            }
          });
      });

      afterEach(function (done) {
        _deleteData(['map:somename'], ['map_somename'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#hgetall()', function () {
      beforeEach(function (done) {
        extrnRedis.hmset('map:somename',
          'hello', 'world',
          'this is', 'sparta',
          'goodness', 'gracious',
          'madame im', 'adam',
          function (err) {
            if (err) {
              done(err);
            } else {
              async.series(
                [
                  function (firstCb) {
                    extrnMySql.query(
                      'CREATE TABLE IF NOT EXISTS map_somename' +
                      '(' +
                      COLUMNS.FIELD + ' VARCHAR(255) PRIMARY KEY, ' +
                      COLUMNS.VALUE + ' VARCHAR(255), ' +
                      COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
                      COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
                      ') ',
                      function (err) {
                        if (err) {
                          firstCb(err);
                        } else {
                          firstCb();
                        }
                      });
                  },
                  function (secondCb) {
                    extrnMySql.query(
                      'INSERT INTO map_somename (field, value) ' +
                      'VALUES (?, ?) , (?, ?) , (?, ?) , (?, ?)',
                      [
                        'hello', 'world',
                        'this is', 'sparta',
                        'goodness', 'gracious',
                        'madame im', 'adam'
                      ],
                      function (err) {
                        if (err) {
                          secondCb(err);
                        } else {
                          secondCb();
                        }
                      });
                  }
                ], function (err) {
                  if (err) {
                    done(err);
                  } else {
                    done();
                  }
                });
            }
          });
      });

      it('should be able to get all the fields and values of a hash from ' +
        'Redis and MySQL',
        function (done) {
          instance.hgetall('somename', function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.deep.equals(
                {
                  goodness: 'gracious',
                  hello: 'world',
                  'madame im': 'adam',
                  'this is': 'sparta'
                });
              extrnRedis.del('map:somename', function (err) {
                if (err) {
                  done(err);
                } else {
                  instance.hgetall('somename', function (err, result) {
                    if (err) {
                      done(err);
                    } else {
                      expect(result).to.be.deep.equals(
                        {
                          goodness: 'gracious',
                          hello: 'world',
                          'madame im': 'adam',
                          'this is': 'sparta'
                        });
                      done();
                    }
                  });
                }
              });
            }
          });
        });

      it('should be able to get a null object if the fields are invalid',
        function (done) {
          instance.hgetall('sAneNAmE', function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.deep.equals({});
              done();
            }
          });
        });

      afterEach(function (done) {
        _deleteData(['map:somename'], ['map_somename'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#hexists()', function () {
      beforeEach(function (done) {
        extrnRedis.hmset('map:somename',
          'hello', 'world',
          'this is', 'sparta',
          'goodness', 'gracious',
          'madame im', 'adam',
          function (err) {
            if (err) {
              done(err);
            } else {
              async.series(
                [
                  function (firstCb) {
                    extrnMySql.query(
                      'CREATE TABLE IF NOT EXISTS map_somename' +
                      '(' +
                      COLUMNS.FIELD + ' VARCHAR(255) PRIMARY KEY, ' +
                      COLUMNS.VALUE + ' VARCHAR(255), ' +
                      COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
                      COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
                      ') ',
                      function (err) {
                        if (err) {
                          firstCb(err);
                        } else {
                          firstCb();
                        }
                      });
                  },
                  function (secondCb) {
                    extrnMySql.query(
                      'INSERT INTO map_somename (field, value) ' +
                      'VALUES (?, ?) , (?, ?) , (?, ?) , (?, ?)',
                      [
                        'hello', 'world',
                        'this is', 'sparta',
                        'goodness', 'gracious',
                        'madame im', 'adam'
                      ],
                      function (err) {
                        if (err) {
                          secondCb(err);
                        } else {
                          secondCb();
                        }
                      });
                  }
                ], function (err) {
                  if (err) {
                    done(err);
                  } else {
                    done();
                  }
                });
            }
          });
      });

      it('should be able to check the existence of a value of a field',
        function (done) {
          instance.hexists('somename', 'madame im', function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals(1);
              done();
            }
          });
        });

      it('should be able to check the non-existence of a value of a field',
        function (done) {
          instance.hexists('somename', 'blabla', function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals(0);
              done();
            }
          });
        });

      afterEach(function (done) {
        _deleteData(['map:somename'], ['map_somename'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#hdel()', function () {
      beforeEach(function (done) {
        extrnRedis.hmset('map:somename',
          'hello', 'world',
          'this is', 'sparta',
          'goodness', 'gracious',
          'madame im', 'adam',
          function (err) {
            if (err) {
              done(err);
            } else {
              async.series(
                [
                  function (firstCb) {
                    extrnMySql.query(
                      'CREATE TABLE IF NOT EXISTS map_somename' +
                      '(' +
                      COLUMNS.FIELD + ' VARCHAR(255) PRIMARY KEY, ' +
                      COLUMNS.VALUE + ' VARCHAR(255), ' +
                      COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
                      COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
                      ') ',
                      function (err) {
                        if (err) {
                          firstCb(err);
                        } else {
                          firstCb();
                        }
                      });
                  },
                  function (secondCb) {
                    extrnMySql.query(
                      'INSERT INTO map_somename (field, value) ' +
                      'VALUES (?, ?) , (?, ?) , (?, ?) , (?, ?)',
                      [
                        'hello', 'world',
                        'this is', 'sparta',
                        'goodness', 'gracious',
                        'madame im', 'adam'
                      ],
                      function (err) {
                        if (err) {
                          secondCb(err);
                        } else {
                          secondCb();
                        }
                      });
                  }
                ], function (err) {
                  if (err) {
                    done(err);
                  } else {
                    done();
                  }
                });
            }
          });
      });

      it('should be able to delete one or more hash field-value',
        function (done) {
          instance.hdel('somename', ['goodness', 'madame im', 'this is'],
            function (err, result) {
              if (err) {
                done(err);
              } else {
                expect(result).to.be.equals(3);
                extrnRedis.hkeys('map:somename', function (err, result) {
                  if (err) {
                    done(err);
                  } else {
                    expect(result).to.deep.equals(['hello']);
                    setTimeout(
                      function () {
                        extrnMySql.query(
                          'SELECT COUNT(*) cnt ' +
                          'FROM map_somename ' +
                          'WHERE field IN (?, ?, ?)',
                          [
                            'goodness',
                            'madame im',
                            'this is'
                          ],
                          function (err, result) {
                            if (err) {
                              done(err);
                            } else {
                              expect(result[0].cnt).to.be.equals(0);
                              instance.hdel('somename', ['hello'],
                                function (err, result) {
                                  if (err) {
                                    done(err);
                                  } else {
                                    expect(result).to.be.equals(1);
                                    extrnRedis.hkeys('map:somename',
                                      function (err, result) {
                                        if (err) {
                                          done(err);
                                        } else {
                                          expect(result).to.deep.equals([]);
                                          setTimeout(function () {
                                            extrnMySql.query(
                                              'SELECT COUNT(*) cnt ' +
                                              'FROM map_somename ' +
                                              'WHERE field IN (?)',
                                              [
                                                'hello'
                                              ],
                                              function (err, result) {
                                                if (err) {
                                                  done(err);
                                                } else {
                                                  expect(result[0].cnt).to.be
                                                    .equals(0);
                                                  done();
                                                }
                                              }, 400);
                                          });
                                        }
                                      });
                                  }
                                });
                            }
                          });
                      }, 400);
                  }
                });
              }
            });
        });

      afterEach(function (done) {
        _deleteData(['map:somename'], ['map_somename'], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });
    });

    describe('#rename()', function () {
      before(function (done) {
        var time;
        async.series([
          function (firstStringCb) {
            extrnRedis.set('str:another_type:another_old_key', 'kiersey',
              function (err) {
                if (err) {
                  firstStringCb(err);
                } else {
                  firstStringCb();
                }
              });
          },
          function (secondStringCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS `str_another_type` ' +
              '(`' +
              COLUMNS.KEY + '` VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.VALUE + ' VARCHAR(255), ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              function (err) {
                if (err) {
                  secondStringCb(err);
                } else {
                  secondStringCb();
                }
              });
          },
          function (thirdStringCb) {
            extrnMySql.query(
              'INSERT INTO str_another_type (`key`, value) VALUES (?, ?)',
              [
                'another_old_key',
                'kiersey'
              ],
              function (err) {
                if (err) {
                  thirdStringCb(err);
                } else {
                  thirdStringCb();
                }
              });
          },
          function (firstListCb) {
            extrnRedis.multi()
              .time()
              .lpush('lst:another_old_key',
              'valueOne',
              'valueTwo')
              .exec(function (err, result) {
                if (err) {
                  firstListCb(err);
                } else {
                  time = result[0][1][0] + result[0][1][1];
                  firstListCb();
                }
              });
          },
          function (secondListCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS lst_another_old_key ' +
              ' (' +
              COLUMNS.SEQ + ' BIGINT PRIMARY KEY, ' +
              COLUMNS.VALUE + ' VARCHAR(255), ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              function (err) {
                if (err) {
                  secondListCb(err);
                } else {
                  secondListCb();
                }
              });
          },
          function (thirdListCb) {
            extrnMySql.query(
              'INSERT INTO lst_another_old_key (time_sequence, value) ' +
              'VALUES (?, ?) , (?, ?) ',
              [
                time,
                'valueOne',
                time + 1,
                'valueTwo'
              ],
              function (err) {
                if (err) {
                  thirdListCb(err);
                } else {
                  thirdListCb();
                }
              });
          },
          function (firstSetCb) {
            extrnRedis.sadd('set:another_old_key',
              'memberOne',
              'memberTwo',
              'memberThree',
              function (err) {
                if (err) {
                  firstSetCb(err);
                } else {
                  firstSetCb();
                }
              });
          },
          function (secondSetCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS set_another_old_key ' +
              '(' +
              COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              function (err) {
                if (err) {
                  secondSetCb(err);
                } else {
                  secondSetCb();
                }
              });
          },
          function (thirdSetCb) {
            extrnMySql.query(
              'INSERT INTO set_another_old_key (member) ' +
              'VALUES (?), (?), (?) ',
              [
                'memberOne',
                'memberTwo',
                'memberThree'
              ],
              function (err) {
                if (err) {
                  thirdSetCb(err);
                } else {
                  thirdSetCb();
                }
              });
          },
          function (firstSortedSetCb) {
            extrnRedis.zadd('zset:another_old_key',
              90,
              'memberOne',
              80.34,
              'memberTwo',
              73.23,
              'memberThree',
              function (err) {
                if (err) {
                  firstSortedSetCb(err);
                } else {
                  firstSortedSetCb();
                }
              });
          },
          function (secondSortedSetCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS zset_another_old_key' +
              '(' +
              COLUMNS.SCORE + ' DOUBLE, ' +
              COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              function (err) {
                if (err) {
                  secondSortedSetCb(err);
                } else {
                  secondSortedSetCb();
                }
              });
          },
          function (thirdSortedSetCb) {
            extrnMySql.query(
              'INSERT INTO zset_another_old_key (score, member) ' +
              'VALUES (?, ?) , (?, ?), (?, ?) ',
              [
                90,
                'memberOne',
                80.34,
                'memberTwo',
                73.23,
                'memberThree'
              ],
              function (err) {
                if (err) {
                  thirdSortedSetCb(err);
                } else {
                  thirdSortedSetCb();
                }
              });
          },
          function (firstHashCb) {
            extrnRedis.hmset('map:another_old_key',
              'fieldOne', 'valueOne',
              'fieldTwo', 'valueTwo',
              'fieldThree', 'valueThree',
              function (err) {
                if (err) {
                  firstHashCb(err);
                } else {
                  firstHashCb();
                }
              });
          },
          function (secondHashCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS map_another_old_key ' +
              '(`' +
              COLUMNS.FIELD + '` VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.VALUE + ' VARCHAR(255), ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              function (err) {
                if (err) {
                  secondHashCb(err);
                } else {
                  secondHashCb();
                }
              });
          },
          function (thirdHashCb) {
            extrnMySql.query(
              'INSERT INTO map_another_old_key (field, value) ' +
              'VALUES (?, ?) , (?, ?), (?, ?) ',
              [
                'fieldOne', 'valueOne',
                'fieldTwo', 'valueTwo',
                'fieldThree', 'valueThree'
              ],
              function (err) {
                if (err) {
                  thirdHashCb(err);
                } else {
                  thirdHashCb();
                }
              });
          }
        ], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });

      it('should be able to rename the key and MySQL table of Redis type ' +
        'string', function (done) {
        instance.rename('str:another_type:another_old_key',
          'str:another_type:another_new_key',
          function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals('OK');
              extrnRedis.get('str:another_type:another_new_key',
                function (err, result) {
                  if (err) {
                    done(err);
                  } else {
                    expect(result).to.be.equals('kiersey');
                    extrnMySql.query(
                      'SELECT value, `key` ' +
                      'FROM str_another_type ' +
                      'WHERE `key` IN (?, ?) ',
                      [
                        'another_new_key',
                        'another_old_key'
                      ],
                      function (err, result) {
                        if (err) {
                          done(err);
                        } else {
                          expect(result.length).to.be.equals(1);
                          expect(result[0].key).to.be.equals('another_new_key');
                          expect(result[0].value).to.be.equals('kiersey');
                          done();
                        }
                      });
                  }
                });
            }
          });
      });

      it('should be able to rename the key and MySQL table of Redis type ' +
        'list', function (done) {
        instance.rename('lst:another_old_key',
          'lst:another_new_key',
          function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals('OK');
              extrnRedis.lindex('lst:another_new_key',
                0,
                function (err, result) {
                  if (err) {
                    done(err);
                  } else {
                    expect(result).to.be.equals('valueTwo');
                    extrnMySql.query(
                      'SELECT value ' +
                      'FROM lst_another_new_key ' +
                      'WHERE value IN (?, ?) ' +
                      'ORDER BY time_sequence ASC',
                      [
                        'valueOne',
                        'valueTwo'
                      ],
                      function (err, result) {
                        if (err) {
                          done(err);
                        } else {
                          expect(result[0].value).to.be.equals('valueOne');
                          expect(result[1].value).to.be.equals('valueTwo');
                          done();
                        }
                      });
                  }
                });
            }
          });
      });

      it('should be able to rename the key and MySQL table of Redis type ' +
        'set', function (done) {
        instance.rename('set:another_old_key',
          'set:another_new_key',
          function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals('OK');
              extrnRedis.smembers('set:another_new_key',
                function (err, result) {
                  if (err) {
                    done(err);
                  } else {
                    expect(result).to.include('memberOne');
                    expect(result).to.include('memberTwo');
                    expect(result).to.include('memberThree');
                    extrnMySql.query(
                      'SELECT member ' +
                      'FROM set_another_new_key ' +
                      'WHERE member IN (?, ?, ?) ' +
                      'ORDER BY member ASC',
                      [
                        'memberOne',
                        'memberTwo',
                        'memberThree'
                      ],
                      function (err, result) {
                        if (err) {
                          done(err);
                        } else {
                          expect(result[0].member).to.be.equals('memberOne');
                          expect(result[1].member).to.be.equals('memberThree');
                          expect(result[2].member).to.be.equals('memberTwo');
                          done();
                        }
                      });
                  }
                });
            }
          });
      });

      it('should be able to rename the key and MySQL table of Redis type ' +
        'sorted set', function (done) {
        instance.rename('zset:another_old_key',
          'zset:another_new_key',
          function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals('OK');
              extrnRedis.zrangebyscore('zset:another_new_key',
                70,
                91,
                function (err, result) {
                  if (err) {
                    done(err);
                  } else {
                    expect(result).to.be.deep.equals(
                      [
                        'memberThree',
                        'memberTwo',
                        'memberOne'
                      ]);
                    extrnMySql.query(
                      'SELECT member ' +
                      'FROM zset_another_new_key ' +
                      'WHERE member IN (?, ?, ?) ' +
                      'ORDER BY score ASC',
                      [
                        'memberOne',
                        'memberTwo',
                        'memberThree'
                      ],
                      function (err, result) {
                        if (err) {
                          done(err);
                        } else {
                          expect(result[0].member).to.be.equals('memberThree');
                          expect(result[1].member).to.be.equals('memberTwo');
                          expect(result[2].member).to.be.equals('memberOne');
                          done();
                        }
                      });
                  }
                });
            }
          });
      });

      it('should be able to rename the key and MySQL table of Redis type ' +
        'hash', function (done) {
        instance.rename('map:another_old_key',
          'map:another_new_key',
          function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals('OK');
              extrnRedis.hgetall('map:another_new_key',
                function (err, result) {
                  if (err) {
                    done(err);
                  } else {
                    expect(result).to.be.deep.equals(
                      {
                        fieldOne: 'valueOne',
                        fieldThree: 'valueThree',
                        fieldTwo: 'valueTwo'
                      }
                    );
                    extrnMySql.query(
                      'SELECT field, value ' +
                      'FROM map_another_new_key ' +
                      'WHERE field IN (?, ?, ?) ' +
                      'ORDER BY field ASC',
                      [
                        'fieldOne',
                        'fieldTwo',
                        'fieldThree'
                      ],
                      function (err, result) {
                        if (err) {
                          done(err);
                        } else {
                          expect(result[0].field).to.be.equals('fieldOne');
                          expect(result[0].value).to.be.equals('valueOne');
                          expect(result[1].field).to.be.equals('fieldThree');
                          expect(result[1].value).to.be.equals('valueThree');
                          expect(result[2].field).to.be.equals('fieldTwo');
                          expect(result[2].value).to.be.equals('valueTwo');
                          done();
                        }
                      });
                  }
                });
            }
          });
      });

      after(function (done) {
        _deleteData(
          [
            'str:another_type:another_old_key',
            'str:another_type:another_new_key',
            'lst:another_old_key',
            'lst:another_new_key',
            'set:another_old_key',
            'set:another_new_key',
            'zset:another_old_key',
            'zset:another_new_key',
            'map:another_old_key',
            'map:another_new_key'
          ],
          [
            'str_another_type',
            'lst_another_old_key',
            'lst_another_new_key',
            'set_another_old_key',
            'set_another_new_key',
            'zset_another_old_key',
            'zset_another_new_key',
            'map_another_old_key',
            'map_another_new_key'
          ], function (err) {
            if (err) {
              done(err);
            } else {
              done();
            }
          });
      });
    });

    describe('#expire()', function () {
      before(function (done) {
        var time;
        async.series([
          function (firstStringCb) {
            extrnRedis.set('str:another_type:another_old_key', 'kiersey',
              function (err) {
                if (err) {
                  firstStringCb(err);
                } else {
                  firstStringCb();
                }
              });
          },
          function (secondStringCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS `str_another_type` ' +
              '(`' +
              COLUMNS.KEY + '` VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.VALUE + ' VARCHAR(255), ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              function (err) {
                if (err) {
                  secondStringCb(err);
                } else {
                  secondStringCb();
                }
              });
          },
          function (thirdStringCb) {
            extrnMySql.query(
              'INSERT INTO str_another_type (`key`, value) VALUES (?, ?)',
              [
                'another_old_key',
                'kiersey'
              ],
              function (err) {
                if (err) {
                  thirdStringCb(err);
                } else {
                  thirdStringCb();
                }
              });
          },
          function (firstListCb) {
            extrnRedis.multi()
              .time()
              .lpush('lst:another_old_key',
              'valueOne',
              'valueTwo')
              .exec(function (err, result) {
                if (err) {
                  firstListCb(err);
                } else {
                  time = result[0][1][0] + result[0][1][1];
                  firstListCb();
                }
              });
          },
          function (secondListCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS lst_another_old_key ' +
              ' (' +
              COLUMNS.SEQ + ' BIGINT PRIMARY KEY, ' +
              COLUMNS.VALUE + ' VARCHAR(255), ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              function (err) {
                if (err) {
                  secondListCb(err);
                } else {
                  secondListCb();
                }
              });
          },
          function (thirdListCb) {
            extrnMySql.query(
              'INSERT INTO lst_another_old_key (time_sequence, value) ' +
              'VALUES (?, ?) , (?, ?) ',
              [
                time,
                'valueOne',
                time + 1,
                'valueTwo'
              ],
              function (err) {
                if (err) {
                  thirdListCb(err);
                } else {
                  thirdListCb();
                }
              });
          },
          function (firstSetCb) {
            extrnRedis.sadd('set:another_old_key',
              'memberOne',
              'memberTwo',
              'memberThree',
              function (err) {
                if (err) {
                  firstSetCb(err);
                } else {
                  firstSetCb();
                }
              });
          },
          function (secondSetCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS set_another_old_key ' +
              '(' +
              COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              function (err) {
                if (err) {
                  secondSetCb(err);
                } else {
                  secondSetCb();
                }
              });
          },
          function (thirdSetCb) {
            extrnMySql.query(
              'INSERT INTO set_another_old_key (member) ' +
              'VALUES (?), (?), (?) ',
              [
                'memberOne',
                'memberTwo',
                'memberThree'
              ],
              function (err) {
                if (err) {
                  thirdSetCb(err);
                } else {
                  thirdSetCb();
                }
              });
          },
          function (firstSortedSetCb) {
            extrnRedis.zadd('zset:another_old_key',
              90,
              'memberOne',
              80.34,
              'memberTwo',
              73.23,
              'memberThree',
              function (err) {
                if (err) {
                  firstSortedSetCb(err);
                } else {
                  firstSortedSetCb();
                }
              });
          },
          function (secondSortedSetCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS zset_another_old_key' +
              '(' +
              COLUMNS.SCORE + ' DOUBLE, ' +
              COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              function (err) {
                if (err) {
                  secondSortedSetCb(err);
                } else {
                  secondSortedSetCb();
                }
              });
          },
          function (thirdSortedSetCb) {
            extrnMySql.query(
              'INSERT INTO zset_another_old_key (score, member) ' +
              'VALUES (?, ?) , (?, ?), (?, ?) ',
              [
                90,
                'memberOne',
                80.34,
                'memberTwo',
                73.23,
                'memberThree'
              ],
              function (err) {
                if (err) {
                  thirdSortedSetCb(err);
                } else {
                  thirdSortedSetCb();
                }
              });
          },
          function (firstHashCb) {
            extrnRedis.hmset('map:another_old_key',
              'fieldOne', 'valueOne',
              'fieldTwo', 'valueTwo',
              'fieldThree', 'valueThree',
              function (err) {
                if (err) {
                  firstHashCb(err);
                } else {
                  firstHashCb();
                }
              });
          },
          function (secondHashCb) {
            extrnMySql.query(
              'CREATE TABLE IF NOT EXISTS map_another_old_key ' +
              '(`' +
              COLUMNS.FIELD + '` VARCHAR(255) PRIMARY KEY, ' +
              COLUMNS.VALUE + ' VARCHAR(255), ' +
              COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',
              function (err) {
                if (err) {
                  secondHashCb(err);
                } else {
                  secondHashCb();
                }
              });
          },
          function (thirdHashCb) {
            extrnMySql.query(
              'INSERT INTO map_another_old_key (field, value) ' +
              'VALUES (?, ?) , (?, ?), (?, ?) ',
              [
                'fieldOne', 'valueOne',
                'fieldTwo', 'valueTwo',
                'fieldThree', 'valueThree'
              ],
              function (err) {
                if (err) {
                  thirdHashCb(err);
                } else {
                  thirdHashCb();
                }
              });
          }
        ], function (err) {
          if (err) {
            done(err);
          } else {
            done();
          }
        });
      });

      it('should be able to expire a Redis type set', function (done) {
        var expiryDuration = 120; // in seconds
        instance.expire('str:another_type:another_old_key', expiryDuration,
          function (err, result) {
            var nowDateTime = new Date();
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals(1);
              extrnRedis.ttl('str:another_type:another_old_key',
                function (err, result) {
                  if (err) {
                    done(err);
                  } else {
                    expect(result).to.be.gte(58).and.lte(expiryDuration);
                    setTimeout(
                      function () {
                        extrnMySql.query(
                          'SELECT `key`, expiry_date ' +
                          'FROM expiry ' +
                          'WHERE `key`= ? ',
                          'str:another_type:another_old_key',
                          function (err, result) {
                            if (err) {
                              done(err);
                            } else {
                              expect(result[0].key).to.be
                                .equals('str:another_type:another_old_key');
                              expect(result[0].expiry_date).to.satisfy(
                                function (expireDt) {
                                  console.log('expiry_date: ' + expireDt +
                                    ' >= now(): ' + nowDateTime);
                                  return (expireDt - nowDateTime) < 121000; // 120 sec ~~ 120000 millisec + tolerance of 1000millisec
                                });
                              done();
                            }
                          });
                      }, 400);
                  }
                });
            }
          });
      });

      it('should be able to expire a Redis type list', function (done) {
        var expiryDuration = 120; // in seconds
        instance.expire('lst:another_old_key', expiryDuration,
          function (err, result) {
            var nowDateTime = new Date();
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals(1);
              extrnRedis.ttl('lst:another_old_key',
                function (err, result) {
                  if (err) {
                    done(err);
                  } else {
                    expect(result).to.be.gte(58).and.lte(expiryDuration);
                    setTimeout(
                      function () {
                        extrnMySql.query(
                          'SELECT `key`, expiry_date ' +
                          'FROM expiry ' +
                          'WHERE `key`= ? ',
                          'lst:another_old_key',
                          function (err, result) {
                            if (err) {
                              done(err);
                            } else {
                              expect(result[0].key).to.be
                                .equals('lst:another_old_key');
                              expect(result[0].expiry_date).to.satisfy(
                                function (expireDt) {
                                  console.log('expiry_date: ' + expireDt +
                                    ' >= now(): ' + nowDateTime);
                                  return (expireDt - nowDateTime) < 121000; // 120 sec ~~ 120000 millisec + tolerance of 1000millisec
                                });
                              done();
                            }
                          });
                      }, 400);
                  }
                });
            }
          });
      });

      it('should be able to expire a Redis type set', function (done) {
        var expiryDuration = 120; // in seconds
        instance.expire('set:another_old_key', expiryDuration,
          function (err, result) {
            var nowDateTime = new Date();
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals(1);
              extrnRedis.ttl('set:another_old_key',
                function (err, result) {
                  if (err) {
                    done(err);
                  } else {
                    expect(result).to.be.gte(58).and.lte(expiryDuration);
                    setTimeout(
                      function () {
                        extrnMySql.query(
                          'SELECT `key`, expiry_date ' +
                          'FROM expiry ' +
                          'WHERE `key`= ? ',
                          'set:another_old_key',
                          function (err, result) {
                            if (err) {
                              done(err);
                            } else {
                              expect(result[0].key).to.be
                                .equals('set:another_old_key');
                              expect(result[0].expiry_date).to.satisfy(
                                function (expireDt) {
                                  console.log('expiry_date: ' + expireDt +
                                    ' >= now(): ' + nowDateTime);
                                  return (expireDt - nowDateTime) < 121000; // 120 sec ~~ 120000 millisec + tolerance of 1000millisec
                                });
                              done();
                            }
                          });
                      }, 400);
                  }
                });
            }
          });
      });

      it('should be able to expire a Redis type sorted set',
        function (done) {
          var expiryDuration = 120; // in seconds
          instance.expire('zset:another_old_key', expiryDuration,
            function (err, result) {
              var nowDateTime = new Date();
              if (err) {
                done(err);
              } else {
                expect(result).to.be.equals(1);
                extrnRedis.ttl('zset:another_old_key',
                  function (err, result) {
                    if (err) {
                      done(err);
                    } else {
                      expect(result).to.be.gte(58).and.lte(expiryDuration);
                      setTimeout(
                        function () {
                          extrnMySql.query(
                            'SELECT `key`, expiry_date ' +
                            'FROM expiry ' +
                            'WHERE `key`= ? ',
                            'zset:another_old_key',
                            function (err, result) {
                              if (err) {
                                done(err);
                              } else {
                                expect(result[0].key).to.be
                                  .equals('zset:another_old_key');
                                expect(result[0].expiry_date).to.satisfy(
                                  function (expireDt) {
                                    console.log('expiry_date: ' + expireDt +
                                      ' >= now(): ' + nowDateTime);
                                    return (expireDt - nowDateTime) < 121000; // 120 sec ~~ 120000 millisec + tolerance of 1000millisec
                                  });
                                done();
                              }
                            });
                        }, 400);
                    }
                  });
              }
            });
        });

      it('should be able to expire a Redis type hash',
        function (done) {
          var expiryDuration = 120; // in seconds ~ 2 minutes
          instance.expire('map:another_old_key', expiryDuration,
            function (err, result) {
              var nowDateTime = new Date();
              if (err) {
                done(err);
              } else {
                expect(result).to.be.equals(1);
                extrnRedis.ttl('map:another_old_key',
                  function (err, result) {
                    if (err) {
                      done(err);
                    } else {
                      expect(result).to.be.gte(58).and.lte(expiryDuration);
                      setTimeout(
                        function () {
                          extrnMySql.query(
                            'SELECT `key`, expiry_date ' +
                            'FROM expiry ' +
                            'WHERE `key`= ? ',
                            'map:another_old_key',
                            function (err, result) {
                              if (err) {
                                done(err);
                              } else {
                                expect(result[0].key).to.be
                                  .equals('map:another_old_key');
                                expect(result[0].expiry_date).to.satisfy(
                                  function (expireDt) {
                                    console.log('expiry_date: ' + expireDt +
                                      ' >= now(): ' + nowDateTime);
                                    return (expireDt - nowDateTime) < 121000; // 120 sec ~~ 120000 millisec + tolerance of 1000millisec
                                  });
                                done();
                              }
                            });
                        }, 400);
                    }
                  });
              }
            });
        });

      after(function (done) {
        _deleteData(
          [
            'str:another_type:another_old_key',
            'lst:another_old_key',
            'set:another_old_key',
            'zset:another_old_key',
            'map:another_old_key'
          ],
          [
            'str_another_type',
            'lst_another_old_key',
            'set_another_old_key',
            'zset_another_old_key',
            'map_another_old_key'
          ], function (err) {
            if (err) {
              done(err);
            } else {
              done();
            }
          });
      });
    });

    after(function (done) {
      _deleteData([testKeys], null, function (err) {
        if (err) {
          done(err);
        } else {
          if (instance) {
            instance.quit();
          }
          done();
        }
      });
    });
  });
  /* End Method Test*/

  /* Concurrency Test */
  describe('concurrency test', function () {

    context('iterative testing for concurrency', function () {

      var count = 0;

      async.whilst(
        function () {
          return count < 10;
        },
        function (callback) {
          count++;
          iterativeTest();
          callback(null);
        },
        function () {
        });

      function iterativeTest() {

        it('should be able to get the last correctly set Redis type string ' +
          'values', function (done) {

          var arrayInputs = [], separateInstance, instance, instances = [],
            values, i;

          separateInstance = new Redis2MySql({
            redis: {
              showFriendlyErrorStack: true
            },
            mysql: {
              user: 'root',
              database: connection.mysql.database,
              charset: connection.mysql.charset
            },
            custom: {
              datatypePrefix: {
                string: 'str',
                list: 'lst',
                set: 'set',
                sortedSet: 'zset',
                hash: 'map'
              }
            }
          });

          values =
            [
              'alpha', // 1
              'bravo', // 2
              'charlie', // 3
              'delta', // 4
              'echo', // 5
              'foxtrot', // 6
              'golf', // 7
              'hotel', // 8
              'india', // 9
              'juliet', // 10
              'kilo', // 11
              'lima', // 12
              'mike', // 13
              'november', // 14
              'oscar', // 15
              'papa', // 16
              'quebec', // 17
              'romeo', // 18
              'sierra', // 19
              'tango', // 20
              'uniform', // 21
              'victor', // 22
              'whiskey', // 23
              'xray', // 24
              'yankee', // 25
              'zulu', // 26
              'twenty-seven', // 27
              'twenty-eight', // 28
              'twenty-nine', // 29
              'thirty', // 30
              'thirty-one' // 31
            ];

          for (i = 0; i < values.length; i++) {
            instance = new Redis2MySql({
              redis: {
                showFriendlyErrorStack: true
              },
              mysql: {
                user: 'root',
                database: connection.mysql.database,
                charset: connection.mysql.charset
              },
              custom: {
                datatypePrefix: {
                  string: 'str',
                  list: 'lst',
                  set: 'set',
                  sortedSet: 'zset',
                  hash: 'map'
                }
              }
            });

            arrayInputs.push({value: values[i], instance: instance});
            instances.push(instance);
          }

          async.map(
            arrayInputs,
            function (item, callback) {
              item.instance.on('error', function (err) {
                console.log('Error from listener: ' + err.error + ' ' + err.message +
                  ' ' + err.redisKey);
              });
              item.instance.set('sometype', 'testkey', item.value,
                function (err, result) {
                  if (err) {
                    return callback(err);
                  }
                  callback(null, result);
                });
            },
            function (err) {
              if (err) {
                return done(err);
              }

              async.waterfall(
                [
                  function (firstCb) {
                    separateInstance.get('sometype', 'testkey', function (err, result) {
                      if (err) {
                        return firstCb(err);
                      }
                      firstCb(null, result);
                    });
                  },
                  function (redisResult, secondCb) {
                    setTimeout(function () {
                      extrnMySql.query(
                        'SELECT `key`, value FROM str_sometype ' +
                        'WHERE `key` = ?',
                        'testkey',
                        function (err, result) {
                          if (err) {
                            return secondCb(err);
                          }
                          console.log('actual: ' + result[0].value + ', reference: ' + redisResult);
                          expect(result[0].value).to.be.equals(redisResult);
                          secondCb();
                        });
                    }, 600);
                  }
                ], function (err) {
                  for (i = 0; i < instances.length; i++) {
                    if (instances[i]) {
                      instances[i].quit();
                    }
                  }
                  if (err) {
                    return done(err);
                  }
                  separateInstance.quit();
                  done();
                });
            });
        });

        it('should be able to get the last correctly set Redis type list ' +
          'values', function (done) {

          var arrayInputs = [], separateInstance, instance, instances = [],
            values, i;

          separateInstance = new Redis2MySql({
            redis: {
              showFriendlyErrorStack: true
            },
            mysql: {
              user: 'root',
              database: connection.mysql.database,
              charset: connection.mysql.charset
            },
            custom: {
              datatypePrefix: {
                string: 'str',
                list: 'lst',
                set: 'set',
                sortedSet: 'zset',
                hash: 'map'
              }
            }
          });

          values =
            [
              'alpha', // 1
              'bravo', // 2
              'charlie', // 3
              'delta', // 4
              'echo', // 5
              'foxtrot', // 6
              'golf', // 7
              'hotel', // 8
              'india', // 9
              'juliet', // 10
              'kilo', // 11
              'lima', // 12
              'mike', // 13
              'november', // 14
              'oscar', // 15
              'papa', // 16
              'quebec', // 17
              'romeo', // 18
              'sierra', // 19
              'tango', // 20
              'uniform', // 21
              'victor', // 22
              'whiskey', // 23
              'xray', // 24
              'yankee', // 25
              'zulu', // 26
              'twenty-seven', // 27
              'twenty-eight', // 28
              'twenty-nine', // 29
              'thirty', // 30
              'thirty-one' // 31
            ];

          for (i = 0; i < values.length; i++) {
            instance = new Redis2MySql({
              redis: {
                showFriendlyErrorStack: true
              },
              mysql: {
                user: 'root',
                database: connection.mysql.database,
                charset: connection.mysql.charset
              },
              custom: {
                datatypePrefix: {
                  string: 'str',
                  list: 'lst',
                  set: 'set',
                  sortedSet: 'zset',
                  hash: 'map'
                }
              }
            });

            arrayInputs.push({value: values[i], instance: instance});
            instances.push(instance);
          }

          async.map(
            arrayInputs,
            function (item, callback) {
              item.instance.on('error', function (err) {
                console.log('Error from listener: ' + err.error + ' ' + err.message +
                  ' ' + err.redisKey);
              });
              item.instance.lpush('some_data', item.value, function (err, result) {
                if (err) {
                  return callback(err);
                }
                callback(null, result);
              });
            },
            function (err) {
              if (err) {
                return done(err);
              }

              async.waterfall(
                [
                  function (firstCb) {
                    separateInstance
                      .lindex('some_data', 0, function (err, result) {
                        if (err) {
                          return firstCb(err);
                        }
                        firstCb(null, result);
                      });
                  },
                  function (redisResult, secondCb) {
                    setTimeout(function () {
                      extrnMySql.query(
                        'SELECT `time_sequence`, value ' +
                        'FROM lst_some_data ' +
                        'ORDER BY time_sequence DESC LIMIT 1',
                        function (err, result) {
                          if (err) {
                            return secondCb(err);
                          }
                          console.log('actual: ' + result[0].value + ', reference: ' + redisResult);
                          expect(result[0].value).to.be.equals(redisResult);
                          secondCb();
                        });
                    }, 600);
                  }
                ], function (err) {
                  for (i = 0; i < instances.length; i++) {
                    if (instances[i]) {
                      instances[i].quit();
                    }
                  }
                  if (err) {
                    return done(err);
                  }
                  separateInstance.quit();
                  done();
                });
            });
        });

        it('should be able to get increment properly a Redis key',
          function (done) {

            var arrayInputs = [], separateInstance, instance, instances = [], i;

            separateInstance = new Redis2MySql({
              redis: {
                showFriendlyErrorStack: true
              },
              mysql: {
                user: 'root',
                database: connection.mysql.database,
                charset: connection.mysql.charset
              },
              custom: {
                datatypePrefix: {
                  string: 'str',
                  list: 'lst',
                  set: 'set',
                  sortedSet: 'zset',
                  hash: 'map'
                }
              }
            });

            for (i = 0; i < 31; i++) {
              instance = new Redis2MySql({
                redis: {
                  showFriendlyErrorStack: true
                },
                mysql: {
                  user: 'root',
                  database: connection.mysql.database,
                  charset: connection.mysql.charset
                },
                custom: {
                  datatypePrefix: {
                    string: 'str',
                    list: 'lst',
                    set: 'set',
                    sortedSet: 'zset',
                    hash: 'map'
                  }
                }
              });

              arrayInputs.push({value: 'testcounter', instance: instance});
              instances.push(instance);
            }

            async.map(
              arrayInputs,
              function (item, callback) {
                item.instance.incr('somenumtype', item.value, function (err, result) {
                  if (err) {
                    return callback(err);
                  }
                  callback(null, result);
                });
              },
              function (err) {
                if (err) {
                  return done(err);
                }

                async.waterfall(
                  [
                    function (firstCb) {
                      separateInstance
                        .get('somenumtype', 'testcounter', function (err, result) {
                          if (err) {
                            return firstCb(err);
                          }
                          firstCb(null, result);
                        });
                    },
                    function (redisResult, secondCb) {
                      setTimeout(function () {
                        extrnMySql.query(
                          'SELECT `key`, value FROM str_somenumtype ' +
                          'WHERE `key` = ?',
                          'testcounter',
                          function (err, result) {
                            if (err) {
                              return secondCb(err);
                            }
                            console.log('actual: ' + result[0].value + ', reference: ' + redisResult);
                            expect(result[0].value).to.be.equals(redisResult);
                            secondCb();
                          });
                      }, 600);
                    }
                  ], function (err) {
                    for (i = 0; i < instances.length; i++) {
                      if (instances[i]) {
                        instances[i].quit();
                      }
                    }
                    if (err) {
                      return done(err);
                    }
                    separateInstance.quit();
                    done();
                  });
              });
          });

        it('should be able to get all the added Redis type set values',
          function (done) {

            var arrayInputs = [], separateInstance, instance, instances = [],
              values, i;

            separateInstance = new Redis2MySql({
              redis: {
                showFriendlyErrorStack: true
              },
              mysql: {
                user: 'root',
                database: connection.mysql.database,
                charset: connection.mysql.charset
              },
              custom: {
                datatypePrefix: {
                  string: 'str',
                  list: 'lst',
                  set: 'set',
                  sortedSet: 'zset',
                  hash: 'map'
                }
              }
            });

            values =
              [
                'alpha_1', // 1
                'bravo_2', // 2
                'charlie_3', // 3
                'delta_4', // 4
                'echo_5', // 5
                'foxtrot_6', // 6
                'golf_7', // 7
                'hotel_8', // 8
                'india_9', // 9
                'juliet_10', // 10
                'kilo_11', // 11
                'lima_12', // 12
                'mike_13', // 13
                'november_14', // 14
                'oscar_15', // 15
                'papa_16', // 16
                'quebec_17', // 17
                'romeo_18', // 18
                'sierra_19', // 19
                'tango_20', // 20
                'uniform_21', // 21
                'victor_22', // 22
                'whiskey_23', // 23
                'xray_24', // 24
                'yankee_25', // 25
                'zulu_26', // 26
                'twenty-seven_27', // 27
                'twenty-eight_28', // 28
                'twenty-nine_29', // 29
                'thirty_30', // 30
                'thirty-one_31' // 31
              ];

            for (i = 0; i < values.length; i++) {
              instance = new Redis2MySql({
                redis: {
                  showFriendlyErrorStack: true
                },
                mysql: {
                  user: 'root',
                  database: connection.mysql.database,
                  charset: connection.mysql.charset
                },
                custom: {
                  datatypePrefix: {
                    string: 'str',
                    list: 'lst',
                    set: 'set',
                    sortedSet: 'zset',
                    hash: 'map'
                  }
                }
              });

              arrayInputs.push({value: values[i], instance: instance});
              instances.push(instance);
            }

            async.map(
              arrayInputs,
              function (item, callback) {
                var members = item.value.split('_'); // just to add two members
                item.instance.sadd('some_data', members, function (err, result) {
                  if (err) {
                    return callback(err);
                  }
                  callback(null, result);
                });
              },
              function (err) {
                if (err) {
                  return done(err);
                }

                async.waterfall(
                  [
                    function (firstCb) {
                      separateInstance
                        .smembers('some_data', function (err, result) {
                          if (err) {
                            return firstCb(err);
                          }
                          firstCb(null, result);
                        });
                    },
                    function (redisResult, secondCb) {
                      setTimeout(function () {
                        extrnMySql.query(
                          'SELECT member ' +
                          'FROM set_some_data ',
                          function (err, result) {
                            if (err) {
                              return secondCb(err);
                            }
                            var i, members = [];
                            for (i = 0; i < result.length; i++) {
                              if (is.existy(result[i].member)) {
                                members.push(result[i].member);
                              }
                            }
                            console.log('actual: ' + members + ', reference: ' + redisResult);
                            expect(members).to.have.members(redisResult);
                            secondCb();
                          });
                      }, 600);
                    }
                  ], function (err) {
                    for (i = 0; i < instances.length; i++) {
                      if (instances[i]) {
                        instances[i].quit();
                      }
                    }
                    if (err) {
                      return done(err);
                    }
                    separateInstance.quit();
                    done();
                  });
              });
          });

        it('should be able to get all the added Redis type sorted set values ' +
          'in order of score', function (done) {

          var arrayInputs = [], separateInstance, instance, instances = [],
            values, i;

          separateInstance = new Redis2MySql({
            redis: {
              showFriendlyErrorStack: true
            },
            mysql: {
              user: 'root',
              database: connection.mysql.database,
              charset: connection.mysql.charset
            },
            custom: {
              datatypePrefix: {
                string: 'str',
                list: 'lst',
                set: 'set',
                sortedSet: 'zset',
                hash: 'map'
              }
            }
          });

          values =
            [
              [1, 'alpha'], // 1
              [2, 'bravo'], // 2
              [3, 'charlie'], // 3
              [4, 'delta'], // 4
              [5, 'echo'], // 5
              [6, 'foxtrot'], // 6
              [7, 'golf'], // 7
              [8, 'hotel'], // 8
              [9, 'india'], // 9
              [10, 'juliet'], // 10
              [11, 'kilo'], // 11
              [12, 'lima'], // 12
              [13, 'mike'], // 13
              [14, 'november'], // 14
              [15, 'oscar'], // 15
              [16, 'papa'], // 16
              [17, 'quebec'], // 17
              [18, 'romeo'], // 18
              [19, 'sierra'], // 19
              [20, 'tango'], // 20
              [21, 'uniform'], // 21
              [22, 'victor'], // 22
              [23, 'whiskey'], // 23
              [24, 'xray'], // 24
              [25, 'yankee'], // 25
              [26, 'zulu'], // 26
              [27, 'twenty-seven'], // 27
              [28, 'twenty-eight'], // 28
              [29, 'twenty-nine'], // 29
              [30, 'thirty'], // 30
              [31, 'thirty-one'] // 31
            ];

          for (i = 0; i < values.length; i++) {
            instance = new Redis2MySql({
              redis: {
                showFriendlyErrorStack: true
              },
              mysql: {
                user: 'root',
                database: connection.mysql.database,
                charset: connection.mysql.charset
              },
              custom: {
                datatypePrefix: {
                  string: 'str',
                  list: 'lst',
                  set: 'set',
                  sortedSet: 'zset',
                  hash: 'map'
                }
              }
            });

            arrayInputs.push({value: values[i], instance: instance});
            instances.push(instance);
          }

          async.map(
            arrayInputs,
            function (item, callback) {
              item.instance.zadd('ssgrade', item.value, function (err, result) {
                if (err) {
                  return callback(err);
                }
                callback(null, result);
              });
            },
            function (err) {
              if (err) {
                return done(err);
              }

              async.waterfall(
                [
                  function (firstCb) {
                    separateInstance
                      .zrangebyscore('ssgrade', '0', '63', 'withscores', null,
                      null, null, function (err, result) {
                        if (err) {
                          return firstCb(err);
                        }
                        firstCb(null, result);
                      });
                  },
                  function (redisResult, secondCb) {
                    setTimeout(function () {
                      extrnMySql.query(
                        'SELECT member, score ' +
                        'FROM zset_ssgrade ' +
                        'ORDER BY score ASC ',
                        function (err, result) {
                          if (err) {
                            return secondCb(err);
                          }
                          var i, sqlMembers = [], sqlScores = [],
                            redisMembers = [], redisScores = [],
                            sqlMemberScores = [], redisMemberScores = [];
                          for (i = 0; i < result.length; i++) {
                            if (is.existy(result[i].member)) {
                              sqlMembers.push(result[i].member);
                              sqlScores.push(result[i].score);
                              sqlMemberScores.push(result[i].member);
                              sqlMemberScores.push(result[i].score);
                            }
                          }
                          for (i = 0; i < redisResult.length; i++) {
                            if (is.even(i)) {
                              redisMembers.push(redisResult[i]);
                            }
                            if (is.odd(i)) {
                              redisScores.push(parseFloat(redisResult[i]));
                            }
                            redisMemberScores.push(redisResult[i]);
                          }

                          console.log('expected: ' + sqlMemberScores + '\n actual: ' + redisMemberScores + '\n');
                          expect(sqlMembers).to.deep.equals(redisMembers);
                          for (i = 0; i < sqlScores.length; i++) {
                            expect(sqlScores[i]).to.be.closeTo(redisScores[i], 0.01);
                          }
                          secondCb();
                        });
                    }, 600);
                  }
                ], function (err) {
                  for (i = 0; i < instances.length; i++) {
                    if (instances[i]) {
                      instances[i].quit();
                    }
                  }
                  if (err) {
                    return done(err);
                  }
                  separateInstance.quit();
                  done();
                });
            });
        });

        it('should be able to get all the added Redis type hash values',
          function (done) {

            var arrayInputs = [], separateInstance, instance, instances = [],
              values, i, tempField;

            separateInstance = new Redis2MySql({
              redis: {
                showFriendlyErrorStack: true
              },
              mysql: {
                user: 'root',
                database: connection.mysql.database,
                charset: connection.mysql.charset
              },
              custom: {
                datatypePrefix: {
                  string: 'str',
                  list: 'lst',
                  set: 'set',
                  sortedSet: 'zset',
                  hash: 'map'
                }
              }
            });

            values =
              [
                [1, 'alpha'], // 1
                [2, 'bravo'], // 2
                [3, 'charlie'], // 3
                [4, 'delta'], // 4
                [5, 'echo'], // 5
                [6, 'foxtrot'], // 6
                [7, 'golf'], // 7
                [8, 'hotel'], // 8
                [9, 'india'], // 9
                [10, 'juliet'], // 10
                [11, 'kilo'], // 11
                [12, 'lima'], // 12
                [13, 'mike'], // 13
                [14, 'november'], // 14
                [15, 'oscar'], // 15
                [16, 'papa'], // 16
                [17, 'quebec'], // 17
                [18, 'romeo'], // 18
                [19, 'sierra'], // 19
                [20, 'tango'], // 20
                [21, 'uniform'], // 21
                [22, 'victor'], // 22
                [23, 'whiskey'], // 23
                [24, 'xray'], // 24
                [25, 'yankee'], // 25
                [26, 'zulu'], // 26
                [27, 'twenty-seven'], // 27
                [28, 'twenty-eight'], // 28
                [29, 'twenty-nine'], // 29
                [30, 'thirty'], // 30
                [31, 'thirty-one'] // 31
              ];

            for (i = 0; i < values.length; i++) {
              instance = new Redis2MySql({
                redis: {
                  showFriendlyErrorStack: true
                },
                mysql: {
                  user: 'root',
                  database: connection.mysql.database,
                  charset: connection.mysql.charset
                },
                custom: {
                  datatypePrefix: {
                    string: 'str',
                    list: 'lst',
                    set: 'set',
                    sortedSet: 'zset',
                    hash: 'map'
                  }
                }
              });

              arrayInputs.push({value: values[i], instance: instance});
              instances.push(instance);
            }

            async.map(
              arrayInputs,
              function (item, callback) {
                item.instance.hmset('somename', item.value, function (err, result) {
                  if (err) {
                    return callback(err);
                  }
                  callback(null, result);
                });
              },
              function (err) {
                if (err) {
                  return done(err);
                }

                async.waterfall(
                  [
                    function (firstCb) {
                      separateInstance
                        .hgetall('somename', function (err, result) {
                          if (err) {
                            return firstCb(err);
                          }
                          firstCb(null, result);
                        });
                    },
                    function (redisResult, secondCb) {
                      setTimeout(function () {
                        extrnMySql.query(
                          'SELECT field, value ' +
                          'FROM map_somename ' +
                          '',
                          function (err, result) {
                            if (err) {
                              return secondCb(err);
                            }
                            var i, sqlFields = [], sqlValues = [],
                              redisFields = [], redisValues = [],
                              sqlFieldValues = [], redisFieldValues = [];
                            for (i = 0; i < result.length; i++) {
                              if (is.existy(result[i].field)) {
                                sqlFields.push(result[i].field);
                                sqlValues.push(result[i].value);
                                sqlFieldValues.push(result[i].field);
                                sqlFieldValues.push(result[i].value);
                              }
                            }
                            for (tempField in redisResult) {
                              if (redisResult.hasOwnProperty(tempField)) {
                                redisFields.push(tempField);
                                redisValues.push(redisResult[tempField]);
                                redisFieldValues.push(tempField);
                                redisFieldValues.push(redisResult[tempField]);
                              }
                            }

                            console.log('expected: ' + sqlFieldValues + '\n actual: ' + redisFieldValues + '\n');
                            for (i = 0; i < sqlFields.length; i++) {
                              expect(redisFields).contains(sqlFields[i]);
                            }
                            for (i = 0; i < sqlValues.length; i++) {
                              expect(redisValues).contains(sqlValues[i]);
                            }
                            secondCb();
                          });
                      }, 600);
                    }
                  ], function (err) {
                    for (i = 0; i < instances.length; i++) {
                      if (instances[i]) {
                        instances[i].quit();
                      }
                    }
                    if (err) {
                      return done(err);
                    }
                    separateInstance.quit();
                    done();
                  });
              });
          });

        it('should be able to get the added Redis type string values ' +
          'set by different Redis2Mysql connections',
          function (done) {
            var arrayInputs = [], separateInstance, instance, instances = [],
              values, i;

            separateInstance = new Redis2MySql({
              redis: {
                showFriendlyErrorStack: true
              },
              mysql: {
                user: 'root',
                database: connection.mysql.database,
                charset: connection.mysql.charset
              },
              custom: {
                datatypePrefix: {
                  string: 'str',
                  list: 'lst',
                  set: 'set',
                  sortedSet: 'zset',
                  hash: 'map'
                }
              }
            });

            values =
              [
                'alpha', // 1
                'bravo', // 2
                'charlie', // 3
                'delta', // 4
                'echo', // 5
                'foxtrot', // 6
                'golf', // 7
                'hotel', // 8
                'india', // 9
                'juliet', // 10
                'kilo', // 11
                'lima', // 12
                'mike', // 13
                'november', // 14
                'oscar', // 15
                'papa', // 16
                'quebec', // 17
                'romeo', // 18
                'sierra', // 19
                'tango', // 20
                'uniform', // 21
                'victor', // 22
                'whiskey', // 23
                'xray', // 24
                'yankee', // 25
                'zulu', // 26
                'twenty-seven', // 27
                'twenty-eight', // 28
                'twenty-nine', // 29
                'thirty', // 30
                'thirty-one' // 31
              ];

            for (i = 0; i < values.length; i++) {
              instance = new Redis2MySql({
                redis: {
                  showFriendlyErrorStack: true
                },
                mysql: {
                  user: 'root',
                  database: connection.mysql.database,
                  charset: connection.mysql.charset
                },
                custom: {
                  datatypePrefix: {
                    string: 'str',
                    list: 'lst',
                    set: 'set',
                    sortedSet: 'zset',
                    hash: 'map'
                  }
                }
              });

              arrayInputs.push({value: values[i], instance: instance});
              instances.push(instance);
            }

            async.map(
              arrayInputs,
              function (item, callback) {
                item.instance.on('error', function (err) {
                  console.log('Error from listener: ' + err.error + ' ' + err.message +
                    ' ' + err.redisKey);
                });
                item.instance.set('sometype', item.value, item.value,
                  function (err, result) {
                    if (err) {
                      return callback(err);
                    }
                    callback(null, result);
                  });
              },
              function (err) {
                if (err) {
                  return done(err);
                }

                async.waterfall(
                  [
                    function (firstCb) {
                      extrnRedis.mget(
                        'str:sometype:alpha', // 1
                        'str:sometype:bravo', // 2
                        'str:sometype:charlie', // 3
                        'str:sometype:delta', // 4
                        'str:sometype:echo', // 5
                        'str:sometype:foxtrot', // 6
                        'str:sometype:golf', // 7
                        'str:sometype:hotel', // 8
                        'str:sometype:india', // 9
                        'str:sometype:juliet', // 10
                        'str:sometype:kilo', // 11
                        'str:sometype:lima', // 12
                        'str:sometype:mike', // 13
                        'str:sometype:november', // 14
                        'str:sometype:oscar', // 15
                        function (err, result) {
                          if (err) {
                            return firstCb(err);
                          }
                          firstCb(null, result);
                        });
                    },
                    function (redisResult, secondCb) {
                      setTimeout(function () {
                        extrnMySql.query(
                          'SELECT value FROM str_sometype ' +
                          'WHERE `key` IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ' +
                          '?, ?, ?, ?) ORDER BY `key` ASC',
                          values,
                          function (err, result) {
                            if (err) {
                              return secondCb(err);
                            }

                            var sqlResult = [];
                            for (i = 0; i < result.length; i++) {
                              sqlResult.push(result[i].value);
                            }

                            expect(sqlResult).to.be.deep.equals(redisResult);
                            secondCb();
                          });
                      }, 600);
                    }
                  ], function (err) {
                    for (i = 0; i < instances.length; i++) {
                      if (instances[i]) {
                        instances[i].quit();
                      }
                    }
                    if (err) {
                      return done(err);
                    }
                    separateInstance.quit();
                    done();
                  });
              });
          });
      }

      afterEach(function (done) {
        _deleteData([testKeys],
          [
            'str_sometype',
            'str_somenumtype',
            'lst_some_data',
            'set_some_data',
            'zset_ssgrade',
            'map_somename'
          ],
          function (err) {
            if (err) {
              return done(err);
            }
            done();
          });
      });
    });
  });
  /* End Concurrency Test */

  function _deleteData(keys, tables, callback) {
    async.series([
      function (firstCb) {
        extrnRedis.del(keys, function (err) {
          if (err) {
            firstCb(err);
          } else {
            firstCb();
          }
        });
      },
      function (secondCb) {
        if (is.existy(tables)) {
          var sqlParams = '', i;

          for (i = 0; i < tables.length; i++) {
            sqlParams += '??, ';
          }

          if (sqlParams.length > 0) {
            sqlParams = sqlParams.substring(0, sqlParams.length - ', '.length);
          }

          extrnMySql.query('DROP TABLE IF EXISTS ' + sqlParams,
            tables,
            function (err) {
              if (err) {
                secondCb(err);
              } else {
                secondCb();
              }
            }
          );
        } else {
          secondCb();
        }
      }
    ], function (err) {
      if (err) {
        callback(err);
      } else {
        callback();
      }
    });
  }

  /* Database and Connection Teardown */
  after(function (done) {
    setTimeout(function () {
      async.series([
        function (firstCb) {
          if (is.existy(extrnRedis)) {
            extrnRedis.quit();
            firstCb();
          } else {
            firstCb();
          }
        },
        function (secondCb) {
          if (is.existy(extrnMySql)) {
            extrnMySql.query(
              'DROP DATABASE IF EXISTS mytestxxx; ',
              function (err) {
                if (err) {
                  secondCb(err);
                } else {
                  extrnMySql.end(function (err) {
                    if (err) {
                      secondCb(err);
                    } else {
                      secondCb();
                    }
                  });
                }
              });
          } else {
            secondCb();
          }
        }
      ], function (err) {
        if (err) {
          done(err);
        } else {
          done();
        }
      });
    }, 400);
  });
  /* End Database and Connection Teardown */
});
