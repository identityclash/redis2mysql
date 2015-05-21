/*
 * Copyright (c) 2015.
 */
'use strict';

var chai = require('chai'),
  expect = chai.expect,
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
    'lst:some_data',
    'set:somenumber',
    'map:somename'
  ],
  testTables = [
    'str_sometype',
    'str_some_new_type',
    'str_somenumtype',
    'lst_some_data',
    'set_somenumber',
    'map_somename'
  ];

describe('Redis2MySQL', function () {

  /* Object Instantiation Test */
  describe('object instantiation test', function () {

    context('positive test', function () {

      var instance;

      before(function () {
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
                set: 'set',
                sortedSet: 'zset',
                hash: 'map'
              }
            }
          }
        );
      });

      it('must create a Redis2MySql instance', function (done) {
        expect(instance).to.be.an.instanceOf(Redis2MySql);
        done();
      });

      it('should possess `redisConn` which will make a connection',
        function (done) {
          instance.redisConn.ping(function (err, result) {
            expect(result).equals('PONG');
          });
          done();
        });

      it('should possess `mysqlConn` which will make a connection',
        function (done) {
          instance.mysqlConn.ping(function (err, result) {
            expect(result).to.be.an.instanceOf(OkPacket);
          });
          done();
        });

      after(function () {
        if (instance) {
          instance.quit();
        }
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
                user: 'root'
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

      after(function () {
        if (instance) {
          instance.quit();
        }
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
                database: 'mytest',
                charset: 'utf8'
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

      after(function () {
        if (instance) {
          instance.quit();
        }
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
                user: 'root',
                database: 'mytest',
                charset: 'utf8'
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

      after(function () {
        if (instance) {
          instance.quit();
        }
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
                user: 'root',
                database: 'mytest',
                charset: 'utf8'
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

      after(function () {
        if (instance) {
          instance.quit();
        }
      });
    });

    context('wrong database', function () {

      var instance;

      it('should emit an error', function (done) {
        instance = new Redis2MySql({
            redis: {
              showFriendlyErrorStack: true
            },
            mysql: {
              user: 'root',
              database: 'xxxx',
              charset: 'utf8'
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
          expect(err).to.be.an('object');
          expect(err).to.include.keys('error', 'message');
          done();
        });
      });

      after(function () {
        if (instance) {
          instance.quit();
        }
      });
    });
  });
  /* End Object Instantiation Test */

  /* Method Test */
  describe('methods test', function methodTest() {

    var instance, extrnRedis, extrnMySql;

    before(function (done) {
      /* connections independent of the object being tested */
      extrnRedis = new Redis();
      extrnRedis.del(testKeys, function (err) {
        if (err) {
          throw err;
        }
      });

      extrnMySql = mysql.createConnection({
        user: 'root',
        database: 'mytest',
        charset: 'utf8'
      });
      extrnMySql.connect();
      extrnMySql.query('DROP TABLE IF EXISTS ??, ??, ??, ??',
        testTables,
        function (err) {
          if (err) {
            done(err);
          }
        }
      );

      /* actual object being tested */
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
                done(err);
              } else {
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
                      done(err);
                    } else {
                      expect(result[0]).to.be.equals('400');
                      expect(result[1]).to.be.equals('name2');
                      expect(result[2]).to.be.equals('name1');
                      expect(result[3]).to.be.equals('300');
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
                              done(err);
                            } else {
                              expect(result[0].value).to.be.equals('400');
                              expect(result[1].value).to.be.equals('name2');
                              expect(result[2].value).to.be.equals('name1');
                              expect(result[3].value).to.be.equals('300');
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
        async.series([
          function (firstCb) {
            extrnRedis.del('lst:some_data', function (err) {
              if (err) {
                firstCb(err);
              } else {
                firstCb();
              }
            });
          },
          function (secondCb) {
            extrnMySql.query('DROP TABLE IF EXISTS ?? ',
              'lst_some_data',
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
                COLUMNS.SEQ + ' BIGINT PRIMARY KEY, ' +
                COLUMNS.VALUE + ' VARCHAR(255), ' +
                COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
                COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
                ') ',
                function (err) {
                  if (err) {
                    done(err);
                  } else {
                    var time;
                    if (result[0][1][1].length > 0) { // result from Redis TIME command
                      /* UNIX time in sec + microseconds */
                      time = result[0][1][0] + result[0][1][1];
                    }
                    extrnMySql.query(
                      'INSERT INTO lst_some_data (`time_sequence` , `value` ) ' +
                      'VALUES (?, ?), (?, ?), (?, ?), (?, ?) ',
                      [
                        time,
                        300,
                        time + 1,
                        'name1',
                        time + 2,
                        'name2',
                        time + 3,
                        '400'
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
                COLUMNS.SEQ + ' BIGINT PRIMARY KEY, ' +
                COLUMNS.VALUE + ' VARCHAR(255), ' +
                COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
                COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
                ') ',
                function (err) {
                  if (err) {
                    done(err);
                  } else {
                    var time;
                    if (result[0][1][1].length > 0) { // result from Redis TIME command
                      /* UNIX time in sec + microseconds */
                      time = result[0][1][0] + result[0][1][1];
                    }
                    extrnMySql.query(
                      'INSERT INTO lst_some_data (`time_sequence` , `value` ) ' +
                      'VALUES (?, ?), (?, ?), (?, ?), (?, ?) ',
                      [
                        time,
                        300,
                        time + 1,
                        'name1',
                        time + 2,
                        'name2',
                        time + 3,
                        '400'
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
        }
      ], function (err) {
        if (err) {
          callback(err);
        } else {
          callback();
        }
      });
    }

    after(function (done) {
      async.series([
        function (firstCb) {
          extrnRedis.del(testKeys, function (err) {
            if (err) {
              firstCb(err);
            } else {
              extrnRedis.quit();
              firstCb();
            }
          });
        },
        function (secondCb) {
          extrnMySql.query('DROP TABLE IF EXISTS ??, ??, ??, ??',
            testTables,
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

      if (instance) {
        instance.quit();
      }
    });
  });
  /* End Method Test*/

});
