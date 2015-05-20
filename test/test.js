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
  ERRORS = {
    missingPrefix: 'All database table prefixes should be defined by the ' +
    'user.',
    duplicatePrefix: 'There are duplicate user-defined database ' +
    'prefixes. Please make all prefixes unique.',
    missingUser: 'Please specify the username',
    missingDatabase: 'Please specify the database'
  };

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
        }).throws(ERRORS.missingDatabase);
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
        }).throws(ERRORS.missingUser);
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
        }).throws(ERRORS.missingPrefix);
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
        }).throws(ERRORS.duplicatePrefix);
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
        }).throws(ERRORS.duplicatePrefix);
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
  describe('individual method test', function methodTest() {

    var instance, extrnRedis, extrnMySql;

    before(function () {
      /* connections independent of the object being tested */
      extrnRedis = new Redis();
      extrnRedis.multi()
        .del('str:sometype:testkey', 'str:sometype:testincrkey',
        'str:somenumtype:testcounter')
        .set('str:sometype:testincrkey', '2')
        .exec(function (err) {
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
      extrnMySql.query('DROP TABLE IF EXISTS str_sometype, str_somenumtype ',
        function (err) {
          if (err) {
            throw err;
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
    });

    context('#set()', function () {
      it('should receive OK from Redis for str:sometype:testkey', function (done) {
        instance.set('sometype', 'testkey', 'the fox jumped',
          function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals('OK');
              done();
            }
          });
      });

      it('should contain a value from Redis for key `str:sometype:testkey`',
        function (done) {
          extrnRedis.get('str:sometype:testkey', function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals('the fox jumped');
              done();
            }
          });
        });

      it('should contain a value from MySQL table `str_sometype`',
        function (done) {

          setTimeout(
            function () {
              extrnMySql.query(
                'SELECT `value` FROM `str_sometype` WHERE `key` = ? ',
                'testkey',
                function (err, result) {
                  if (err) {
                    done(err);
                  } else {
                    expect(result[0].value).to.be.equals('the fox jumped');
                    done();
                  }
                }
              );
            }, 400);
        });
    });

    context('#incr()', function () {
      it('should receive a value of 3', function (done) {
        instance.incr('sometype', 'testincrkey', function (err, result) {
          if (err) {
            done(err);
          } else {
            expect(result).to.be.equals(3); // converts from string to numeric
            done();
          }
        });
      });

      it('should contain a value from Redis for `str:sometype:testincrkey`',
        function (done) {
          extrnRedis.get('str:sometype:testincrkey', function (err, result) {
            if (err) {
              done(err);
            } else {
              expect(result).to.be.equals('3'); // get returns a string
              done();
            }
          });
        });

      it('should contain a value from MySQL table `str_sometype`', function (done) {

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
      });
    });

    after(function () {
      async.series([
        function (cb1) {
          extrnRedis.del('str:sometype:testkey', 'str:sometype:testincrkey',
            'str:somenumtype:testcounter', function (err) {
              if (err) {
                cb1(err);
              } else {
                extrnRedis.quit();
                cb1();
              }
            });
        },
        function (cb2) {
          extrnMySql.query('DROP TABLE IF EXISTS str_sometype, str_somenumtype ',
            function (err) {
              if (err) {
                cb2(err);
              } else {
                extrnMySql.end(function (err) {
                  if (err) {
                    cb2(err);
                  } else {
                    cb2();
                  }
                });
              }
            }
          );
        }
      ], function (err) {
        if (err) {
          throw err;
        }
      });

      if (instance) {
        instance.quit();
      }
    });
  });
  /* End Method Test*/

});
