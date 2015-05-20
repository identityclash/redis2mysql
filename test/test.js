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
            done();
          });
        });

      it('should possess `mysqlConn` which will make a connection',
        function (done) {
          instance.mysqlConn.ping(function (err, result) {
            expect(result).to.be.an.instanceOf(OkPacket);
            done();
          });
        });

      after(function () {
        instance.quit();
      });
    });

    context('wrong database', function () {

      var instance;

      it('should access Redis2MySQL instance', function (done) {
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
    });

    context('missing MySQL database', function (done) {

      it('should throw error', function (done) {

        var instance;

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

    });

    context('missing prefixes', function () {

      it('missing prefixes', function (done) {

        var instance;

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
    });
  });
  /* End Object Instantiation Test */

  /* Method Test */
  describe('method test', function methodTest() {

    var instance, redisConn, mysqlConn;

    before(function () {
      /* connections independent of the object being tested */
      redisConn = new Redis();
      redisConn.del('str:email:x', function (err) {
        if (err) {
          throw err;
        } else {
          console.log('deleted key `str:email:x` ');
        }
      });

      mysqlConn = mysql.createConnection({
        user: 'root',
        database: 'mytest',
        charset: 'utf8'
      });
      mysqlConn.connect();
      mysqlConn.query('DROP TABLE IF EXISTS `str_email` ',
        function (err) {
          if (err) {
            throw err;
          } else {
            console.log('deleted table `str_email` ');
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

    context('Redis `set` function', function () {
      it('should get receive OK from Redis for str:email:x', function () {
        instance.set('email', 'x', 'the fox jumped', function (err, result) {
          expect(result).to.be.equals('OK');
        });
      });

      it('should get a value from Redis for str:email:x', function () {
        redisConn.get('str:email:x', function (err, result) {
          expect(result).to.be.equals('the fox jumped');
        });
      });

      it('should get a value from MySQL table str_email', function () {
        mysqlConn.query(
          'SELECT `key` FROM str_email WHERE `key` = ' + mysqlConn.escape('x'),
          function (err, result) {
            expect(result).to.be.equals('the fox jumped');
          });
      });
    });

    after(function () {
      async.series([
        function (cb1) {
          redisConn.del('str:email:x', function (err) {
            if (err) {
              cb1(err);
            } else {
              redisConn.quit();
              console.log('deleted key `str:email:x` ');
              cb1();
            }
          });
        },
        function (cb2) {
          mysqlConn.query('DROP TABLE IF EXISTS `str_email` ',
            function (err) {
              if (err) {
                cb2(err);
              } else {
                mysqlConn.end(function (err) {
                  if (err) {
                    cb2(err);
                  } else {
                    console.log('delete table `str_email` ');
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

      instance.quit();
    });
  });
  /* End Method Test*/

});
