/*
 * Copyright (c) 2015.
 */
'use strict';

var async = require('async')
  , util = require('util')
  , is = require('is_js')
  , EventEmitter = require('events')
  , logger = require('winston')
  , SqlBuilder = require('../util/sqlBuilder')
  , myUtil = new (require('../util/util'))();

var COLUMNS = {
    SEQ: 'time_sequence',
    KEY: 'key',
    FIELD: 'field',
    VALUE: 'value',
    MEMBER: 'member',
    SCORE: 'score',
    EXPIRY_DT: 'expiry_date',
    CREATION_DT: 'creation_date',
    LAST_UPDT_DT: 'last_update_date'
  }
  , ERRORS = {
    NON_EXISTENT_TABLE: 'ER_NO_SUCH_TABLE'
  };

function SetCommands() {
  if (!(this instanceof SetCommands)) {
    return new SetCommands();
  }
}

util.inherits(SetCommands, EventEmitter);

SetCommands.prototype.sadd = function (key, members, cb) {

  if (!(key && members)) {
    cb('Incomplete SADD parameter(s)');
  } else if (is.not.string(key)) {
    cb('SADD `key` parameter must be a string');
  } else if (is.not.string(members) && is.not.array(members)) {
    cb('SADD `members` parameter must be a string OR ' +
      'an array of strings');
  } else {

    var self = this, redisKey, arrayMembers = [], i,
      tableColumns = [COLUMNS.MEMBER], sql, sqlParams = [], ordSetTableName,
      sqlCreateOrdSetTable;

    if (is.string(members)) {
      arrayMembers.push(members);
    } else if (is.array(members)) {
      arrayMembers = arrayMembers.concat(members);
    }

    redisKey =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.set, key], ':');

    ordSetTableName =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.set, key], '_');

    sqlCreateOrdSetTable =
      'CREATE TABLE IF NOT EXISTS ' + self.mysqlWriteConn.escapeId(ordSetTableName) +
      '(' +
      COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
      COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
      COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
      ') ';

    async.series(
      [
        function (createTblCb) {
          logger.info('Created table, SQL Create: ' + sqlCreateOrdSetTable);
          self.mysqlWriteConn.query(
            sqlCreateOrdSetTable,
            function (err) {
              if (err) {
                return createTblCb(err);
              }
              createTblCb();
            });
        },
        function (redisCb) {
          logger.info('redis input: ' + redisKey + ' members: ' + members);
          self.redisConn.sadd(redisKey, members, function (err, result) {
            if (err) {
              return redisCb(err);
            }
            redisCb(null, result);
          });
        }
      ],
      function (err, result) {
        if (err) {
          return cb(err);
        }

        cb(null, result[1]);

        sql = new SqlBuilder().insertIgnore('??', tableColumns)
          .values(tableColumns.length, arrayMembers.length);

        sqlParams.push(ordSetTableName);

        /* For the values of the INSERT phrase */
        for (i = 0; i < arrayMembers.length; i++) {
          sqlParams.push(arrayMembers[i]);
        }

        logger.info('SQL for insert / update: ' + sql);
        logger.info('SQL for insert / update parameters: ' + sqlParams.toString());

        self.mysqlWriteConn.query(
          sql,
          sqlParams,
          function (err, result) {
            if (err) {
              self.emit('error', {
                error: 'mysql', message: err.message,
                redisKey: redisKey
              });

              /* for rollback purposes */
              self.redisConn.srem(redisKey, arrayMembers, function (err, result) {
                if (err) {
                  self.emit('error', {
                    error: 'redis', message: err,
                    redisKey: redisKey
                  });
                } else {
                  logger.warn('Redis SADD rollback via SREM: ' + result);
                }
              });
            } else if (is.existy(result)) {
              logger.info('Redis SADD MySQL result: ' + result.message);
            }
          });
      });
  }
};

SetCommands.prototype.srem = function (key, members, cb) {

  if (!(key && members)) {
    cb('Incomplete SREM parameter(s)');
  } else if (is.not.string(key)) {
    cb('SREM `key` parameter must be a string');
  } else if (is.not.string(members) && is.not.array(members)) {
    cb('SREM `members` parameter must be a string OR ' +
      'an array of strings');
  } else {

    var self = this, redisKey, arrayMembers = [],
      ordSetTableName, sql, sqlParams = [], i;

    if (is.string(members)) {
      arrayMembers.push(members);
    } else if (is.array(members)) {
      arrayMembers = arrayMembers.concat(members);
    }

    redisKey =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.set, key], ':');

    self.redisConn.srem(redisKey, arrayMembers, function (err, result) {

      if (err) {
        cb(err);
      } else {

        cb(null, result);

        ordSetTableName =
          myUtil.prefixAppender([self.options.custom.datatypePrefix.set, key], '_');

        sql = new SqlBuilder().deleteFrom('??').where(arrayMembers.length, 'OR');

        sqlParams.push(ordSetTableName);

        for (i = 0; i < arrayMembers.length; i++) {
          sqlParams.push(COLUMNS.MEMBER);
          sqlParams.push(arrayMembers[i]);
        }

        self.mysqlWriteConn.query(
          sql,
          sqlParams,
          function (err, result) {

            if (err) {
              self.emit('error', {error: 'mysql', message: err.message});

              /* for rollback purposes */
              self.redisConn.sadd(redisKey, arrayMembers, function (err, result) {
                if (err) {
                  self.emit('error', {
                    error: 'redis', message: err,
                    redisKey: redisKey
                  });
                } else {
                  logger.warn('Redis SREM rollback via SADD: ' + result);
                }
              });
            } else if (is.existy(result)) {
              logger.info('Redis SREM MySQL result: ' + result.affectedRows);
            }
          }
        );
      }
    });
  }
};

SetCommands.prototype.smembers = function (key, cb) {

  if (!key) {
    return cb('Incomplete SMEMBERS parameter');
  }
  if (is.not.string(key)) {
    return cb('SMEMBERS `key` parameter must be a string');
  }

  var self = this,

    redisKey =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.set, key], ':');

  this.redisConn.smembers(redisKey, function (err, result) {

    if (err) {
      return cb(err);
    }
    if (is.existy(result) && is.not.empty(result)) {
      return cb(null, result);
    }

    var ordSetTableName =
        myUtil.prefixAppender([self.options.custom.datatypePrefix.set, key], '_'),

      sql = new SqlBuilder().select(1).from(1).toString();

    self.mysqlReadConn.query(
      sql,
      [
        COLUMNS.MEMBER,
        ordSetTableName
      ],
      function (err, result) {

        if (err) {
          if (err.code !== ERRORS.NON_EXISTENT_TABLE) {
            self.emit('error', {
              error: 'mysql', message: err.message, redisKey: redisKey
            });
          }

          return cb(null, []);
        }

        var i, members = [];
        for (i = 0; i < result.length; i++) {
          if (is.existy(result[i].member)) {
            members.push(result[i].member);
          }
        }

        if (members.length === 0) {
          return cb(null, []);
        }

        async.parallel(
          [
            function (mysqlCb) {
              mysqlCb(null, members);
            },
            function (copy2RedisCb) {
              self.redisConn.sadd(redisKey, members, function (err, result) {
                if (err) {
                  return copy2RedisCb(err);
                }

                logger.info('MySQL to Redis copied result: ' + result);
                copy2RedisCb();
              });
            }
          ], function (err, result) {
            if (err) {
              return cb(err);
            }

            cb(null, result[0]);
          });
      });
  });
};

SetCommands.prototype.sismember = function (key, member, cb) {

  if (!key) {
    return cb('Incomplete SISMEMBER parameter(s)');
  }
  if (is.not.string(key)) {
    return cb('SISMEMBER `key` parameter must be a string');
  }
  if (is.not.string(member)) {
    return cb('SISMEMBER `member` parameter must be a string');
  }

  var self = this,

    redisKey =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.set, key], ':');

  this.redisConn.sismember(redisKey, member, function (err, result) {

    if (err) {
      return cb(err);
    }
    if (result) {
      return cb(null, result);
    }

    var sql = new SqlBuilder().select(1).from(1).where(1).toString(),

      ordSetTableName =
        myUtil.prefixAppender([self.options.custom.datatypePrefix.set, key], '_');

    self.mysqlReadConn.query(
      sql,
      [
        COLUMNS.MEMBER,
        ordSetTableName,
        COLUMNS.MEMBER,
        member
      ],
      function (err, result) {

        if (err) {
          if (err.code !== ERRORS.NON_EXISTENT_TABLE) {
            self.emit('error', {
              error: 'mysql', message: err.message,
              redisKey: redisKey
            });
          }

          return cb(null, 0);
        }

        var mysqlCount = is.existy(result) && result.length > 0 ?
          result.length : 0;

        if (mysqlCount === 0) {
          return cb(null, 0);
        }

        async.parallel(
          [
            function (mysqlCb) {
              mysqlCb(null, mysqlCount);
            },
            function (copy2RedisCb) {
              self.redisConn.sadd(redisKey, member, function (err, result) {
                if (err) {
                  return copy2RedisCb(err);
                }

                logger.info('MySQL to Redis copied result: ' + result);
                return copy2RedisCb();
              });
            }
          ], function (err, result) {
            if (err) {
              return cb(err);
            }

            cb(null, result[0]);
          });
      });
  });
};

SetCommands.prototype.scard = function (key, cb) {

  if (!key) {
    return cb('Incomplete SCARD parameter');
  }
  if (is.not.string(key)) {
    return cb('SCARD `key` parameter must be a string');
  }

  var self = this,

    redisKey =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.set, key], ':');

  this.redisConn.scard(redisKey, function (err, result) {

    if (err) {
      return cb(err);
    }
    if (result) {
      return cb(null, result);
    }

    var ordSetTableName =
        myUtil.prefixAppender([self.options.custom.datatypePrefix.set, key], '_'),

      sql = 'SELECT member FROM ??';

    self.mysqlReadConn.query(sql, ordSetTableName, function (err, result) {

      if (err) {
        if (err.code !== ERRORS.NON_EXISTENT_TABLE) {
          self.emit('error', {
            error: 'mysql', message: err.message,
            redisKey: redisKey
          });
        }

        return cb(null, 0);
      }

      var i, members = [];
      for (i = 0; i < result.length; i++) {
        if (is.existy(result[i].member)) {
          members.push(result[i].member);
        }
      }

      if (members.length === 0) {
        return cb(null, 0);
      }

      async.parallel(
        [
          function (mysqlCb) {
            mysqlCb(null, members.length);
          },
          function (copy2RedisCb) {
            self.redisConn.sadd(redisKey, members, function (err, result) {
              if (err) {
                return copy2RedisCb(err);
              }

              logger.info('MySQL to Redis copied result: ' + result);
              copy2RedisCb();
            });
          }
        ], function (err, result) {
          if (err) {
            return cb(err);
          }

          cb(null, result[0]);
        });
    });
  });
};

if (typeof module !== 'undefined' &&
  module.exports) {
  module.exports = SetCommands;
}
