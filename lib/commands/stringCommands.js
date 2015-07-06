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

function StringCommands() {
  if (!(this instanceof StringCommands)) {
    return new StringCommands();
  }
}

util.inherits(StringCommands, EventEmitter);

StringCommands.prototype.incr = function (type, key, cb) {

  if (!key) {
    cb('Incomplete SET parameter(s)');
  } else if (is.not.string(key)) {
    cb('INCR `key` parameter must be a string');
  } else {

    var self = this, redisKey, tableName, sqlCreateTable, sql,
      sqlParams,
      tableColumns = [self.mysqlWriteConn.escapeId(COLUMNS.KEY), COLUMNS.VALUE];

    redisKey =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.string, type, key], ':');

    tableName =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.string, type], '_');

    sqlCreateTable =
      'CREATE TABLE IF NOT EXISTS ' + self.mysqlWriteConn.escapeId(tableName) +
      '(' +
      self.mysqlWriteConn.escapeId(COLUMNS.KEY) + ' VARCHAR(255) PRIMARY KEY, ' +
      COLUMNS.VALUE + ' VARCHAR(255), ' +
      COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
      COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
      ') ';

    async.series(
      [
        function (createTblCb) {
          logger.info('Created table, SQL Create: ' + sqlCreateTable);
          self.mysqlWriteConn.query(
            sqlCreateTable,
            function (err) {
              if (err) {
                return createTblCb(err);
              }

              createTblCb();
            });
        },
        function (redisCb) {
          /* For read or update of value */
          var sqlQueryLatest = new SqlBuilder().select(1).from(1).where(1).toString(),

            sqlQueryLatestParams = [
              COLUMNS.VALUE,
              tableName,
              COLUMNS.KEY,
              key
            ];

          self.mysqlReadConn.query(
            sqlQueryLatest,
            sqlQueryLatestParams,
            function (err, latestSqlResult) {
              if (err) {
                return redisCb(err);
              }

              var latestValue = latestSqlResult.length === 0 ? 0 :
                latestSqlResult[0].value;

              self.redisConn.multi().set(redisKey, latestValue)
                .incr(redisKey).exec(function (err, result) {
                  if (err) {
                    return redisCb(err);
                  }

                  redisCb(null, result[1][1]);
                });
            });
        }
      ], function (err, result) {
        if (err) {
          return cb(err);
        }

        var redisValue = result[1];
        cb(null, redisValue);

        /* The incremented value from Redis to be used in MySQL commands */
        sql = new SqlBuilder().insert('??', tableColumns)
          .values(tableColumns.length).onDuplicate(tableColumns.length)
          .toString();

        sqlParams = [tableName, key, redisValue, COLUMNS.KEY,
          key, COLUMNS.VALUE, redisValue];

        logger.info('sql: ' + sql);
        logger.info('sqlParams: ' + sqlParams);

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
              self.redisConn.decr(redisKey, function (err, result) {
                if (err) {
                  self.emit('error', {
                    error: 'redis', message: err,
                    redisKey: redisKey
                  });
                } else {
                  logger.warn('Redis INCR rollback via DECR: ' + result);
                }
              });
            } else {
              logger.info('Redis INCR MySQL result: ' + result.affectedRows);
            }
          });
      });
  }
};

StringCommands.prototype.set = function (type, param1, param2, cb) {

  if (!(param1 && type)) {
    cb('Incomplete SET parameter(s)');
  } else if (is.not.string(type)) {
    cb('SET `type` parameter must be a string');
  } else if (is.not.string(param1) && is.not.array(param1)) {
    cb('SET `param1` parameter must be a string or an array containing ' +
      'the key and the value to be set');
  } else {

    if (typeof param2 === 'function') {
      cb = param2;
      param2 = '';
    }

    var self = this, redisKey, key, value, originalValue,
      stringTableName, sqlCreateStringTable = '';

    if (is.array(param1)) {

      redisKey =
        myUtil.prefixAppender([self.options.custom.datatypePrefix.string, type, param1[0]], ':');

      key = param1[0];
      value = param1[1] || '';
    } else {

      redisKey =
        myUtil.prefixAppender([self.options.custom.datatypePrefix.string, type, param1], ':');

      key = param1;
      value = param2 || '';
    }

    stringTableName =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.string, type], '_');

    sqlCreateStringTable =
      'CREATE TABLE IF NOT EXISTS ' + self.mysqlWriteConn.escapeId(stringTableName) +
      '(' +
      self.mysqlWriteConn.escapeId(COLUMNS.KEY) + ' VARCHAR(255) PRIMARY KEY, ' +
      COLUMNS.VALUE + ' VARCHAR(255), ' +
      COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
      COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
      ') ';

    async.series(
      [
        function (createTblCb) {
          logger.info('Created table, SQL Create: ' + sqlCreateStringTable);
          self.mysqlWriteConn.query(
            sqlCreateStringTable,
            function (err) {
              if (err) {
                return createTblCb(err);
              }
              createTblCb();
            });
        },
        function (redisCb) {
          logger.info('redis input: ' + redisKey + ' value: ' + value);
          /* for rollback purposes, use GETSET instead of SET */
          self.redisConn.getset(redisKey, value, function (err, result) {
            if (err) {
              return redisCb(err);
            }
            redisCb(null, result); // GETSET returns the original (previous) value when a value is set
          });
        }
      ], function (err, result) {
        if (err) {
          return cb(err);
        }

        originalValue = result[1];

        cb(null, 'OK');

        var tableColumns = [self.mysqlWriteConn.escapeId(COLUMNS.KEY), COLUMNS.VALUE],
          sql, sqlParams;

        sql = new SqlBuilder().insert('??', tableColumns)
          .values(tableColumns.length).onDuplicate(1).toString();

        sqlParams = [stringTableName, key, value, COLUMNS.VALUE, value];

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
              self.redisConn.set(redisKey, originalValue, function (err, result) {
                if (err) {
                  self.emit('error', {
                    error: 'redis', message: err,
                    redisKey: redisKey
                  });
                } else {
                  logger.warn('Redis SET rollback: ' + result);
                }
              });
            } else {
              logger.info('Redis SET MySQL result: ' + result.affectedRows + ' value: ' + value);
            }
          });
      });
  }
};

StringCommands.prototype.get = function (type, key, cb) {

  if (!key) {
    return cb('Incomplete GET parameters');
  }
  if (is.not.string(type)) {
    return cb('GET `type` parameter must be a string');
  }
  if (is.not.string(key)) {
    return cb('GET `key` parameter must be a string');
  }

  var self = this,

    redisKey =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.string, type, key], ':');

  this.redisConn.get(redisKey, function (err, result) {

    if (err) {
      return cb(err);
    }
    if (is.existy(result)) {
      return cb(null, result);
    }

    var stringTableName =
        myUtil.prefixAppender([self.options.custom.datatypePrefix.string, type], '_'),

      sql = new SqlBuilder().select(1).from(1).where(1).toString(),

      sqlParams = [
        COLUMNS.VALUE,
        stringTableName,
        COLUMNS.KEY,
        key
      ];

    logger.info('sql: ' + sql);
    logger.info('sqlParams: ' + sqlParams);

    self.mysqlReadConn.query(
      sql,
      sqlParams,
      function (err, result) {

        if (err) {
          if (err.code !== ERRORS.NON_EXISTENT_TABLE) {
            self.emit('error', {
              error: 'mysql', message: err.message,
              redisKey: redisKey
            });
          }

          return cb(null, null); // return nothing from MySQL since table cannot be read
        }

        var mysqlResult = is.existy(result) && is.existy(result[0]) ?
          result[0].value : null;

        // ensures only one callback
        async.parallel([
          function (mysqlCb) {
            mysqlCb(null, mysqlResult);
          },
          function (copy2RedisCb) {
            self.redisConn.set(redisKey, mysqlResult,
              function (err, result) {
                if (err) {
                  return copy2RedisCb(err);
                }

                logger.info('MySQL copy to Redis: ' + result);
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
  module.exports = StringCommands;
}
