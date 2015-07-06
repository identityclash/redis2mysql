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

function HashCommands() {
  if (!(this instanceof HashCommands)) {
    return new HashCommands();
  }
}

util.inherits(HashCommands, EventEmitter);

HashCommands.prototype.hset = function (hashKey, param1, param2, cb) {

  if (!(param1 && hashKey)) {
    cb('Incomplete HSET parameter(s)');
  } else if (is.not.string(hashKey)) {
    cb('HSET `hashKey` parameter must be a string');
  } else if (is.not.string(param1) && is.not.array(param1)) {
    cb('HSET `param1` parameter must be a string or an array containing ' +
      'the field and the value to be set');
  } else {

    if (typeof param2 === 'function') {
      cb = param2;
      param2 = '';
    }

    var self = this, redisHashKey, field, value;

    redisHashKey =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

    if (is.array(param1)) {

      field = param1[0];
      value = param1[1] || '';
    } else {

      field = param1;
      value = param2 || '';
    }

    this.redisConn.hget(redisHashKey, field, function (err, result) {
      if (err) {
        cb(err);
      } else {

        var originalValue = result,

          hashTableName =
            myUtil.prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_'),

          sqlCreateHashTable =
            'CREATE TABLE IF NOT EXISTS ' + self.mysqlWriteConn.escapeId(hashTableName) +
            '(' +
            self.mysqlWriteConn.escapeId(COLUMNS.FIELD) + ' VARCHAR(255) PRIMARY KEY, ' +
            COLUMNS.VALUE + ' VARCHAR(255), ' +
            COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
            COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
            ') ';

        async.series(
          [
            function (createTblCb) {
              logger.info('Created table, SQL Create: ' + sqlCreateHashTable);
              self.mysqlWriteConn.query(
                sqlCreateHashTable,
                function (err) {
                  if (err) {
                    return createTblCb(err);
                  }
                  createTblCb();
                });
            },
            function (redisCb) {
              self.redisConn.hset(redisHashKey, field, value, function (err, result) {
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

            var tableColumns = [self.mysqlWriteConn.escapeId(COLUMNS.FIELD), COLUMNS.VALUE],
              sql, sqlParams;

            sql = new SqlBuilder().insert('??', tableColumns)
              .values(tableColumns.length).onDuplicate(tableColumns.length).toString();

            sqlParams = [hashTableName, field, value, COLUMNS.FIELD,
              field, COLUMNS.VALUE, value];

            logger.info('SQL for insert / update: ' + sql);
            logger.info('SQL for insert / update parameters: ' + sqlParams.toString());

            self.mysqlWriteConn.query(
              sql,
              sqlParams,
              function (err, result) {
                if (err) {
                  self.emit('error', {
                    error: 'mysql', message: err.message,
                    redisKey: redisHashKey
                  });

                  /* for rollback purposes */
                  self.redisConn.hset(redisHashKey, field, originalValue,
                    function (err, result) {
                      if (err) {
                        self.emit('error', {
                          error: 'redis', message: err,
                          redisKey: redisHashKey
                        });
                      } else {
                        logger.warn('Redis HSET rollback: ' + result);
                      }
                    });
                } else {
                  logger.info('Redis HSET MySQL result: ' + result.affectedRows);
                }
              });
          });
      }
    });
  }
};

HashCommands.prototype.hmset = function (hashKey, param1, param2, cb) {

  if (!(param1 && hashKey)) {
    cb('Incomplete HSET parameter(s)');
  } else if (is.not.string(hashKey)) {
    cb('HSET `hashKey` parameter must be a string');
  } else if (is.not.string(param1) && is.not.array(param1)) {
    cb('HSET `param1` parameter must be a string or an array containing ' +
      'the field and the value to be set');
  } else {

    if (typeof param2 === 'function') {
      cb = param2;
      param2 = '';
    }

    var self = this, redisHashKey, i, fields = [], fieldValues = [], hashTableName;

    redisHashKey =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

    if (is.array(param1)) {
      for (i = 0; i < param1.length; i++) {
        if (is.odd(i)) {
          fields.push(param1[i]);
        }
      }
      fieldValues = fieldValues.concat(param1);
    } else {
      fields.push(param1);
      fieldValues.push(param1);
      fieldValues.push(param2);
    }

    hashTableName =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_');

    /* for rollback purposes, use getset instead of set */
    this.redisConn.hmget(redisHashKey, fields, function (err, result) {
      if (err) {
        cb(err);
      } else {

        var originalValues = result, i,

          sqlCreateHashTable =
            'CREATE TABLE IF NOT EXISTS ' + self.mysqlWriteConn.escapeId(hashTableName) +
            '(' +
            self.mysqlWriteConn.escapeId(COLUMNS.FIELD) + ' VARCHAR(255) PRIMARY KEY, ' +
            COLUMNS.VALUE + ' VARCHAR(255), ' +
            COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
            COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
            ') ';

        async.series(
          [
            function (createTblCb) {
              logger.info('Created table, SQL Create: ' + sqlCreateHashTable);
              self.mysqlWriteConn.query(
                sqlCreateHashTable,
                function (err) {
                  if (err) {
                    return createTblCb(err);
                  }
                  createTblCb();
                });
            },
            function (redisCb) {
              self.redisConn.hmset(redisHashKey, fieldValues, function (err, result) {
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

            var tableColumns = [self.mysqlWriteConn.escapeId(COLUMNS.FIELD), COLUMNS.VALUE],
              sqlParams = [],

              sql = new SqlBuilder().insertIgnore('??', tableColumns)
                .values(tableColumns.length, fieldValues.length / 2)
                .onDuplicate({value: 'VALUES(' + COLUMNS.VALUE + ')'}).toString();

            sqlParams.push(hashTableName);

            for (i = 0; i < fieldValues.length; i++) {
              sqlParams.push(fieldValues[i]);
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
                    redisKey: redisHashKey
                  });

                  /* for rollback purposes */
                  self.redisConn.hmset(redisHashKey, originalValues,
                    function (err, result) {
                      if (err) {
                        self.emit('error', {
                          error: 'redis', message: err,
                          redisKey: redisHashKey
                        });
                      } else {
                        logger.warn('Redis HMSET rollback: ' + result);
                      }
                    });
                } else {
                  logger.info('Redis HMSET MySQL result: ' + result.affectedRows);
                }
              });
          });
      }
    });
  }
};

HashCommands.prototype.hget = function (hashKey, field, cb) {

  if (!field) {
    cb('Incomplete HGET parameters');
  } else if (is.not.string(hashKey)) {
    cb('HGET `hashKey` must be a string');
  } else if (is.not.string(field)) {
    cb('HGET `field` must be a string');
  } else {

    var self = this, hashTableName, redisHashKey =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

    this.redisConn.hget(redisHashKey, field, function (err, result) {
      if (err) {
        cb(err);
      } else if (is.existy(result)) {
        cb(null, result);
      } else {

        hashTableName =
          myUtil.prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_');

        self.mysqlReadConn.query(
          'SELECT ?? FROM ?? WHERE ?? = ?',
          [
            COLUMNS.VALUE,
            hashTableName,
            COLUMNS.FIELD,
            field
          ],
          function (err, result) {
            if (err) {
              if (err.code !== ERRORS.NON_EXISTENT_TABLE) {
                self.emit('error', {
                  error: 'mysql', message: err.message,
                  redisKey: redisHashKey
                });
              }

              return cb(null, null);
            }

            var value = is.existy(result[0]) ? result[0].value : null;

            if (is.not.existy(value)) {
              return cb(null, null);
            }

            async.parallel(
              [
                function (mysqlCb) {
                  mysqlCb(null, value);
                },
                function (copy2RedisCb) {
                  self.redisConn.hset(redisHashKey, field, value,
                    function (err, result) {
                      if (err) {
                        return copy2RedisCb(err);
                      }

                      logger.info('MySQL to Redis copied result: ' + result);
                      copy2RedisCb();
                    });
                }
              ],
              function (err, result) {
                if (err) {
                  return cb(err);
                }

                cb(null, result[0]);
              });
          }
        );
      }
    });
  }
};

HashCommands.prototype.hmget = function (hashKey, fields, cb) {

  if (!fields) {
    return cb('Incomplete HMGET parameters');
  }
  if (is.not.string(hashKey)) {
    return cb('HMGET `hashKey`parameter must be a string');
  }
  if (is.not.array(fields)) {
    return cb('HMGET `fields` parameter must be an array');
  }

  var self = this, redisHashKey =
    myUtil.prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

  this.redisConn.hmget(redisHashKey, fields, function (err, result) {

    var i, hasNull = false, hashTableName, sqlSelect, sql, sqlParams = [],
      fieldParams = '';

    for (i = 0; i < result.length; i++) {
      if (is.not.existy(result[i])) {
        hasNull = true;
        break;
      }
    }

    if (err) {
      return cb(err);
    }
    if (is.existy(result) && !hasNull) { // if one of those fields has a null value, need to cross-check with MySQL
      return cb(null, result);
    }

    hashTableName =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_');

    sqlParams.push(COLUMNS.FIELD);
    sqlParams.push(COLUMNS.VALUE);
    sqlParams.push(hashTableName);

    for (i = 0; i < fields.length; i++) {
      sqlParams.push(COLUMNS.FIELD);
      sqlParams.push(fields[i]);
      fieldParams += ', ?';
    }

    sqlParams = sqlParams.concat(fields);

    sqlSelect = new SqlBuilder().select(2).from(1).where(fields.length, 'OR')
      .toString();

    sql = ('SELECT ' + COLUMNS.FIELD + ', ' + COLUMNS.VALUE +
    ' FROM ( ' + sqlSelect +
    ') inner_table ORDER BY FIELD(' + COLUMNS.FIELD + fieldParams + ')');

    logger.info('sql: ' + sql);

    self.mysqlReadConn.query(
      sql,
      sqlParams,
      function (err, result) {
        if (err) {
          if (err.code !== ERRORS.NON_EXISTENT_TABLE) {
            self.emit('error', {
              error: 'mysql', message: err.message,
              redisKey: redisHashKey
            });
          }

          return cb(null, []);
        }

        var arrayResult = [], i, fieldValues = [];

        for (i = 0; i < result.length; i++) {
          fieldValues.push(result[i].field);
          fieldValues.push(result[i].value);
          arrayResult.push(result[i].value);
        }

        if (arrayResult.length === 0) {
          return cb(null, []);
        }

        async.parallel(
          [
            function (mysqlCb) {
              mysqlCb(null, arrayResult);
            },
            function (copy2RedisCb) {
              self.redisConn.hmset(redisHashKey, fieldValues,
                function (err, result) {
                  if (err) {
                    return copy2RedisCb(err);
                  }

                  logger.info('MySQL to Redis copied result: ' + result);
                  copy2RedisCb();
                });
            }
          ],
          function (err, result) {
            if (err) {
              return cb(err);
            }

            cb(null, result[0]);
          });
      });
  });
};

HashCommands.prototype.hgetall = function (hashKey, cb) {

  if (!hashKey) {
    return cb('Incomplete HGETALL parameters');
  }
  if (is.not.string(hashKey)) {
    return cb('HGETALL `hashKey` parameter must be a string');
  }

  var self = this, hashTableName, sql, redisHashKey =
    myUtil.prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

  this.redisConn.hgetall(redisHashKey, function (err, result) {

    if (err) {
      return cb(err);
    }
    if (is.existy(result) && is.not.empty(result)) {
      return cb(null, result);
    }

    hashTableName =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_');

    sql = new SqlBuilder().select(2).from(1)
      .orderBy({field: 'ASC'}).toString();

    logger.info('sql: ' + sql);

    self.mysqlReadConn.query(
      sql,
      [
        COLUMNS.FIELD,
        COLUMNS.VALUE,
        hashTableName
      ],
      function (err, result) {
        if (err) {
          if (err.code !== ERRORS.NON_EXISTENT_TABLE) {
            self.emit('error', {
              error: 'mysql', message: err.message,
              redisKey: redisHashKey
            });
          }

          return cb(null, {});
        }

        var i, temp, objResults = {};

        if (Array.isArray(result)) {
          for (i = 0; i < result.length; i++) {
            if (is.object(result[i])) {
              for (temp in result[i]) {
                if (result[i].hasOwnProperty(temp)) {
                  objResults[result[i].field] = result[i].value;
                }
              }
            }
          }
        }

        if (is.empty(objResults)) {
          return cb(null, {});
        }

        async.parallel(
          [
            function (mysqlCb) {
              mysqlCb(null, objResults);
            },
            function (copy2RedisCb) {
              self.redisConn.hmset(redisHashKey, objResults,
                function (err, result) {
                  if (err) {
                    return copy2RedisCb(err);
                  }

                  logger.info('MySQL to Redis copied result: ' + result);
                  copy2RedisCb();
                });
            }
          ],
          function (err, result) {
            if (err) {
              return cb(err);
            }

            cb(null, result[0]);
          });
      });
  });
};

HashCommands.prototype.hexists = function (hashKey, field, cb) {

  if (!field) {
    return cb('Incomplete HEXISTS parameters');
  }
  if (is.not.string(hashKey)) {
    return cb('HEXISTS `hashKey` must be a string');
  }
  if (is.not.string(field)) {
    return cb('HEXISTS `field` must be a string');
  }

  var self = this, redisHashKey =
    myUtil.prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

  this.redisConn.hexists(redisHashKey, field, function (err, result) {

    if (err) {
      return cb(err);
    }
    if (result) {
      return cb(null, result);
    }

    var hashTableName =
        myUtil.prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_'),

      sql = new SqlBuilder().select(2).from(1).where(1).toString();

    self.mysqlReadConn.query(
      sql,
      [
        COLUMNS.FIELD,
        COLUMNS.VALUE,
        hashTableName,
        COLUMNS.FIELD,
        field
      ],
      function (err, result) {
        if (err) {
          if (err.code !== ERRORS.NON_EXISTENT_TABLE) {
            self.emit('error', {
              error: 'mysql', message: err.message,
              redisKey: redisHashKey
            });
          }

          return cb(null, 0);
        }

        var value = result.length > 0 ? 1 : 0;

        if (value === 0) {
          return cb(null, 0);
        }

        async.parallel(
          [
            function (mysqlCb) {
              mysqlCb(null, value);
            },
            function (copy2RedisCb) {
              self.redisConn.hset(redisHashKey, result[0].field, result[0].value,
                function (err, result) {
                  if (err) {
                    return copy2RedisCb(err);
                  }

                  logger.info('MySQL to Redis copied result: ' + result);
                  copy2RedisCb();
                });
            }
          ],
          function (err, result) {
            if (err) {
              return cb(err);
            }

            cb(null, result[0]);
          });
      });
  });
};

HashCommands.prototype.hdel = function (hashKey, fields, cb) {

  if (!fields) {
    cb('Incomplete HDEL parameters');
  } else if (is.not.string(hashKey)) {
    cb('HDEL `hashKey`parameter must be a string');
  } else if (is.not.array(fields)) {
    cb('HDEL `fields` parameter must be an array');
  } else {

    var self = this, redisHashKey =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

    this.redisConn.hmget(redisHashKey, fields, function (err, result) {
      if (err) {
        cb(err);
      } else {

        var originalValue = result;

        self.redisConn.hdel(redisHashKey, fields, function (err, result) {
          if (err) {
            cb(err);
          } else {

            cb(null, result);

            var i, sql, sqlParams = [], hashTableName =
              myUtil.prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_');

            for (i = 0; i < fields.length; i++) {
              if (fields.hasOwnProperty(i)) {
                sqlParams.push(COLUMNS.FIELD);
                sqlParams.push(fields[i]);
              }
            }

            sql = new SqlBuilder().deleteFrom(hashTableName).where(fields.length)
              .toString();

            self.mysqlWriteConn.query(
              sql,
              sqlParams,
              function (err, result) {
                if (err) {
                  if (err.code === ERRORS.NON_EXISTENT_TABLE) {
                    cb(null, 0);
                  } else {
                    self.emit('error', {
                      error: 'mysql', message: err.message,
                      redisKey: redisHashKey
                    });
                  }

                  /* for rollback purposes */
                  self.redisConn.hmset(redisHashKey, originalValue, function (err, result) {
                    if (err) {
                      self.emit('error', {
                        error: 'redis', message: err,
                        redisKey: redisHashKey
                      });
                    } else {
                      logger.warn('Redis HDEL rollback via HMSET: ' + result);
                    }
                  });
                } else {
                  logger.info('Redis HDEL MySQL result: ' + result.affectedRows);
                }
              }
            );
          }
        });
      }
    });
  }
};

if (typeof module !== 'undefined' &&
  module.exports) {
  module.exports = HashCommands;
}
