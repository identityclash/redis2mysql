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

function ListCommands() {
  if (!(this instanceof ListCommands)) {
    return new ListCommands();
  }
}

util.inherits(ListCommands, EventEmitter);

ListCommands.prototype.lpush = function (key, values, cb) {

  if (!(key && values)) {
    cb('Incomplete LPUSH parameters');
  } else if (is.not.string(values) && is.not.array(values)) {
    cb('LPUSH `values` parameter must be a string OR ' +
      'an array of strings');
  } else {

    var self = this, redisKey, arrayValues = [], time, listTableName,
      sqlCreateListTable;

    if (is.string(values)) {
      arrayValues.push(values);
    } else if (is.array(values)) {
      arrayValues = arrayValues.concat(values);
    }

    redisKey =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.list, key], ':');

    listTableName =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.list, key], '_');

    sqlCreateListTable =
      'CREATE TABLE IF NOT EXISTS ' + self.mysqlWriteConn.escapeId(listTableName) +
      ' (' +
      COLUMNS.SEQ + ' DOUBLE PRIMARY KEY, ' +
      COLUMNS.VALUE + ' VARCHAR(255), ' +
      COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
      COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
      ') ';

    async.series(
      [
        function (createTblCb) {
          logger.info('Created table, SQL Create: ' + sqlCreateListTable);
          self.mysqlWriteConn.query(
            sqlCreateListTable,
            function (err) {
              if (err) {
                return createTblCb(err);
              }
              createTblCb();
            });
        },
        function (redisCb) {
          logger.info('redis input: ' + redisKey + ' values : ' + values);
          self.redisConn.multi().time().lpush(redisKey, values).exec(function (err, result) {
            if (err) {
              redisCb(err);
            } else {

              if (result[0][1][1].length > 0) { // result from Redis TIME command
                /* UNIX time in sec + time elapsed in microseconds converted to seconds */
                time = result[0][1][0] + (result[0][1][1] / 1000000);
              }

              redisCb(null, result[1][1]); // return result from LPUSH to callback
            }
          });
        }
      ],
      function (err, res) {
        if (err) {
          return cb(err);
        }

        cb(null, res[1]);

        if (res[1] > 0) {

          var tableColumns = [COLUMNS.SEQ, COLUMNS.VALUE], i, sqlParams = [],

            sql = new SqlBuilder().insert('??', tableColumns)
              .values(tableColumns.length, arrayValues.length).toString();

          sqlParams.push(listTableName);
          for (i = 0; i < arrayValues.length; i++) {
            sqlParams.push(parseFloat(time) + (i / 100000));
            sqlParams.push(arrayValues[i]);
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
                self.redisConn.lpop(redisKey, function (err, result) {
                  if (err) {
                    self.emit('error', {
                      error: 'redis', message: err,
                      redisKey: redisKey
                    });
                  } else {
                    logger.warn('Redis LPUSH rollback via LPOP: ' + result);
                  }
                });
              } else {
                logger.info('Redis LPUSH MySQL result ' + result.affectedRows + ' value: ' + values);
              }
            });
        }
      });
  }
};

ListCommands.prototype.lindex = function (key, redisIndex, cb) {

  if (!key && is.not.existy(redisIndex)) {
    cb('Incomplete LINDEX parameter(s)');
  } else if (is.not.string(key)) {
    cb('LINDEX `key` parameter must be a string');
  } else if (is.not.integer(redisIndex)) {
    cb('LINDEX `index` parameter must be an integer');
  } else {

    var self = this, redisKey =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.list, key], ':');

    this.redisConn.lindex(redisKey, redisIndex, function (err, result) {
      if (err) {
        cb(err);
      } else if (is.existy(result)) {
        cb(null, result);
      } else {

        var sql, startingCounter, offset, order,
          listTableName =
            myUtil.prefixAppender([self.options.custom.datatypePrefix.list, key], '_');

        if (redisIndex >= 0) { // zero or positive index
          offset = 1;
          startingCounter = -1;
          order = 'DESC';
        } else {
          offset = -1;
          startingCounter = 0;
          order = 'ASC';
        }

        sql = 'SELECT inner_table.value, inner_table.redis_index ' +
          ' FROM ' +
          '(  ' +
          '  SELECT @i := @i + (' + offset + ') AS redis_index, ??, ?? ' +
          '    FROM ?? , (SELECT @i := ?) counter ' +
          '  ORDER BY ?? ' + order +
          ') inner_table ' +
          'WHERE inner_table.redis_index <= ?';

        logger.info('sql: ' + sql);

        self.mysqlReadConn.query(
          sql,
          [
            COLUMNS.VALUE,
            COLUMNS.SEQ,
            listTableName,
            startingCounter,
            COLUMNS.SEQ,
            redisIndex
          ],
          function (err, mysqlResult) {
            if (err) {
              if (err.code !== ERRORS.NON_EXISTENT_TABLE) {
                self.emit('error', {
                  error: 'mysql', message: err.message,
                  redisKey: redisKey
                });
              }

              return cb(null, null);
            }

            var targetIndex = redisIndex >= 0 && is.existy(mysqlResult) ?
              mysqlResult.length - 1 : 0,

              returnValue = is.existy(mysqlResult) && mysqlResult.length > 0 &&
              mysqlResult[targetIndex].redis_index === redisIndex && // specified index has to exist in the result set
              is.existy(mysqlResult[targetIndex].value) ?
                mysqlResult[targetIndex].value : null;

            if (is.not.existy(returnValue)) {
              return cb(null, null);
            }

            async.parallel(
              [
                function (mysqlCb) {
                  mysqlCb(null, returnValue);
                },
                /* Compare each element from MySQL */
                function (compareCb) {

                  var previousLindexResult,

                    insertDirection = 'BEFORE'; // defaulted to BEFORE

                  async.forEachOfSeries(mysqlResult,
                    function (item, index, forEachOfSeriesCb) {

                      // given the row's Redis index (in sequence) from MySQL,
                      // the row value must equate to the result found in Redis for the same index
                      self.redisConn.lindex(redisKey, index,
                        function (err, lindexResult) {
                          if (err) {
                            return forEachOfSeriesCb(err);
                          }

                          // get the latest non-null value retrieved from lindex
                          if (is.existy(lindexResult)) {
                            previousLindexResult = lindexResult;
                          } else {
                            insertDirection = 'AFTER'; // change to AFTER only when lindexResult becomes null
                          }

                          console.log('index: ' + index + ' @ ' + lindexResult);

                          if (item.value === lindexResult) {
                            return forEachOfSeriesCb();
                          }

                          /* if item.value != lindexResult */
                          self.redisConn.linsert(redisKey, insertDirection,
                            previousLindexResult, item.value, function (err) {
                              if (err) {
                                return forEachOfSeriesCb(err);
                              }

                              forEachOfSeriesCb();
                            });
                        });
                    }, function (err) {
                      if (err) {
                        return compareCb(err);
                      }

                      compareCb();
                    });
                }
              ], function (err, result) {
                if (err) {
                  return cb(err);
                }

                return cb(null, result[0]);
              });
          });
      }
    });
  }
};

ListCommands.prototype.lset = function (key, redisIndex, value, cb) {

  if (!key && is.not.existy(redisIndex) && is.not.existy(value)) {
    cb('Incomplete LSET parameter(s)');
  } else if (is.not.string(key)) {
    cb('LSET `key` parameter must be a string');
  } else if (is.not.integer(redisIndex)) {
    cb('LSET `redisIndex` parameter must be an integer');
  } else if (is.not.string(value)) {
    cb('LSET `value` parameter must be a string');
  } else {

    var self = this, redisKey =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.list, key], ':');

    this.redisConn.lindex(redisKey, redisIndex, function (err, result) {
      if (err) {
        cb(err);
      } else {

        var originalValue = result;

        self.redisConn.lset(redisKey, redisIndex, value, function (err, result) {
          if (err) {
            cb(err);
          } else {

            cb(null, result);

            var sql, selectSql, startingCounter, offset, order,
              listTableName =
                myUtil.prefixAppender([self.options.custom.datatypePrefix.list, key], '_');

            if (redisIndex >= 0) { // zero or positive index
              offset = 1;
              startingCounter = -1;
              order = 'DESC';
            } else {
              offset = -1;
              startingCounter = 0;
              order = 'ASC';
            }

            selectSql = 'SELECT inner_table.mysql_sequence ' +
              ' FROM ' +
              '(  ' +
              '  SELECT @i := @i + (' + offset + ') AS redis_index, ' +
              '         ??, ' +
              '         ?? AS mysql_sequence ' +
              '    FROM ?? , (SELECT @i := ?) counter ' +
              '  ORDER BY ?? ' + order +
              ') inner_table ' +
              'WHERE inner_table.redis_index = ?';

            sql = new SqlBuilder().update('??').set(1)
              .where({time_sequence: '(' + selectSql + ') '}, 'AND').toString();

            logger.info('sql: ' + sql);

            self.mysqlWriteConn.query(
              sql,
              [
                listTableName,
                COLUMNS.VALUE,
                value,
                COLUMNS.VALUE,
                COLUMNS.SEQ,
                listTableName,
                startingCounter,
                COLUMNS.SEQ,
                redisIndex
              ],
              function (err, result) {
                if (err) {
                  self.emit('error', {
                    error: 'mysql', message: err.message,
                    redisKey: redisKey
                  });

                  /* for rollback purposes */
                  self.redisConn.lset(redisKey, redisIndex, originalValue, function (err, result) {
                    if (err) {
                      self.emit('error', {
                        error: 'redis', message: err,
                        redisKey: redisKey
                      });
                    } else {
                      logger.warn('Redis LSET rollback: ' + result);
                    }
                  });
                } else if (is.existy(result)) {
                  logger.info('Redis LSET MySQL result: ' + result.message);
                }
              }
            );
          }
        });
      }
    });
  }
};

ListCommands.prototype.rpop = function (key, cb) {

  if (!(key)) {
    cb('Incomplete RPOP parameters');
  } else if (is.not.string(key)) {
    cb('RPOP `key` parameter must be a string');
  } else {

    var self = this,

      redisKey =
        myUtil.prefixAppender([self.options.custom.datatypePrefix.list, key], ':');

    this.redisConn.rpop(redisKey, function (err, result) {

      if (err) {
        cb(err);
      } else if (is.existy(result)) {

        cb(null, result);

        var originalValue = result,

          listTableName =
            myUtil.prefixAppender([self.options.custom.datatypePrefix.list, key], '_'),

          maxSql = new SqlBuilder().select(['MIN(' + COLUMNS.SEQ + ') AS minmo '])
            .from(1).toString(),

          sql = new SqlBuilder()
              .deleteFrom(self.mysqlWriteConn.escapeId(listTableName)).toString() +
            ' WHERE ' + COLUMNS.SEQ + ' = (SELECT minmo FROM (' + maxSql + ') b)',

          sqlParams = [listTableName];

        logger.info('sql: ' + sql);
        logger.info('sqlParams: ' + sqlParams);

        self.mysqlWriteConn.query(
          sql,
          sqlParams,
          function (err, result) {
            if (err) {
              self.emit('error', {
                error: 'mysql', message: err.message, redisKey: redisKey
              });

              /* for rollback purposes */
              self.redisConn.rpush(redisKey, originalValue,
                function (err, result) {
                  if (err) {
                    self.emit('error', {
                      error: 'redis', message: err, redisKey: redisKey
                    });
                  } else {
                    logger.warn('Redis RPOP MySQL rollback via RPUSH result: ' + result);
                  }
                }
              );
            } else {
              logger.info('Redis RPOP MySQL result: ' + result.affectedRows);
            }
          }
        );
      } else {
        cb(null, null);
      }
    });
  }
};

if (typeof module !== 'undefined' &&
  module.exports) {
  module.exports = ListCommands;
}
