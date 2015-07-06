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

function SortedSetCommands() {
  if (!(this instanceof SortedSetCommands)) {
    return new SortedSetCommands();
  }
}

util.inherits(SortedSetCommands, EventEmitter);

SortedSetCommands.prototype.zadd = function (key, scoreMembers, cb) {

  if (!(key && scoreMembers)) {
    cb('Incomplete ZADD parameter(s)');
  } else if (is.not.array(scoreMembers) || is.odd(scoreMembers.length)) {
    cb('ZADD `scoreMembers` parameter must be an array containing sequentially ' +
      'at least a score and member pair, where the score is a floating point ' +
      'and the member is a string');
  } else {

    var self = this,

      redisKey =
        myUtil.prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], ':'),

      sortedSetTableName =
        myUtil.prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], '_'),

      sqlCreateSortedSetTable =
        'CREATE TABLE IF NOT EXISTS ' + self.mysqlWriteConn.escapeId(sortedSetTableName) +
        '(' +
        COLUMNS.SCORE + ' DOUBLE, ' +
        COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
        COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
        COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
        ') ';

    async.series(
      [
        function (createTblCb) {
          logger.info('Created table, SQL Create: ' + sqlCreateSortedSetTable);
          self.mysqlWriteConn.query(
            sqlCreateSortedSetTable,
            function (err) {
              if (err) {
                return createTblCb(err);
              }
              createTblCb();
            });
        },
        function (redisCb) {
          logger.info('redis input: ' + redisKey + ' scoreMembers: ' + scoreMembers);
          self.redisConn.zadd(redisKey, scoreMembers, function (err, result) {
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

        var tableColumns = [COLUMNS.SCORE, COLUMNS.MEMBER], sql, sqlParams = [],
          members = [], i;

        sql = new SqlBuilder().insertIgnore('??', tableColumns)
          .values(tableColumns.length, scoreMembers.length / 2)
          .onDuplicate({score: 'VALUES(' + COLUMNS.SCORE + ')'}).toString();

        sqlParams.push(sortedSetTableName);

        /* for the values of the INSERT phrase */
        for (i = 0; i < scoreMembers.length; i++) {
          if (is.even(i)) {
            members.push(scoreMembers[i]);
          }
          sqlParams.push(scoreMembers[i]);
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
              self.redisConn.zrem(redisKey, members, function (err, result) {
                if (err) {
                  self.emit('error', {
                    error: 'redis', message: err,
                    redisKey: redisKey
                  });
                } else {
                  logger.warn('Redis ZADD rollback via ZREM: ' + result);
                }
              });
            } else if (is.existy(result)) {
              logger.info('Redis ZADD MySQL result: ' + result.message);
            }
          });
      });
  }
};

SortedSetCommands.prototype.zincrby = function (key, incrDecrValue, member, cb) {

  if (!(key && incrDecrValue && member)) {
    cb('Incomplete ZINCRBY parameter(s)');
  } else if (!is.string(key)) {
    cb('ZINCRBY `key` parameter must be a string');
  } else if (!is.decimal(incrDecrValue)) {
    cb('ZINCRBY `incrDecrValue` parameter must be a floating point');
  } else if (!is.string(member)) {
    cb('ZINCRBY `member` parameter must be a string');
  } else {

    var self = this, tableColumns = [COLUMNS.SCORE, COLUMNS.MEMBER], redisKey,
      sortedSetTableName, sqlCreateSortedSetTable, sql, sqlParams = [];

    redisKey =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], ':');

    sortedSetTableName =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], '_');

    sqlCreateSortedSetTable =
      'CREATE TABLE IF NOT EXISTS ' + self.mysqlWriteConn.escapeId(sortedSetTableName) +
      '(' +
      COLUMNS.SCORE + ' DOUBLE, ' +
      COLUMNS.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
      COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
      COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
      ') ';

    async.series(
      [
        function (createTblCb) {
          logger.info('Created table, SQL Create: ' + sqlCreateSortedSetTable);
          self.mysqlWriteConn.query(
            sqlCreateSortedSetTable,
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
              COLUMNS.SCORE,
              sortedSetTableName,
              COLUMNS.MEMBER,
              member
            ];

          self.mysqlReadConn.query(
            sqlQueryLatest,
            sqlQueryLatestParams,
            function (err, latestSqlResult) {
              if (err) {
                return redisCb(err);
              }

              var latestScore = latestSqlResult.length === 0 ? 0.0 :
                latestSqlResult[0].score;

              logger.info('redis input: ' + redisKey + ' incrDecrValue: ' + incrDecrValue + ' member: ' + member);

              self.redisConn.multi().zadd(redisKey, latestScore, member)
                .zincrby(redisKey, incrDecrValue, member)
                .exec(function (err, result) {
                  if (err) {
                    return redisCb(err);
                  }

                  redisCb(null, result[1][1]);
                });
            });
        }
      ],
      function (err, result) {
        if (err) {
          return cb(err);
        }

        cb(null, result[1]);

        sql = new SqlBuilder().insertIgnore('??', tableColumns)
          .values(tableColumns.length, 1)
          .onDuplicate({score: COLUMNS.SCORE + ' + VALUES(' + COLUMNS.SCORE + ')'})
          .toString();

        sqlParams.push(sortedSetTableName);

        /* for the values of the INSERT phrase */
        sqlParams.push(incrDecrValue);
        sqlParams.push(member);

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
              self.redisConn.zincrby(redisKey, -incrDecrValue, member,
                function (err, result) {

                  if (err) {
                    self.emit('error', {
                      error: 'redis', message: err,
                      redisKey: redisKey
                    });
                  } else {
                    logger.warn('Redis ZINCRBY rollback: ' + result);
                  }
                });
            } else if (is.existy(result)) {
              logger.info('Redis ZINCRBY MySQL result: ' + result.affectedRows);
            }
          });
      });
  }
};

SortedSetCommands.prototype.zscore = function (key, member, cb) {

  if (!(key && member)) {
    cb('Incomplete ZSCORE parameter(s)');
  } else if (!is.string(key)) {
    cb('ZSCORE `key` parameter must be a string');
  } else if (!is.string(member)) {
    cb('ZSCORE `member` parameter must be a string');
  } else {

    var self = this, redisKey =
      myUtil.prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], ':');

    this.redisConn.zscore(redisKey, member, function (err, result) {
      if (err) {
        cb(err);
      } else if (is.existy(result)) {
        cb(null, result);
      } else {

        var sql,
          sortedSetTableName =
            myUtil.prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], '_');

        sql = 'SELECT score FROM ?? WHERE member = ? ';

        self.mysqlReadConn.query(
          sql,
          [
            sortedSetTableName,
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

              return cb(null, null);
            }

            var score = is.existy(result) && result.length > 0 ?
              result[0].score : null;

            if (is.not.existy(score)) {
              return cb(null, null);
            }

            async.parallel(
              [
                function (mysqlCb) {
                  mysqlCb(null, score.toString());
                },
                function (copy2RedisCb) {
                  self.redisConn.zadd(redisKey, score, member,
                    function (err, result) {
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

          }
        );
      }
    });
  }
};

SortedSetCommands.prototype.zrank = function (key, member, cb) {

  if (!(key && member)) {
    cb('Incomplete ZRANK parameter(s)');
  } else if (is.not.string(key)) {
    cb('ZRANK `key` parameter must be a string');
  } else if (is.not.string(member)) {
    cb('ZRANK `member` parameter must be a string');
  } else {

    var self = this,

      redisKey =
        myUtil.prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], ':');

    this.redisConn.zrank(redisKey, member, function (err, result) {

      if (err) {
        cb(err);
      } else if (is.existy(result)) {
        cb(null, result);
      } else {

        var offset = 1, startingCounter = -1,

          sortedSetTableName =
            myUtil.prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], '_'),

          sqlSelect = 'SELECT inner_table.redis_index ' +
            ' FROM ' +
            '(  ' +
            '  SELECT @i := @i + (' + offset + ') AS redis_index, ' +
            '         ??, ' +
            '         ?? ' +
            '    FROM ?? , (SELECT @i := ' + startingCounter + ') counter ' +
            '  ORDER BY ?? ASC ' +
            ') inner_table ' +
            'WHERE inner_table.member = ?',

          sqlParams =
            [
              COLUMNS.SCORE,
              COLUMNS.MEMBER,
              sortedSetTableName,
              COLUMNS.SCORE,
              member
            ];

        logger.info('sql: ' + sqlSelect);
        logger.info('sqlParams: ' + sqlParams);

        self.mysqlReadConn.query(
          sqlSelect,
          sqlParams,
          function (err, result) {
            if (err) {
              if (err.code !== ERRORS.NON_EXISTENT_TABLE) {
                self.emit('error', {
                  error: 'mysql', message: err.message,
                  redisKey: redisKey
                });
              }

              return cb(null, null);
            }

            var value = is.existy(result[0]) ? result[0].redis_index : null;

            if (is.not.existy(value)) {
              return cb(null, null);
            }

            async.parallel(
              [
                function (mysqlCb) {
                  mysqlCb(null, value);
                },
                function (copy2RedisCb) {

                  var sqlAllScoreMembers =
                      new SqlBuilder().select(2).from(1).toString(),

                    sqlAllScoreMembersParams = [
                      COLUMNS.SCORE,
                      COLUMNS.MEMBER,
                      sortedSetTableName
                    ];

                  self.mysqlReadConn.query(
                    sqlAllScoreMembers,
                    sqlAllScoreMembersParams,
                    function (err, result) {
                      if (err) {
                        return copy2RedisCb(err);
                      }

                      var i, scoreMembers = [];
                      for (i = 0; i < result.length; i++) {
                        scoreMembers.push(result[i].score);
                        scoreMembers.push(result[i].member);
                      }

                      self.redisConn.zadd(redisKey, scoreMembers,
                        function (err, result) {
                          if (err) {
                            return copy2RedisCb(err);
                          }

                          logger.info('MySQL to Redis copied result: ' + result);
                          return copy2RedisCb();
                        });

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
      }
    });
  }
};

SortedSetCommands.prototype.zrangebyscore = function (key, min, max, withscores, limit,
                                                      offset, count, cb) {

  var self = this, pattern = new RegExp(/(^\(?[0-9\.])|(^\-?inf)/),
    redisParams = [], redisKey;

  if (!(key && min && max)) {
    return cb('Incomplete ZSCORE parameter(s)');
  }
  if (is.not.string(key)) {
    return cb('ZSCORE `key` parameter must be a string');
  }
  if (!pattern.test(min)) {
    return cb('ZSCORE `min` parameters must be floating point OR `inf` OR `-inf`');
  }
  if (!pattern.test(max)) {
    return cb('ZSCORE `max` parameters must be floating point OR `inf` OR `-inf`');
  }
  if (limit && (is.not.integer(offset) || is.not.integer(count))) {
    return cb('ZSCORE `offset` and `count` parameters are optional.  When LIMIT exists, ' +
      'both `offset` and `count` must also exist');
  }

  if (withscores) {
    redisParams.push('withscores');
  }
  if (limit) {
    redisParams.push('limit');
  }
  if (is.integer(offset) && is.integer(count)) {
    redisParams.push(offset);
    redisParams.push(count);
  }

  redisKey =
    myUtil.prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], ':');

  this.redisConn.zrangebyscore(redisKey, min, max, redisParams,
    function (err, result) {

      if (err) {
        return cb(err);
      }
      if (is.existy(result) && is.not.empty(result)) {
        return cb(null, result);
      }

      var sql = '',
        sortedSetTableName =
          myUtil.prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], '_');

      if (withscores) {
        sql += 'SELECT ' + COLUMNS.MEMBER + ', ' + COLUMNS.SCORE + ' FROM ' +
          sortedSetTableName + ', ';
      } else {
        sql += 'SELECT ' + COLUMNS.MEMBER + ' FROM ' +
          sortedSetTableName + ', ';
      }

      sql += '(SELECT MIN(' + COLUMNS.SCORE + ') AS minimum ' +
        'FROM ' + sortedSetTableName +
        ') min_score, ';
      sql += '(SELECT MAX(' + COLUMNS.SCORE + ') AS maximum ' +
        'FROM ' + sortedSetTableName +
        ') max_score ';

      if (is.startWith(min, '(')) {
        sql += ('WHERE score > ' + min.replace('(', ''));
      } else if (min === '-inf') {
        sql += 'WHERE score >= min_score.minimum';
      } else if (min === 'inf') {
        sql += 'WHERE score >= max_score.maximum';
      } else {
        sql += ('WHERE score >= ' + min);
      }

      if (is.startWith(max, '(')) {
        sql += (' AND score < ' + max.replace('(', ''));
      } else if (max === '-inf') {
        sql += ' AND score <= min_score.minimum';
      } else if (max === 'inf') {
        sql += ' AND score <= max_score.maximum';
      } else {
        sql += (' AND score <= ' + max);
      }

      sql += ' ORDER BY ' + COLUMNS.SCORE + ' ASC ';

      if (limit) {
        sql += (' LIMIT ' + count + ' OFFSET ' + offset);
      }

      logger.info('sql: ' + sql);
      self.mysqlReadConn.query(
        sql,
        function (err, result) {
          if (err) {
            if (err.code !== ERRORS.NON_EXISTENT_TABLE) {
              self.emit('error', {
                error: 'mysql', message: err.message,
                redisKey: redisKey
              });
            }

            return cb(null, []);
          }

          var arrayResult = [], i, tempKey;

          for (i = 0; i < result.length; i++) {
            for (tempKey in result[i]) {
              if (result[i].hasOwnProperty(tempKey)) {
                arrayResult.push(result[i][tempKey].toString());
              }
            }
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

                var sqlAllScoreMembers =
                    new SqlBuilder().select(2).from(1).toString(),

                  sqlAllScoreMembersParams = [
                    COLUMNS.SCORE,
                    COLUMNS.MEMBER,
                    sortedSetTableName
                  ];

                self.mysqlReadConn.query(
                  sqlAllScoreMembers,
                  sqlAllScoreMembersParams,
                  function (err, result) {
                    if (err) {
                      return copy2RedisCb(err);
                    }

                    var i, scoreMembers = [];
                    for (i = 0; i < result.length; i++) {
                      scoreMembers.push(result[i].score.toString());
                      scoreMembers.push(result[i].member);
                    }

                    self.redisConn.zadd(redisKey, scoreMembers,
                      function (err, result) {
                        if (err) {
                          return copy2RedisCb(err);
                        }

                        logger.info('MySQL to Redis copied result: ' + result);
                        return copy2RedisCb();
                      });

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

SortedSetCommands.prototype.zrevrangebyscore = function (key, max, min, withscores, limit,
                                                         offset, count, cb) {

  var pattern = new RegExp(/(^\(?[0-9\.])|(^\-?inf)/),
    self = this, redisKey, redisParams = [];

  if (!(key && min && max)) {
    return cb('Incomplete ZSCORE parameter(s)');
  }
  if (is.not.string(key)) {
    return cb('ZSCORE `key` parameter must be a string');
  }
  if (!pattern.test(min)) {
    return cb('ZSCORE `min` parameters must be floating point OR `inf` OR `-inf`');
  }
  if (!pattern.test(max)) {
    return cb('ZSCORE `max` parameters must be floating point OR `inf` OR `-inf`');
  }
  if (limit && (is.not.integer(offset) || is.not.integer(count))) {
    return cb('ZSCORE `offset` and `count` parameters are optional.  When LIMIT exists, ' +
      'both `offset` and `count` must also exist');
  }

  if (withscores) {
    redisParams.push('withscores');
  }
  if (limit) {
    redisParams.push('limit');
  }
  if (is.integer(offset) && is.integer(count)) {
    redisParams.push(offset);
    redisParams.push(count);
  }

  redisKey =
    myUtil.prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], ':');

  this.redisConn.zrevrangebyscore(redisKey, max, min, redisParams,
    function (err, result) {

      if (err) {
        return cb(err);
      }
      if (is.existy(result) && is.not.empty(result)) {
        return cb(null, result);
      }

      var sql = '',
        sortedSetTableName =
          myUtil.prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], '_');

      if (withscores) {
        sql += 'SELECT ' + COLUMNS.MEMBER + ', ' + COLUMNS.SCORE + ' FROM ' +
          sortedSetTableName + ', ';
      } else {
        sql += 'SELECT ' + COLUMNS.MEMBER + ' FROM ' +
          sortedSetTableName + ', ';
      }

      sql += '(SELECT MIN(' + COLUMNS.SCORE + ') AS minimum ' +
        'FROM ' + sortedSetTableName +
        ') min_score, ';
      sql += '(SELECT MAX(' + COLUMNS.SCORE + ') AS maximum ' +
        'FROM ' + sortedSetTableName +
        ') max_score ';

      if (is.startWith(min, '(')) {
        sql += ('WHERE score > ' + min.replace('(', ''));
      } else if (min === '-inf') {
        sql += 'WHERE score >= min_score.minimum';
      } else if (min === 'inf') {
        sql += 'WHERE score >= max_score.maximum';
      } else {
        sql += ('WHERE score >= ' + min);
      }

      if (is.startWith(max, '(')) {
        sql += (' AND score < ' + max.replace('(', ''));
      } else if (max === '-inf') {
        sql += ' AND score <= min_score.minimum';
      } else if (max === 'inf') {
        sql += ' AND score <= max_score.maximum';
      } else {
        sql += (' AND score <= ' + max);
      }

      sql += ' ORDER BY ' + COLUMNS.SCORE + ' DESC, ' + COLUMNS.MEMBER + ' DESC';

      if (limit) {
        sql += (' LIMIT ' + count + ' OFFSET ' + offset);
      }

      logger.info('sql: ' + sql);

      self.mysqlReadConn.query(
        sql,
        function (err, result) {
          if (err) {
            if (err.code !== ERRORS.NON_EXISTENT_TABLE) {
              self.emit('error', {
                error: 'mysql', message: err.message,
                redisKey: redisKey
              });
            }

            return cb(null, []);
          }

          var arrayResult = [], i, tempKey;

          for (i = 0; i < result.length; i++) {
            for (tempKey in result[i]) {
              if (result[i].hasOwnProperty(tempKey)) {
                arrayResult.push(result[i][tempKey].toString());
              }
            }
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

                var sqlAllScoreMembers =
                    new SqlBuilder().select(2).from(1).toString(),

                  sqlAllScoreMembersParams = [
                    COLUMNS.SCORE,
                    COLUMNS.MEMBER,
                    sortedSetTableName
                  ];

                self.mysqlReadConn.query(
                  sqlAllScoreMembers,
                  sqlAllScoreMembersParams,
                  function (err, result) {
                    if (err) {
                      return copy2RedisCb(err);
                    }

                    var i, scoreMembers = [];
                    for (i = 0; i < result.length; i++) {
                      scoreMembers.push(result[i].score.toString());
                      scoreMembers.push(result[i].member);
                    }

                    self.redisConn.zadd(redisKey, scoreMembers,
                      function (err, result) {
                        if (err) {
                          return copy2RedisCb(err);
                        }

                        logger.info('MySQL to Redis copied result: ' + result);
                        return copy2RedisCb();
                      });

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

if (typeof module !== 'undefined' &&
  module.exports) {
  module.exports = SortedSetCommands;
}
