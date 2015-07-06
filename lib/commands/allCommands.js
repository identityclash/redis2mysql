/*
 * Copyright (c) 2015.
 */
'use strict';

var async = require('async')
  , util = require('util')
  , is = require('is_js')
  , lazy = require('lazy.js')
  , EventEmitter = require('events')
  , logger = require('winston')
  , SqlBuilder = require('../util/sqlBuilder');

var TABLE_EXPIRY = 'expiry'
  , COLUMNS = {
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

function AllCommands() {
  if (!(this instanceof AllCommands)) {
    return new AllCommands();
  }
}

util.inherits(AllCommands, EventEmitter);

AllCommands.prototype.exists = function (key, cb) {

  if (!(key)) {
    return cb('Incomplete EXISTS parameter(s)');
  }
  if (is.not.string(key)) {
    return cb('EXISTS `key` parameter must be a string');
  }

  var self = this;

  this.redisConn.exists(key, function (err, result) {

    if (err) {
      return cb(err);
    }
    if (result > 0) {
      return cb(null, result);
    }

    var sql, sqlParams = [], dataArray, tableName, tablePrefix, tableKey;

    if (key.indexOf(':') > -1) {
      dataArray = key.split(':');
      if (dataArray.length > 0) {
        tablePrefix = dataArray[0];
        tableName = dataArray[0] + '_' + dataArray[1];
        if (dataArray.length > 2) {
          tableKey = dataArray[2];
        }
      }
    }

    /* Starting retrieval process of existing data in MySQL */
    // for Redis string datatype only
    if (is.existy(tableKey) &&
      tablePrefix === self.options.custom.datatypePrefix.string) {
      sql = new SqlBuilder().select(1).from(1).where(1).toString();
      sqlParams = [
        COLUMNS.VALUE,
        tableName,
        COLUMNS.KEY,
        tableKey
      ];
    } else if (tablePrefix === self.options.custom.datatypePrefix.list) {
      sql = new SqlBuilder().select(1).from(1).orderBy({time_sequence: 'ASC'})
        .toString();
      sqlParams = [COLUMNS.VALUE, tableName];
    } else if (tablePrefix === self.options.custom.datatypePrefix.set) {
      sql = new SqlBuilder().select(1).from(1).toString();
      sqlParams = [COLUMNS.MEMBER, tableName];
    } else if (tablePrefix === self.options.custom.datatypePrefix.sortedSet) {
      sql = new SqlBuilder().select(2).from(1).toString();
      sqlParams = [COLUMNS.SCORE, COLUMNS.MEMBER, tableName];
    } else if (tablePrefix === self.options.custom.datatypePrefix.hash) {
      sql = new SqlBuilder().select(2).from(1).toString();
      sqlParams = [COLUMNS.FIELD, COLUMNS.VALUE, tableName];
    } else {
      return cb(null, 0);
    }

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
              redisKey: key
            });
          }

          return cb(null, 0); // return nothing from MySQL since table cannot be read
        }

        var mysqlCount = is.existy(result) && is.existy(result.length) &&
          result.length > 0 ? 1 : 0,

          mysqlResult = result;

        if (mysqlCount === 0) {
          return cb(null, mysqlCount);
        }

        async.parallel([
          function (mysqlCb) {
            mysqlCb(null, mysqlCount);
          },
          function (copy2RedisCb) {

            var i, array = [];

            /* Start insertion process into Redis from MySQL's data */
            if (tablePrefix === self.options.custom.datatypePrefix.string) {
              self.redisConn.set(key, mysqlResult[0].value,
                function (err, result) {
                  if (err) {
                    return copy2RedisCb(err);
                  }

                  logger.info('MySQL to Redis copied result: ' + result);
                  return copy2RedisCb();
                });
            }
            if (tablePrefix === self.options.custom.datatypePrefix.list) {
              for (i = 0; i < mysqlResult.length; i++) {
                array.push(mysqlResult[i].value);
              }

              self.redisConn.lpush(key, array, function (err, result) {
                if (err) {
                  return copy2RedisCb(err);
                }

                logger.info('MySQL to Redis copied result: ' + result);
                return copy2RedisCb();
              });
            }
            if (tablePrefix === self.options.custom.datatypePrefix.set) {
              for (i = 0; i < mysqlResult.length; i++) {
                array.push(mysqlResult[i].member);
              }

              self.redisConn.sadd(key, array, function (err, result) {
                if (err) {
                  return copy2RedisCb(err);
                }

                logger.info('MySQL to Redis copied result: ' + result);
                return copy2RedisCb();
              });
            }
            if (tablePrefix === self.options.custom.datatypePrefix.sortedSet) {
              for (i = 0; i < mysqlResult.length; i++) {
                array.push(mysqlResult[i].score);
                array.push(mysqlResult[i].member);
              }

              self.redisConn.zadd(key, array, function (err, result) {
                if (err) {
                  return copy2RedisCb(err);
                }

                logger.info('MySQL to Redis copied result: ' + result);
                return copy2RedisCb();
              });
            }
            if (tablePrefix === self.options.custom.datatypePrefix.hash) {
              for (i = 0; i < mysqlResult.length; i++) {
                array.push(mysqlResult[i].field);
                array.push(mysqlResult[i].value);
              }

              self.redisConn.hmset(key, array, function (err, result) {
                if (err) {
                  return copy2RedisCb(err);
                }

                logger.info('MySQL to Redis copied result: ' + result);
                return copy2RedisCb();
              });
            }
          }
        ], function (err, result) {
          if (err) {
            return cb(err);
          }

          return cb(null, result[0]);
        });
      }
    );
  });
};

AllCommands.prototype.del = function (keys, cb) {

  if (!keys) {
    cb('DEL `keys` parameter must be a string or an array');
  } else {

    var self = this, items = [];

    if (is.string(keys)) {
      items.push(keys);
    } else {
      items = keys;
    }

    this.redisConn.del(keys, function (err, result) {
      if (err) {
        cb(err);
      } else {

        logger.info('Redis DEL result: ' + result);
        cb(null, result); // number of keys that were removed

        async.each(items, function (item, cbEachItem) {

          var lazySeq, dataArray, tableName, tableKey;

          self.redisConn.type(item, function (err, result) {
            if (err) {
              cbEachItem(err);
            } else {
              if (lazy(item).contains(':')) {
                lazySeq = lazy(item).split(':');

                dataArray = lazySeq.toArray();
                if (dataArray.length > 0) {
                  tableName = dataArray[0] + '_' + dataArray[1];
                  if (is.string(result)) {
                    tableKey = dataArray[2];
                  } else {
                    tableKey = undefined;
                  }
                }
              }

              if (tableName) {
                async.series([
                  function (firstCb) {
                    if (tableKey === undefined) {
                      firstCb(); // do nothing for non-string
                    } else {
                      var sqlDeleteTable =
                        new SqlBuilder().deleteFrom('??').where(1).toString();
                      self.mysqlWriteConn.query(sqlDeleteTable,
                        [
                          tableName,
                          COLUMNS.KEY,
                          tableKey
                        ],
                        function (err, result) {
                          if (is.not.existy(err)) {
                            logger.info('Redis DEL MySQL row ' + tableKey +
                              ' deletion ' + result.affectedRows + ' ');
                            firstCb();
                          } else if (err.code === ERRORS.NON_EXISTENT_TABLE) {
                            logger.warn('Table does not exist: ' + tableName);
                          } else {
                            firstCb(err);
                          }
                        }
                      );
                    }
                  },
                  function (secondCb) {

                    var _dropTable = function () {
                      self.mysqlWriteConn.query(
                        'DROP TABLE IF EXISTS ' + tableName,
                        function (err) {
                          if (err) {
                            secondCb(err);
                          } else {
                            logger.info('Redis DEL MySQL dropped table ' +
                              tableName + ' ');
                            secondCb();
                          }
                        }
                      );
                    };

                    /* Auto-drop table if any type other than string */
                    if (tableKey === undefined) {
                      _dropTable();
                    } else {
                      /* Drop table for type string */
                      self.mysqlReadConn.query(
                        'SELECT EXISTS (SELECT 1 FROM ?? LIMIT 1) AS has_rows',
                        tableName,
                        function (err, result) {
                          if (err) {
                            secondCb(err);
                          } else if (result[0].has_rows === 0) {
                            _dropTable();
                          } else {
                            logger.warn('MySQL Table still has existing data ');
                            secondCb();
                          }
                        }
                      );
                    }
                  }
                ], function (err) { // no need to return result
                  if (err) {
                    cbEachItem(err);
                  } else {
                    cbEachItem();
                  }
                });
              } else {
                cbEachItem();
              }
            }
          });
        }, function (err) {
          if (err) {
            self.emit('error', {
              error: 'mysql', message: err.message,
              redisKey: keys
            });
          }
        });
      }
    });
  }
};

AllCommands.prototype.rename = function (key, newKey, cb) {

  var self = this, lazySeqOld, lazySeqNew, dataArrayOld, dataArrayNew, prefixOld,
    prefixNew, tableNameOld, tableNameNew, tableKeyOld, tableKeyNew,
    prefixOldExists = false, prefixNewExists = true, tempPrefix, type = '';

  if (!(key && newKey)) {
    cb('Incomplete RENAME parameter(s)');
  } else if (is.not.string(key) || is.not.string(newKey)) {
    cb('RENAME `key` and `newKey` parameters must both be strings');
  } else {
    if (lazy(key).contains(':')) {
      lazySeqOld = lazy(key).split(':');

      dataArrayOld = lazySeqOld.toArray();
      if (dataArrayOld.length > 0) {
        prefixOld = dataArrayOld[0];
        tableNameOld = dataArrayOld[0] + '_' + dataArrayOld[1];
      }
    }

    if (lazy(newKey).contains(':')) {
      lazySeqNew = lazy(newKey).split(':');

      dataArrayNew = lazySeqNew.toArray();
      if (dataArrayNew.length > 0) {
        prefixNew = dataArrayNew[0];
        tableNameNew = dataArrayNew[0] + '_' + dataArrayNew[1];
      }
    }

    if (self.options.custom.datatypePrefix) {
      for (tempPrefix in self.options.custom.datatypePrefix) {
        if (self.options.custom.datatypePrefix.hasOwnProperty(tempPrefix)) {
          if (self.options.custom.datatypePrefix[tempPrefix] === prefixOld) {
            prefixOldExists = true;
            type = tempPrefix;
          }
          if (self.options.custom.datatypePrefix[tempPrefix] === prefixNew) {
            prefixNewExists = true;
          }
        }
      }
    }

    if (is.empty(prefixOld) || is.empty(prefixNew) ||
      (prefixOld !== prefixNew) || !prefixOld || !prefixNew) {
      cb('Both the old and the new table prefixes should be the same and ' +
        'both one of them should not be empty');
    } else if (dataArrayOld.length !== dataArrayNew.length) {
      cb('There should be an equal number of involvement of prefix and/or ' +
        'type and/or key between the former table name and the latter name');
    } else if (dataArrayOld.length === dataArrayNew.length) {

      this.redisConn.rename(key, newKey, function (err, result) {
        if (err) {
          cb(err);
        } else {

          cb(null, result);

          tableKeyOld = is.empty(dataArrayOld[2]) ? undefined : dataArrayOld[2];
          tableKeyNew = is.empty(dataArrayNew[2]) ? undefined : dataArrayNew[2];

          var sqlRenameTable = 'RENAME TABLE ?? TO ??';

          async.series(
            [
              function (firstCb) {
                if (tableNameOld === tableNameNew) {
                  firstCb();
                } else if (type === 'string') {
                  /* for string, two-step process for renaming both type and key */
                  async.series([
                      function (innerFirstCb) {
                        var sqlCreateNewTable =
                          'CREATE TABLE IF NOT EXISTS ' + self.mysqlWriteConn.escapeId(tableNameNew) +
                          '(' +
                          self.mysqlWriteConn.escapeId(COLUMNS.KEY) + ' VARCHAR(255) PRIMARY KEY, ' +
                          COLUMNS.VALUE + ' VARCHAR(255), ' +
                          COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
                          COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
                          ') ';

                        self.mysqlWriteConn.query(
                          sqlCreateNewTable,
                          function (err) {
                            if (err) {
                              innerFirstCb(err);
                            } else {
                              logger.info('Redis RENAME new table ' +
                                tableNameNew + ' creation result');
                              innerFirstCb();
                            }
                          }
                        );
                      },
                      function (innerSecondCb) {
                        self.mysqlWriteConn.beginTransaction(function (err) {
                          if (err) {
                            innerSecondCb(err);
                          } else {

                            logger.info('Redis RENAME MySQL string ' +
                              'begin transaction');

                            var sqlCopy = new SqlBuilder().insert(tableNameNew)
                              .select(['old.*']).from([tableNameOld + ' old '])
                              .where(1).toString();

                            logger.info('sqlCopy: ' + sqlCopy);

                            self.mysqlWriteConn.query(
                              sqlCopy,
                              [
                                COLUMNS.KEY,
                                tableKeyOld
                              ],
                              function (err, result) {
                                if (err) {
                                  self.mysqlWriteConn.rollback(function (err) {
                                    if (err) {
                                      innerSecondCb(err);
                                    } else {
                                      logger.warn('Redis RENAME MySQL string ' +
                                        'rollback');
                                      innerSecondCb();
                                    }
                                  });
                                } else {
                                  logger.info('Redis RENAME MySQL string row ' +
                                    'copy (to new table) result: ' + result.message);

                                  var sqlDeleteOld =
                                    new SqlBuilder().deleteFrom(tableNameOld)
                                      .where(1).toString();

                                  self.mysqlWriteConn.query(
                                    sqlDeleteOld,
                                    [
                                      COLUMNS.KEY,
                                      tableKeyOld
                                    ],
                                    function (err, result) {
                                      if (err) {
                                        innerSecondCb(err);
                                      } else {
                                        logger.info('Redis RENAME MySQL string ' +
                                          'old row deletion result: ' + result.affectedRows);

                                        self.mysqlWriteConn.commit(function (err) {
                                          if (err) {
                                            logger.warn('Redis RENAME MySQL ' +
                                              'commit issue');

                                            self.mysqlWriteConn.rollback(function (err) {
                                              if (err) {
                                                innerSecondCb(err);
                                              } else {
                                                logger.warn('Redis RENAME MySQL ' +
                                                  'string rollback');
                                                innerSecondCb();
                                              }
                                            });
                                          } else {
                                            logger.info('Redis RENAME MySQL ' +
                                              'string commit');
                                            innerSecondCb();
                                          }
                                        });
                                      }
                                    }
                                  );
                                }
                              }
                            );
                          }
                        });
                      }
                    ],
                    function (err) {
                      if (err) {
                        firstCb(err);
                      } else {
                        firstCb();
                      }
                    });
                } else {

                  self.mysqlWriteConn.query(
                    sqlRenameTable,
                    [
                      tableNameOld,
                      tableNameNew
                    ],
                    function (err) { // `result` not used
                      if (err) {
                        firstCb(err);
                      } else {
                        logger.info('Redis RENAME table MySQL result: OK'); // no message, only OkPacket
                        firstCb();
                      }
                    }
                  );
                }
              },
              function (secondCb) {
                if (tableKeyOld === tableKeyNew) {
                  secondCb();
                } else if (is.existy(tableKeyOld) && is.existy(tableKeyNew) &&
                  tableKeyOld !== tableKeyNew) {

                  /* update key changes for string */
                  var sqlUpdateTable =
                    new SqlBuilder().update(tableNameNew).set(1).where(1).toString();

                  logger.info('sqlUpdateTable: ' + sqlUpdateTable);

                  self.mysqlWriteConn.query(
                    sqlUpdateTable,
                    [
                      COLUMNS.KEY,
                      tableKeyNew,
                      COLUMNS.KEY,
                      tableKeyOld
                    ],
                    function (err, result) {
                      if (err) {
                        secondCb(err);

                        /* for rollback purposes */
                        self.mysqlWriteConn.query(
                          sqlRenameTable,
                          [
                            tableNameNew,
                            tableNameOld
                          ],
                          function (err) { // result not used
                            if (err) {
                              secondCb(err);
                            } else {
                              logger.warn('Redis RENAME table rollback MySQL result: OK'); // no message, only OkPacket
                              secondCb();
                            }
                          }
                        );
                      } else {
                        logger.info('Redis RENAME key MySQL result: ' + result.message);
                        secondCb();
                      }
                    }
                  );
                } else {
                  secondCb();
                }
              },
              function (thirdCb) {
                var sqlUpdateExpiryTable = new SqlBuilder().update(TABLE_EXPIRY)
                  .set(1).where(1).toString();

                self.mysqlWriteConn.query(
                  sqlUpdateExpiryTable,
                  [
                    COLUMNS.KEY,
                    newKey,
                    COLUMNS.KEY,
                    key
                  ],
                  function (err) {
                    if (is.not.existy(err)) {
                      thirdCb();
                    } else if (err.code === ERRORS.NON_EXISTENT_TABLE) {
                      logger.warn('Table does not exist: ' + TABLE_EXPIRY);
                      thirdCb();
                    } else {
                      thirdCb(err);
                    }
                  });
              }
            ],
            function (err) {
              if (err) {
                self.emit('error', {
                  error: 'mysql', message: err.message, redisKey: [key, newKey]
                });

                self.redisConn.rename(newKey, key, function (err, result) {
                  if (err) {
                    self.emit('error', {
                      error: 'redis', message: err, redisKey: [newKey, key]
                    });
                  } else {
                    logger.warn('Redis RENAME rollback: ' + result);
                  }
                });

                var sqlUpdateExpiryTable = new SqlBuilder().update(TABLE_EXPIRY)
                  .set(1).where(1).toString();

                logger.info('sql: ' + sqlUpdateExpiryTable);

                self.mysqlWriteConn.query(
                  sqlUpdateExpiryTable,
                  [
                    COLUMNS.KEY,
                    key,
                    COLUMNS.KEY,
                    newKey
                  ],
                  function (err, result) {
                    if (err) {
                      self.emit('error', {
                        error: 'mysql', message: err.message,
                        redisKey: [newKey, key]
                      });
                    } else {
                      logger.warn('Redis RENAME rollback in table `expiry` ' +
                        'result: ' + result.message);
                    }
                  }
                );
              }
            });
        }
      });
    }
  }
};

AllCommands.prototype.expire = function (key, seconds, cb) {

  if (!(key && seconds)) {
    cb('Incomplete EXPIRE parameter(s)');
  } else if (is.not.string(key)) {
    cb('EXPIRE `key`parameter must be a string');
  } else if (is.not.decimal(seconds)) {

    var self = this,

      sqlCreateExpiryTable =
        'CREATE TABLE IF NOT EXISTS ' + TABLE_EXPIRY + ' ' +
        '(' +
        self.mysqlWriteConn.escapeId(COLUMNS.KEY) + ' VARCHAR(255) PRIMARY KEY, ' +
        COLUMNS.EXPIRY_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
        COLUMNS.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
        COLUMNS.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
        ') ';

    async.series(
      [
        function (createTblCb) {
          logger.info('Created table, SQL Create: ' + sqlCreateExpiryTable);
          self.mysqlWriteConn.query(
            sqlCreateExpiryTable,
            function (err) {
              if (err) {
                return createTblCb(err);
              }
              createTblCb();
            });
        },
        function (redisCb) {
          self.redisConn.expire(key, seconds, function (err, result) {
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

        var sql =
            'INSERT IGNORE INTO ?? (??, ??) ' +
            'VALUES ' +
            '( ' +
            '?,  ' +
            '(SELECT DATE_ADD(NOW(3), INTERVAL ? SECOND) from DUAL) ' +
            ')' +
            'ON DUPLICATE KEY UPDATE ?? = VALUES (??)',

          sqlParams = [
            TABLE_EXPIRY,
            COLUMNS.KEY,
            COLUMNS.EXPIRY_DT,
            key,
            seconds,
            COLUMNS.EXPIRY_DT,
            COLUMNS.EXPIRY_DT
          ];

        logger.info('SQL for insert / update: ' + sql);
        logger.info('SQL for insert / update parameters: ' + sqlParams.toString());

        self.mysqlWriteConn.query(
          sql,
          sqlParams,
          function (err, result) {
            if (err) {
              self.emit('error', {
                error: 'mysql', message: err.message,
                redisKey: key
              });
            } else {
              logger.info('Redis EXPIRE MySQL result: ' + result.affectedRows);
            }
          });
      });
  }
};

AllCommands.prototype.quit = function () {

  this.redisConn.quit();

  var self = this;
  this.mysqlReadConn.end(function (err) {
    logger.info('Ending id ' + self.mysqlReadConn.threadId + '\n');
    if (err) {
      self.emit('error', {error: 'mysql', message: err.message});
    }
  });

  this.mysqlWriteConn.end(function (err) {
    logger.info('Ending id ' + self.mysqlWriteConn.threadId + '\n');
    if (err) {
      self.emit('error', {error: 'mysql', message: err.message});
    }
  });
};

if (typeof module !== 'undefined' &&
  module.exports) {
  module.exports = AllCommands;
}
