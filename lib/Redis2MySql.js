/*
 * Copyright (c) 2015.
 */
'use strict';

var async = require('async'),
  util = require('util'),
  is = require('is_js'),
  lazy = require('lazy.js'),
  EventEmitter = require('events'),
  Redis = require('ioredis'),
  mysql = require('mysql'),
  logger = require('winston'),
  SqlBuilder = require('./SqlBuilder'),
  TABLE_EXPIRY = 'expiry',
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
  PREFIXES = [
    'string',
    'list',
    'set',
    'sortedSet',
    'hash'
  ],
  ERRORS = {
    NON_EXISTENT_TABLE: 'ER_NO_SUCH_TABLE'
  };

/**
 * Module functions
 */
function Redis2MySql(options) {

  var self = this, key, comparedKey, i;

  if (!(this instanceof Redis2MySql)) {
    return new Redis2MySql(options);
  }

  if (options.custom.datatypePrefix) {
    for (i = 0; i < PREFIXES.length; i++) {
      if (!lazy(options.custom.datatypePrefix).keys().contains(PREFIXES[i])) {
        throw new Error('All database table prefixes should be defined by the ' +
          'user.');
      }
    }

    for (key in options.custom.datatypePrefix) {
      if (options.custom.datatypePrefix.hasOwnProperty(key)) {
        for (comparedKey in options.custom.datatypePrefix) {
          if (key !== comparedKey &&
            options.custom.datatypePrefix[key] ===
            options.custom.datatypePrefix[comparedKey]) {

            throw new Error('There are duplicate user-defined database ' +
              'prefixes. Please make all prefixes unique.');
          }
        }
      }
    }
  }

  if (is.not.existy(options.mysql.user)) {
    throw new Error('Please specify the username');
  }

  if (is.not.existy(options.custom.schemaName) &&
    is.not.existy(options.custom.schemaCharset) &&
    is.not.existy(options.mysql.database)) {
    throw new Error('Please specify the database');
  }

  // Redis connection
  this.redisConn = new Redis(options.redis);

  if (is.not.existy(options.mysql.host) && is.not.existy(options.mysql.port)) {
    options.mysql.host = '';
    options.mysql.port = '';
  }

  options.mysql.multipleStatements = 'true';

  // MySQL read connection
  this.mysqlReadConn = mysql.createConnection(options.mysql);
  this.mysqlReadConn.connect(function (err) {
    if (err) {
      throw err;
    }
    logger.info('connected as id ' + self.mysqlReadConn.threadId + '\n');
  });

  // MySQL write connection
  this.mysqlWriteConn = mysql.createConnection(options.mysql);
  this.mysqlWriteConn.connect(function (err) {
    if (err) {
      throw err;
    }
    logger.info('connected as id ' + self.mysqlWriteConn.threadId + '\n');
  });

  options.mysql.database = options.mysql.database || options.custom.schemaName;
  options.mysql.charset = options.mysql.charset || options.custom.schemaCharset;

  this.options = options;

  // error emissions
  this.redisConn.on('error', function (err) {
    self.emit('error', {error: 'redis', message: err.message});
  });

  this.mysqlReadConn.on('error', function (err) {
    self.emit('error', {error: 'mysql', message: err.message});
  });

  this.mysqlWriteConn.on('error', function (err) {
    self.emit('error', {error: 'mysql', message: err.message});
  });
}

util.inherits(Redis2MySql, EventEmitter);

Redis2MySql.prototype.quit = function () {

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

Redis2MySql.prototype.incr = function (type, key, cb) {

  if (!key) {
    cb('Incomplete SET parameter(s)');
  } else if (is.not.string(key)) {
    cb('INCR `key` parameter must be a string');
  } else {

    var self = this, redisKey, tableName, sqlCreateTable, sql,
      sqlParams,
      tableColumns = [self.mysqlWriteConn.escapeId(COLUMNS.KEY), COLUMNS.VALUE];

    redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.string, type, key], ':');

    tableName =
      _prefixAppender([self.options.custom.datatypePrefix.string, type], '_');

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

Redis2MySql.prototype.set = function (type, param1, param2, cb) {

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
        _prefixAppender([self.options.custom.datatypePrefix.string, type, param1[0]], ':');

      key = param1[0];
      value = param1[1] || '';
    } else {

      redisKey =
        _prefixAppender([self.options.custom.datatypePrefix.string, type, param1], ':');

      key = param1;
      value = param2 || '';
    }

    stringTableName =
      _prefixAppender([self.options.custom.datatypePrefix.string, type], '_');

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
            redisCb(null, result);
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

Redis2MySql.prototype.get = function (type, key, cb) {

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
      _prefixAppender([self.options.custom.datatypePrefix.string, type, key], ':');

  this.redisConn.get(redisKey, function (err, result) {

    if (err) {
      return cb(err);
    }
    if (is.existy(result)) {
      return cb(null, result);
    }

    var stringTableName =
        _prefixAppender([self.options.custom.datatypePrefix.string, type], '_'),

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

Redis2MySql.prototype.exists = function (key, cb) {

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
      cb(null, 0);
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

          cb(null, result[0]);
        });
      }
    );
  });
};

Redis2MySql.prototype.del = function (keys, cb) {

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

Redis2MySql.prototype.lpush = function (key, values, cb) {

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
      _prefixAppender([self.options.custom.datatypePrefix.list, key], ':');

    listTableName =
      _prefixAppender([self.options.custom.datatypePrefix.list, key], '_');

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

Redis2MySql.prototype.lindex = function (key, redisIndex, cb) {

  if (!key && is.not.existy(redisIndex)) {
    cb('Incomplete LINDEX parameter(s)');
  } else if (is.not.string(key)) {
    cb('LINDEX `key` parameter must be a string');
  } else if (is.not.integer(redisIndex)) {
    cb('LINDEX `index` parameter must be an integer');
  } else {

    var self = this, redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.list, key], ':');

    this.redisConn.lindex(redisKey, redisIndex, function (err, result) {
      if (err) {
        cb(err);
      } else if (is.existy(result)) {
        cb(null, result);
      } else {

        var sql, startingCounter, offset, order,
          listTableName =
            _prefixAppender([self.options.custom.datatypePrefix.list, key], '_');

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

                cb(null, result[0]);
              });
          });
      }
    });
  }
};

Redis2MySql.prototype.lset = function (key, redisIndex, value, cb) {

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
      _prefixAppender([self.options.custom.datatypePrefix.list, key], ':');

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
                _prefixAppender([self.options.custom.datatypePrefix.list, key], '_');

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

Redis2MySql.prototype.rpop = function (key, cb) {

  if (!(key)) {
    cb('Incomplete RPOP parameters');
  } else if (is.not.string(key)) {
    cb('RPOP `key` parameter must be a string');
  } else {

    var self = this,

      redisKey =
        _prefixAppender([self.options.custom.datatypePrefix.list, key], ':');

    self.redisConn.rpop(redisKey, function (err, result) {

      if (err) {
        cb(err);
      } else if (is.existy(result)) {

        cb(null, result);

        var originalValue = result,

          listTableName =
            _prefixAppender([self.options.custom.datatypePrefix.list, key], '_'),

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

Redis2MySql.prototype.sadd = function (key, members, cb) {

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
      _prefixAppender([self.options.custom.datatypePrefix.set, key], ':');

    ordSetTableName =
      _prefixAppender([self.options.custom.datatypePrefix.set, key], '_');

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

Redis2MySql.prototype.srem = function (key, members, cb) {

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
      _prefixAppender([self.options.custom.datatypePrefix.set, key], ':');

    self.redisConn.srem(redisKey, arrayMembers, function (err, result) {

      if (err) {
        cb(err);
      } else {

        cb(null, result);

        ordSetTableName =
          _prefixAppender([self.options.custom.datatypePrefix.set, key], '_');

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

Redis2MySql.prototype.smembers = function (key, cb) {

  if (!key) {
    return cb('Incomplete SMEMBERS parameter');
  }
  if (is.not.string(key)) {
    return cb('SMEMBERS `key` parameter must be a string');
  }

  var self = this,

    redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.set, key], ':');

  this.redisConn.smembers(redisKey, function (err, result) {

    if (err) {
      return cb(err);
    }
    if (is.existy(result) && is.not.empty(result)) {
      return cb(null, result);
    }

    var ordSetTableName =
        _prefixAppender([self.options.custom.datatypePrefix.set, key], '_'),

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

Redis2MySql.prototype.sismember = function (key, member, cb) {

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
      _prefixAppender([self.options.custom.datatypePrefix.set, key], ':');

  this.redisConn.sismember(redisKey, member, function (err, result) {

    if (err) {
      return cb(err);
    }
    if (result) {
      return cb(null, result);
    }

    var sql = new SqlBuilder().select(1).from(1).where(1).toString(),

      ordSetTableName =
        _prefixAppender([self.options.custom.datatypePrefix.set, key], '_');

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

Redis2MySql.prototype.scard = function (key, cb) {

  if (!key) {
    return cb('Incomplete SCARD parameter');
  }
  if (is.not.string(key)) {
    return cb('SCARD `key` parameter must be a string');
  }

  var self = this,

    redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.set, key], ':');

  this.redisConn.scard(redisKey, function (err, result) {

    if (err) {
      return cb(err);
    }
    if (result) {
      return cb(null, result);
    }

    var ordSetTableName =
        _prefixAppender([self.options.custom.datatypePrefix.set, key], '_'),

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

Redis2MySql.prototype.zadd = function (key, scoreMembers, cb) {

  if (!(key && scoreMembers)) {
    cb('Incomplete ZADD parameter(s)');
  } else if (is.not.array(scoreMembers) || is.odd(scoreMembers.length)) {
    cb('ZADD `scoreMembers` parameter must be an array containing sequentially ' +
      'at least a score and member pair, where the score is a floating point ' +
      'and the member is a string');
  } else {

    var self = this,

      redisKey =
        _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], ':'),

      sortedSetTableName =
        _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], '_'),

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

Redis2MySql.prototype.zincrby = function (key, incrDecrValue, member, cb) {

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
      _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], ':');

    sortedSetTableName =
      _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], '_');

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

Redis2MySql.prototype.zscore = function (key, member, cb) {

  if (!(key && member)) {
    cb('Incomplete ZSCORE parameter(s)');
  } else if (!is.string(key)) {
    cb('ZSCORE `key` parameter must be a string');
  } else if (!is.string(member)) {
    cb('ZSCORE `member` parameter must be a string');
  } else {

    var self = this, redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], ':');

    this.redisConn.zscore(redisKey, member, function (err, result) {
      if (err) {
        cb(err);
      } else if (is.existy(result)) {
        cb(null, result);
      } else {

        var sql,
          sortedSetTableName =
            _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], '_');

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

Redis2MySql.prototype.zrank = function (key, member, cb) {

  if (!(key && member)) {
    cb('Incomplete ZRANK parameter(s)');
  } else if (is.not.string(key)) {
    cb('ZRANK `key` parameter must be a string');
  } else if (is.not.string(member)) {
    cb('ZRANK `member` parameter must be a string');
  } else {

    var self = this,

      redisKey =
        _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], ':');

    this.redisConn.zrank(redisKey, member, function (err, result) {

      if (err) {
        cb(err);
      } else if (is.existy(result)) {
        cb(null, result);
      } else {

        var offset = 1, startingCounter = -1,

          sortedSetTableName =
            _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], '_'),

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

Redis2MySql.prototype.zrangebyscore = function (key, min, max, withscores, limit,
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
    _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], ':');

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
          _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], '_');

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

Redis2MySql.prototype.zrevrangebyscore = function (key, max, min, withscores, limit,
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
    _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], ':');

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
          _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], '_');

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

Redis2MySql.prototype.hset = function (hashKey, param1, param2, cb) {

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
      _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

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
            _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_'),

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

Redis2MySql.prototype.hmset = function (hashKey, param1, param2, cb) {

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
      _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

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
      _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_');

    /* for rollback purposes, use getset instead of set */
    self.redisConn.hmget(redisHashKey, fields, function (err, result) {
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

Redis2MySql.prototype.hget = function (hashKey, field, cb) {

  if (!field) {
    cb('Incomplete HGET parameters');
  } else if (is.not.string(hashKey)) {
    cb('HGET `hashKey` must be a string');
  } else if (is.not.string(field)) {
    cb('HGET `field` must be a string');
  } else {

    var self = this, hashTableName, redisHashKey =
      _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

    this.redisConn.hget(redisHashKey, field, function (err, result) {
      if (err) {
        cb(err);
      } else if (is.existy(result)) {
        cb(null, result);
      } else {

        hashTableName =
          _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_');

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

Redis2MySql.prototype.hmget = function (hashKey, fields, cb) {

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
    _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

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
      _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_');

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

Redis2MySql.prototype.hgetall = function (hashKey, cb) {

  if (!hashKey) {
    return cb('Incomplete HGETALL parameters');
  }
  if (is.not.string(hashKey)) {
    return cb('HGETALL `hashKey` parameter must be a string');
  }

  var self = this, hashTableName, sql, redisHashKey =
    _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

  this.redisConn.hgetall(redisHashKey, function (err, result) {

    if (err) {
      return cb(err);
    }
    if (is.existy(result) && is.not.empty(result)) {
      return cb(null, result);
    }

    hashTableName =
      _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_');

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

Redis2MySql.prototype.hexists = function (hashKey, field, cb) {

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
    _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

  this.redisConn.hexists(redisHashKey, field, function (err, result) {

    if (err) {
      return cb(err);
    }
    if (result) {
      return cb(null, result);
    }

    var hashTableName =
        _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_'),

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

Redis2MySql.prototype.hdel = function (hashKey, fields, cb) {

  if (!fields) {
    cb('Incomplete HDEL parameters');
  } else if (is.not.string(hashKey)) {
    cb('HDEL `hashKey`parameter must be a string');
  } else if (is.not.array(fields)) {
    cb('HDEL `fields` parameter must be an array');
  } else {

    var self = this, redisHashKey =
      _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

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
              _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_');

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

Redis2MySql.prototype.rename = function (key, newKey, cb) {

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

Redis2MySql.prototype.expire = function (key, seconds, cb) {

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

Redis2MySql.prototype.createUseSchema = function () {

  var self = this,

    sqlCreateSchema =
      'CREATE DATABASE IF NOT EXISTS ' + this.mysqlWriteConn.escapeId(this.options.custom.schemaName) +
      ' CHARACTER SET = ' + this.mysqlWriteConn.escape(this.options.custom.schemaCharset),

    sqlUseSchema =
      'USE ' + this.mysqlWriteConn.escapeId(this.options.custom.schemaName);

  this.mysqlReadConn.query(
    'SELECT DATABASE() AS used FROM DUAL',
    function (err, result) {
      if (err) {
        throw err;
      }
      if (result[0].used === null) {
        async.series([
            function (firstCb) {
              self.mysqlWriteConn.query(
                sqlCreateSchema,
                function (err) {
                  if (err) {
                    firstCb(err);
                  } else {
                    logger.info('Created schema (if not exists) ' +
                      self.options.custom.schemaName);
                    firstCb();
                  }
                }
              );
            },
            function (secondCb) {
              self.mysqlReadConn.query(
                sqlUseSchema,
                function (err) {
                  if (err) {
                    secondCb(err);
                  } else {
                    logger.info('Using ' + self.options.custom.schemaName);
                    secondCb();
                  }

                }
              );
            }
          ],
          function (err) {
            if (err) {
              throw err;
            }
          }
        );
      }
    }
  );
};

function _prefixAppender(prefixes, delimiter) {

  var str = '', prefix;

  for (prefix in prefixes) {
    if (prefixes.hasOwnProperty(prefix)) {
      str += (prefixes[prefix] + delimiter);
    }
  }

  return str.substring(0, str.length - delimiter.length);
}

if (typeof module !== 'undefined' &&
  module.exports) {
  module.exports = Redis2MySql;
}
