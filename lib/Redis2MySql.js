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
  SqlBuilder = require('./SqlBuilder'),
  TABLE_EXPIRY = 'expiry',
  COLUMN = {
    SEQ: 'time_sequence',
    KEY: 'key',
    FIELD: 'field',
    VALUE: 'value',
    MEMBER: 'member',
    SCORE: 'score',
    EXPIRY_DT: 'expiry_date',
    CREATION_DT: 'creation_date',
    LAST_UPDT_DT: 'last_update_date'
  };

/**
 * Module functions
 */
function Redis2MySql(options) {

  var self = this, key, comparedKey;

  if (!(this instanceof Redis2MySql)) {
    return new Redis2MySql(options);
  }

  if (options.custom.datatypePrefix) {
    for (key in options.custom.datatypePrefix) {
      if (options.custom.datatypePrefix.hasOwnProperty(key)) {
        for (comparedKey in options.custom.datatypePrefix) {
          if (key !== comparedKey && options.custom.datatypePrefix[key] === options.custom.datatypePrefix[comparedKey]) {

            throw new Error('There are user-defined database prefixes having ' +
              'the same value. Please make all prefixes unique.');
          }
        }
      }
    }
  }

  this.redisConn = new Redis(options.redis);
  this.redisConn.connect();

  if (options.mysql === undefined) {
    options.mysql = {
      host: '',
      port: ''
    };
  }

  this.mysqlConn = mysql.createConnection(options.mysql);

  this.mysqlConn.connect();

  options.mysql.database = options.mysql.database || options.custom.schemaName;
  options.mysql.charset = options.mysql.charset || options.custom.schema_charset;

  this.options = options;

  // on error emission
  this.redisConn.on('error', function (err) {
    self.emit('error', {error: 'redis', message: err.message});
  });

  this.mysqlConn.on('error', function (err) {
    self.emit('error', {error: 'mysql', message: err.message});
  });
}

util.inherits(Redis2MySql, EventEmitter);

Redis2MySql.prototype.quit = function (cb) {

  this.redisConn.quit();

  this.mysqlConn.end(function (err) {
    cb(err);
  });
};

Redis2MySql.prototype.incr = function (type, key, cb) {

  if (!key) {
    cb('Incomplete SET parameter(s)');
  } else if (is.not.string(key)) {
    cb('INCR `key` parameter must be a string');
  } else {

    var self = this, redisKey, tableName, redisValue;

    redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.string, type, key], ':');

    tableName =
      _prefixAppender([self.options.custom.datatypePrefix.string, type], '_');

    self.redisConn.incr(redisKey, function (err, result) {
      if (err) {
        cb(err);
      } else {
        cb(null, result);

        /* The incremented value from Redis to be used in MySQL commands */
        redisValue = result;

        var COLUMNS = [self.mysqlConn.escapeId(COLUMN.KEY), COLUMN.VALUE],
          sqlCreateTable, sql, sqlParams;

        sqlCreateTable =
          'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(tableName) +
          '(' +
          self.mysqlConn.escapeId(COLUMN.KEY) + ' VARCHAR(255) PRIMARY KEY, ' +
          COLUMN.VALUE + ' VARCHAR(255), ' +
          COLUMN.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
          COLUMN.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
          ') ';

        sql = new SqlBuilder().insert('??', COLUMNS)
          .values(COLUMNS.length).onDuplicate(COLUMNS.length).toString();

        sqlParams = [tableName, key, redisValue, COLUMN.KEY,
          key, COLUMN.VALUE, redisValue];

        _createInsertUpdate(self.mysqlConn, sqlCreateTable, sql,
          sqlParams, function (err, result) {

            if (err) {
              self.emit('error', {
                error: 'mysql', message: err.message,
                redisKey: redisKey
              });

              // for rollback purposes
              self.redisConn.decr(redisKey, function (err, result) {
                if (err) {
                  self.emit('error', {
                    error: 'redis', message: err,
                    redisKey: redisKey
                  });
                } else {
                  console.log('Redis INCR rollback via DECR: ' + result);
                }
              });
            } else {
              console.log('Redis INCR MySQL result: ' + result.affectedRows);
            }
          }
        );
      }
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

    var self = this, redisKey, key, value,
      stringTableName;

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

    /* for rollback purposes, use GETSET instead of SET */
    self.redisConn.getset(redisKey, value, function (err, result) {
      if (err) {
        cb(err);
      } else {

        cb(null, result);

        var COLUMNS = [self.mysqlConn.escapeId(COLUMN.KEY), COLUMN.VALUE],
          sqlCreateStringTable, sql, sqlParams, originalValue = result;

        sqlCreateStringTable =
          'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(stringTableName) +
          '(' +
          self.mysqlConn.escapeId(COLUMN.KEY) + ' VARCHAR(255) PRIMARY KEY, ' +
          COLUMN.VALUE + ' VARCHAR(255), ' +
          COLUMN.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
          COLUMN.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
          ') ';

        sql = new SqlBuilder().insert('??', COLUMNS)
          .values(COLUMNS.length).onDuplicate(COLUMNS.length).toString();

        sqlParams = [stringTableName, key, value, COLUMN.KEY,
          key, COLUMN.VALUE, value];

        _createInsertUpdate(self.mysqlConn, sqlCreateStringTable, sql,
          sqlParams, function (err, result) {
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
                  console.log('Redis SET rollback: ' + result);
                }
              });
            } else {
              console.log('Redis SET MySQL result: ' + result.affectedRows);
            }
          }
        );
      }
    });
  }
};

Redis2MySql.prototype.get = function (type, key, cb) {

  if (!key) {
    cb('Incomplete GET parameters');
  } else if (is.not.string(type)) {
    cb('GET `type` parameter must be a string');
  } else if (is.not.string(key)) {
    cb('GET `key` parameter must be a string');
  } else {

    var self = this, redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.string, type, key], ':');

    this.redisConn.get(redisKey, function (err, result) {
      if (err) {
        cb(err);
      } else if (is.existy(result)) {
        cb(null, result);
      } else {

        var stringTableName =
          _prefixAppender([self.options.custom.datatypePrefix.string, type], '_');

        self.mysqlConn.query(
          new SqlBuilder().select(1).from(1).where(1).toString(),
          [
            COLUMN.VALUE,
            stringTableName,
            COLUMN.KEY,
            key
          ],
          function (err, result) {
            if (err) {
              if (err.code === 'ER_NO_SUCH_TABLE') {
                cb(null, null);
              } else {
                self.emit('error', {
                  error: 'mysql', message: err.message,
                  redisKey: redisKey
                });
              }
            } else if (is.existy(result)) {
              cb(null, is.existy(result[0]) ? result[0].value : null);
            } else {
              cb(null, null);
            }
          }
        );
      }
    });
  }
};

Redis2MySql.prototype.exists = function (type, key, cb) {

  if (!(key && type)) {
    cb('Incomplete EXISTS parameter(s)');
  } else if (is.not.string(type)) {
    cb('EXISTS `type` parameter must be a string');
  } else if (is.not.string(key)) {
    cb('EXISTS `key` parameter must be a string');
  } else {

    var self = this, redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.string, type, key], ':');

    this.redisConn.exists(redisKey, function (err, result) {
      if (err) {
        cb(err);
      } else if (is.existy(undefined)) {
        cb(null, result);
      } else {

        var sql,
          stringTableName =
            _prefixAppender([self.options.custom.datatypePrefix.string, type], '_');

        sql = new SqlBuilder().select(['1']).from(1).where(1).toString();

        self.mysqlConn.query(
          sql,
          [
            stringTableName,
            COLUMN.KEY,
            key
          ],
          function (err, result) {
            if (err) {
              if (err.code === 'ER_NO_SUCH_TABLE') {
                cb(null, 0);
              } else {
                self.emit('error', {
                  error: 'mysql', message: err.message,
                  redisKey: redisKey
                });
              }
            } else if (is.existy(result)) {
              cb(null, result[0] ? 1 : 0);
            } else {
              cb(null, 0);
            }
          }
        );
      }
    });
  }
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
                      self.mysqlConn.query(sqlDeleteTable,
                        [
                          tableName,
                          COLUMN.KEY,
                          tableKey
                        ],
                        function (err, result) {
                          if (err) {
                            firstCb(err);
                          } else {
                            console.log('Redis DEL MySQL row deletion ' +
                              result.affectedRows);
                          }
                        }
                      );
                    }
                  },
                  function (secondCb) {

                    var _dropTable = function () {
                      self.mysqlConn.query(
                        'DROP TABLE IF EXISTS ' + tableName,
                        function (err) {
                          if (err) {
                            secondCb(err);
                          } else {
                            console.log('Redis DEL MySQL dropped table ' +
                              tableName);
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
                      self.mysqlConn.query(
                        'SELECT EXISTS (SELECT 1 FROM ?? LIMIT 1) AS has_rows',
                        tableName,
                        function (err, result) {
                          if (err) {
                            secondCb(err);
                          } else if (result[0].has_rows === 0) {
                            _dropTable();
                          } else {
                            console.log('MySQL Table still has existing data');
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

    var self = this, redisKey, arrayValues = [], time;

    if (is.string(values)) {
      arrayValues.push(values);
    } else if (is.array(values)) {
      arrayValues = arrayValues.concat(values);
    }

    redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.list, key], ':');

    self.redisConn.multi().time().lpush(redisKey, values).exec(function (err, result) {

      if (err) {
        cb(err);
      } else {

        if (result[0][1][1].length > 0) { // result from Redis TIME command
          /* UNIX time in sec + microseconds */
          time = result[0][1][0] + result[0][1][1];
        }

        cb(null, result[1][1]); // return result from LPUSH to callback

        if (result[1][1] > 0) {

          var COLUMNS = [COLUMN.SEQ, COLUMN.VALUE], i, sqlParams = [],

            listTableName =
              _prefixAppender([self.options.custom.datatypePrefix.list, key], '_'),

            sqlCreateListTable =
              'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(listTableName) +
              '(' +
              COLUMN.SEQ + ' BIGINT PRIMARY KEY, ' +
              COLUMN.VALUE + ' VARCHAR(255), ' +
              COLUMN.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMN.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ',

            sql = new SqlBuilder().insert('??', COLUMNS)
              .values(COLUMNS.length, arrayValues.length).toString();

          sqlParams.push(listTableName);
          for (i = 0; i < arrayValues.length; i++) {
            sqlParams.push(time + i);
            sqlParams.push(arrayValues[i]);
          }

          _createInsertUpdate(self.mysqlConn, sqlCreateListTable, sql,
            sqlParams, function (err, result) {

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
                    console.log('Redis LPUSH rollback via LPOP: ' + result);
                  }
                });
              } else {
                console.log('Redis LPUSH MySQL result ' + result.message);
              }
            }
          );
        }
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

        sql = 'SELECT inner_table.value ' +
          ' FROM ' +
          '(  ' +
          '  SELECT @i := @i + (' + offset + ') AS redis_index, ??, ?? ' +
          '    FROM ?? , (SELECT @i := ?) counter ' +
          '  ORDER BY ?? ' + order +
          ') inner_table ' +
          'WHERE inner_table.redis_index = ?';

        self.mysqlConn.query(
          sql,
          [
            COLUMN.VALUE,
            COLUMN.SEQ,
            listTableName,
            startingCounter,
            COLUMN.SEQ,
            redisIndex
          ],
          function (err, result) {
            if (err) {
              if (err.code === 'ER_NO_SUCH_TABLE') {
                cb(null, null);
              } else {
                self.emit('error', {
                  error: 'mysql', message: err.message,
                  redisKey: redisKey
                });
              }
            } else if (is.existy(result)) {
              cb(null, is.existy(result[0]) ? result[0].value : null);
            } else {
              cb(null, null);
            }
          }
        );
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

            self.mysqlConn.query(
              sql,
              [
                listTableName,
                COLUMN.VALUE,
                value,
                COLUMN.VALUE,
                COLUMN.SEQ,
                listTableName,
                startingCounter,
                COLUMN.SEQ,
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
                      console.log('Redis LSET rollback: ' + result);
                    }
                  });
                } else if (is.existy(result)) {
                  console.log('Redis LSET MySQL result: ' + result.message);
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

          maxSql = new SqlBuilder().select(['MIN(' + COLUMN.SEQ + ') AS minmo '])
            .from(1).toString(),

          sql = new SqlBuilder()
              .deleteFrom(self.mysqlConn.escapeId(listTableName)).toString() +
            ' WHERE ' + COLUMN.SEQ + ' = (SELECT minmo FROM (' + maxSql + ') b)',

          sqlParams = [listTableName];

        console.log('sql: ' + sql);
        console.log('sqlParams: ' + sqlParams);

        self.mysqlConn.query(
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
                    console.log('Redis RPOP MySQL rollback via RPUSH result: ' + result);
                  }
                }
              );
            } else {
              console.log('Redis RPOP MySQL result: ' + result.affectedRows);
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

    var self = this, redisKey, arrayMembers = [];

    if (is.string(members)) {
      arrayMembers.push(members);
    } else if (is.array(members)) {
      arrayMembers = arrayMembers.concat(members);
    }

    redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.set, key], ':');

    self.redisConn.sadd(redisKey, members, function (err, result) {

      if (err) {
        cb(err);
      } else {

        cb(null, result);

        var i, COLUMNS = [COLUMN.MEMBER], sql, sqlParams = [], ordSetTableName,
          sqlCreateOrdSetTable;

        ordSetTableName =
          _prefixAppender([self.options.custom.datatypePrefix.set, key], '_');

        sqlCreateOrdSetTable =
          'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(ordSetTableName) +
          '(' +
          COLUMN.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
          COLUMN.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
          COLUMN.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
          ') ';

        sql = new SqlBuilder().insertIgnore('??', COLUMNS)
          .values(COLUMNS.length, arrayMembers.length);

        sqlParams.push(ordSetTableName);

        /* For the values of the INSERT phrase */
        for (i = 0; i < arrayMembers.length; i++) {
          sqlParams.push(arrayMembers[i]);
        }

        _createInsertUpdate(self.mysqlConn, sqlCreateOrdSetTable, sql,
          sqlParams, function (err, result) {

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
                  console.log('Redis SADD rollback via SREM: ' + result);
                }
              });
            } else if (is.existy(result)) {
              console.log('Redis SADD MySQL result: ' + result.message);
            }
          }
        );
      }
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
          sqlParams.push(COLUMN.MEMBER);
          sqlParams.push(arrayMembers[i]);
        }

        self.mysqlConn.query(
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
                  console.log('Redis SREM rollback via SADD: ' + result);
                }
              });
            } else if (is.existy(result)) {
              console.log('Redis SREM MySQL result: ' + result.message);
            }
          }
        );
      }
    });
  }
};

Redis2MySql.prototype.smembers = function (key, cb) {

  if (!key) {
    cb('Incomplete SMEMBERS parameter');
  } else if (is.not.string(key)) {
    cb('SMEMBERS `key` parameter must be a string');
  } else {

    var self = this, redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.set, key], ':');

    this.redisConn.smembers(redisKey, function (err, result) {
      if (err) {
        cb(err);
      } else if (is.existy(result) && is.not.empty(result)) {
        cb(null, result);
      } else {

        var sql,
          ordSetTableName =
            _prefixAppender([self.options.custom.datatypePrefix.set, key], '_');

        sql = new SqlBuilder().select(1).from(1).toString();

        self.mysqlConn.query(
          sql,
          [
            COLUMN.MEMBER,
            ordSetTableName
          ],
          function (err, result) {
            if (err) {
              if (err.code === 'ER_NO_SUCH_TABLE') {
                cb(null, []);
              } else {
                self.emit('error', {
                  error: 'mysql', message: err.message,
                  redisKey: redisKey
                });
              }
            } else {
              var i, members = [];
              for (i = 0; i < result.length; i++) {
                if (is.existy(result[i].member)) {
                  members.push(result[i].member);
                }
              }
              cb(null, members);
            }
          }
        );
      }
    });
  }
};

Redis2MySql.prototype.sismember = function (key, member, cb) {

  if (!key) {
    cb('Incomplete SISMEMBER parameter(s)');
  } else if (is.not.string(key)) {
    cb('SISMEMBER `key` parameter must be a string');
  } else if (is.not.string(member)) {
    cb('SISMEMBER `member` parameter must be a string');
  } else {

    var self = this, redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.set, key], ':');

    this.redisConn.sismember(redisKey, member, function (err, result) {
      if (err) {
        cb(err);
      } else if (result) {
        cb(null, result);
      } else {

        var sql,
          ordSetTableName =
            _prefixAppender([self.options.custom.datatypePrefix.set, key], '_');

        sql = new SqlBuilder().select(1).from(1).where(1).toString();

        self.mysqlConn.query(
          sql,
          [
            COLUMN.MEMBER,
            ordSetTableName,
            COLUMN.MEMBER,
            member
          ],
          function (err, result) {
            if (err) {
              if (err.code === 'ER_NO_SUCH_TABLE') {
                cb(null, 0);
              } else {
                self.emit('error', {
                  error: 'mysql', message: err.message,
                  redisKey: redisKey
                });
              }
            } else if (is.existy(result)) {
              cb(null, result.length);
            } else {
              cb(null, 0);
            }
          }
        );
      }
    });
  }
};

Redis2MySql.prototype.scard = function (key, cb) {

  if (!key) {
    cb('Incomplete SCARD parameter');
  } else if (is.not.string(key)) {
    cb('SCARD `key` parameter must be a string');
  } else {

    var self = this, redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.set, key], ':');

    this.redisConn.scard(redisKey, function (err, result) {
      if (err) {
        cb(err);
      } else if (result) {
        cb(null, result);
      } else {

        var ordSetTableName =
            _prefixAppender([self.options.custom.datatypePrefix.set, key], '_'),

          sql = 'SELECT COUNT(1) AS cnt FROM ??';

        self.mysqlConn.query(sql, ordSetTableName, function (err, result) {
          if (err) {
            if (err.code === 'ER_NO_SUCH_TABLE') {
              cb(null, 0);
            } else {
              self.emit('error', {
                error: 'mysql', message: err.message,
                redisKey: redisKey
              });
            }
          } else if (is.existy(result)) {
            cb(null, is.existy(result[0]) ? result[0].cnt : 0);
          } else {
            cb(null, 0);
          }
        });
      }
    });
  }
};

Redis2MySql.prototype.zadd = function (key, scoreMembers, cb) {

  if (!(key && scoreMembers)) {
    cb('Incomplete ZADD parameter(s)');
  } else if (is.not.array(scoreMembers) || is.odd(scoreMembers.length)) {
    cb('ZADD `scoreMembers` parameter must be an array containing sequentially ' +
      'at least a score and member pair, where the score is a floating point ' +
      'and the member is a string');
  } else {

    var self = this, redisKey;

    redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], ':');

    self.redisConn.zadd(redisKey, scoreMembers, function (err, result) {

      if (err) {
        cb(err);
      } else {

        var COLUMNS = [COLUMN.SCORE, COLUMN.MEMBER], sortedSetTableName,
          sqlCreateSortedSetTable, sql, sqlParams = [], members = [], i;

        cb(null, result);

        sortedSetTableName =
          _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], '_');

        sqlCreateSortedSetTable =
          'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(sortedSetTableName) +
          '(' +
          COLUMN.SCORE + ' DOUBLE, ' +
          COLUMN.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
          COLUMN.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
          COLUMN.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
          ') ';

        sql = new SqlBuilder().insertIgnore('??', COLUMNS)
          .values(COLUMNS.length, scoreMembers.length / 2)
          .onDuplicate({score: 'VALUES(' + COLUMN.SCORE + ')'}).toString();

        sqlParams.push(sortedSetTableName);

        /* for the values of the INSERT phrase */
        for (i = 0; i < scoreMembers.length; i++) {
          if (is.even(i)) {
            members.push(scoreMembers[i]);
          }
          sqlParams.push(scoreMembers[i]);
        }

        _createInsertUpdate(self.mysqlConn, sqlCreateSortedSetTable, sql,
          sqlParams, function (err, result) {

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
                  console.log('Redis ZADD rollback via ZREM: ' + result);
                }
              });
            } else if (is.existy(result)) {
              console.log('Redis ZADD MySQL result: ' + result.message);
            }
          }
        );
      }
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

    var self = this, COLUMNS = [COLUMN.SCORE, COLUMN.MEMBER], redisKey,
      sortedSetTableName, sqlCreateSortedSetTable, sql, sqlParams = [];

    redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], ':');

    self.redisConn.zincrby(redisKey, incrDecrValue, member, function (err, result) {

      if (err) {
        cb(err);
      } else {

        cb(null, result);

        sortedSetTableName =
          _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], '_');

        sqlCreateSortedSetTable =
          'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(sortedSetTableName) +
          '(' +
          COLUMN.SCORE + ' DOUBLE, ' +
          COLUMN.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
          COLUMN.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
          COLUMN.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
          ') ';

        sql = new SqlBuilder().insertIgnore('??', COLUMNS)
          .values(COLUMNS.length, 1)
          .onDuplicate({score: COLUMN.SCORE + ' + VALUES(' + COLUMN.SCORE + ')'});

        sqlParams.push(sortedSetTableName);

        /* for the values of the INSERT phrase */
        sqlParams.push(incrDecrValue);
        sqlParams.push(member);

        _createInsertUpdate(self.mysqlConn, sqlCreateSortedSetTable, sql,
          sqlParams, function (err, result) {

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
                    console.log('Redis ZINCRBY rollback: ' + result);
                  }
                });
            } else if (is.existy(result)) {
              console.log('Redis ZINCRBY MySQL result: ' + result.message);
            }
          }
        );
      }
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
      } else if (is.existy(undefined)) {
        cb(null, result);
      } else {

        var sql,
          ordSetTableName =
            _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], '_');

        sql = 'SELECT score FROM ?? WHERE member = ? ';

        self.mysqlConn.query(
          sql,
          [
            ordSetTableName,
            member
          ],
          function (err, result) {
            if (err) {
              if (err.code === 'ER_NO_SUCH_TABLE') {
                cb(null, null);
              } else {
                self.emit('error', {
                  error: 'mysql', message: err.message,
                  redisKey: redisKey
                });
              }
            } else if (is.existy(result)) {
              cb(null, is.existy(result[0]) ? result[0].score : null);
            } else {
              cb(null, null);
            }
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

          ordSetTableName =
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
              COLUMN.SCORE,
              COLUMN.MEMBER,
              ordSetTableName,
              COLUMN.SCORE,
              member
            ];

        console.log('sql: ' + sqlSelect);
        console.log('sqlParams: ' + sqlParams);

        self.mysqlConn.query(
          sqlSelect,
          sqlParams,
          function (err, result) {
            if (err) {
              self.emit('error', {
                error: 'mysql', message: err.message,
                redisKey: redisKey
              });
            } else {
              cb(null, result);
            }
          }
        );
      }
    });
  }
};

Redis2MySql.prototype.zrangebyscore = function (key, min, max, withscores, limit,
                                                offset, count, cb) {

  var self = this, pattern = new RegExp(/(^\(?[0-9\.])|(^\-?inf)/),
    redisParams = [], redisKey;

  if (!(key && min && max)) {
    cb('Incomplete ZSCORE parameter(s)');
  } else if (is.not.string(key)) {
    cb('ZSCORE `key` parameter must be a string');
  } else if (!pattern.test(min)) {
    cb('ZSCORE `min` parameters must be floating point OR `inf` OR `-inf`');
  } else if (!pattern.test(max)) {
    cb('ZSCORE `max` parameters must be floating point OR `inf` OR `-inf`');
  } else if (limit && (is.not.integer(offset) || is.not.integer(count))) {
    cb('ZSCORE `offset` and `count` parameters are optional.  When LIMIT exists, ' +
      'both `offset` and `count` must also exist');
  } else {

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
          cb(err);
        } else if (is.existy(result) && is.not.empty(result)) {
          cb(null, result);
        } else {

          var sql = '',
            ordSetTableName =
              _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], '_');

          if (withscores) {
            sql += 'SELECT ' + COLUMN.MEMBER + ', ' + COLUMN.SCORE + ' FROM ' +
              ordSetTableName + ', ';
          } else {
            sql += 'SELECT ' + COLUMN.MEMBER + ' FROM ' +
              ordSetTableName + ', ';
          }

          sql += '(SELECT MIN(' + COLUMN.SCORE + ') AS minimum ' +
            'FROM ' + ordSetTableName +
            ') min_score, ';
          sql += '(SELECT MAX(' + COLUMN.SCORE + ') AS maximum ' +
            'FROM ' + ordSetTableName +
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

          sql += ' ORDER BY ' + COLUMN.SCORE + ' ASC ';

          if (limit) {
            sql += (' LIMIT ' + count + ' OFFSET ' + offset);
          }

          self.mysqlConn.query(
            sql,
            function (err, result) {
              if (err) {
                if (err.code === 'ER_NO_SUCH_TABLE') {
                  cb(null, []);
                } else {
                  self.emit('error', {
                    error: 'mysql', message: err.message,
                    redisKey: redisKey
                  });
                }
              } else {

                var arrayResult = [], i, tempKey;

                for (i = 0; i < result.length; i++) {
                  for (tempKey in result[i]) {
                    if (result[i].hasOwnProperty(tempKey)) {
                      arrayResult.push(result[i][tempKey]);
                    }
                  }
                }

                cb(null, arrayResult);
              }
            }
          );
        }
      });
  }
};

Redis2MySql.prototype.zrevrangebyscore = function (key, max, min, withscores, limit,
                                                   offset, count, cb) {

  var pattern = new RegExp(/(^\(?[0-9\.])|(^\-?inf)/),
    self = this, redisKey, redisParams = [];

  if (!(key && min && max)) {
    cb('Incomplete ZSCORE parameter(s)');
  } else if (is.not.string(key)) {
    cb('ZSCORE `key` parameter must be a string');
  } else if (!pattern.test(min)) {
    cb('ZSCORE `min` parameters must be floating point OR `inf` OR `-inf`');
  } else if (!pattern.test(max)) {
    cb('ZSCORE `max` parameters must be floating point OR `inf` OR `-inf`');
  } else if (limit && (is.not.integer(offset) || is.not.integer(count))) {
    cb('ZSCORE `offset` and `count` parameters are optional.  When LIMIT exists, ' +
      'both `offset` and `count` must also exist');
  } else {

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
          cb(err);
        } else if (is.existy(result) && is.not.empty(result)) {
          cb(null, result);
        } else {

          var sql = '',
            ordSetTableName =
              _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], '_');

          if (withscores) {
            sql += 'SELECT ' + COLUMN.MEMBER + ', ' + COLUMN.SCORE + ' FROM ' +
              ordSetTableName + ', ';
          } else {
            sql += 'SELECT ' + COLUMN.MEMBER + ' FROM ' +
              ordSetTableName + ', ';
          }

          sql += '(SELECT MIN(' + COLUMN.SCORE + ') AS minimum ' +
            'FROM ' + ordSetTableName +
            ') min_score, ';
          sql += '(SELECT MAX(' + COLUMN.SCORE + ') AS maximum ' +
            'FROM ' + ordSetTableName +
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

          sql += ' ORDER BY ' + COLUMN.SCORE + ' DESC ';

          if (limit) {
            sql += (' LIMIT ' + count + ' OFFSET ' + offset);
          }

          self.mysqlConn.query(
            sql,
            function (err, result) {
              if (err) {
                if (err.code === 'ER_NO_SUCH_TABLE') {
                  cb(null, []);
                } else {
                  self.emit('error', {
                    error: 'mysql', message: err.message,
                    redisKey: redisKey
                  });
                }
              } else {

                var arrayResult = [], i, tempKey;

                for (i = 0; i < result.length; i++) {
                  for (tempKey in result[i]) {
                    if (result[i].hasOwnProperty(tempKey)) {
                      arrayResult.push(result[i][tempKey]);
                    }
                  }
                }

                cb(null, arrayResult);
              }
            }
          );
        }
      });
  }
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

    var self = this, redisHashKey, field, value, hashTableName;

    redisHashKey =
      _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

    if (is.array(param1)) {

      field = param1[0];
      value = param1[1] || '';
    } else {

      field = param1;
      value = param2 || '';
    }

    hashTableName =
      _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_');

    this.redisConn.hget(redisHashKey, field, function (err, result) {
      if (err) {
        cb(err);
      } else {

        var originalValue = result;

        self.redisConn.hset(redisHashKey, field, value, function (err, result) {
          if (err) {
            cb(err);
          } else {

            cb(null, result);

            var COLUMNS = [self.mysqlConn.escapeId(COLUMN.FIELD), COLUMN.VALUE],
              sqlCreateHashTable, sql, sqlParams;

            sqlCreateHashTable =
              'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(hashTableName) +
              '(' +
              self.mysqlConn.escapeId(COLUMN.FIELD) + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMN.VALUE + ' VARCHAR(255), ' +
              COLUMN.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMN.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ';

            sql = new SqlBuilder().insert('??', COLUMNS)
              .values(COLUMNS.length).onDuplicate(COLUMNS.length).toString();

            sqlParams = [hashTableName, field, value, COLUMN.FIELD,
              field, COLUMN.VALUE, value];

            _createInsertUpdate(self.mysqlConn, sqlCreateHashTable, sql,
              sqlParams, function (err, result) {

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
                        console.log('Redis HSET rollback: ' + result);
                      }
                    });
                } else {
                  console.log('Redis HSET MySQL result: ' + result.affectedRows);
                }
              }
            );
          }
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

        var COLUMNS = [self.mysqlConn.escapeId(COLUMN.FIELD), COLUMN.VALUE],
          sqlCreateHashTable, sql, sqlParams = [], originalValues = result, i;

        self.redisConn.hmset(redisHashKey, fieldValues, function (err, result) {
          if (err) {
            cb(err);
          } else {
            cb(null, result);

            sqlCreateHashTable =
              'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(hashTableName) +
              '(' +
              self.mysqlConn.escapeId(COLUMN.FIELD) + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMN.VALUE + ' VARCHAR(255), ' +
              COLUMN.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
              COLUMN.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
              ') ';

            sql = new SqlBuilder().insertIgnore('??', COLUMNS)
              .values(COLUMNS.length, fieldValues.length / 2)
              .onDuplicate({value: 'VALUES(' + COLUMN.VALUE + ')'});

            sqlParams.push(hashTableName);

            for (i = 0; i < fieldValues.length; i++) {
              sqlParams.push(fieldValues[i]);
            }

            _createInsertUpdate(self.mysqlConn, sqlCreateHashTable, sql,
              sqlParams, function (err, result) {

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
                        console.log('Redis HMSET rollback: ' + result);
                      }
                    });
                } else {
                  console.log('Redis HMSET MySQL result: ' + result.affectedRows);
                }
              }
            );
          }
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

        self.mysqlConn.query(
          'SELECT ?? FROM ?? WHERE ?? = ?',
          [
            COLUMN.VALUE,
            hashTableName,
            COLUMN.FIELD,
            field
          ],
          function (err, result) {
            if (err) {
              if (err.code === 'ER_NO_SUCH_TABLE') {
                cb(null, null);
              } else {
                self.emit('error', {
                  error: 'mysql', message: err.message,
                  redisKey: redisHashKey
                });
              }
            } else if (is.existy(result)) {
              cb(null, is.existy(result[0]) ? result[0].value : null);
            } else {
              cb(null, null);
            }
          }
        );
      }
    });
  }
};

Redis2MySql.prototype.hmget = function (hashKey, fields, cb) {

  if (!fields) {
    cb('Incomplete HMGET parameters');
  } else if (is.not.string(hashKey)) {
    cb('HMGET `hashKey`parameter must be a string');
  } else if (is.not.array(fields)) {
    cb('HMGET `fields` parameter must be an array');
  } else {

    var self = this, redisHashKey =
      _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

    this.redisConn.hmget(redisHashKey, fields, function (err, result) {
      if (err) {
        cb(err);
      } else if (is.existy(result) && is.not.empty(result)) {
        cb(null, result);
      } else {

        var hashTableName, sqlSelect, sql, sqlParams = [], i, fieldParams = '';

        hashTableName =
          _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_');

        sqlParams.push(COLUMN.FIELD);
        sqlParams.push(COLUMN.VALUE);
        sqlParams.push(hashTableName);

        for (i = 0; i < fields.length; i++) {
          sqlParams.push(COLUMN.FIELD);
          sqlParams.push(fields[i]);
          fieldParams += ', ?';
        }

        sqlParams = sqlParams.concat(fields);

        sqlSelect = new SqlBuilder().select(2).from(1).where(fields.length, 'OR')
          .toString();

        sql = ('SELECT ' + COLUMN.VALUE + ' FROM ( ' + sqlSelect +
        ') inner_table ORDER BY FIELD(' + COLUMN.FIELD + fieldParams + ')');

        self.mysqlConn.query(
          sql,
          sqlParams,
          function (err, result) {
            if (err) {
              if (err.code === 'ER_NO_SUCH_TABLE') {
                cb(null, []);
              } else {
                self.emit('error', {
                  error: 'mysql', message: err.message,
                  redisKey: redisHashKey
                });
              }
            } else {

              var arrayResult = [], i, tempKey;

              for (i = 0; i < result.length; i++) {
                for (tempKey in result[i]) {
                  if (result[i].hasOwnProperty(tempKey)) {
                    arrayResult.push(result[i][tempKey]);
                  }
                }
              }

              cb(null, arrayResult);
            }
          }
        );
      }
    });
  }
};

Redis2MySql.prototype.hgetall = function (hashKey, cb) {

  if (!hashKey) {
    cb('Incomplete HGETALL parameters');
  } else if (is.not.string(hashKey)) {
    cb('HGETALL `hashKey` parameter must be a string');
  } else {

    var self = this, hashTableName, sql, redisHashKey =
      _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

    this.redisConn.hgetall(redisHashKey, function (err, result) {
      if (err) {
        cb(err);
      } else if (is.existy(result) && is.not.empty(result)) {
        cb(null, result);
      } else {

        hashTableName =
          _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_');

        sql = new SqlBuilder().select(2).from(1).toString();

        self.mysqlConn.query(
          sql,
          [
            COLUMN.FIELD,
            COLUMN.VALUE,
            hashTableName
          ],
          function (err, result) {
            if (err) {
              if (err.code === 'ER_NO_SUCH_TABLE') {
                cb(null, {});
              } else {
                self.emit('error', {
                  error: 'mysql', message: err.message,
                  redisKey: redisHashKey
                });
              }
            } else {

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
              cb(null, objResults);
            }
          }
        );
      }
    });
  }
};

Redis2MySql.prototype.hexists = function (hashKey, field, cb) {

  if (!field) {
    cb('Incomplete HEXISTS parameters');
  } else if (is.not.string(hashKey)) {
    cb('HEXISTS `hashKey` must be a string');
  } else if (is.not.string(field)) {
    cb('HEXISTS `field` must be a string');
  } else {

    var self = this, redisHashKey =
      _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], ':');

    this.redisConn.hexists(redisHashKey, field, function (err, result) {
      if (err) {
        cb(err);
      } else if (is.existy(result)) {
        cb(null, result);
      } else {

        var hashTableName =
            _prefixAppender([self.options.custom.datatypePrefix.hash, hashKey], '_'),

          sql = new SqlBuilder().select(['1']).from(1).where(1).toString();

        self.mysqlConn.query(
          sql,
          [
            hashTableName,
            COLUMN.FIELD,
            field
          ],
          function (err, result) {
            if (err) {
              if (err.code === 'ER_NO_SUCH_TABLE') {
                cb(null, 0);
              } else {
                self.emit('error', {
                  error: 'mysql', message: err.message,
                  redisKey: redisHashKey
                });
              }
            } else if (is.existy(result)) {
              cb(null, result[0] ? 1 : 0);
            } else {
              cb(null, 0);
            }
          }
        );
      }
    });
  }
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
                sqlParams.push(COLUMN.FIELD);
                sqlParams.push(fields[i]);
              }
            }

            sql = new SqlBuilder().deleteFrom(hashTableName).where(fields.length)
              .toString();

            self.mysqlConn.query(
              sql,
              sqlParams,
              function (err, result) {
                if (err) {
                  if (err.code === 'ER_NO_SUCH_TABLE') {
                    cb(null, 0);
                  } else {
                    self.emit('error', {
                      error: 'mysql', message: err.message,
                      redisKey: redisHashKey
                    });
                  }

                  /* for rollback purposes */
                  this.redisConn.hmset(redisHashKey, originalValue, function (err, result) {
                    if (err) {
                      self.emit('error', {
                        error: 'redis', message: err,
                        redisKey: redisHashKey
                      });
                    } else {
                      console.log('Redis HDEL rollback via HMSET: ' + result);
                    }
                  });
                } else {
                  console.log('Redis HDEL MySQL result: ' + result.affectedRows);
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
      cb('Both the old and the new table prefixes should be the same or ' +
        'either one of them should not be empty');
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

          /* for string, two-step process for renaming both type and key */
          async.series(
            [
              function (firstCb) {
                if (tableNameOld === tableNameNew) {
                  firstCb();
                } else if (type === 'string') {

                  async.series([
                      function (innerFirstCb) {
                        var sqlCreateNewTable =
                          'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(tableNameNew) +
                          '(' +
                          self.mysqlConn.escapeId(COLUMN.KEY) + ' VARCHAR(255) PRIMARY KEY, ' +
                          COLUMN.VALUE + ' VARCHAR(255), ' +
                          COLUMN.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
                          COLUMN.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
                          ') ';

                        self.mysqlConn.query(
                          sqlCreateNewTable,
                          function (err) {
                            if (err) {
                              innerFirstCb(err);
                            } else {
                              console.log('Redis RENAME new table ' +
                                tableNameNew + ' creation result');
                              innerFirstCb();
                            }
                          }
                        );
                      },
                      function (innerSecondCb) {
                        self.mysqlConn.beginTransaction(function (err) {
                          if (err) {
                            innerSecondCb(err);
                          } else {

                            console.log('Redis RENAME MySQL string ' +
                              'begin transaction');

                            var sqlCopy = new SqlBuilder().insert(tableNameNew)
                              .select(['old.*']).from([tableNameOld + ' old '])
                              .where(1).toString();

                            self.mysqlConn.query(
                              sqlCopy,
                              [
                                COLUMN.KEY,
                                tableKeyOld
                              ],
                              function (err, result) {
                                if (err) {
                                  self.mysqlConn.rollback(function (err) {
                                    if (err) {
                                      innerSecondCb(err);
                                    } else {
                                      console.log('Redis RENAME MySQL string ' +
                                        'rollback');
                                      innerSecondCb();
                                    }
                                  });
                                } else {
                                  console.log('Redis RENAME MySQL string row ' +
                                    'copy (to new table) result: ' + result.message);

                                  var sqlDeleteOld =
                                    new SqlBuilder().deleteFrom(tableNameOld)
                                      .where(1).toString();

                                  self.mysqlConn.query(
                                    sqlDeleteOld,
                                    [
                                      COLUMN.KEY,
                                      tableKeyOld
                                    ],
                                    function (err, result) {
                                      if (err) {
                                        innerSecondCb(err);
                                      } else {
                                        console.log('Redis RENAME MySQL string ' +
                                          'old row deletion result: ' + result.affectedRows);

                                        self.mysqlConn.commit(function (err) {
                                          if (err) {
                                            console.log('Redis RENAME MySQL ' +
                                              'commit issue');

                                            self.mysqlConn.rollback(function (err) {
                                              if (err) {
                                                innerSecondCb(err);
                                              } else {
                                                console.log('Redis RENAME MySQL ' +
                                                  'string rollback');
                                                innerSecondCb();
                                              }
                                            });
                                          } else {
                                            console.log('Redis RENAME MySQL ' +
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

                  self.mysqlConn.query(
                    sqlRenameTable,
                    [
                      tableNameOld,
                      tableNameNew
                    ],
                    function (err) { // `result` not used
                      if (err) {
                        firstCb(err);
                      } else {
                        console.log('Redis RENAME table MySQL result: OK'); // no message, only OkPacket
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

                  self.mysqlConn.query(
                    sqlUpdateTable,
                    [
                      COLUMN.KEY,
                      tableKeyNew,
                      COLUMN.KEY,
                      tableKeyOld
                    ],
                    function (err, result) {
                      if (err) {
                        secondCb(err);

                        /* for rollback purposes */
                        self.mysqlConn.query(
                          sqlRenameTable,
                          [
                            tableNameNew,
                            tableNameOld
                          ],
                          function (err) { // result not used
                            if (err) {
                              secondCb(err);
                            } else {
                              console.log('Redis RENAME table rollback MySQL result: OK'); // no message, only OkPacket
                              secondCb();
                            }
                          }
                        );
                      } else {
                        console.log('Redis RENAME key MySQL result: ' + result.message);
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

                self.mysqlConn.query(
                  sqlUpdateExpiryTable,
                  [
                    COLUMN.KEY,
                    newKey,
                    COLUMN.KEY,
                    key
                  ],
                  function (err, result) {
                    if (err) {
                      thirdCb(err);
                    } else {
                      console.log('Redis RENAME key renaming in table `expiry` ' +
                        'result: ' + result.message);
                    }
                  }
                );
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
                    console.log('Redis RENAME rollback: ' + result);
                  }
                });

                var sqlUpdateExpiryTable = new SqlBuilder().update(TABLE_EXPIRY)
                  .set(1).where(1).toString();

                self.mysqlConn.query(
                  sqlUpdateExpiryTable,
                  [
                    COLUMN.KEY,
                    key,
                    COLUMN.KEY,
                    newKey
                  ],
                  function (err, result) {
                    if (err) {
                      self.emit('error', {
                        error: 'mysql', message: err.message,
                        redisKey: [newKey, key]
                      });
                    } else {
                      console.log('Redis RENAME rollback in table `expiry` ' +
                        'result: ' + result.message);
                    }
                  }
                );
              }
              cb();
            }
          );
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

    var self = this;

    this.redisConn.expire(key, seconds, function (err, result) {
      if (err) {
        cb(err);
      } else {

        cb(null, result);

        var sqlCreateExpiryTable =
            'CREATE TABLE IF NOT EXISTS ' + TABLE_EXPIRY + ' ' +
            '(' +
            self.mysqlConn.escapeId(COLUMN.KEY) + ' VARCHAR(255) PRIMARY KEY, ' +
            COLUMN.EXPIRY_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
            COLUMN.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
            COLUMN.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
            ') ',

          sqlInsert =
            'INSERT IGNORE INTO ?? (??, ??) ' +
            'VALUES ' +
            '( ' +
            '?,  ' +
            '(SELECT DATE_ADD(NOW(3), INTERVAL ? SECOND) from DUAL) ' +
            ')' +
            'ON DUPLICATE KEY UPDATE ?? = VALUES (??)';

        _createInsertUpdate(self.mysqlConn, sqlCreateExpiryTable,
          sqlInsert,
          [
            TABLE_EXPIRY,
            COLUMN.KEY,
            COLUMN.EXPIRY_DT,
            key,
            seconds,
            COLUMN.EXPIRY_DT,
            COLUMN.EXPIRY_DT
          ], function (err, result) {

            if (err) {
              self.emit('error', {
                error: 'mysql', message: err.message,
                redisKey: key
              });
            } else {
              console.log('Redis EXPIRE MySQL result: ' + result.affectedRows);
            }
          }
        );
      }
    });
  }
};

Redis2MySql.prototype.createUseSchemaAndExpiry = function () {

  var self = this,

    sqlCreateSchema =
      'CREATE DATABASE IF NOT EXISTS ' + this.mysqlConn.escapeId(this.options.mysql.database) +
      ' CHARACTER SET = ' + this.mysqlConn.escape(this.options.mysql.charset),

    sqlUseSchema =
      'USE ' + this.mysqlConn.escapeId(this.options.mysql.database),

    sqlCreateExpiryTable =
      'CREATE TABLE IF NOT EXISTS ' + TABLE_EXPIRY + ' (' +
      this.mysqlConn.escapeId(COLUMN.KEY) + ' VARCHAR(255) PRIMARY KEY, ' +
      COLUMN.EXPIRY_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
      COLUMN.CREATION_DT + ' TIMESTAMP(3) DEFAULT NOW(3), ' +
      COLUMN.LAST_UPDT_DT + ' TIMESTAMP(3) DEFAULT NOW(3) ON UPDATE NOW(3)' +
      ') ';

  this.mysqlConn.query(
    'SELECT DATABASE() AS used FROM DUAL',
    function (err, result) {
      if (err) {
        throw err;
      }
      if (result[0].used === null) {
        async.series([
            function (firstCb) {
              self.mysqlConn.query(
                sqlCreateSchema,
                function (err) {
                  if (err) {
                    firstCb(err);
                  } else {
                    console.log('Created schema (if not exists) ' +
                      self.options.custom.schemaName);
                    firstCb();
                  }
                }
              );
            },
            function (secondCb) {
              self.mysqlConn.query(
                sqlUseSchema,
                function (err) {
                  if (err) {
                    secondCb(err);
                  } else {
                    console.log('Using ' + self.options.custom.schemaName);
                    secondCb();
                  }

                }
              );
            },
            function (thirdCb) {
              self.mysqlConn.query(
                sqlCreateExpiryTable,
                function (err) {
                  if (err) {
                    thirdCb(err);
                  } else {
                    console.log('Created table `expiry`');
                    thirdCb();
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

function _createInsertUpdate(connection, sqlCreate, sqlInsertUpdate, sqlParams, callback) {

  console.log('Created table, SQL Create: ' + sqlCreate);
  console.log('SQL for insert / update: ' + sqlInsertUpdate);
  console.log('SQL for insert / update parameters: ' + sqlParams.toString());

  connection.query(
    sqlCreate,
    function (err) {
      if (err) {
        callback(err);
      } else {

        connection.query(
          sqlInsertUpdate,
          sqlParams,
          function (err, result) {
            if (err) {
              callback(err);
            } else {
              callback(null, result);
            }

          }
        );
      }
    }
  );
}

if (typeof module !== 'undefined' &&
  module.exports) {
  module.exports = Redis2MySql;
}
