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
  COLUMN = {
    SEQ: 'time_sequence',
    KEY: 'key',
    VALUE: 'value',
    MEMBER: 'member',
    SCORE: 'score',
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
          COLUMN.CREATION_DT + ' TIMESTAMP DEFAULT NOW(), ' +
          COLUMN.LAST_UPDT_DT + ' TIMESTAMP DEFAULT NOW() ON UPDATE NOW()' +
          ') ';

        sql = new SqlBuilder().insert('??', COLUMNS)
          .values(COLUMNS.length).onDuplicate(COLUMNS.length).toString();

        sqlParams = [tableName, key, redisValue, COLUMN.KEY,
          key, COLUMN.VALUE, redisValue];

        _createInsertUpdate(self.mysqlConn, sqlCreateTable, sql,
          sqlParams, function (err) {

            if (err) {
              self.emit('error', {error: 'mysql', message: err.message});

              // for rollback purposes
              self.redisConn.decr(redisKey, function (err) {
                if (err) {
                  cb(err);
                }
              });
            } else {
              console.log('MySQL INCR result: ' + result);
              cb(); // do not return result
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

    /* for rollback purposes, use getset instead of set */
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
          COLUMN.CREATION_DT + ' TIMESTAMP DEFAULT NOW(), ' +
          COLUMN.LAST_UPDT_DT + ' TIMESTAMP DEFAULT NOW() ON UPDATE NOW()' +
          ') ';

        sql = new SqlBuilder().insert('??', COLUMNS)
          .values(COLUMNS.length).onDuplicate(COLUMNS.length).toString();

        sqlParams = [stringTableName, key, value, COLUMN.KEY,
          key, COLUMN.VALUE, value];

        _createInsertUpdate(self.mysqlConn, sqlCreateStringTable, sql,
          sqlParams, function (err) {
            if (err) {
              self.emit('error', {error: 'mysql', message: err.message});

              // for rollback purposes
              self.redisConn.set(redisKey, originalValue, function (err) {
                if (err) {
                  cb(err);
                }
              });
            } else {
              console.log('MySQL SET result: ' + result);
              cb();
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

    var self = this, stringTableName, redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.string, type, key], ':');

    this.redisConn.get(redisKey, function (err, result) {
      if (err) {
        cb(err);
      } else if (is.existy(result)) {
        cb(null, result);
      } else {

        stringTableName =
          _prefixAppender([self.options.custom.datatypePrefix.string, type], '_');

        self.mysqlConn.query(
          'SELECT ?? FROM ?? WHERE ?? = ?',
          [
            COLUMN.VALUE,
            stringTableName,
            COLUMN.KEY,
            key
          ],
          function (err, result) {
            if (err) {
              self.emit('error', {error: 'mysql', message: err.message});
            } else if (is.existy(result)) {
              cb(null, result[0].value);
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
      } else if (is.existy(result)) {
        cb(null, result);
      } else {

        var sql,
          stringTableName =
            _prefixAppender([self.options.custom.datatypePrefix.string, type], '_');

        sql = 'SELECT 1 FROM ?? WHERE ?? = ?';

        self.mysqlConn.query(
          sql,
          [
            stringTableName,
            COLUMN.KEY,
            key
          ],
          function (err, result) {
            if (err) {
              self.emit('error', {error: 'mysql', message: err.message});
            } else if (is.existy(result)) {
              cb(null, result[0] ? 1 : 0);
            } else {
              cb(null, null);
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

          var lazySeq, dataArray, deleteSql, tableName, tableKey;

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
                async.series({
                  deleteRow: function (firstCb) {

                    var _rollback = function () {
                      self.mysqlConn.rollback(function (err) {
                        if (err) {
                          firstCb(err);
                        } else {
                          console.log('Rolled back transaction');
                          firstCb();
                        }
                      });
                    };

                    if (tableKey === undefined) {
                      firstCb(); // do nothing for type not being Redis string
                    } else {
                      deleteSql = new SqlBuilder().deleteFrom('??').where(1).toString();
                      self.mysqlConn.query(deleteSql,
                        [
                          tableName,
                          COLUMN.KEY,
                          tableKey
                        ],
                        function (err, result) {
                          if (err) {
                            firstCb(err);
                            _rollback();
                          } else if (result.affectedRows > 0) {
                            self.mysqlConn.commit(function (err) {
                              if (err) {
                                firstCb(err);
                                _rollback();
                              } else {
                                console.log('Row deletion committed');
                                firstCb();
                              }
                            });
                          } else {
                            firstCb();
                          }
                        }
                      );
                    }
                  },
                  dropTable: function (secondCb) {

                    var _dropTable = function () {
                      self.mysqlConn.query(
                        'DROP TABLE IF EXISTS ' + tableName,
                        function (err) {
                          if (err) {
                            secondCb(err);
                          } else {
                            console.log('Dropped table ' + tableName);
                            secondCb();
                          }
                        }
                      );
                    };

                    /* Auto-drop table if any type other than string */
                    if (tableKey === undefined) {
                      _dropTable();
                    } else {
                      self.mysqlConn.query(
                        'SELECT EXISTS (SELECT 1 FROM ?? LIMIT 1) AS has_rows',
                        tableName,
                        function (err, result) {
                          if (err) {
                            secondCb(err);
                          } else if (result[0].has_rows === 0) {
                            _dropTable();
                          } else {
                            console.log('Table still has existing data');
                            secondCb();
                          }
                        }
                      );
                    }
                  }
                }, function (err) { // no need to return result
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
            self.emit('error', {error: 'mysql', message: err.message});
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

    var self = this, redisKey, arrayValues = [], listTableName, i, time;

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

        cb(null, result);

        if (result[0][1][1].length > 0) { // result from TIME
          /* UNIX time in sec + microseconds */
          time = result[0][1][0] + result[0][1][1];
        }

        if (result[1][1] > 0) {

          cb(null, result[1][1]); // return result from LPUSH to callback

          listTableName =
            _prefixAppender([self.options.custom.datatypePrefix.list, key], '_');

          var COLUMNS = [COLUMN.SEQ, COLUMN.VALUE],
            sqlCreateListTable, sql, sqlParams = [];

          sqlCreateListTable =
            'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(listTableName) +
            '(' +
            COLUMN.SEQ + ' BIGINT PRIMARY KEY, ' +
            COLUMN.VALUE + ' VARCHAR(255), ' +
            COLUMN.CREATION_DT + ' TIMESTAMP DEFAULT NOW(), ' +
            COLUMN.LAST_UPDT_DT + ' TIMESTAMP DEFAULT NOW() ON UPDATE NOW()' +
            ') ';

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
                self.emit('error', {error: 'mysql', message: err.message});

                self.redisConn.lpop(redisKey, function (err) {
                  self.emit('error', {error: 'mysql', message: err});
                });
              } else if (is.existy(result)) {
                cb(null, result);
              } else {
                cb(null, null);
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

        if (redisIndex >= 0) { // positive index
          offset = 1;
          startingCounter = -1;
          order = 'ASC';
        } else {
          offset = -1;
          startingCounter = 0;
          order = 'DESC';
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
              self.emit('error', {error: 'mysql', message: err.message});
            } else if (is.existy(result)) {
              cb(null, result[0].value);
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
              order = 'ASC';
            } else {
              offset = -1;
              startingCounter = 0;
              order = 'DESC';
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
                  cb(err);

                  self.redisConn.lset(redisKey, redisIndex, originalValue, function (err) {
                    if (err) {
                      cb(err);
                    }
                  });
                } else if (is.existy(result)) {
                  cb(null, result.message);
                } else {
                  cb(null, null);
                }
              }
            );
          }
        });
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

    var self = this, COLUMNS = [COLUMN.MEMBER], redisKey, arrayMembers = [],
      ordSetTableName, sqlCreateOrdSetTable, sql, sqlParams = [], i;

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

        ordSetTableName =
          _prefixAppender([self.options.custom.datatypePrefix.set, key], '_');

        sqlCreateOrdSetTable =
          'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(ordSetTableName) +
          '(' +
          COLUMN.MEMBER + ' VARCHAR(255) PRIMARY KEY, ' +
          COLUMN.CREATION_DT + ' TIMESTAMP DEFAULT NOW(), ' +
          COLUMN.LAST_UPDT_DT + ' TIMESTAMP DEFAULT NOW() ON UPDATE NOW()' +
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
              self.emit('error', {error: 'mysql', message: err.message});
            } else if (is.existy(result)) {
              cb(null, result);
            } else {
              cb(null, null);
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

    self.redisConn.srem(redisKey, members, function (err, result) {

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
              cb(err);
            } else if (is.existy(result)) {
              cb(null, result.affectedRows);
            } else {
              cb(null, null);
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
      } else if (is.existy(result)) {
        cb(null, result);
      } else {

        var sql,
          ordSetTableName =
            _prefixAppender([self.options.custom.datatypePrefix.set, key], '_');

        sql = 'SELECT ?? FROM ?? ';

        self.mysqlConn.query(
          sql,
          [
            COLUMN.MEMBER,
            ordSetTableName
          ],
          function (err, result) {
            if (err) {
              self.emit('error', {error: 'mysql', message: err.message});
            } else if (is.existy(result)) {
              cb(null, result);
            } else {
              cb(null, null);
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

        sql = 'SELECT ?? FROM ?? WHERE ?? = ?';

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
              self.emit('error', {error: 'mysql', message: err.message});
            } else if (is.existy(result)) {
              cb(null, result.length);
            } else {
              cb(null, null);
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

        var sql,
          ordSetTableName =
            _prefixAppender([self.options.custom.datatypePrefix.set, key], '_');

        sql = 'SELECT COUNT(1) AS cnt FROM ??';

        self.mysqlConn.query(
          sql,
          [
            ordSetTableName
          ],
          function (err, result) {
            if (err) {
              self.emit('error', {error: 'mysql', message: err.message});
            } else if (is.existy(result)) {
              cb(null, result[0].cnt);
            } else {
              cb(null, null);
            }
          }
        );
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

    var self = this, COLUMNS = [COLUMN.SCORE, COLUMN.MEMBER], redisKey,
      sortedSetTableName, sqlCreateSortedSetTable, sql, sqlParams = [], i;

    redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], ':');

    /* For now, no rollback mechanism in favor of efficiency */
    self.redisConn.zadd(redisKey, scoreMembers, function (err, result) {

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
          COLUMN.CREATION_DT + ' TIMESTAMP DEFAULT NOW(), ' +
          COLUMN.LAST_UPDT_DT + ' TIMESTAMP DEFAULT NOW() ON UPDATE NOW()' +
          ') ';

        sql = new SqlBuilder().insertIgnore('??', COLUMNS)
          .values(COLUMNS.length, scoreMembers.length / 2)
          .onDuplicate({score: 'VALUES(' + COLUMN.SCORE + ')'});

        sqlParams.push(sortedSetTableName);

        /* For the values of the INSERT phrase */
        for (i = 0; i < scoreMembers.length; i++) {
          sqlParams.push(scoreMembers[i]);
        }

        _createInsertUpdate(self.mysqlConn, sqlCreateSortedSetTable, sql,
          sqlParams, function (err, result) {

            if (err) {
              self.emit('error', {error: 'mysql', message: err.message});
            } else if (is.existy(result)) {
              cb(null, result);
            } else {
              cb(null, null);
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
          COLUMN.CREATION_DT + ' TIMESTAMP DEFAULT NOW(), ' +
          COLUMN.LAST_UPDT_DT + ' TIMESTAMP DEFAULT NOW() ON UPDATE NOW()' +
          ') ';

        sql = new SqlBuilder().insertIgnore('??', COLUMNS)
          .values(COLUMNS.length, 1)
          .onDuplicate({score: COLUMN.SCORE + ' + VALUES(' + COLUMN.SCORE + ')'});

        sqlParams.push(sortedSetTableName);

        /* For the values of the INSERT phrase */
        sqlParams.push(incrDecrValue);
        sqlParams.push(member);

        _createInsertUpdate(self.mysqlConn, sqlCreateSortedSetTable, sql,
          sqlParams, function (err, result) {

            if (err) {
              self.emit('error', {error: 'mysql', message: err.message});
            } else if (is.existy(result)) {
              cb(null, result);
            } else {
              cb(null, null);
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
      } else if (is.existy(result)) {
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
              self.emit('error', {error: 'mysql', message: err.message});
            } else if (is.existy(result)) {
              cb(null, result[0].score);
            } else {
              cb(null, null);
            }
          }
        );
      }
    });
  }
};

Redis2MySql.prototype.zrangebyscore = function (key, min, max, withscores, limit,
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

    this.redisConn.zrangebyscore(redisKey, min, max, redisParams,
      function (err, result) {

        if (err) {
          cb(err);
        } else if (is.existy(result)) {
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
                self.emit('error', {error: 'mysql', message: err.message});
              } else if (result) {
                cb(null, is.existy(result));
              } else {
                cb(null, null);
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
        } else if (is.existy(result)) {
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
                self.emit('error', {error: 'mysql', message: err.message});
              } else if (is.existy(result)) {
                cb(null, result);
              } else {
                cb(null, null);
              }
            }
          );
        }
      });
  }
};

Redis2MySql.prototype.hset = function (hash, param1, param2, cb) {

  if (!(param1 && hash)) {
    cb('Incomplete HSET parameter(s)');
  } else if (is.not.string(hash)) {
    cb('HSET `type` parameter must be a string');
  } else if (is.not.string(param1) && is.not.array(param1)) {
    cb('HSET `param1` parameter must be a string or an array containing ' +
      'the key and the value to be set');
  } else {

    if (typeof param2 === 'function') {
      cb = param2;
      param2 = '';
    }

    var self = this, redisHash, key, value,
      hashTableName;

    redisHash =
      _prefixAppender([self.options.custom.datatypePrefix.hash, hash], ':');

    if (is.array(param1)) {

      key = param1[0];
      value = param1[1] || '';
    } else {

      key = param1;
      value = param2 || '';
    }

    hashTableName =
      _prefixAppender([self.options.custom.datatypePrefix.hash, hash], '_');

    self.redisConn.hget(redisHash, key, function (err, result) {
      if (err) {
        cb(err);
      } else {

        var COLUMNS = [self.mysqlConn.escapeId(COLUMN.KEY), COLUMN.VALUE],
          sqlCreateHashTable, sql, sqlParams, originalValue = result;

        self.redisConn.hset(redisHash, key, value, function (err, result) {
          if (err) {
            cb(err);
          } else {
            cb(null, 'OK'); // return to client optimistically

            sqlCreateHashTable =
              'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(hashTableName) +
              '(' +
              self.mysqlConn.escapeId(COLUMN.KEY) + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMN.VALUE + ' VARCHAR(255), ' +
              COLUMN.CREATION_DT + ' TIMESTAMP DEFAULT NOW(), ' +
              COLUMN.LAST_UPDT_DT + ' TIMESTAMP DEFAULT NOW() ON UPDATE NOW()' +
              ') ';

            sql = new SqlBuilder().insert('??', COLUMNS)
              .values(COLUMNS.length).onDuplicate(COLUMNS.length).toString();

            sqlParams = [hashTableName, key, value, COLUMN.KEY,
              key, COLUMN.VALUE, value];

            _createInsertUpdate(self.mysqlConn, sqlCreateHashTable, sql,
              sqlParams, function (err) {

                if (err) {
                  self.emit('error', {error: 'mysql', message: err.message});

                  // for rollback purposes
                  self.redisConn.hset(redisHash, key, originalValue, function (err) {
                    if (err) {
                      cb(err);
                    }
                  });
                } else if (result) {
                  cb(null, result);
                } else {
                  cb(null, null);
                }
              }
            );
          }
        });
      }
    });
  }
};

Redis2MySql.prototype.hmset = function (hash, param1, param2, cb) {

  if (!(param1 && hash)) {
    cb('Incomplete HSET parameter(s)');
  } else if (is.not.string(hash)) {
    cb('HSET `type` parameter must be a string');
  } else if (is.not.string(param1) && is.not.array(param1)) {
    cb('HSET `param1` parameter must be a string or an array containing ' +
      'the key and the value to be set');
  } else {

    if (typeof param2 === 'function') {
      cb = param2;
      param2 = '';
    }

    var self = this, redisHash, i, keys = [], keyValues = [], hashTableName;

    redisHash =
      _prefixAppender([self.options.custom.datatypePrefix.hash, hash], ':');

    if (is.array(param1)) {
      for (i = 0; i < param1.length; i++) {
        if (is.odd(i)) {
          keys.push(param1[i]);
        }
      }
      keyValues = keyValues.concat(param1);
    } else {
      keys.push(param1);
      keyValues.push(param1);
      keyValues.push(param2);
    }

    hashTableName =
      _prefixAppender([self.options.custom.datatypePrefix.hash, hash], '_');

    /* for rollback purposes, use getset instead of set */
    self.redisConn.hmget(redisHash, keys, function (err, result) {
      if (err) {
        cb(err);
      } else {

        var COLUMNS = [self.mysqlConn.escapeId(COLUMN.KEY), COLUMN.VALUE],
          sqlCreateHashTable, sql, sqlParams = [], originalValues = result, i;

        self.redisConn.hmset(redisHash, keyValues, function (err, result) {
          if (err) {
            cb(err);
          } else {
            cb(null, 'OK'); // return to client optimistically

            sqlCreateHashTable =
              'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(hashTableName) +
              '(' +
              self.mysqlConn.escapeId(COLUMN.KEY) + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMN.VALUE + ' VARCHAR(255), ' +
              COLUMN.CREATION_DT + ' TIMESTAMP DEFAULT NOW(), ' +
              COLUMN.LAST_UPDT_DT + ' TIMESTAMP DEFAULT NOW() ON UPDATE NOW()' +
              ') ';

            sql = new SqlBuilder().insertIgnore('??', COLUMNS)
              .values(COLUMNS.length, keyValues.length / 2)
              .onDuplicate({value: 'VALUES(' + COLUMN.VALUE + ')'});

            sqlParams.push(hashTableName);

            for (i = 0; i < keyValues.length; i++) {
              sqlParams.push(keyValues[i]);
            }

            _createInsertUpdate(self.mysqlConn, sqlCreateHashTable, sql,
              sqlParams, function (err) {

                if (err) {
                  self.emit('error', {error: 'mysql', message: err.message});

                  // for rollback purposes
                  self.redisConn.hmset(redisHash, originalValues, function (err) {
                    if (err) {
                      cb(err);
                    }
                  });
                } else if (result) {
                  cb(null, result);
                } else {
                  cb(null, null);
                }
              }
            );
          }
        });
      }
    });
  }
};

Redis2MySql.prototype.hget = function (hash, key, cb) {

  if (!key) {
    cb('Incomplete GET parameters');
  } else if (is.not.string(hash)) {
    cb('GET `type` must be a string');
  } else if (is.not.string(key)) {
    cb('GET `key` must be a string');
  } else {

    var self = this, hashTableName, redisHash =
      _prefixAppender([self.options.custom.datatypePrefix.hash, hash], ':');

    this.redisConn.hget(redisHash, key, function (err, result) {
      if (err) {
        cb(err);
      } else if (is.existy(result)) {
        cb(null, result);
      } else {

        hashTableName =
          _prefixAppender([self.options.custom.datatypePrefix.hash, hash], '_');

        self.mysqlConn.query(
          'SELECT ?? FROM ?? WHERE ?? = ?',
          [
            COLUMN.VALUE,
            hashTableName,
            COLUMN.KEY,
            key
          ],
          function (err, result) {
            if (err) {
              self.emit('error', {error: 'mysql', message: err.message});
            } else if (result[0]) {
              cb(null, result[0].value);
            } else {
              cb(null, null);
            }
          }
        );
      }
    });
  }
};

Redis2MySql.prototype.hmget = function (hash, keys, cb) {

  if (!keys) {
    cb('Incomplete HMGET parameters');
  } else if (is.not.string(hash)) {
    cb('HMGET `type`parameter must be a string');
  } else if (is.not.array(keys)) {
    cb('HMGET `key` parameter must be an array');
  } else {

    var self = this, i, hashTableName, sql, sqlParams = [], redisHash =
      _prefixAppender([self.options.custom.datatypePrefix.hash, hash], ':');

    this.redisConn.hmget(redisHash, keys, function (err, result) {
      if (err) {
        cb(err);
      } else if (is.existy(result)) {
        cb(null, result);
      } else {

        hashTableName =
          _prefixAppender([self.options.custom.datatypePrefix.hash, hash], '_');

        sqlParams.push(COLUMN.VALUE);
        sqlParams.push(hashTableName);

        for (i = 0; i < keys.length; i++) {
          sqlParams.push(COLUMN.KEY);
          sqlParams.push(keys[i]);
        }

        sql = new SqlBuilder().select(1).from(1).where(keys.length, 'OR').toString();

        self.mysqlConn.query(
          sql,
          sqlParams,
          function (err, result) {
            if (err) {
              self.emit('error', {error: 'mysql', message: err.message});
            } else if (result) {
              cb(null, result);
            } else {
              cb(null, null);
            }
          }
        );
      }
    });
  }
};

Redis2MySql.prototype.hgetall = function (hash, cb) {

  if (!hash) {
    cb('Incomplete HGETALL parameters');
  } else if (is.not.string(hash)) {
    cb('HGETALL `type`parameter must be a string');
  } else {

    var self = this, hashTableName, sql, redisHash =
      _prefixAppender([self.options.custom.datatypePrefix.hash, hash], ':');

    this.redisConn.hgetall(redisHash, function (err, result) {
      if (err) {
        cb(err);
      } else if (is.existy(result)) {
        cb(null, result);
      } else {

        hashTableName =
          _prefixAppender([self.options.custom.datatypePrefix.hash, hash], '_');

        sql = new SqlBuilder().select(2).from(1).toString();

        self.mysqlConn.query(
          sql,
          [
            COLUMN.KEY,
            COLUMN.VALUE,
            hashTableName
          ],
          function (err, result) {
            if (err) {
              self.emit('error', {error: 'mysql', message: err.message});
            } else if (result) {

              var i, temp, objResults = {};

              if (Array.isArray(result)) {
                for (i = 0; i < result.length; i++) {
                  if (is.object(result[i])) {
                    for (temp in result[i]) {
                      if (result[i].hasOwnProperty(temp)) {
                        objResults[result[i].key] = result[i].value;
                      }
                    }
                  }
                }
              }
              cb(null, objResults);
            } else {
              cb(null, null);
            }
          }
        );
      }
    });
  }
};

Redis2MySql.prototype.hexists = function (hash, key, cb) {

  if (!key) {
    cb('Incomplete HEXISTS parameters');
  } else if (is.not.string(hash)) {
    cb('HEXISTS `type` must be a string');
  } else if (is.not.string(key)) {
    cb('HEXISTS `key` must be a string');
  } else {

    var self = this, hashTableName, redisHash =
      _prefixAppender([self.options.custom.datatypePrefix.hash, hash], ':');

    this.redisConn.hexists(redisHash, key, function (err, result) {
      if (err) {
        cb(err);
      } else if (is.existy(result)) {
        cb(null, result);
      } else {

        hashTableName =
          _prefixAppender([self.options.custom.datatypePrefix.hash, hash], '_');

        self.mysqlConn.query(
          'SELECT 1 FROM ?? WHERE ?? = ?',
          [
            hashTableName,
            COLUMN.KEY,
            key
          ],
          function (err, result) {
            if (err) {
              self.emit('error', {error: 'mysql', message: err.message});
            } else if (is.existy(result)) {
              cb(null, result[0] ? 1 : 0);
            } else {
              cb(null, null);
            }
          }
        );
      }
    });
  }
};

Redis2MySql.prototype.hdel = function (hash, keys, cb) {

  if (!keys) {
    cb('Incomplete HDEL parameters');
  } else if (is.not.string(hash)) {
    cb('HDEL `type`parameter must be a string');
  } else if (is.not.array(keys)) {
    cb('HMGET `key` parameter must be an array');
  } else {

    var self = this, i, originalValue, hashTableName, sql, sqlParams = [],
      redisHash =
        _prefixAppender([self.options.custom.datatypePrefix.hash, hash], ':');

    this.redisConn.hget(redisHash, keys, function (err, result) {
      if (err) {
        cb(err);
      } else {

        originalValue = result;

        this.redisConn.hdel(redisHash, keys, function (err, result) {
          if (err) {
            cb(err);
          } else if (is.existy(result)) {
            cb(null, result);
          } else {

            hashTableName =
              _prefixAppender([self.options.custom.datatypePrefix.hash, hash], '_');

            for (i = 0; i < keys.length; i++) {
              if (keys.hasOwnProperty(i)) {
                sqlParams.push(COLUMN.KEY);
                sqlParams.push(keys[i]);
              }
            }

            sql = new SqlBuilder().deleteFrom(hashTableName).where(keys.length)
              .toString();

            self.mysqlConn.query(
              sql,
              sqlParams,
              function (err, result) {
                if (err) {
                  self.emit('error', {error: 'mysql', message: err.message});

                  this.redisConn.hmset(redisHash, originalValue, function (err, result) {
                    if (err) {
                      cb(err);
                    } else {
                      cb(null, result);
                    }
                  });
                } else if (result) {
                  cb(null, result.affectedRows);
                } else {
                  cb(null, null);
                }
              }
            );
          }
        });
      }
    });
  }
};

Redis2MySql.prototype.createUseSchema = function () {

  var self = this,

    sqlCreateSchema =
      'CREATE DATABASE IF NOT EXISTS ' + this.mysqlConn.escapeId(this.options.mysql.database) +
      ' CHARACTER SET = ' + this.mysqlConn.escape(this.options.mysql.charset),

    sqlUseSchema =
      'USE ' + this.mysqlConn.escapeId(this.options.mysql.database),

    _createSchema = function (firstCb) {
      self.mysqlConn.query(
        sqlCreateSchema,
        function (err) {
          if (!err) {
            console.log('Created schema (if not exists) ' + self.options.custom.schemaName);
          }
          firstCb(err);
        }
      );
    },

    _useSchema = function (secondCb) {
      self.mysqlConn.query(
        sqlUseSchema,
        function (err) {
          if (!err) {
            console.log('Using ' + self.options.custom.schemaName);
          }
          secondCb(err);
        }
      );
    };

  this.mysqlConn.query(
    'SELECT DATABASE() AS used FROM DUAL',
    function (err, result) {
      if (err) {
        throw err;
      }
      if (result[0].used === null) {
        async.series([
            _createSchema,
            _useSchema
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

  connection.query(
    sqlCreate,
    function (err) {
      if (err) {
        callback(err);
      } else {

        console.log('Created table, SQL Create: ' + sqlCreate);
        console.log('SQL for insert / update: ' + sqlInsertUpdate);

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
