/*
 * Copyright (c) 2015.
 */
'use strict';

var async = require('async'),
  util = require('util'),
  is = require('is_js'),
  EventEmitter = require('events'),
  Redis = require('ioredis'),
  mysql = require('mysql'),
  SqlBuilder = require('./SqlBuilder'),
  COLUMN = {
    SEQ: 'sequence',
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

Redis2MySql.prototype.set = function (type, param1, param2, cb) {

  if (!(param1 && type)) {
    cb('Incomplete SET parameters');
  } else if (!is.string(type)) {
    cb('SET `type` parameter must be a string');
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

        cb(null, 'OK'); // return to client optimistically

        var COLUMNS = [self.mysqlConn.escapeId(COLUMN.KEY), COLUMN.VALUE],
          sqlCreateStringTable, sql, sqlParam, originalValue = result;

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

        sqlParam = [stringTableName, key, value, COLUMN.KEY,
          key, COLUMN.VALUE, value];

        _createInsertUpdate(self.mysqlConn, sqlCreateStringTable, sql,
          sqlParam, function (err) {

            if (err) {
              self.emit('error', {error: 'mysql', message: err.message});

              // for rollback purposes
              self.redisConn.set(redisKey, originalValue, function (err) {
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
};

Redis2MySql.prototype.get = function (type, key, cb) {

  if (!key) {
    cb('Incomplete GET parameters');
  } else if (!is.string(type)) {
    cb('GET `type` must be a string');
  } else if (!is.string(key)) {
    cb('GET `key` must be a string');
  } else {

    var self = this, redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.string, type, key], ':');

    this.redisConn.get(redisKey, function (err, result) {
      if (err) {
        cb(err);
      } else if (result) {
        cb(null, result);
      } else {

        self.stringTableName =
          _prefixAppender([self.options.custom.datatypePrefix.string, type], '_');

        self.mysqlConn.query(
          'SELECT ?? FROM ?? WHERE ?? = ?',
          [
            COLUMN.VALUE,
            self.stringTableName,
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

Redis2MySql.prototype.lpush = function (key, values, cb) {

  if (!(key && values)) {
    cb('Incomplete LPUSH parameters');
  } else if (!(is.string(values) || is.array(values))) {
    cb('LPUSH `values` parameter must be a string OR ' +
      'an array of strings');
  } else {

    var self = this, redisKey, arrayValues = [], listTableName, sqlLatestSeq;

    if (is.string(values)) {
      arrayValues.push(values);
    } else if (is.array(values)) {
      arrayValues = arrayValues.concat(values);
    }

    redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.list, key], ':');

    self.redisConn.lpush(redisKey, values, function (err, result) {

      if (err) {
        cb(err);
      } else {

        cb(null, result);

        listTableName =
          _prefixAppender([self.options.custom.datatypePrefix.list, key], '_');

        sqlLatestSeq = 'SELECT ' + COLUMN.SEQ +
          ' FROM ?? ORDER BY sequence ASC LIMIT 1 ';

        self.mysqlConn.query(
          sqlLatestSeq,
          listTableName,
          function (err, result) {

            var COLUMNS = [COLUMN.SEQ, COLUMN.VALUE], seq = 0, i,
              sqlCreateListTable, sql, sqlParams = [listTableName];

            if (!err && result !== undefined) {
              seq = result[0].sequence;
            }

            sqlCreateListTable =
              'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(listTableName) +
              '(' +
              COLUMN.SEQ + ' INTEGER PRIMARY KEY, ' +
              COLUMN.VALUE + ' VARCHAR(255), ' +
              COLUMN.CREATION_DT + ' TIMESTAMP DEFAULT NOW(), ' +
              COLUMN.LAST_UPDT_DT + ' TIMESTAMP DEFAULT NOW() ON UPDATE NOW()' +
              ') ';

            sql = new SqlBuilder().insert('??', COLUMNS)
              .values(COLUMNS.length, arrayValues.length).toString();

            for (i = 0; i < arrayValues.length; i++) {

              if (err || result === undefined) {
                sqlParams.push(seq--);
              } else {
                sqlParams.push(--seq);
              }

              sqlParams.push(arrayValues[i]);
            }

            _createInsertUpdate(self.mysqlConn, sqlCreateListTable, sql,
              sqlParams, function (err, result) {

                if (err) {
                  self.emit('error', {error: 'mysql', message: err.message});

                  // TODO: delete table IF table creation was done to set a certain value

                  self.redisConn.lpop(redisKey, function (err) {
                    self.emit('error', {error: 'mysql', message: err});
                  });
                } else if (result && result !== 'Created table') {
                  cb(null, result);
                } else {
                  cb(null, null);
                }
              });
          }
        );
      }
    });
  }
};

Redis2MySql.prototype.lindex = function (key, redisIndex, cb) {

  if (!(key && is.existy(redisIndex))) {
    cb('Incomplete LINDEX parameter(s)');
  } else if (!is.string(key)) {
    cb('LINDEX `key` parameter must be a string');
  } else if (!is.number(redisIndex)) {
    cb('LINDEX `index` parameter must be an integer');
  } else {

    var self = this, redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.list, key], ':');

    this.redisConn.lindex(redisKey, redisIndex, function (err, result) {
      if (err) {
        cb(err);
      } else if (result) {
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

Redis2MySql.prototype.lset = function (key, redisIndex, value, cb) {

  if (!(key && is.existy(redisIndex) && is.existy(value))) {
    cb('Incomplete LSET parameter(s)');
  } else if (!is.string(key)) {
    cb('LSET `key` parameter must be a string');
  } else if (!is.number(redisIndex)) {
    cb('LSET `redisIndex` parameter must be an integer');
  } else if (!is.string(value)) {
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
              .where({sequence: '(' + selectSql + ') '}, 'AND').toString();

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
                  _rollbackOnErrorIfNotCommit(err, result, self.mysqlConn, function (err) {
                    self.emit('error', {error: 'mysql', message: err.message});
                  });

                  // for rollback purposes
                  self.redisConn.lset(redisKey, redisIndex, originalValue, function (err) {
                    if (err) {
                      cb(err);
                    }
                  });
                } else if (result) {
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
  } else if (!is.string(key)) {
    cb('SADD `key` parameter must be a string');
  } else if (!(is.alphaNumeric(members) || is.array(members))) {
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

    /* For now, no rollback mechanism in favor of efficiency */
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

Redis2MySql.prototype.srem = function (key, members, cb) {

  if (!(key && members)) {
    cb('Incomplete SREM parameter(s)');
  } else if (!is.string(key)) {
    cb('SREM `key` parameter must be a string');
  } else if (!(is.string(members) || is.array(members))) {
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

    /* For now, no rollback mechanism in favor of efficiency */
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
              _rollbackOnErrorIfNotCommit(err, result, self.mysqlConn, function (err) {
                self.emit('error', {error: 'mysql', message: err.message});
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
};

Redis2MySql.prototype.smembers = function (key, cb) {

  if (!key) {
    cb('Incomplete SMEMBERS parameter');
  } else if (!is.string(key)) {
    cb('SMEMBERS `key` parameter must be a string');
  } else {

    var self = this, redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.set, key], ':');

    this.redisConn.smembers(redisKey, function (err, result) {
      if (err) {
        cb(err);
      } else if (result) {
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

Redis2MySql.prototype.sismember = function (key, member, cb) {

  if (!key) {
    cb('Incomplete SISMEMBER parameter(s)');
  } else if (!is.string(key)) {
    cb('SCARD `key` parameter must be a string');
  } else if (!is.string(member)) {
    cb('SCARD `member` parameter must be a string');
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
            } else if (result) {
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
  } else if (!is.string(key)) {
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

        sql = 'SELECT COUNT(*) as cnt FROM ??';

        self.mysqlConn.query(
          sql,
          [
            ordSetTableName
          ],
          function (err, result) {
            if (err) {
              self.emit('error', {error: 'mysql', message: err.message});
            } else if (result) {
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
  } else if (!(is.array(scoreMembers) && scoreMembers.length % 2 === 0)) {
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

Redis2MySql.prototype.zincrby = function (key, incrDecrValue, member, cb) {

  if (!(key && incrDecrValue && member)) {
    cb('Incomplete ZINCRBY parameter(s)');
  } else if (!is.string(key)) {
    cb('ZINCRBY `key` parameter must be a string');
  } else if (!is.number(incrDecrValue)) {
    cb('ZINCRBY `incrDecrValue` parameter must be a floating point');
  } else if (!is.string(member)) {
    cb('ZINCRBY `member` parameter must be a string');
  } else {

    var self = this, COLUMNS = [COLUMN.SCORE, COLUMN.MEMBER], redisKey,
      sortedSetTableName, sqlCreateSortedSetTable, sql, sqlParams = [], redisResult;

    redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], ':');

    /* For now, no rollback mechanism in favor of efficiency */
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

Redis2MySql.prototype.zscore = function (key, member, cb) {

  if (!(key && member)) {
    cb('Incomplete ZSCORE parameter(s)');
  } if (!is.string(key)) {
    cb('ZSCORE `key` parameter must be a string');
  } if (!is.string(member)) {
    cb('ZSCORE `member` parameter must be a string');
  } else {

    var self = this, redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.sortedSet, key], ':');

    this.redisConn.zscore(redisKey, member, function (err, result) {
      if (err) {
        cb(err);
      } else if (result) {
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
            } else if (result) {
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

Redis2MySql.prototype.createUseSchema = function () {

  var self = this,

    sqlCreateSchema =
      'CREATE DATABASE IF NOT EXISTS ' + this.mysqlConn.escapeId(this.options.mysql.database) +
      ' CHARACTER SET = ' + this.mysqlConn.escape(this.options.mysql.charset),

    sqlUseSchema =
      'USE ' + this.mysqlConn.escapeId(this.options.mysql.database),

    createSchema = function (firstCb) {
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

    useSchema = function (secondCb) {
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
            createSchema,
            useSchema
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
            _rollbackOnErrorIfNotCommit(err, result, connection, function (err) {
              callback(err);
            });
          }
        );
      }
    }
  );
}

function _rollbackOnErrorIfNotCommit(err, result, connection, callback) {
  if (err) {
    connection.rollback(function () {
      callback(err);
    });
  } else {
    connection.commit(function (err) {
      if (err) {
        connection.rollback(function () {
          callback(err);
        });
      } else {
        callback();
        console.log('Commit successful: ' +
          ' Affected rows - ' + result.affectedRows +
          ' Changed rows - ' + result.changedRows);
      }
    });
  }
}

if (typeof module !== 'undefined' &&
  module.exports) {
  module.exports = Redis2MySql;
}
