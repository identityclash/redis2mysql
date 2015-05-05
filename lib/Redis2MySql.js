/*
 * Copyright (c) 2015.
 */
'use strict';

var async = require('async'),
  util = require('util'),
  EventEmitter = require('events'),
  Redis = require('ioredis'),
  mysql = require('mysql'),
  SqlBuilder = require('./SqlBuilder'),
  COLUMN = {
    SEQ: 'sequence',
    KEY: 'key',
    VALUE: 'value',
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

  if (!param1) {
    cb('Incomplete SET parameters');
  } else {

    if (typeof param2 === 'function') {
      cb = param2;
      param2 = '';
    }

    var self = this, redisKey;

    if (Array.isArray(param1)) {

      redisKey =
        _prefixAppender([self.options.custom.datatypePrefix.string, type, param1[0]], ':');

      self.key = param1[0];
      self.value = param1[1] || '';
    } else {

      redisKey =
        _prefixAppender([self.options.custom.datatypePrefix.string, type, param1], ':');

      self.key = param1;
      self.value = param2 || '';
    }

    self.stringTableName =
      _prefixAppender([self.options.custom.datatypePrefix.string, type], '_');

    // for rollback purposes
    this.redisConn.get(redisKey, function (err, originalValue) {

      if (err) {
        cb(err);
      } else {

        self.redisConn.set(redisKey, self.value, function (err) {
          if (err) {
            cb(err);
          } else {

            cb(); // return to client optimistically

            var COLUMNS = [self.mysqlConn.escapeId(COLUMN.KEY), COLUMN.VALUE], sqlCreateStringTable, sql,
              updateParam, sqlParam;

            sqlCreateStringTable =
              'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(self.stringTableName) +
              '(' +
              self.mysqlConn.escapeId(COLUMN.KEY) + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMN.VALUE + ' VARCHAR(255), ' +
              COLUMN.CREATION_DT + ' TIMESTAMP DEFAULT NOW(), ' +
              COLUMN.LAST_UPDT_DT + ' TIMESTAMP DEFAULT NOW() ON UPDATE NOW()' +
              ') ';

            sql = new SqlBuilder().insert('??', COLUMNS)
              .values(COLUMNS.length).onDuplicate(2).toString();

            sqlParam = [self.stringTableName, self.key, self.value, COLUMN.KEY,
              self.key, COLUMN.VALUE, self.value];

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
                }
              }
            );
          }
        });
      }
    });
  }
};

Redis2MySql.prototype.get = function (type, value, cb) {

  if (!value) {
    cb('Incomplete GET parameters');
  } else {

    var self = this, redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.string, type, value], ':');

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
            value
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
    cb('Incomplete SET parameters');
  } else {

    var self = this, redisKey;

    if (typeof values === 'string') {
      self.key = key;
    } else {
      cb('Incorrect LPUSH key parameter');
    }

    if (typeof values === 'string') {
      self.values = [values];
    } else if (Array.isArray(values)) {
      self.values = values;
    } else {
      cb('Incorrect LPUSH value parameter(s)');
    }

    redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.list, key], ':');

    self.redisConn.lpush(redisKey, self.values, function (err) {

      if (err) {
        cb(err);
      } else {

        cb();

        self.listTableName =
          _prefixAppender([self.options.custom.datatypePrefix.list, self.key], '_');

        var sqlLatestSeq = 'SELECT ' + COLUMN.SEQ +
          ' FROM ?? ORDER BY sequence ASC LIMIT 1 ';

        self.mysqlConn.query(
          sqlLatestSeq,
          self.listTableName,
          function (err, result) {

            var COLUMNS = [COLUMN.SEQ, COLUMN.VALUE], seq = 0,
              sqlCreateListTable, sql, sqlParams = [self.listTableName];

            if (!err && result !== undefined) {
              seq = result[0].sequence;
            }

            sqlCreateListTable =
              'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(self.listTableName) +
              '(' +
              COLUMN.SEQ + ' INTEGER PRIMARY KEY, ' +
              COLUMN.VALUE + ' VARCHAR(255), ' +
              COLUMN.CREATION_DT + ' TIMESTAMP DEFAULT NOW(), ' +
              COLUMN.LAST_UPDT_DT + ' TIMESTAMP DEFAULT NOW() ON UPDATE NOW()' +
              ') ';

            sql = new SqlBuilder().insert('??', COLUMNS)
              .values(COLUMNS.length, self.values.length).toString();

            for (var i = 0; i < self.values.length; i++) {

              if (err || result === undefined) {
                sqlParams.push(seq--);
              } else {
                sqlParams.push(--seq);
              }

              sqlParams.push(self.values[i]);
            }

            _createInsertUpdate(self.mysqlConn, sqlCreateListTable, sql, sqlParams, function (err) {
              if (err) {
                self.emit('error', {error: 'mysql', message: err.message});

                self.redisConn.lpop(redisKey, function (err) {
                  self.emit('error', {error: 'mysql', message: err});
                });
              }
            });
          }
        );
      }
    });
  }
};

Redis2MySql.prototype.lindex = function (key, redisIndex, cb) {

  if (!key || util.isNullOrUndefined(redisIndex)) {
    cb('Incomplete LINDEX parameter(s)');
  } else {

    if (typeof redisIndex !== 'number') {
      cb('Incorrect LINDEX index parameter');
    }

    var self = this, redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.list, key], ':');

    self.key = key;

    this.redisConn.lindex(redisKey, redisIndex, function (err, result) {
      if (err) {
        cb(err);
      } else if (result) {
        cb(null, result);
      } else {

        self.listTableName =
          _prefixAppender([self.options.custom.datatypePrefix.list, self.key], '_');

        var sql, startingCounter, offset;

        if (redisIndex > 0) {
          offset = 1;
          startingCounter = -1;
        } else {
          offset = -1;
          startingCounter = 0;
        }

        sql = 'SELECT inner_table.value ' +
          ' FROM ' +
          '(  ' +
          '  SELECT @i := @i + (' + offset + ') AS row_num, ??, ?? ' +
          '    FROM ?? , (SELECT @i := ?) counter ' +
          '  ORDER BY row_num ASC ' +
          ') inner_table ' +
          'WHERE row_num = ?';

        self.mysqlConn.query(
          sql,
          [
            COLUMN.VALUE,
            COLUMN.SEQ,
            self.listTableName,
            startingCounter,
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
        callback(null, result);
        if (result.changedRows > 0) {
          console.log('Modified ' + result.changedRows + ' row(s).');
        }
        if (result.affectedRows > 0) {
          console.log('Affected ' + result.affectedRows + ' row(s).');
        }
      }
    });
  }
}

if (typeof module !== 'undefined' &&
  module.exports) {
  module.exports = Redis2MySql;
}
