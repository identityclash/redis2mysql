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

    var self = this, redisKey, sqlOptions;

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

    self.stringTable =
      _prefixAppender([self.options.custom.datatypePrefix.string, type], '_');

    sqlOptions = {
      tableName: self.stringTable,
      setParam: {
        key: self.key,
        value: self.value
      },
      whereParam: {
        key: self.key
      },
      sqlCommand: 'DECODE_INSERT_UPDATE'
    };

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

            var sqlCreateStringTable =
              'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(self.stringTable) +
              '(' +
              self.mysqlConn.escapeId(COLUMN.KEY) + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMN.VALUE + ' VARCHAR(255), ' +
              COLUMN.CREATION_DT + ' TIMESTAMP DEFAULT NOW(), ' +
              COLUMN.LAST_UPDT_DT + ' TIMESTAMP DEFAULT NOW() ON UPDATE NOW()' +
              ') ';

            _createInsertUpdateTable(self, sqlCreateStringTable, sqlOptions, function (err) {
              if (err) {
                self.emit('error', {error: 'mysql', message: err.message});

                // for rollback purposes
                self.redisConn.set(redisKey, originalValue, function (err) {
                  if (err) {
                    cb(err);
                  }
                });
              }
            });
          }
        });
      }
    });
  }
};

Redis2MySql.prototype.get = function (type, param, cb) {

  if (!param) {
    cb('Incomplete GET parameters');
  } else {

    var self = this, redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.string, type, param], ':');

    this.redisConn.get(redisKey, function (err, result) {
      if (err) {
        cb(err);
      } else if (result) {
        cb(null, result);
      } else {

        self.stringTable =
          _prefixAppender([self.options.custom.datatypePrefix.string, type], '_');

        self.mysqlConn.query(
          'SELECT ?? FROM ?? WHERE ?? = ?',
          [
            COLUMN.VALUE,
            self.stringTable,
            COLUMN.KEY,
            param
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

Redis2MySql.prototype.lpush = function (param1, param2, cb) {

  if (!param1) {
    cb('Incomplete SET parameters');
  } else {

    if (typeof param2 === 'function') {
      cb = param2;
      param2 = '';
    }

    var self = this, redisKey = '', sqlOptions = {}, sqlCreateListTable = '';

    if (is.array(param1)) {

      redisKey =
        _prefixAppender([self.options.custom.datatypePrefix.list, param1[0]], ':');

      self.key = param1[0];
      self.value = param1[1];
    } else {

      redisKey =
        _prefixAppender([self.options.custom.datatypePrefix.list, param1], ':');

      self.key = param1;
      self.value = param2;
    }

    if (!self.value) {
      cb('Incomplete SET parameters');
    }

    self.redisConn.lpush(redisKey, self.value, function (err) {
      if (err) {
        cb(err);
      } else {

        cb();

        self.listTable =
          _prefixAppender([self.options.custom.datatypePrefix.list, self.key], '_');

        var sqlLatestSeq = 'SELECT ' + COLUMN.SEQ +
          ' FROM ?? ORDER BY sequence ASC LIMIT 1 ';

        self.mysqlConn.query(
          sqlLatestSeq,
          [
            self.listTable
          ],
          function (err, result) {
            var sequence;
            if (err || lazy(result).isEmpty()) {
              sequence = 0;
            } else {
              sequence = result[0].sequence - 1;
            }

            sqlOptions = {
              tableName: self.listTable,
              setParam: {
                sequence: sequence,
                value: self.value
              },
              sqlCommand: 'INSERT'
            };

            sqlCreateListTable =
              'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(self.listTable) +
              '(' +
              COLUMN.SEQ + ' INTEGER PRIMARY KEY, ' +
              COLUMN.VALUE + ' VARCHAR(255), ' +
              COLUMN.CREATION_DT + ' TIMESTAMP DEFAULT NOW(), ' +
              COLUMN.LAST_UPDT_DT + ' TIMESTAMP DEFAULT NOW() ON UPDATE NOW()' +
              ') ';

            _createInsertUpdateTable(self, sqlCreateListTable, sqlOptions, function (err) {
              if (err) {
                self.emit('error', {error: 'mysql', message: err.message});

                self.redisConn.lpop(redisKey, function (err) {
                  self.emit('error', {error: 'mysql', message: err.message});
                });
              }
            });
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

function _createInsertUpdateTable(obj, sqlCreate, options, callback) {

  var insertSql, insertSqlParams, updateSql, updateSqlParams;

  obj.mysqlConn.query(
    sqlCreate,
    function (err) {
      if (err) {
        callback(err);
      } else {
        console.log('Created table (if not exists) ' + options.tableName);

        if (options.sqlCommand === 'INSERT') {

          insertSql = _sqlBuilder(options);
          insertSqlParams = _sqlParamBuilder(options);

          obj.mysqlConn.query(
            insertSql,
            insertSqlParams,
            function (err, result) {
              _rollbackOnErrorIfNotCommit(obj, err, result, function (err) {
                callback(err);
              });
            }
          );

        } else if (options.sqlCommand === 'UPDATE') {

          updateSql = _sqlBuilder(options);
          updateSqlParams = _sqlParamBuilder(options);

          obj.mysqlConn.query(
            updateSql,
            updateSqlParams,
            function (err, result) {
              _rollbackOnErrorIfNotCommit(obj, err, result, function (err) {
                callback(err);
              });
            }
          );

        } else if (options.sqlCommand === 'DECODE_INSERT_UPDATE') {

          options.sqlCommand = 'INSERT';

          insertSql = _sqlBuilder(options);
          insertSqlParams = _sqlParamBuilder(options);

          obj.mysqlConn.query(
            insertSql,
            insertSqlParams,
            function (err, result) {
              if (err) {

                if (err.code === 'ER_DUP_ENTRY') {

                  options.sqlCommand = 'UPDATE';

                  updateSql = _sqlBuilder(options);
                  updateSqlParams = _sqlParamBuilder(options);

                  obj.mysqlConn.query(
                    updateSql,
                    updateSqlParams,
                    function (err, result) {
                      _rollbackOnErrorIfNotCommit(obj, err, result, function (err) {
                        callback(err);
                      });
                    }
                  );

                } else {
                  obj.mysqlConn.rollback(function () {
                    callback(err);
                  });
                }
              } else {

                obj.mysqlConn.commit(function (err) {
                  if (err) {
                    obj.mysqlConn.rollback(function () {
                      callback(err);
                    });
                  } else {
                    callback(null, result);
                    if (result.affectedRows > 0) {
                      console.log('Affected ' + result.affectedRows + ' row(s).');
                    }
                  }
                });
              }
            }
          );
        }
      }
    }
  );
}

function _rollbackOnErrorIfNotCommit(obj, err, result, callback) {
  if (err) {
    obj.mysqlConn.rollback(function () {
      callback(err);
    });
  } else {
    obj.mysqlConn.commit(function (err) {
      if (err) {
        obj.mysqlConn.rollback(function () {
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

function _sqlBuilder(options) {
  var sql =
      (options.sqlCommand === 'INSERT') ? 'INSERT ?? ' : 'UPDATE ?? ',
    key;

  sql += 'SET ';
  for (key in options.setParam) {
    if (options.setParam.hasOwnProperty(key)) {
      if (!(key === COLUMN.CREATION_DT && key === COLUMN.LAST_UPDT_DT)) {
        sql += '?? = ?, ';
      }
    }
  }

  sql = sql.substring(0, sql.length - ', '.length);
  if (options.sql_cmd === 'UPDATE') {
    if (options.whereParam) {
      sql += ' WHERE ';
    }
    for (key in options.whereParam) {
      if (options.whereParam.hasOwnProperty(key)) {
        sql += '?? = ? AND ';
      }
    }
    sql = sql.substring(0, sql.length - ' AND '.length);
  }
  return sql;
}

function _sqlParamBuilder(options) {
  var array = [
    options.tableName
  ], key;

  for (key in options.setParam) {
    if (options.setParam.hasOwnProperty(key)) {
      if (!(key === COLUMN.CREATION_DT && key === COLUMN.LAST_UPDT_DT)) {
        array.push(key);
        array.push(options.setParam[key]);
      }
    }
  }

  if (options.sqlCommand === 'UPDATE' && options.whereParam) {
    for (key in options.whereParam) {
      if (options.whereParam.hasOwnProperty(key)) {
        array.push(key);
        array.push(options.whereParam[key]);
      }
    }
  }

  return array;
}

if (typeof module !== 'undefined' &&
  module.exports) {
  module.exports = Redis2MySql;
}
