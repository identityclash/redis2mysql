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
    cb('Incorrect SET type parameter');
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

    // for rollback purposes
    this.redisConn.get(redisKey, function (err, originalValue) {

      if (err) {
        cb(err);
      } else {

        self.redisConn.set(redisKey, value, function (err, result) {
          if (err) {
            cb(err);
          } else {

            cb(null, result); // return to client optimistically

            var COLUMNS = [self.mysqlConn.escapeId(COLUMN.KEY), COLUMN.VALUE],
              sqlCreateStringTable, sql, sqlParam;

            sqlCreateStringTable =
              'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(stringTableName) +
              '(' +
              self.mysqlConn.escapeId(COLUMN.KEY) + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMN.VALUE + ' VARCHAR(255), ' +
              COLUMN.CREATION_DT + ' TIMESTAMP DEFAULT NOW(), ' +
              COLUMN.LAST_UPDT_DT + ' TIMESTAMP DEFAULT NOW() ON UPDATE NOW()' +
              ') ';

            sql = new SqlBuilder().insert('??', COLUMNS)
              .values(COLUMNS.length).onDuplicate(2).toString();

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
                }
              }
            );
          }
        });
      }
    });
  }
};

Redis2MySql.prototype.get = function (type, key, cb) {

  if (!key) {
    cb('Incomplete GET parameters');
  } else if (!(is.string(type) && is.string(key))) {
    cb('Incorrect GET type parameter');
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
    cb('Incomplete SET parameters');
  } else if (!(is.string(values) || is.number(values) ||
    is.array(values))) {
    cb('Incorrect LPUSH value parameter(s)');
  } else {

    var self = this, redisKey, arrayValues = [], listTableName, sqlLatestSeq;

    if (is.string(values) || is.number(values)) {
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
              sqlParams, function (err) {

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

  if (!(key && is.existy(redisIndex))) {
    cb('Incomplete LINDEX parameter(s)');
  } else if (!is.number(redisIndex)) {
    cb('Incorrect LINDEX index parameter');
  } else {

    var self = this, redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.list, key], ':');

    this.redisConn.lindex(redisKey, redisIndex, function (err, result) {
      if (err) {
        cb(err);
      } else if (false) {
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
    cb('Incomplete LINDEX parameter(s)');
  } else if (!is.number(redisIndex)) {
    cb('Incorrect LINDEX index parameter');
  } else if (!(is.string(value) || is.number(value))) {
    cb('Incorrect LINDEX value parameter');
  } else {

    var self = this, redisKey =
      _prefixAppender([self.options.custom.datatypePrefix.list, key], ':');

    this.redisConn.lset(redisKey, redisIndex, value, function (err, result) {
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
          .where({sequence: '(' + selectSql + ') '}).toString();

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
              self.emit('error', {error: 'mysql', message: err.message});
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
