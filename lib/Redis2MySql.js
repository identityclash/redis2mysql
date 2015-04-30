/*
 * Copyright (c) 2015.
 */
'use strict';

var async = require('async'),
  util = require('util'),
  EventEmitter = require('events'),
  redis = require('redis'),
  mysql = require('mysql'),
  my_util = require('./util'),
  COLUMN = {
    'ID': 'id',
    'KEY': 'key',
    'VALUE': 'value',
    'SCORE': 'score',
    'CREATION_DT': 'creation_date',
    'LAST_UPDT_DT': 'last_update_date'
  };

/**
 * Module functions
 */
function Redis2MySql(options) {

  if (!this instanceof Redis2MySql) {
    return new Redis2MySql(options);
  }

  if (options.custom.datatype_prefix){

  }

  if (options.redis === undefined) {
    options.redis = {
      'host': '',
      'port': ''
    };
  }

  this.redisConn = redis.createClient(options.redis.port, options.redis.host, options.redis);

  if (options.mysql === undefined) {
    options.mysql = {
      'host': '',
      'port': ''
    };
  }

  this.mysqlConn = mysql.createConnection(options.mysql);

  this.mysqlConn.connect();

  options.mysql.database = options.mysql.database || options.custom.schema_name;
  options.mysql.charset = options.mysql.charset || options.custom.schema_charset;

  this.options = options;

  var self = this;

  // on error emission
  this.redisConn.on('error', function (err) {
    self.emit('error', {error: 'redis', 'message': err.message});
  });

  this.mysqlConn.on('error', function (err) {
    self.emit('error', {'error': 'mysql', 'message': err.message});
  });
}

util.inherits(Redis2MySql, EventEmitter);

//Redis2MySql.prototype.quit = function (cb) {
//
//  this.redisConn.quit();
//
//  this.mysqlConn.end(function (err) {
//    cb(err);
//  });
//}

Redis2MySql.prototype.set = function (type, param1, param2, cb) {

  var self = this;

  if (!param1) {
    cb('Incomplete function parameters');
  } else {

    if (typeof param2 == 'function') {
      cb = param2;
      param2 = '';
    }

    var redis_key = '';

    if (my_util.isArray(param1)) {

      redis_key =
        _prefixAppender([self.options.custom.datatype_prefix.string, type, param1[0]], ':');

      self.key = param1[0];
      self.value = param1[1] || '';
    } else {

      redis_key =
        _prefixAppender([self.options.custom.datatype_prefix.string, type, param1], ':');

      self.key = param1;
      self.value = param2 || '';
    }

    // for rollback purposes
    this.redisConn.get(redis_key, function (err, original_value) {

      if (err) {
        cb(err);
      } else {

        self.redisConn.set(redis_key, self.value, function (err) {
          if (err) {
            cb(err);
          } else {

            cb(); // return to client optimistically

            self.string_table =
              _prefixAppender([self.options.custom.datatype_prefix.string, type], '_');

            var sql_create_string_table =
              'CREATE TABLE IF NOT EXISTS ' + self.mysqlConn.escapeId(self.string_table) +
              '(' +
              self.mysqlConn.escapeId(COLUMN.KEY) + ' VARCHAR(255) PRIMARY KEY, ' +
              COLUMN.VALUE + ' VARCHAR(255), ' +
              COLUMN.CREATION_DT + ' TIMESTAMP DEFAULT NOW(), ' +
              COLUMN.LAST_UPDT_DT + ' TIMESTAMP DEFAULT NOW() ON UPDATE NOW()' +
              ') ';

            var sql_options = {
              'table_name': self.string_table,
              'set_param': {
                'key': self.key,
                'value': self.value
              },
              'where_param': {
                'key': self.key
              }
            };

            _insertUpdateTbl(self, sql_create_string_table, sql_options, function (err) {
              if (err) {
                self.emit('error', {'error': 'mysql', 'message': err.message});

                // for rollback purposes
                self.redisConn.set(redis_key, original_value, function (err) {
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
}

Redis2MySql.prototype.get = function (type, param, cb) {

  if (!param) {
    cb('Incomplete function parameters');
  } else {
    var self = this;

    var redis_key =
      _prefixAppender([self.options.custom.datatype_prefix.string, type, param], ':');

    this.redisConn.get(redis_key, function (err, result) {
      if (err) {
        cb(err);
      }
      else if (result) {
        cb(null, result);
      } else {

        self.string_table =
          _prefixAppender([self.options.custom.datatype_prefix.string, type], '_');

        self.mysqlConn.query(
          'SELECT ?? FROM ?? WHERE ?? = ?',
          [
            COLUMN.VALUE,
            self.string_table,
            COLUMN.KEY,
            param
          ],
          function (err, result) {
            if (err) {
              self.emit('error', {'error': 'mysql', 'message': err.message});
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
}

Redis2MySql.prototype.lset = function (params, cb) {

  var self = this;

  if (typeof params == 'Object') {

    var options = {
      'table_name': self.hash_tbl,
      'set_param': {
        'key': self.key,
        'value': self.value
      },
      'where_param': {
        'key': self.key
      }
    };

  }
}

Redis2MySql.prototype.createUseSchema = function () {

  var self = this;

  var sql_create_schema =
    'CREATE DATABASE IF NOT EXISTS ' + this.mysqlConn.escapeId(this.options.mysql.database) +
    ' CHARACTER SET = ' + this.mysqlConn.escape(this.options.mysql.charset);

  var sql_use_schema =
    'USE ' + this.mysqlConn.escapeId(this.options.mysql.database);

  var createSchema = function (firstCb) {
    self.mysqlConn.query(
      sql_create_schema,
      function (err) {
        if (!err) {
          console.log('Created schema (if not exists) ' + self.options.custom.schema_name);
        }
        firstCb(err);
      }
    )
  };

  var useSchema = function (secondCb) {
    self.mysqlConn.query(
      sql_use_schema,
      function (err) {
        if (!err) {
          console.log('Using ' + self.options.custom.schema_name);
        }
        secondCb(err);
      }
    )
  }

  this.mysqlConn.query(
    'SELECT DATABASE() AS used FROM DUAL',
    function (err, result) {
      if (err) {
        throw err;
      }
      if (result[0].used == null) {
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
}

function _prefixAppender(prefices, delimiter) {

  var str = '';

  for (var prefix in prefices){
    str += (prefices[prefix] + delimiter);
  }

  return str.substring(0, str.length - delimiter.length);
}

function _insertUpdateTbl(obj, sql_create, options, callback) {

  obj.mysqlConn.query(
    sql_create,
    function (err) {
      if (err) {
        callback(err);
      } else {
        console.log('Created table (if not exists) ' + obj.string_table);

        options.sql_cmd = 'INSERT';

        var insert_sql = _sqlBuilder(options);
        var insert_sql_params = _sqlParamBuilder(options);

        obj.mysqlConn.query(
          insert_sql,
          insert_sql_params,
          function (err, result) {
            if (err) {

              if (err.code == 'ER_DUP_ENTRY') {

                options.sql_cmd = 'UPDATE';

                var update_sql = _sqlBuilder(options);
                var update_sql_params = _sqlParamBuilder(options);

                obj.mysqlConn.query(
                  update_sql,
                  update_sql_params,
                  function (err, result) {
                    if (err) {
                      obj.mysqlConn.rollback(function () {
                        callback(err);
                      });
                    } else {
                      obj.mysqlConn.commit(function (err) {
                        if (err) {
                          self.mysqlConn.rollback(function () {
                            callback(err);
                          });
                        } else {
                          callback(null, result);
                          console.log('Modified ' + result.changedRows + ' row(s).');
                        }
                      });
                    }
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
                  console.log('Inserted row ID: ' + result.insertId + '');
                }
              });
            }
          }
        );
        ;
      }
    }
  );
}

function _sqlBuilder(options) {
  var sql =
    (options.sql_cmd == 'INSERT') ? 'INSERT ?? ' : 'UPDATE ?? ';
  sql += 'SET ';
  for (var key in options.set_param) {
    if (!(key == COLUMN.CREATION_DT && key == COLUMN.LAST_UPDT_DT)){
      sql += '?? = ?, ';
    }
  }

  sql = sql.substring(0, sql.length - ', '.length);
  if (options.sql_cmd == 'UPDATE') {
    if (options.where_param) {
      sql += ' WHERE ';
    }
    for (var key in options.where_param) {
      sql += '?? = ? AND ';
    }
    sql = sql.substring(0, sql.length - ' AND '.length);
  }
  return sql;
}

function _sqlParamBuilder(options) {
  var array = [
    options.table_name
  ];

  for (var key in options.set_param) {
    if (!(key == COLUMN.CREATION_DT && key == COLUMN.LAST_UPDT_DT)){
      array.push(key);
      array.push(options.set_param[key]);
    }
  }

  if (options.sql_cmd == 'UPDATE' && options.where_param) {
    for (var key in options.where_param) {
      array.push(key);
      array.push(options.where_param[key]);
    }
  }

  return array;
}

if (typeof module !== 'undefined' &&
  module.exports) {
  module.exports = Redis2MySql;
}
