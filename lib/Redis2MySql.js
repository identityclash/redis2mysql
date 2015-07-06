/*
 * Copyright (c) 2015.
 */
'use strict';

var async = require('async')
  , is = require('is_js')
  , lazy = require('lazy.js')
  , EventEmitter = require('events')
  , Redis = require('ioredis')
  , mysql = require('mysql')
  , logger = require('winston')
  , modelo = require('modelo')
  , StringCommands = require('./commands/stringCommands')
  , ListCommands = require('./commands/listCommands')
  , SetCommands = require('./commands/setCommands')
  , SortedSetCommands = require('./commands/sortedSetCommands')
  , HashCommands = require('./commands/hashCommands')
  , AllCommands = require('./commands/allCommands');

var PREFIXES = [
    'string',
    'list',
    'set',
    'sortedSet',
    'hash'
  ];

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

modelo.inherits(Redis2MySql, EventEmitter, StringCommands, ListCommands,
  SetCommands, SortedSetCommands, HashCommands, AllCommands);

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

if (typeof module !== 'undefined' &&
  module.exports) {
  module.exports = Redis2MySql;
}
