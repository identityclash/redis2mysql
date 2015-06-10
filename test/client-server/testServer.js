/*
 * Copyright (c) 2015.
 */
'use strict';

var
  is = require('is_js'),
  async = require('async'),
  Redis = require('ioredis'),
  mysql = require('mysql'),
  Redis2MySql = require('../../lib/Redis2MySql'),
  http = require('http'),
  sockjs = require('sockjs'),
  webServer = http.createServer(),
  connection = {
    mysql: {
      user: 'root',
      database: 'mytestxxx',
      charset: 'utf8'
    }
  },
  sockjsServer = sockjs.createServer(
    {
      //sockjs_url: 'ws://localhost'
      sockjs_url: 'https://cdnjs.cloudflare.com/ajax/libs/sockjs-client/0.3.4/sockjs.min.js',
      prefix: '/ws'
    }),
  instance = new Redis2MySql({
    redis: {
      showFriendlyErrorStack: true
    },
    mysql: {
      user: connection.mysql.user,
      database: connection.mysql.database,
      charset: connection.mysql.charset,
      multipleStatements: 'true'
    },
    custom: {
      datatypePrefix: {
        string: 'str',
        list: 'lst',
        set: 'set',
        sortedSet: 'zset',
        hash: 'map'
      }
      ,
      schemaName: connection.mysql.database,
      schemaCharset: connection.mysql.charset
    }
  });

instance.createUseSchema();

sockjsServer.installHandlers(webServer);//, {prefix:'/echo/websocket'});
webServer.listen(9999, '0.0.0.0'); // 8081

sockjsServer.on('connection', function (conn) {

  console.log('connection' + conn);

  conn.on('open', function () {
    console.log('open');
  });

  conn.on('close', function () {
    console.log('close ' + conn);
  });

  conn.on('data', function (message) {
    console.log('message ' + conn, message);

    var dataKey, ctx, fnArgs, fnName, jsonObj;

    jsonObj = JSON.parse(message);
    jsonObj.params.push(function (err, result) {
      if (err) {
        return console.log('error: ' + err);
      }
      console.log('result of function call: ' + result);
    });

    for (dataKey in jsonObj) {
      if (jsonObj.hasOwnProperty(dataKey)) {

        console.log('dataKey: ' + dataKey);
        console.log('message[dataKey]: ' + jsonObj[dataKey]);

        if (dataKey === 'object') {
          ctx = jsonObj[dataKey];
        }
        if (dataKey === 'command') {
          fnName = jsonObj[dataKey];
        }
        if (dataKey === 'params') {
          fnArgs = jsonObj[dataKey];
        }
      }
    }

    executeFunctionByName(fnName, ctx, fnArgs);
  });
});

//setTimeout(function () {
//
//  console.log('Delaying...');
//
//  var dataKey, ctx, fnName, fnArgs, jsonObj = {
//    object: 'Redis2MySql',
//    command: 'set',
//    params: ['typ', ['ky', 'val'], function (err, result) {
//      if (err) {
//        return console.log('error: ' + err);
//      }
//      console.log(result);
//    }]
//  };
//
//  for (dataKey in jsonObj) {
//    if (jsonObj.hasOwnProperty(dataKey)) {
//
//      console.log('dataKey: ' + dataKey);
//      console.log('message[dataKey]: ' + jsonObj[dataKey]);
//
//      if (dataKey === 'object') {
//        ctx = jsonObj[dataKey];
//      }
//      if (dataKey === 'command') {
//        fnName = jsonObj[dataKey];
//      }
//      if (dataKey === 'params') {
//        fnArgs = jsonObj[dataKey];
//      }
//    }
//  }
//
//  executeFunctionByName(fnName, ctx, fnArgs);
//}, 1000);

function executeFunctionByName(functionName, context /*, args */) {
  console.log(functionName);
  console.log(context);
  console.log(arguments);

  if (context === 'Redis2MySql') {
    return instance[functionName](arguments[2]);//.apply(instance, arguments[2]);
  }
}
