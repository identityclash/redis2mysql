/*
 * Copyright (c) 2015.
 */
'use strict';

var
  is = require('is_js'),
  async = require('async'),
  Redis = require('ioredis'),
  mysql = require('mysql'),
  SockJS = require('sockjs-client'),
  client = new SockJS('http://localhost:9999/ws');

client.onopen = function () {
  console.log('connected');
  client.send(JSON.stringify(
    {
      command: 'set',
      params: [
        'sometype',
        ['somekey', '10']
      ]
    }));
  client.send(JSON.stringify(
    {
      command: 'get',
      params: [
        'sometype',
        'somekey'
      ]
    }));
  client.send(JSON.stringify(
    {
      command: 'incr',
      params: [
        'sometype',
        'somekey'
      ]
    }));
  client.send(JSON.stringify(
    {
      command: 'get',
      params: [
        'sometype',
        'somekey'
      ]
    }));
  setTimeout(function () {
    client.send(JSON.stringify(
      {
        command: 'del',
        params: ['str:sometype:somekey']
      }));
  }, 400);
};

client.onclose = function () {
  console.log('disconnected');
  client.close();
};

client.onmessage = function (msg) {
  console.log(msg);
};
