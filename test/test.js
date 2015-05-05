/*
 * Copyright (c) 2014. LetsBlumIt Corp.
 */
'use strict';

var Mappy = require('../lib/Redis2MySql'),
  mappy = new Mappy({
      mysql: {
        user: 'root',
        database: 'mytest',
        charset: 'utf8'
      },
      custom: {
        schemaName: 'mytest',
        schema_charset: 'utf8',
        datatypePrefix: {
          string: 'str',
          list: 'lst',
          set: 'set',
          sorted_set: 'sset',
          hash: 'map'
        }
      }
    }
  );

mappy.createUseSchema();

mappy.on('error', function (err) {
  console.log('Error from listener: ' + err.message);
});

mappy.set('email', ['x', 'hello@blumr.com'], function (err) {
  if (err) {
    console.log('Error on set: ' + err);
  }
});

mappy.get('email', 'x', function (err, result) {
  if (err) {
    console.log('Error on get: ' + err);
  } else {
    console.log('x is finally ' + result);
  }
});

mappy.lpush('name', ['four', 'five', 'six'], function (err) {
  if (err) {
    console.log('Error inserting in names: ' + err);
  }
});

mappy.lindex('name', -7, function (err, result) {
  if (err) {
    console.log('Error on lindex: ' + err);
  } else {
    console.log('lst:name is ' + result);
  }
});


setTimeout(function (err) {
  if (err) {
    console.log(err);
  } else {
    mappy.quit(function (err) {
      if (err) {
        console.log('Error on quit: ' + err);
      }
    });
  }
}, 5000);
