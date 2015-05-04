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

setTimeout(function (err) {
  if (err) {
    console.log(err);
  } else {

    mappy.get('email', 'x', function (err, result) {
      if (err) {
        console.log('Error on get: ' + err);
      } else {
        console.log('x is initially ' + result);
      }
    });

    mappy.set('email', ['x', 'honey@blumr.com'], function (err) {
      if (err) {
        console.log('Error on set: ' + err);
      }
    });

    setTimeout(function (err) {
      if (err) {
        console.log(err);
      } else {
        mappy.get('email', 'x', function (err, result) {
          if (err) {
            console.log('Error on get: ' + err);
          } else {
            console.log('x is finally ' + result);
          }
        });
      }
    }, 1000);
  }
}, 2000);

mappy.lpush('name', 'three three', function (err) {
  if (err) {
    console.log('Error inserting in names: ' + err);
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
