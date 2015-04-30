/*
 * Copyright (c) 2014. LetsBlumIt Corp.
 */

var Mappy = require('../lib/Redis2MySql');

var mappy = new Mappy({
    'mysql': {
      'user': 'root'
    },
    'custom': {
      'schema_name': 'mytest',
      'schema_charset': 'utf8',
      'datatype_prefix': {
        'string': 'str',
        'list': 'lst',
        'set': 'set',
        'sorted_set': 'sset',
        'hash': 'map'
      }
    }
  }
);

//,
//'database': 'mytest',
//  'charset': 'utf8'

mappy.createUseSchema();
setTimeout(function (err) {
  if (err) {
    console.log(err);
  } else {
    mappy.on('error', function (err) {
      console.log('Error from listener: ' + err.message);
    });

    mappy.get('email', 'x', function (err, result) {
      if (err) {
        console.log('Error on get: ' + err);
      } else {
        console.log('x is initially ' + result);
      }
    });

    mappy.set('email', ['x', 'john@blumr.com'], function (err) {
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
