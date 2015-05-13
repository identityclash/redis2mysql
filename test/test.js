/*
 * Copyright (c) 2014. LetsBlumIt Corp.
 */
'use strict';

var Mappy = require('../lib/Redis2MySql'),
  is = require('is_js'),
  mappy = new Mappy({
      redis: {
        showFriendlyErrorStack: true
      },
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
          sortedSet: 'zset',
          hash: 'map'
        }
      }
    }
  );

mappy.createUseSchema();

mappy.on('error', function (err) {
  console.log('Error from listener: ' + err);
});

mappy.incr('email', 'd', function (err, result) {
  if (err) {
    console.log('Error on INCR: ' + err);
  } else {
    console.log('email INCR: ' + result);
  }
});

mappy.set('email', ['a', 'abc'], function (err, result) {
  if (err) {
    console.log('Error on SET: ' + err);
  } else {
    console.log('email SET: ' + result);
  }
});

mappy.get('email', 'd', function (err, result) {
  if (err) {
    console.log('Error on GET: ' + err);
  } else {
    console.log('x GET finally: ' + result);
  }
});

mappy.del(['str:email:a', 'str:email:b', 'map:email'], function (err, result) {
  if (err) {
    console.log('Error on DEL ' + err);
  } else {
    console.log('DEL result: ' + result);
  }
});

mappy.exists('email', 'x', function (err, result) {
  if (err) {
    console.log('Error on EXISTS: ' + err);
  } else {
    console.log('x EXISTS finally: ' + result);
  }
});

mappy.lpush('name', ['one', 'two', 'three', 'four'], function (err, result) {
  if (err) {
    console.log('Error on LPUSH: ' + err);
  } else {
    console.log('names LPUSH: ' + result);
  }
});

mappy.lindex('nameZ', 0, function (err, result) {
  if (err) {
    console.log('Error on LINDEX: ' + err);
  } else {
    console.log('name LINDEX: ' + result);
  }
});

mappy.lset('name', -5, 'VALFIVE', function (err, result) {
  if (err) {
    console.log('Error on LSET: ' + err);
  } else {
    console.log('name LSET: ' + result);
  }
});

mappy.sadd('sname', [1, 2, 3, 'a', 'b', 'c'], function (err, result) {
  if (err) {
    console.log('Error on SADD: ' + err);
  } else {
    console.log('snames SADD: ' + result);
  }
});

mappy.srem('sname', [3, 2], function (err, result) {
  if (err) {
    console.log('Error on SREM: ' + err);
  } else {
    console.log('snames SREM: ' + result);
  }
});

mappy.smembers('sname', function (err, result) {
  if (err) {
    console.log('Error on SMEMBERS: ' + err);
  } else if (is.existy(result) && result.length > 0){
    for (var i = 0; i < result.length; i++) {
      console.log('sname SMEMBERS: ' + result[i]);
    }
  }
});

mappy.sismember('sname', 'a', function (err, result) {
  if (err) {
    console.log('Error on SISMEMBER: ' + err);
  } else {
    console.log('snames SISMEMBER: ' + result);
  }
});

mappy.scard('sname', function (err, result) {
  if (err) {
    console.log('Error on SCARD: ' + err);
  } else {
    console.log('sname SCARD: ' + result);
  }
});

mappy.zadd('zname', [4.4, 'four point four',
  5.5, 'five point five',
  6.5, 'six point three',
  7.1, 'seven point one'], function (err, result) {
  if (err) {
    console.log('Error on ZADD: ' + err);
  } else {
    console.log('sname ZADD: ' + result);
  }
});

mappy.zincrby('zname', 1.9, 'one', function (err, result) {
  if (err) {
    console.log('Error on ZINCRBY: ' + err);
  } else {
    console.log('sname ZINCRBY: ' + result);
  }
});

mappy.zscore('zname', 'four point four', function (err, result) {
  if (err) {
    console.log('Error on ZSCORE: ' + err);
  } else {
    console.log('zname ZSCORE: ' + result);
  }
});

mappy.zrangebyscore('zname', '-inf', 7, 'withscores', 'limit', 1, 5,
  function (err, result) {
    if (err) {
      console.log('Error on ZRANGEBYSCORE: ' + err);
    } else {
      if (is.existy(result) && is.not.empty(result)) {
        for (var i = 0; i < result.length; i++) {
          console.log('zname ZRANGEBYSCORE: ' + result[i]);
        }
      } else {
        console.log('zname ZRANGEBYSCORE: ' + result);
      }
    }
  });

mappy.zrevrangebyscore('zname', 'inf', '(4.40', 'withscores', 'limit', 1, 4,
  function (err, result) {
    if (err) {
      console.log('Error on ZREVRANGEBYSCORE: ' + err);
    } else if (is.existy(result) && is.not.empty(result)) {
      for (var i = 0; i < result.length; i++) {
        console.log('zname ZREVRANGEBYSCORE: ' + result[i]);
      }
    } else {
      console.log('zname ZREVRANGEBYSCORE: ' + result);
    }
  });

mappy.hset('email', 'b', 'byyGffuts@blumr.com', function (err, result) {
  if (err) {
    console.log('Error on HSET: ' + err);
  } else {
    console.log('email HSET: ' + result);
  }
});

mappy.hmset('email', ['x', 'nuts@blumr.com', 'y', 'go@blumr.com',
  'z', 'noooo@blumr.com', 'a', 'howzy@blumr.com'], function (err, result) {
  if (err) {
    console.log('Error on HSET: ' + err);
  } else {
    console.log('email HSET: ' + result);
  }
});

mappy.hget('email', 'x', function (err, result) {
  if (err) {
    console.log('Error on HGET: ' + err);
  } else {
    console.log('x HGET finally: ' + result);
  }
});

mappy.hmget('email', ['x', 'y', 'z', 'a'], function (err, result) {

  var i, value;
  if (err) {
    console.log('Error on HMGET: ' + err);
  } else {
    for (i = 0; i < result.length; i++) {
      if (typeof result[i] === 'object') {
        for (value in result[i]) {
          if (result[i].hasOwnProperty(value)) {
            console.log('email HGET: ' + result[i][value]);
          }
        }
      } else {
        console.log('email HMGET: ' + result[i]);
      }
    }
  }
});

mappy.hgetall('email', function (err, result) {

  var value;
  if (err) {
    console.log('Error on HGETALL: ' + err);
  } else {
    if (typeof result === 'object') {
      for (value in result) {
        if (result.hasOwnProperty(value)) {
          console.log('email HGETALL: ' + value + ' ' + result[value]);
        }
      }
    } else {
      console.log('email HGETALL: ' + result);
    }
  }
});

mappy.hexists('email', 'x', function (err, result) {
  if (err) {
    console.log('Error on HEXISTS: ' + err);
  } else {
    console.log('x HEXISTS finally: ' + result);
  }
});

mappy.hdel('email', ['aaaa', 'aaax'], function (err, result) {
  if (err) {
    console.log('Error on HDEL: ' + err);
  } else {
    console.log('email HDEL finally: ' + result);
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
}, 3000);
