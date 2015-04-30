/*
 * Copyright (c) 2014. LetsBlumIt Corp.
 */
/**
 * Helper functions
 */
module.exports = {
  noop: function _noop() {
    return; // no operation
  },

  isObject: function _isObject(value) {
    var type = typeof value;
    return type === 'function' ||
      (value && type === 'object') ||
      false;
  },

  isArray: function _isArray(value) {
    var check = Array.isArray ||
      function (value) {
        return (value &&
          typeof value === 'object' &&
          typeof value.length === 'number' &&
          _toString.call(value) === '[object Array]') ||
          false;
      };
    return check(value);
  },

  isBoolean: function _isBoolean(value) {
    return (value === true ||
      value === false ||
      value &&
      typeof value === 'object' &&
      _toString.call(value) === '[object Boolean]') ||
      false;
  },

  isString: function _isString(value) {
    return typeof value === 'string' ||
      (value &&
      typeof value === 'object' &&
      _toString.call(value) === '[object String]') ||
      false;
  },

  isFunction: function _isFunction(value) {
    return typeof value === 'function' || false;
  },

  isNonNegativeInt: function _isNonNegativeInt(value) {
    return value === Number(value) &&
      value % 1 === 0 &&
      value > -1;
  },

  mergeObjects: function _mergeObjects(obj1, obj2) {
    var key, obj3 = {};
    for (key in obj1) {
      if (obj1.hasOwnProperty(key)) {
        obj3[key] = obj1[key];
      }
    }
    for (key in obj2) {
      if (obj2.hasOwnProperty(key)) {
        obj3[key] = obj2[key];
      }
    }
    return obj3;
  },

  arrayDiff: function _arrayDiff(array1, array2) {
    return array1.filter(
      function (item) {
        return array2.indexOf(item) > 0;
      });
  },

  arrayUnique: function _arrayUnique(array) {
    return array.reduce(function (previous, current) {
      if (previous.indexOf(current) < 0) {
        previous.push(current);
      }
      return previous;
    }, []);
  },

  arrayLowerCase: function _arrayLowerCase(array) {
    return array.map(function (item) {
      return item.toLowerCase();
    });
  },

  addPreset: function _addPreset(language, stopwords) {
    _stopwords.presets[language] = _arrayUnique(
      _stopwords.presets[language].concat(
        _arrayLowerCase(
          stopwords.filter(function (stopword) {
            return _isString(stopword);
          })
        )
      )
    );
  }
}
