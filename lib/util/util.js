/*
 * Copyright (c) 2015.
 */

'use strict';

function Util() {
  if (!(this instanceof Util)) {
    return new Util();
  }
}

Util.prototype.prefixAppender = function (prefixes, delimiter) {

  var str = '', prefix;

  for (prefix in prefixes) {
    if (prefixes.hasOwnProperty(prefix)) {
      str += (prefixes[prefix] + delimiter);
    }
  }

  return str.substring(0, str.length - delimiter.length);
};

if (typeof module !== 'undefined' &&
  module.exports) {
  module.exports = Util;
}
