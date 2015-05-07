/*
 * Copyright (c) 2015.
 */
'use strict';

var is = require('is_js');

function SqlBuilder() {

  this.sql = '';

  if (!(this instanceof SqlBuilder)) {
    return new SqlBuilder();
  }
}

SqlBuilder.prototype.insert = function (tableName, columns) {

  this.sql += 'INSERT INTO ';

  this.sql += (tableName + ' ');

  var name, i;

  if (Array.isArray(columns) && columns.length > 0) {

    this.sql += '(';

    for (name in columns) {
      if (columns.hasOwnProperty(name)) {
        this.sql += columns[name] + ', ';
      }
    }
    this.sql = this.sql.substring(0, this.sql.length - ', '.length);

    this.sql += ') ';

  } else if (is.number(columns) && columns > 0) {

    this.sql += '( ';

    for (i = 0; i < columns; i++) {
      this.sql += '??, ';
    }
    this.sql = this.sql.substring(0, this.sql.length - ', '.length);

    this.sql += ') ';
  }

  return this;
};

SqlBuilder.prototype.insertIgnore = function (tableName, columns) {

  this.sql = this.insert(tableName, columns).toString().replace('INSERT INTO ', 'INSERT IGNORE INTO ');

  return this;
};

SqlBuilder.prototype.set = function (keyValues) {

  if (keyValues) {

    var i, value;

    this.sql += 'SET ';

    if (is.object(keyValues)) {
      for (value in keyValues) {
        if (keyValues.hasOwnProperty(value)) {
          this.sql += (value + ' = ' + keyValues[value] + ', ');
        }
      }
    } else if (is.number(keyValues)) {
      for (i = 0; i < keyValues; i++) {
        this.sql += '?? = ?, ';
      }
    }

    this.sql = this.sql.substring(0, this.sql.length - ', '.length) + ' ';
  }

  return this;
}
;

SqlBuilder.prototype.values = function (singleRowValuesCount, insertRowCount) {

  var i, singleRowParam = '';

  insertRowCount = insertRowCount || 1;

  this.sql += 'VALUES ';

  if (singleRowValuesCount > 0) {
    singleRowParam += '(';
  }
  for (i = 0; i < singleRowValuesCount; i++) {
    singleRowParam += '?, ';
  }
  if (singleRowValuesCount > 0) {
    singleRowParam = singleRowParam.substring(0, singleRowParam.length - ', '.length);
    singleRowParam += ') ';
  }

  if (insertRowCount > 1) {
    for (i = 0; i < insertRowCount; i++) {
      this.sql += (singleRowParam + ', ');
    }
    this.sql = this.sql.substring(0, this.sql.length - ', '.length);
  } else {
    this.sql += singleRowParam;
  }

  return this;
};

SqlBuilder.prototype.onDuplicate = function (keyValues) {

  if (keyValues) {

    var i, value;

    this.sql += 'ON DUPLICATE KEY UPDATE ';

    if (is.object(keyValues)) {
      for (value in keyValues) {
        if (keyValues.hasOwnProperty(value)) {
          this.sql += (value + ' = ' + keyValues[value] + ', ');
        }
      }
    } else if (is.number(keyValues)) {
      for (i = 0; i < keyValues; i++) {
        this.sql += '?? = ?, ';
      }
    }

    this.sql = this.sql.substring(0, this.sql.length - ', '.length) + ' ';
  }

  return this;
};

SqlBuilder.prototype.update = function (tableName) {

  if (tableName) {
    this.sql += ('UPDATE ' + tableName + ' ');
  }

  return this;
};

SqlBuilder.prototype.where = function (keyValues, andOr) {

  if (keyValues) {

    var i, value;

    this.sql += 'WHERE ';

    if (is.object(keyValues)) {
      for (value in keyValues) {
        if (keyValues.hasOwnProperty(value)) {
          this.sql += (value + ' = ' + keyValues[value] +
          ((andOr === 'AND') ? ' AND ' : ' OR '));
        }
      }
    } else if (is.number(keyValues)) {
      for (i = 0; i < keyValues; i++) {
        this.sql += ('?? = ? ' + ((andOr === 'AND') ? ' AND ' : ' OR '));
      }
    }

    this.sql = this.sql.substring(0, this.sql.length -
        ((andOr === 'AND') ? ' AND '.length : ' OR '.length)) + ' ';
  }

  return this;

};

SqlBuilder.prototype.deleteFrom = function (tableName) {

  if (tableName) {
    this.sql += ('DELETE FROM ' + tableName + ' ');
  }

  return this;
};

SqlBuilder.prototype.toString = function () {

  return this.sql || '';
};

if (typeof module !== 'undefined' &&
  module.exports) {
  module.exports = SqlBuilder;
}

//console.log(new SqlBuilder().insertIgnore('??', ['sequence', 'values']).values(2).onDuplicate({id: '1', id2: '2'})
//  .toString());
