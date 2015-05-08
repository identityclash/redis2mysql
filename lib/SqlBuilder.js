/*
 * Copyright (c) 2015.
 */
'use strict';

var is = require('is_js');

function SqlBuilder(sql) {

  this.sql = sql || '';

  if (!(this instanceof SqlBuilder)) {
    return new SqlBuilder(this.sql);
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

SqlBuilder.prototype.set = function (keyValuesOrKvCount) {

  if (keyValuesOrKvCount) {

    var i, value;

    this.sql += 'SET ';

    if (is.object(keyValuesOrKvCount)) {
      for (value in keyValuesOrKvCount) {
        if (keyValuesOrKvCount.hasOwnProperty(value)) {
          this.sql += (value + ' = ' + keyValuesOrKvCount[value] + ', ');
        }
      }
    } else if (is.number(keyValuesOrKvCount)) {
      for (i = 0; i < keyValuesOrKvCount; i++) {
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

SqlBuilder.prototype.onDuplicate = function (keyValuesOrKvCount) {

  if (keyValuesOrKvCount) {

    var i, value;

    this.sql += 'ON DUPLICATE KEY UPDATE ';

    if (is.object(keyValuesOrKvCount)) {
      for (value in keyValuesOrKvCount) {
        if (keyValuesOrKvCount.hasOwnProperty(value)) {
          this.sql += (value + ' = ' + keyValuesOrKvCount[value] + ', ');
        }
      }
    } else if (is.number(keyValuesOrKvCount)) {
      for (i = 0; i < keyValuesOrKvCount; i++) {
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

SqlBuilder.prototype.where = function (keyValuesOrKvCount, andOr) {

  if (keyValuesOrKvCount) {

    var i, value;

    this.sql += 'WHERE ';

    if (is.object(keyValuesOrKvCount)) {
      for (value in keyValuesOrKvCount) {
        if (keyValuesOrKvCount.hasOwnProperty(value)) {
          this.sql += (value + ' = ' + keyValuesOrKvCount[value] +
          ((andOr === 'AND') ? ' AND ' : ' OR '));
        }
      }
    } else if (is.number(keyValuesOrKvCount)) {
      for (i = 0; i < keyValuesOrKvCount; i++) {
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

SqlBuilder.prototype.select = function (columnsOrColCount) {

  var i, name;

  if (columnsOrColCount) {
    this.sql += 'SELECT ';

    if (Array.isArray(columnsOrColCount) && columnsOrColCount.length > 0) {

      for (name in columnsOrColCount) {
        if (columnsOrColCount.hasOwnProperty(name)) {
          this.sql += columnsOrColCount[name] + ', ';
        }
      }

    } else if (is.number(columnsOrColCount)) {
      for (i = 0; i < columnsOrColCount; i++) {
        this.sql += '?? , ';
      }
    }

    this.sql = this.sql.substring(0, this.sql.length - ', '.length);
  }

  return this;
};

SqlBuilder.prototype.from = function (tableNamesOrTblCount) {

  var i, name;

  if (tableNamesOrTblCount) {
    this.sql += 'FROM ';

    if (Array.isArray(tableNamesOrTblCount) && tableNamesOrTblCount.length > 0) {

      for (name in tableNamesOrTblCount) {
        if (tableNamesOrTblCount.hasOwnProperty(name)) {
          this.sql += tableNamesOrTblCount[name] + ', ';
        }
      }

    } else if (is.number(tableNamesOrTblCount)) {
      for (i = 0; i < tableNamesOrTblCount; i++) {
        this.sql += '?? , ';
      }
    }

    this.sql = this.sql.substring(0, this.sql.length - ', '.length);
  }

  return this;

};

SqlBuilder.prototype.orderBy = function (columnsAndOrder) {

  if (columnsAndOrder) {

    var column;

    this.sql += 'ORDER BY ';

    if (is.object(columnsAndOrder)) {
      for (column in columnsAndOrder) {
        if (columnsAndOrder.hasOwnProperty(column)) {
          this.sql += (column + ' ' + columnsAndOrder[column] + ', ');
        }
      }
    }

    this.sql = this.sql.substring(0, this.sql.length - ', '.length) + ' ';
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

//console.log(new SqlBuilder().insertIgnore('??', ['sequence', 'values'])
//  .values(2).onDuplicate({id1: '1', id2: '2'}).toString());
