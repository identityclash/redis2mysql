/*
 * Copyright (c) 2015.
 */
function SqlBuilder() {

  this.sql = '';

  if (!(this instanceof SqlBuilder)) {
    return new SqlBuilder();
  }
}

SqlBuilder.prototype.insert = function (tableName, columns) {

  this.sql += 'INSERT INTO ';

  this.sql += (tableName + ' ');

  var name;

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

    for (var i = 0; i < columns; i++) {
      this.sql += '??, ';
    }
    this.sql = this.sql.substring(0, this.sql.length - ', '.length);

    this.sql += ') ';
  }
  return this;
}

SqlBuilder.prototype.set = function (keyValueCount) {

  this.sql += 'SET ';

  for (var i = 0; i < keyValueCount; i++) {
    this.sql += '?? = ?, ';
  }

  this.sql = this.sql.substring(0, this.sql.length - ', '.length);

  return this;
}

SqlBuilder.prototype.values = function (valuesCount, insertRowCount) {

  var i, singleRowParam = '', insertRowCount = insertRowCount || 1;

  this.sql += 'VALUES ';

  if (valuesCount > 0) {
    singleRowParam += '(';
  }
  for (i = 0; i < valuesCount; i++) {
    singleRowParam += '?, ';
  }
  if (valuesCount > 0) {
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
}

SqlBuilder.prototype.onDuplicate = function (keyValues) {

  if (keyValues) {

    this.sql += 'ON DUPLICATE KEY UPDATE ';

    if (typeof keyValues === 'object') {
      for (var value in keyValues) {
        this.sql += (value + ' = ' + keyValues[value] + ', ');
      }
    } else if (typeof keyValues === 'number') {
      for (var i = 0; i < keyValues; i++) {
        this.sql += '?? = ?, ';
      }
    }

    this.sql = this.sql.substring(0, this.sql.length - ', '.length);
  }

  return this;
}

SqlBuilder.prototype.toString = function () {

  return this.sql || '';
}

if (typeof module !== 'undefined' &&
  module.exports) {
  module.exports = SqlBuilder;
}

//console.log(new SqlBuilder().insert('??', ['sequence', 'values']).values(2).onDuplicate({id: '1', id2: '2'})
//  .toString());
