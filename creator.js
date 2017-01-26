function createBank(execlib, leveldbwithloglib, leveldblib) {
  'use strict';

  var lib = execlib.lib,
    q = lib.q,
    qlib = lib.qlib,
    LevelDBWithLog = leveldbwithloglib.LevelDBWithLog,
    BankMixin = require('./mixincreator')(execlib, leveldblib, leveldbwithloglib);

  function Bank (prophash) {
    BankMixin.call(this, prophash);
    LevelDBWithLog.call(this, prophash);
  }
  lib.inherit(Bank, LevelDBWithLog);
  BankMixin.addMethods(Bank);
  Bank.prototype.destroy = function () {
    LevelDBWithLog.prototype.destroy.call(this);
    BankMixin.prototype.destroy.call(this);
  };

  return q({
    BankMixin: BankMixin,
    Bank: Bank,
    Hook: leveldbwithloglib.Hook
  });
}


module.exports = createBank;
