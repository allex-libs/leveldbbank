var randomBytes = require('crypto').randomBytes,
  Path = require('path');

function createMixin (execlib, leveldblib, leveldbwithloglib) {
  'use strict';

  var lib = execlib.lib,
    q = lib.q,
    qlib = lib.qlib,
    LevelDBWithLog = leveldbwithloglib.LevelDBWithLog;

  function pathjoiner (path1, item) {
    var p;
    if (lib.isArray(path1)) {
      p = path1.slice();
      p.push(item);
      return Path.join.apply(Path, p);
    }
    return Path.join(path1, item);
  }

  function BankMixin (prophash) {
    prophash.kvstorage = {
      dbname: 'accounts.db',
      dbcreationoptions: {
        leveldbValueEncoding: 'UInt64BECodec'
      }
    };
    prophash.log = {
      dbname: 'transactions.db',
      dbcreationoptions: {
      }
    };
    this.reservations = null;
  }
  BankMixin.prototype.destroy = function () {
    if (this.reservations) {
      this.reservations.destroy();
    }
    this.reservations = null;
  };

  function superDropper (bm) {
    return LevelDBWithLog.prototype.drop.call(bm);
  };
  BankMixin.prototype.drop = function () {
    var sd;
    if (this.reservations) {
      sd = superDropper.bind(null, this);
      return qlib.promise2decision(this.reservations.drop(), sd, sd);
    }
    return superDropper(this);
  };

  BankMixin.prototype.createStartDBPromises = function () {
    var rd = q.defer();

    this.logopts.dbcreationoptions.bufferValueEncoding = ['String', 'Int64BE', 'UInt64BE'].concat(this.referenceUserNames).concat(['UInt64BE']); //username, amount, balance after txn; timestamp

    this.reservations = new (leveldblib.DBArray)({
      dbname: pathjoiner(this.dbdirpath, 'reservations.db'),
      dbcreationoptions: {
        bufferValueEncoding: ['String', 'UInt64BE'].concat(this.referenceUserNames).concat(['UInt64BE', 'String'])
        //username, amount (>=0); timestamp, secretstring
      },
      starteddefer: rd,
      startfromone: true
    });
    return LevelDBWithLog.prototype.createStartDBPromises.call(this).concat([rd.promise]);
  };

  BankMixin.prototype.readAccount = function (username) {
    return this.get(username);
  };

  BankMixin.prototype.readAccountWDefault = function (username, deflt) {
    //console.log('reading account with default', username, deflt);
    return this.getWDefault(username, deflt);
  };

  BankMixin.prototype.readAccountSafe = function (username, deflt) {
    //console.log('reading account with default', username, deflt);
    return this.safeGet(username, deflt);
  };

  BankMixin.prototype.closeAccount = function (username) {
    return this.del(username);
  };

  BankMixin.prototype.purgeAccount = function (username, referencearry) {
    return this.locks.run(new qlib.PromiseChainerJob([
      this.emptyAccount(username, referencearry),
      this.closeAccount.bind(this, username)
    ]));
  };

  function chargeallowance(username, balance, amount) {
    //console.log('chargeallowance?', record, amount);
    if (balance >= amount) {
      //console.log('chargeallowance is ok', balance, '>', amount);
      username = null;
      return true;
    }
    var errstring = 'Cannot charge amount '+amount+' from account '+username+' that currently has balance '+balance;
    username = null;
    throw new lib.Error('INSUFFICIENT_FUNDS', errstring);
  }
  BankMixin.prototype.charge = function (username, amount, referencearry) {
    return this.locks.run(username, this.chargeJob(username, amount, referencearry));
  };
  BankMixin.prototype.chargeJobTasks = function (username, amount, referencearry) {
    if (!referencearry) {
      console.trace();
      console.error('no reference array');
      process.exit(0);
      return;
    }
    //console.log('charge', username, 'for', amount);
    var decoptions = {
        defaultrecord: function () {
          if (amount>0) {
            amount = null;
            throw new lib.Error('NO_USERNAME');
          }
          amount = null;
          return 0;
        },
        criterionfunction: chargeallowance.bind(null, username)
      },
      ret;
    if (!username) {
      return [q.reject.bind(q, new lib.Error('NO_USERNAME'))];
    }
    if (!lib.isNumber(amount)) {
      return [q.reject.bind(q, new lib.Error('AMOUNT_MUST_BE_A_NUMBER'))];
    }
    ret = [
      this.kvstorage.dec.bind(this.kvstorage, username, null, amount, decoptions),
      this.recordTransaction.bind(this, username, amount, referencearry)
    ];
    username = null;
    amount = null;
    referencearry = null;
    return ret;
  };
  BankMixin.prototype.chargeJob = function (username, amount, referencearry) {
    return new qlib.PromiseChainerJob(this.chargeJobTasks(username, amount, referencearry));
  };

  BankMixin.prototype.emptyAccount = function (username, doemptyreference) {
    return this.locks.run(username, this.emptyAccountJob(username, doemptyreference));
  };
  BankMixin.prototype.emptyAccountJob = function (username, doemptyreference) {
    return qlib.PromiseChainerJob([
      this.readAccount.bind(this),
      this.onAccountReadForEmptying.bind(this, username, doemptyreference)
    ]);
  };
  function emptyAccountReporter (originalbalance, emptyingresult) {
    emptyingresult[emptyingresult.length-1] = originalbalance;
    return q(emptyingresult);
  }
  BankMixin.prototype.onAccountReadForEmptying = function (username, doemptyreference, balance) {
    return this.charge(username, balance, doemptyreference).then(
      emptyAccountReporter.bind(null, balance)
    );
  };

  BankMixin.prototype.reserve = function (username, amount, referencearry) {
    if (!username) {
      return q.reject(new lib.Error('NO_USERNAME'));
    }
    if (!lib.isNumber(amount)) {
      return q.reject(new lib.Error('AMOUNT_MUST_BE_A_NUMBER'));
    }
    var decoptions = {
      defaultrecord: function () {throw new lib.Error('NO_ACCOUNT_YET');},
      criterionfunction: chargeallowance.bind(null, username)
    };
    return this.locks.run(username, new qlib.PromiseChainerJob([
      this.kvstorage.dec.bind(this.kvstorage, username, null, amount, decoptions),
      this.recordReservation.bind(this, username, amount, referencearry)
    ]));
  };

  BankMixin.prototype.commitReservation = function (reservationid, controlcode, referencearry) {
    //console.log('commitReservation', reservationid, controlcode);
    if (!(lib.isNumber(reservationid) && reservationid>0)) {
      return q.reject(new lib.Error('RESERVATIONID_MUST_BE_A_POSITIVE_NUMBER'));
    }
    if (!(controlcode && lib.isString(controlcode))) {
      return q.reject(new lib.Error('CONTROL_CODE_MUST_BE_A_STRING'));
    }
    var pc = new qlib.PromiseChainerJob([
      this.reservations.get.bind(this.reservations, reservationid),
      this.voidOutReservationForCommit.bind(this, reservationid, controlcode, null, referencearry)
    ]),
      pcp = pc.defer.promise;
    pc.go();
    return pcp;
  };

  BankMixin.prototype.partiallyCommitReservation = function (reservationid, controlcode, commitamount, referencearry) {
    //console.log('partiallyCommitReservation', reservationid, controlcode, commitamount);
    if (!(lib.isNumber(reservationid) && reservationid>0)) {
      return q.reject(new lib.Error('RESERVATIONID_MUST_BE_A_POSITIVE_NUMBER'));
    }
    if (!(controlcode && lib.isString(controlcode))) {
      return q.reject(new lib.Error('CONTROL_CODE_MUST_BE_A_STRING'));
    }
    var pc = new qlib.PromiseChainerJob([
      this.reservations.get.bind(this.reservations, reservationid),
      this.voidOutReservationForCommit.bind(this, reservationid, controlcode, commitamount, referencearry)
    ]),
      pcp = pc.defer.promise;
    pc.go();
    return pcp;
  };

  BankMixin.prototype.cancelReservation = function (reservationid, controlcode, referencearry) {
    //console.log('cancelReservation', reservationid, controlcode);
    if (!(lib.isNumber(reservationid) && reservationid>0)) {
      return q.reject(new lib.Error('RESERVATIONID_MUST_BE_A_POSITIVE_NUMBER'));
    }
    if (!(controlcode && lib.isString(controlcode))) {
      return q.reject(new lib.Error('CONTROL_CODE_MUST_BE_A_STRING'));
    }
    var pc = new qlib.PromiseChainerJob([
      this.reservations.get.bind(this.reservations, reservationid),
      this.voidOutReservationForCancel.bind(this, reservationid, controlcode, referencearry)
    ]),
      pcp = pc.defer.promise;
    pc.go();
    return pcp;
  };

  function secretString () {
    return randomBytes(4).toString('hex');
  }

  function reserver(balance, reservation) {
    //console.log('reservation id', reservation, reservation[1], '?');
    return q([reservation[0], reservation[1][reservation[1].length-1], balance]);
  }
  BankMixin.prototype.recordReservation = function (username, amount, referencearry, result) {
    //console.log('recording reservation', username, amount, referencearry, result);
    var balance = result[1],
      rsrvarry = [username, amount].concat(referencearry),
      rt = this.recordTransaction.bind(this);
    rsrvarry.push(Date.now());
    rsrvarry.push(secretString());
    result = null;
    return this.reservations.push(rsrvarry).then(
      reserver.bind(null, balance)
    ).then(
      function (reserveresult) {
        rt(username, amount, referencearry, [0, reserveresult[2]]);
        rt = null;
        username = null;
        amount = null;
        referencearry = null;
        return reserveresult;
      }
    );
  };
  
  function transactor(balance, transaction) {
    //console.log('transaction id', transaction, '?');
    var ret = q([transaction[0], balance]);
    return ret;
  }
  BankMixin.prototype.recordTransaction = function (username, amount, referencearry, result) {
    var balance = result[1], tranarry = [username, -amount, balance].concat(referencearry), ret;
    tranarry.push(Date.now());
    //console.log('result', result, 'balance', balance);
    //console.log('log <=', tranarry, '(referencearry', referencearry, ')');
    ret = this.log.push(tranarry)
      .then(transactor.bind(null, balance));
    username = null;
    amount = null;
    referencearry = null;
    return ret;
  };
  BankMixin.prototype.voidOutReservationForCommit = function (reservationid, controlcode, commitamount, referencearry, reservation) {
    var tranarry;
    //console.log('what should I do with', arguments, 'to voidOutReservationForCommit?');
    if (controlcode !== reservation[reservation.length-1]) {
      console.error('wrong control code, controlcode', controlcode, 'against', reservation);
      return q.reject(new lib.Error('WRONG_CONTROL_CODE', controlcode));
    }
    var username = reservation[0], commitmoney = reservation[1], voidreservation, chargeamount;
    if (commitamount !== null) {
      chargeamount = commitmoney-commitamount;
    } else {
      chargeamount = 0;
    }
    if (!username) {
      return q.reject(new lib.Error('NO_USERNAME_IN_RESERVATION'));
    }
    voidreservation = reservation.slice();
    voidreservation[0] = '';
    voidreservation[1] = 0;
    return this.reservations.put(reservationid, voidreservation).then(
      this.onReservationVoidForCommit.bind(this, username, commitmoney, -chargeamount, referencearry)
    );
  };
  BankMixin.prototype.onReservationVoidForCommit = function (username, commitmoney, chargeamount, referencearry) {
    //charge 
    return this.chargeJob(username, chargeamount, referencearry).go().then(
      chargeResultEnhancerWithReservationMoney.bind(null, commitmoney)
    );
  };

  BankMixin.prototype.voidOutReservationForCancel = function (reservationid, controlcode, referencearry, reservation) {
    if (!reservation) {
      return q.reject(new lib.Error('NO_RESERVATION'));
    }
    if (controlcode !== reservation[reservation.length-1]) {
      console.error('wrong control code, controlcode', controlcode, 'against', reservation);
      return q.reject(new lib.Error('WRONG_CONTROL_CODE', controlcode));
    }
    var username = reservation[0], cancelmoney = reservation[1], voidreservation;
    if (!username) {
      return q.reject(new lib.Error('NO_USERNAME_IN_RESERVATION'));
    }
    voidreservation = reservation.slice();
    voidreservation[0] = '';
    voidreservation[1] = 0;
    return this.reservations.put(reservationid, voidreservation).then(
      this.onReservationVoidForCancellation.bind(this, username, cancelmoney, referencearry)
    );
  };
  BankMixin.prototype.onReservationVoidForCancellation = function (username, cancelmoney, referencearry) {
    //charge 
    var ret = this.chargeJob(username, -cancelmoney, referencearry).go().then(
      chargeResultEnhancerWithReservationMoney.bind(null, cancelmoney)
    );
    username = null;
    cancelmoney = null;
    referencearry = null;
    return ret;
  };

  function chargeResultEnhancerWithReservationMoney (money, result) {
    return q([result[0], result[1], money]);
  }

  BankMixin.prototype.reset = function (username, resetreference) {
    return this.locks.run(username, this.resetJob(username, resetreference));
  };

  BankMixin.prototype.resetTo = function (username, newbalance, closingreference, resetreference, openingreference) {
    return this.locks.run(username, this.resetToJob(username, newbalance, closingreference, resetreference, openingreference));
  };


  BankMixin.prototype.resetJobTasks = function (username, resetreference) {
    var resetid = lib.uid(),
      ret = [
        this.prepareResetLog.bind(this, resetid, username),
        this.collectUserTxns.bind(this, resetid, username),
        this.writeInitialTransactionAfterReset.bind(this, username, resetreference)
      ];
    username = null;
    resetreference = null;
    return ret;
  };

  BankMixin.prototype.resetJob = function (username, resetreference) {
    return new qlib.PromiseChainerJob(this.resetJobTasks(username, resetreference));
  };

  function collector(collectobj, kvobj) {
    var txn = kvobj.value, txnmoment = txn[txn.length-1];
    if (txn[0] === collectobj.username) {
      collectobj.count ++;
      collectobj.delbatch.del(kvobj.key);
      collectobj.writebatch.put(kvobj.key, kvobj.value);
      if (txnmoment < collectobj.minmoment) {
        collectobj.minmoment = txnmoment;
      }
      if (txnmoment > collectobj.maxmoment) {
        collectobj.maxmoment = txnmoment;
      }
    }
  };

  BankMixin.prototype.prepareResetLog = function (resetid, username) {
    var d = q.defer(), ro = this.logCreateObj();
    ro.dbname = Path.join(this.dbdirpath, 'resets', resetid);
    ro.starteddefer = d;
    new leveldblib.LevelDBHandler(ro);
    return d.promise;
  };

  BankMixin.prototype.collectUserTxns = function (resetid, username, resetdb) {
    var collectobj = {
      id: resetid,
      db: resetdb,
      writebatch: resetdb.db.batch(),
      delbatch: this.log.db.batch(),
      username: username,
      minmoment: Infinity,
      maxmoment: -Infinity,
      count: 0
    },
     d = q.defer(),
     rr = this.recordReset.bind(this, d, collectobj),
     traverser = this.log.traverse(collector.bind(null, collectobj)),
     ender = function () {
       if (collectobj.count) {
         collectobj.delbatch.write(
           collectobj.writebatch.write.bind(
             collectobj.writebatch,
             rr
           )
         );
       } else {
         collectobj.minmoment = collectobj.maxmoment = Date.now();
         rr(true);
       }
       collectobj = null;
       rr = null;
     };

    traverser.then(
      ender,
      ender
    );
    return d.promise;
  };

  BankMixin.prototype.writeInitialTransactionAfterReset = function (username, resetreference) {
    var rt = this.recordTransaction.bind(this);
    return this.readAccount(username).then(
      function (balance) {
        var ret = rt(username, balance, resetreference, [0, balance]);
        rt = null;
        username = null;
        resetreference = null;
        return ret;
      }
    );
  };

  BankMixin.prototype.recordReset = function (defer, resetobj) {
    resetobj.db.destroy();
    qlib.promise2defer(LevelDBWithLog.prototype.recordReset.call(this, resetobj.id, resetobj.username, resetobj.minmoment, resetobj.maxmoment, resetobj.count), defer);
    resetobj.id = null;
    resetobj.db = null;
    resetobj.writebatch = null;
    resetobj.delbatch = null;
    resetobj.username = null;
    resetobj.minmoment = null;
    resetobj.maxmoment = null;
    resetobj.count = null;
    defer = null;
  };

  BankMixin.prototype.resetToJob = function (username, newbalance, closingreference, resetreference, openingreference) {
    return new qlib.PromiseChainerJob(this.resetToJobTasks(username, newbalance, closingreference, resetreference, openingreference));
  };

  BankMixin.prototype.resetToJobTasks = function (username, newbalance, closingreference, resetreference, openingreference) {
    return [
      this.safeGet.bind(this, username, 'NOT_FOUND'),
      this.onAccountForResetTo.bind(this, username, newbalance, closingreference, resetreference, openingreference)
    ];
  };

  BankMixin.prototype.onAccountForResetTo = function (username, newbalance, closingreference, resetreference, openingreference, balance) {
    var promiseproviders, ret;
    if (!lib.isNumber(balance)) {
      ret = this.chargeJob(username, -newbalance, openingreference).go();
    } else {
      promiseproviders = [];
      if (balance) {
        promiseproviders.push(this.emptyAccountForResetTo.bind(this, username, balance, closingreference));
      }
      promiseproviders.push.apply(promiseproviders, this.resetJobTasks(username, resetreference));
      promiseproviders.push.apply(promiseproviders, this.chargeJobTasks(username, -newbalance, openingreference));
      ret = new qlib.PromiseChainerJob(promiseproviders).go();
    }
    username = null;
    newbalance = null;
    closingreference = null;
    resetreference = null;
    openingreference = null;
    return ret;
  };

  BankMixin.prototype.emptyAccountForResetTo = function (username, balance, closingreference) {
    var ret = (this.chargeJob(username, balance, closingreference)).go();
    username = null;
    balance = null;
    closingreference = null;
    return ret;
  };

  BankMixin.prototype.queryReservations = function (filterdesc, defer, starteddefer) {
    if (!this.reservations) {
      return q.reject(new lib.Error('RESERVATIONS_NOT_CREATED', 'Reservations storage does not exist at the moment'));
    }
    return this.reservations.query(filterdesc, defer, starteddefer);
  };

  BankMixin.prototype.dumpToConsole = function (options) {
    console.log('accounts');
    return this.kvstorage.dumpToConsole(options).then(
      qlib.executor(console.log.bind(console, 'transactions'))
    ).then(
      qlib.executor(this.log.dumpToConsole.bind(this.log, options))
    );
  };

  BankMixin.prototype.referenceUserNames = ['String'];


  BankMixin.addMethods = function (klass) {
    lib.inheritMethods(klass, BankMixin,
      'drop',
      'createStartDBPromises',
      'readAccount',
      'readAccountWDefault',
      'readAccountSafe',
      'closeAccount',
      'purgeAccount',
      'charge',
      'chargeJobTasks',
      'chargeJob',
      'emptyAccount',
      'emptyAccountJob',
      'onAccountReadForEmptying',
      'reserve',
      'commitReservation',
      'partiallyCommitReservation',
      'cancelReservation',
      'recordReservation',
      'recordTransaction',
      'voidOutReservationForCommit',
      'onReservationVoidForCommit',
      'voidOutReservationForCancel',
      'onReservationVoidForCancellation',
      'reset',
      'resetTo',
      'resetJobTasks',
      'resetJob',
      'prepareResetLog',
      'collectUserTxns',
      'writeInitialTransactionAfterReset',
      'recordReset',
      'resetToJob',
      'resetToJobTasks',
      'onAccountForResetTo',
      'emptyAccountForResetTo',
      'queryReservations',
      'dumpToConsole',
      'referenceUserNames'
    );
  };

  return BankMixin;
}

module.exports = createMixin;
