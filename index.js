function initLib (execlib) {
  return execlib.loadDependencies('client', ['allex:leveldbwithlog:lib', 'allex:leveldb:lib'], require('./creator').bind(null, execlib));
}

module.exports = initLib;
