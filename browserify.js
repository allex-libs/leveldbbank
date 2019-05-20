var lR = ALLEX.execSuite.libRegistry;

lR.register('allex_leveldbbanklib',
  require('./creator')(
    ALLEX,
    lR.get('allex_leveldbwithloglib'),
    lR.get('allex_leveldblib')
  )
);
