#!/usr/bin/python
# Copyright The Brookhaven Group, LLC. 2009.
#           All Rights Reserved.
# Authors: Marc Schwarzschild

import re
import os

from strnum import num2str, removeComments, replaceMany
from utils import FileLocker, findFiles, guaranteePath, removeFilesAndDirs, \
    maxYYYYMMDDPath

home = os.getenv('HOME')

class PhoneDB:
  delim = '=' * 79
  def __init__(self, fn=home + '/Notes/phone.doc'):
    self.data = data = []  # data[0] = {Name:'', Phone:'' ...}
    self.field_order = []
    delim = PhoneDB.delim
    inrec_f = False
    rec_lines = []
    file = open(fn)
    lines = file.readlines()
    file.close()
    for line in lines:
      line = line.strip('\n')
      if inrec_f:
        if line == delim:
          rec = self.parseRec(rec_lines)
          data.append(rec)
          rec_lines = []
        else:
          rec_lines.append(line)
      else:
        if line == delim: inrec_f = True
    self.fn = fn

  def __str__(self):
    return self.fn

  def parseRec(self, rec):
    results = {}
    key = ''
    value = ''
    field_order = self.field_order
    for i in rec:
      items = re.split(r'(^.{1,8}\:)?(.*)', i)
      if (len(items) == 4) and (items[1] is not None) and \
             not re.search(r'\t', items[1]):
        if len(key): 
          results[key] = value
          if key not in field_order: field_order.append(key)
        key = items[1]
        key = re.sub(r'^\*', r'', key)
        key = key.strip()
        key = key.strip(':')
        value = items[2].strip()
      else:
        value += i + '\n'
    if len(key): results[key] = value
    return results

  def getData(self):
    return self.data

  def fieldOrder(self):
    return self.field_order


class SpecsRecordNotFound(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return self.value

class SpecsFieldNotFound(Exception): 
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return self.value

def holdThis(key=None, obj=None):
  s = Specs('__holder__')
  if key is None: key = id(obj)
  s.addRec('__holderrec__', {key:obj})
  return key

def getThis(key):
  s = Specs('__holder__')
  try:
    o = s.get('__holderrec__', key)
  except:
    o = None
  return o

def removeThis(key):
  s = Specs('__holder__')
  s.remove('__holderrec__', key)

scache = {}
def clearCache(globals_f=True):
  global scache
  scache = {}
  if globals_f: Specs('globals.dat', p=home + '/')

class Specs(object):
  # don't usually define __new__.  Doing it here for self caching.
  def __new__(cls, fn=None, p='./', persist=False, rstrs=None):
    k = replaceMany(os.path.join(p, str(fn)), rstrs)
    try:
      return scache[k]
    except:
      s = object.__new__(cls)
      s.init_flag = False
      scache[k] = s
      return s

  # SEE __new__()
  # SEE __new__()
  # SEE __new__()
  def __init__(self, fn=None, p=None, persist=False, rstrs=None):
    if self.init_flag: return # Found Specs in cache

    if isinstance(fn, PhoneDB):
      self.init(p, persist)
      data = self.data
      rec_order = self.rec_order
      self.field_order = field_order = ['Item']
      for i in fn.fieldOrder(): field_order.append(i)
      
      count = 1
      for rec in fn.getData():
        k = str(count)
        rec_order.append(k)
        rec['Item'] = k
        data[k] = rec
        count += 1
      self.init_flag = True
      self.fn = fn.fn
      return

    self.init(p, persist)

    p = self.path
    self.lockfn = lockfn = os.path.join(p, '_lockfile_')

    if fn is not None:
      self.fn = fn = os.path.join(p, fn)

      if os.path.exists(fn):
        fl = FileLocker(lockfn)
        if os.path.isdir(fn):
          self.dir_f = True
          for i in findFiles(path=fn):
            if i == '_lockfile_': continue
            self.readSpecs(os.path.join(fn,i), rstrs)
        else:
          self.readSpecs(fn, rstrs)
        del fl

      if rstrs is not None: self.fn = replaceMany(self.fn, rstrs)
    else:
      self.fn = fn

    data = self.data

    # Check for import files
    if 'Import' in self.get(): self.importdata()
                                  
    self.resolveRely()

    self.init_flag = True
      
  def resolveRely(self):
    # Check for rely key in all records and copy data.
    # Exception if there is an self-reliance endless loop.
    def getRelyList(rec):
      if 'rely' in rec:
        x = rec['rely']
        if type(x) != list: x = [x]
        return x
      else:
        return []

    data = self.data
    for item, rec in data.iteritems():
      rely_list = getRelyList(rec)
      i = 0
      while i < len(rely_list):
        r = data[rely_list[i]]
        x = getRelyList(r)
        rely_list += filter(lambda j: j not in rely_list, x)
        i += 1

      for i in rely_list:
        rely_rec = data[i]
        for k, v in rely_rec.iteritems():
          if k != 'rely': rec[k] = v

  def importdata(self):
    fn = self.get('Import', 'fn')
    if type(fn) is not list: fn = [fn]
    for f in fn:
      s = Specs(f, self.path)
      for k in s.get():
        if k not in self.data: self.addRec(k, s.get(k), rely_f=False)
        
  def init(self, p, persist):
    self.persist = persist
    self.data = {}
    self.rec_order = []
    self.field_order = [] # Used to store field order
    self.dir_f = False
    self.touched_new_recs = set()
    self.touched_del_recs = set()

    if p is None: p = './'
    self.path = p
    guaranteePath(p)

  def readSpecs(self, fn, rstrs=None):
    if not os.path.getsize(fn):
      print fn, 'is zero length'
      return

    data = self.data
    rec_order = self.rec_order
    field_order = self.field_order

    rec = None
    dir_f = self.dir_f

    # If busted specs then use fn to set key.
    if dir_f:
      reckey = replaceMany(os.path.basename(fn), rstrs)
      rec = data[reckey] = {}
      rec_order.append(reckey)

    try: 
      inblock_f = False
      file = open(fn, 'r')
      lines = file.readlines()
      file.close()
      for line in lines:
        line = replaceMany(line, rstrs)
        if inblock_f:
          if heredoc in line:
            value = value.strip('\n')
            if value.startswith('{') and value.endswith('}'): value= eval(value)
            rec[key] = value
            if key not in field_order: field_order.append(key)
            inblock_f = False
            continue
          else:
            value += line
            continue

        line = removeComments(line)
        if '' == line:
          continue

        line = line.strip(' \n')  # remove leading and trailing blanks
        if '' == line: 
          inblock_f = False
          continue

        if re.search(r'^\[.*\]$', line) or re.search(r'^Item\ ', line):
          if dir_f: continue
          inblock_f = False
          reckey = line.strip('[]')
          reckey = re.sub(r'^Item\ ', r'', reckey)
          rec = data[reckey] = {}
          rec_order.append(reckey)
        else:
          key = line.split(None, 1)
          if 1 == len(key):
            [key] = key
            value = ''
          else:
            [key, value] = key

          if re.search(r'^<<<', value):
            heredoc = value.strip('<')
            value = ''
            inblock_f = True
          else:
            a = value.split('|')
            if 1 < len(a):
              for i in a:
                i = i.strip(' \n')
                value = a
            else:
              value = value.strip(' \n')
              if value.startswith('{') and value.endswith('}'):
                value= eval(value)
            rec[key] = value
            if key not in field_order: field_order.append(key)

    except:  
      file.close()
      raise
    # End of readSpecs()

  def getFN(self):
    if self.fn is None: return self.path, None
    path, fn = os.path.split(self.fn)
    return path, fn

  def recExists(self, rec=None): return rec in self.data

  def fieldExists(self, rec=None, field=None):
    if (rec is None) or (field is None): return None
    if rec in self.data:
      rec = self.data[rec]
      return field in rec
    else:
      return False

  def __iter__(self): return iter(self.rec_order)
  def __getitem__(self, key): return self.data[key]

  def get(self, rec=None, field=None, tf_f=False, default=None):
    data = self.data
    if rec is None:
      return self.rec_order
    else:
      if rec in data:
        r = data[rec]
      else:
        raise SpecsRecordNotFound(self.fn + ': ' + rec)

    if field is None:
      return r
    else:
      if field in r:
        x = r[field]
        if (type(x) is str) and tf_f:
          x = x.lower()
          x = True if ((x == 'on') or (x == 'yes') or \
                       (x == 'true') or (x == 't')) else False
          return x
        else:
          return x
      else:
        return default

  # return keys; * is a wildcard for value
  def find(self, field=None, value='*'):
    keys = []
    if field is None: return self.rec_order
    for k in self.rec_order:
      v = self.get(rec=k, field=field)
      if v is None: continue

      if (value == '*') or (value == v): keys.append(k)
    return keys
    
  def addRec(self, item, rec, rely_f=True):
    for k in rec: self.set(item, k, rec[k], rely_f=rely_f) 

  def set(self, rec, field, value, rely_f=True):
    data = self.data
    field_order = self.field_order
    if field not in field_order: field_order.append(field)
    if rec not in data:
      data[rec] = {}
      self.rec_order.append(rec)
      
    self.touched_new_recs.add(rec)
    if rec in self.touched_del_recs: self.touched_del_recs.remove(rec)

    data[rec][field] = value

    if rec == 'Import': self.importdata()

    if rely_f: self.resolveRely()

    if self.persist: self.save()
 
  def remove(self, rec, field=None):
    data = self.data
    if field is None:
      try:
        del data[rec]
        self.rec_order.remove(rec)
      except:
        pass
    else:
      try:
        del data[rec][field]
      except:
        pass
  
    self.touched_del_recs.add(rec)
    if rec in self.touched_new_recs: self.touched_new_recs.remove(rec)
    if self.persist: self.save()

  def setPersist(self, persist=True):
    self.persist = persist

  def save(self, fn=None):
    if fn is None:
      if self.fn is None: return
      fn = self.fn
    else:
      self.fn = fn
    if fn is None: raise NameError, 'Specs cannot save without a file name'


    fl = FileLocker(self.lockfn)

    dir_f = self.dir_f
    if (not dir_f) or (dir_f is not os.path.isdir(fn)):
      try:
        removeFilesAndDirs(fn)
      except:
        pass
    elif dir_f:
      for f in self.touched_del_recs:
        try:
          removeFilesAndDirs(os.path.join(fn, f))
        except:
          pass
    else:
      raise IOError, 'Specs: cannot save because dir_f is confused'
    
    if dir_f:
      if not os.path.exists(fn): os.mkdir(fn)
      for r in self.touched_new_recs:
        fl = FileLocker(os.path.join(fn, r), 'w')
        fl.getFH().write(self.recStr(r))
        del fl
    else:
      fl = FileLocker(fn, 'w')
      fh = fl.getFH()
      for r in self: fh.write(self.recStr(r))
      del fl

  def bustUp(self):
    self.dir_f = True
    self.touched_new_recs = \
        set([i for i in self if i not in self.touched_del_recs])
    self.save()

  def unBustUp(self):
    self.dir_f = False
    self.touched_new_recs = \
        set([i for i in self if i not in self.touched_del_recs])
    self.save()

  def recStr(self, rec):
    data = self.data
    field_order = self.field_order
    s = ''
    if rec not in data: return s
    v = data[rec]
    if self.dir_f:
      s += 'Item ' + rec + '\n'
    else:
      s += '[' + rec + ']' + '\n'
    for kk in field_order:
      if kk not in v: continue
      vv = v[kk]
      if type(vv) is list: vv = '|'.join([num2str(i) for i in vv])
      if '\n' in str(vv):
        s += kk + ' ' + '<<<__EOD__\n' + vv + '\n__EOD__\n'
        pass
      else:
        s += kk + ' ' + num2str(vv) + '\n'
    s += '\n'
    return s

  # return an list of keys sorted by given field
  def fieldSort(self, field, findkey=None, findvalue='*'):
    data = self.data
    if findkey is None:
      return sorted(data, lambda a, b: cmp(data[a][field], data[b][field]))
    else:
      x = self.find(field=findkey, value=findvalue)
      return sorted(x, lambda a, b: cmp(data[a][field], data[b][field]))

  def __str__(self):
    s = ''
    for k in self.rec_order: s += self.recStr(k)
    return s

  def __len__(self):
    return len(self.data)

##############################################################################
# special - /home/ms/globals.dat is the only specs file Specs knows by heart.
# this is because the next line reads it in and Specs() saves it in the cache.
# Calling Specs.clearCache(globals_f=False) causes the cache to forget.
Specs('globals.dat', p=home + '/')
##############################################################################

def getGlobal(rec, key, tf_f=False):
  x = Specs('globals.dat',p=home + '/').get(rec,key)
  if tf_f:
    x = True if x.lower() == 'true' else False
  return x

def getConfig(key, fn): return Specs(fn, p=getGlobal(key, 'datapath'))

def getBSTPHTradesFN():
  path = getGlobal('SimulatedResults', 'bstphpath')
  path = maxYYYYMMDDPath(path)
  fn = os.path.join(path, 'trades.dat')
  return fn

def serverTrialDir():
  server = 'ms@risksheet.com:~/public_html/'
  outdir = getGlobal('SimulatedResults', 'url_root')
  outdir = outdir.replace('http://TheBrookhavenGroup.com/','') # trials/
  return server, outdir

########
# MAIN #
########

if __name__ == "__main__":

  if False:
    # s = Specs('Investments.dat', 'SP500', )
    s = Specs('Default.analysis', p=home + '/tsa/src/lighthouse/data/')
    #print s

    print '1:', s.get('MenuItems', 'Inclusion')
    print '2:', 'Inclusion' in s.get('MenuItems')
    print '3:', s.get('Global', 'banner')
    print '4:', s.get('Global')

    print
    print

  if False:
    s = Specs('foo.dat', p='/opt/marc/tmp/', persist=True)

    s.set('Renee', 'Name', 'Renee Koplon')
    s.set('Renee', 'Phone', '1-917-123-4567')
    s.setPersist(False)
    s.set('Renee', 'Sex', 'F')
    s.set('Marc', 'Name', 'Marc Schwarzschild')
    s.set('Marc', 'Phone', '1-212-123-4567')
    s.save()
    s.set('Marc', 'Sex', 'M')
    s.set('Gila', 'Name', 'Gila Schwarzschild')
    s.set('Gila', 'Phone', '1-917-123-4568')
    s.set('Gila', 'Sex', 'F')

    print 'Sorted by Name:'
    klist = s.fieldSort('Name')
    for k in klist: print s.get(k, 'Name')
    print
    print

    print s.recStr('Marc')
    s.save()

    print 'removing Name field'
    s.setPersist(True)
    s.remove('Marc', 'Name')
    s.setPersist(False)
    print s.recStr('Marc')

    print 'removing record'
    s.remove('Marc')
    s.save()
    try:
      print s.get('Marc')
    except:
      print 'Could not get Marc record because it was removed.  This is OK.'

    print s.fieldExists()
    print s.fieldExists('Renee', 'Sex')
    print s.fieldExists('Renee', 'Age')
    print s.fieldExists('Marc', 'Sex')
    

  if False:
    s = Specs('foo.dat', p='/opt/marc/tmp/')
    s.bustUp()
    s.setPersist(False)
    s.set('Renee', 'Sex', 'F')
    s.set('Marc', 'Name', 'Marc Schwarzschild')
    s.set('Marc', 'Phone', '1-212-580-1175')
    s.save()
    s = Specs('foo.dat', p='/opt/marc/tmp/')
    s.remove('Marc')
    s.save()
    s.unBustUp()

  if False:
    s = Specs('Investments.dat', p=home + '/tsa/src/lighthouse/data/')
    for i in s.get():
      print i, s.get(rec=i)['Name']

  if False:
    s = Specs('Investments.dat', p=home + '/tsa/src/lighthouse/data/')
    print 'SP500' in s
    print 'Marc' in s

  if False:
    s = Specs('bs.dat', p=home + '/bs/data/')
    print s
    print
    print 'Gila Entity'
    print s.get('GilaSchwarzschild', 'Entity')
    print s.get('foo', 'mtds')

  if False:
    s = Specs('globals.dat', p=home + '/')
    print s
    print getGlobal('BS', 'datapath')


  if False:
    print getConfig('BS', 'bs.dat')

  if False:
    p = PhoneDB()
    s = Specs(p)
    print s

  if False:
    s = Specs('Investments.dat', p=home + '/tsa/src/lighthouse/data/')
    print s.find('BarclaySymbol')
    path, fn = s.getFN()
    print path, fn
    s = Specs('Types.dat', p=path)
    print s.get('Benchmark', 'Name')
    

  if False:
    print "Testing Caching"
    s = Specs('Default.analysis', p=home + '/tsa/src/lighthouse/data/')
    print s.fn
    s = Specs('Default.analysis', p=home + '/tsa/src/lighthouse/data/')
    print s.fn
    s = Specs('Investments.dat', p=home + '/tsa/src/lighthouse/data/')
    print s.fn
    s = Specs('Default.analysis', p=home + '/tsa/src/lighthouse/data/')
    print s.fn
    s = Specs('Investments.dat', p=home + '/tsa/src/lighthouse/data/')
    print s.fn

  if False:
    # Testing rely.
    p = getGlobal('BS', 'datapath')
    s = Specs('AAFSummaryReport.dat', p)
    print s

  if False:
    s = Specs('Investments.dat', p=home + '/tsa/src/lighthouse/data/')
    s.addRec('MarcFoo', {'ReturnFile':'MarcFoo.returns', \
                         'Name':'Marc Schwarzschild'})
    s.save()
    s.remove('MarcFoo')
    s.save()

  if False:
    p = getGlobal('BS', 'datapath')
    j = Specs('JEM.dat', p)
    m = Specs('manager.dat', p)
    jlist = j.get()
    for k in m.get():
      if k not in jlist: j.addRec(k, m.get(k))
    print j
      
  if False:
    p = getGlobal('BS', 'datapath')
    j = Specs('JEM.dat', p)
    print j

  if False:
    p = getGlobal('BS', 'datapath')
    s = Specs(p=p)
    s.set('Manager', 'manager', 'JEM')
    s.set('Import', 'fn', 'manager.dat')
    print s

  if False:
    s = Specs('DianeRecipes.dat', p = home + '/Food/Diane')
    # Sort Cat='Side' dishes by 'Name'
    items = s.fieldSort('Name', findkey='Cat', findvalue='Side')
    for k in items: print s.get(k, 'Cat'), s.get(k, 'Name')

  if False:
    s = getConfig('BS', 'instruments.dat')
    for i in s: print i
    print s['SP.CME']['Name']

  if False:
    rstrs = {'<<symbol>>':'NG', '<<symbol_lc>>':'ng', '<<exchange>>':'NYM'}
    s = Specs('<<symbol>>.experiments.tpl', p = home + '/bs/data/',rstrs=rstrs)
    s = Specs('NG.experiments.tpl', p=home + '/bs/data/')
    print s

  # KEEP THIS AS LAST TEST
  if False:
    s = Specs('foo.dat')
    print s

  if False:
    import cProfile
    cProfile.run('Specs("/home/ms/tsa/src/lighthouse/data/Investments.dat")')
    
  if False:
    s = Specs(fn=home + '/bs/data/signals.dat')
    print s
    s = Specs(fn=home + '/bs/data/signals.dat')
    clearCache()
    s = Specs(fn=home + '/bs/data/signals.dat')
    clearCache(globals_f=False)
