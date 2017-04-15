from multiprocessing.pool import ThreadPool

def multithread(fn, fields, threading = False):
  '''
  Multithreading codes
  :param fn [python function]: Function on each thread
  :param fields [list of int]: Fields to multithread on
  :returns [dict]: Dictionary of keys with results
  '''
  #Multithreading if threading is true
  if threading:
    pool = ThreadPool(processes = len(fields))
    results = pool.map(fn, fields)
    pool.close()
    pool.terminate()
  #Serialize if threading is false
  else:
    results = {}
    for f in fields:
      results[f] = fn(f)
  return results
