from multiprocessing.pool import ThreadPool

def multithread(fn, fields, index_field = None, threading = False):
  '''
  Multithreading codes
  :param fn [python function]: Function on each thread
  :param fields [list of int]: Fields to multithread on
  :returns [dict]: Dictionary of keys with results
  '''
  if threading:
    pool = ThreadPool(processes = len(fields))
    results = pool.map(fn, fields)
    pool.close()
    pool.terminate()
    
  else:
    results = {}
    for f in fields:
      results[f] = fn(f)
      
  return results
