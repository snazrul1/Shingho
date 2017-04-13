from multiprocessing.pool import ThreadPool

def multThread(fn, keys):
  '''
  Multithreading codes
  :param fn [python function]: Function on each thread
  :param keys [list of int]: Keys to multithread on
  :returns [dict]: Dictionary of keys with results
  '''
  pool = ThreadPool(processes = len(keys))
  results = pool.map(fn, keys)
  pool.close()
  pool.terminate()
  return results
