import numpy

class rdd_stats(object):
  '''
  Calculate sample statistics on a Spark RDD
  '''
  
  def __init__(self, rdd, sampling = None):
    '''
    :param rdd [Spark RDD]: Spark RDD for analytics
    :param sampling [float]: Sampling rate between 0 and 1
    '''
    self.sampling = sampling
    if sampling == None:
      self.rdd = rdd
    else:
      self.rdd = rdd.sample(sampling)
    
  def mean(self, field, index_field = None):
     '''
     Calculates mean value
     :param fields [list of int]: list of fields
     :param index_field [int]: Field for performing groupby operation
     :param threading [bool]: Multithread each field on a thread
     :returns [dict]: dictionary of values with keys
     '''
    if index_field == None:
      mean_value = self.rdd.map(lambda row: (row[field], 1))\
                    .reduce(lambda x,y : x+y)
                    .map(lambda x,y : x/y)
                    .top(1)
      mean_dict = ('ALL', mean_value)

    else:
      key_total = self.rdd.map(lambda row: (row[index_field], row[field]))\
          .reduceByKey(lambda x,y : x+y)\
          .collect()
      key_count = self.rdd.map(lambda row: (row[index_field], 1))\
          .reduceByKey(lambda x,y : x+y)\
          .collect()    
      mean_dict = {}
      for k in key_total.keys:
        mean_dict[k] = key_total[k] / key_count(k)

    return mean_dict

  def median(self, fields = 'ALL', index_field = None):
     '''
     Calculates median value 
     :param field [int]: field index 
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
      raise NotImplementedError
    
  def mode(self, fields = 'ALL', index_field = None):
     '''
     Calculates mode value 
     :param fields [list of int]: list of fields
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
    if index_field == None:
      mode_value = self.rdd.map(lambda row: (row[f], 1))\
                    .reduceByKey(lambda x,y : x+y)\
                    .sortBy(lambda row: row[1], False)\
                    .top(1)
      mode_dict = ('ALL', mode_value)

    else:
      mode_dict = {}
      index_keys = self.rdd.map(lambda row: row[index_field]).distinct()
      for k in index_keys:
        mode_value_k = self.rdd.filter(map row: row[index_field] == k)
                      .map(lambda row: row[f] 1)\
                      .reduceByKey(lambda x,y : x+y)\
                      .sortBy(lambda row: row[1], False)\
                      .top(1)
        mode_dict[k] = mode_value_k
    return mode_dict
    
  def std(self, fields = 'ALL', index_field = None, threading = False):
     '''
     Calculates standard deviation value 
     :param field [int]: field index 
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
      raise NotImplementedError
      

class df_stats(object):
  '''
  Calculate sample statistics on a Saprk DataFrame
  '''
  
  def __init__(self, rdd, sampling = None):
    '''
    :param rdd [Spark DF]: Spark DF for analytics
    :param sampling [float]: Sampling rate between 0 and 1
    '''
    self.sampling = sampling
    if sampling == None:
      self.df = df
    else:
      self.df = df.sample(sampling)
    
  def mean(self, fields = 'ALL', index_field = None):
     '''
     Calculates mean value of Specific RDD
     :param field [int]: field index
     :param index_field [int]: Field for performing groupby operation
     :param threading [bool]: Multithread each field on a thread
     :returns [dict]: dictionary of values with keys
     '''
      raise NotImplementedError

  def median(self, fields = 'ALL', index_field = None):
     '''
     Calculates median value 
     :param field [int]: field index
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
      raise NotImplementedError
    
  def mode(self, fields = 'ALL', index_field = None):
     '''
     Calculates mode value 
     :param field [int]: field index
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
      raise NotImplementedError
    
  def std(self, fields = 'ALL', index_field = None, threading = False):
     '''
     Calculates standard deviation value 
     :param field [int]: field index
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
      raise NotImplementedError
    
  def mode(self, fields = 'ALL', index_field = None):
     '''
     Calculates variance value 
     :param field [int]: field index
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
      raise NotImplementedError
