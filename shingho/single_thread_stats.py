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
    
  def mean(self, fields = 'ALL', index_field = None):
     '''
     Calculates mean value of Specific RDD
     :param fields [list of int]: list of fields
     :param index_field [int]: Field for performing groupby operation
     :param threading [bool]: Multithread each field on a thread
     :returns [dict]: dictionary of mean value with keys
     '''
    if index_field == None:
      mean_value = self.rdd.map(lambda row: (row[f], 1))\
                    .reduce(lambda x,y : x+y)
                    .map(lambda x,y : x/y)
                    .top(1)
      mean_dict = ('ALL', mean_value)

    else:
      key_total = self.rdd.map(lambda row: (row[index_field], row[f]))\
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
     :param fields [list of int]: list of fields 
     :param threading [bool]: Multithread each key on a thread
     :returns [float]: mean value
     '''
      raise NotImplementedError
    
  def mode(self, fields = 'ALL', index_field = None):
     '''
     Calculates mode value 
     :param fields [list of int]: list of fields
     :param threading [bool]: Multithread each key on a thread
     :returns [float]: mean value
     '''
      raise NotImplementedError
    
  def std(self, fields = 'ALL', index_field = None, threading = False):
     '''
     Calculates standard deviation value 
     :param fields [list of int]: list of fields
     :param threading [bool]: Multithread each key on a thread
     :returns [float]: mean value
     '''
      raise NotImplementedError
    
  def mode(self, fields = 'ALL', index_field = None):
     '''
     Calculates variance value 
     :param fields [list of int]: list of fields
     :param threading [bool]: Multithread each key on a thread
     :returns [float]: mean value
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
     :param fields [list of int]: list of fields
     :param index_field [int]: Field for performing groupby operation
     :param threading [bool]: Multithread each field on a thread
     :returns [dict]: dictionary of mean value with keys
     '''
      raise NotImplementedError

  def median(self, fields = 'ALL', index_field = None):
     '''
     Calculates median value 
     :param fields [list of int]: list of fields 
     :param threading [bool]: Multithread each key on a thread
     :returns [float]: mean value
     '''
      raise NotImplementedError
    
  def mode(self, fields = 'ALL', index_field = None):
     '''
     Calculates mode value 
     :param fields [list of int]: list of fields
     :param threading [bool]: Multithread each key on a thread
     :returns [float]: mean value
     '''
      raise NotImplementedError
    
  def std(self, fields = 'ALL', index_field = None, threading = False):
     '''
     Calculates standard deviation value 
     :param fields [list of int]: list of fields
     :param threading [bool]: Multithread each key on a thread
     :returns [float]: mean value
     '''
      raise NotImplementedError
    
  def mode(self, fields = 'ALL', index_field = None):
     '''
     Calculates variance value 
     :param fields [list of int]: list of fields
     :param threading [bool]: Multithread each key on a thread
     :returns [float]: mean value
     '''
      raise NotImplementedError
