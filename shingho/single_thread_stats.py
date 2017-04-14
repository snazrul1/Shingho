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
      mean_dict = {'ALL': mean_value}

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

  def median(self, field, index_field = None):
     '''
     Calculates median value 
     :param field [int]: field index 
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
      if index_field == None:
        rdd_count = rdd.count()
        median_value = self.rdd.map(lambda row: row[f])\
                             .sortBy(lambda row: row[f], False)\
                             .zipWithIndex\
                             .filter(lambda row: row[1] == int(rdd_count/2))\
                             .top(1)
        median_dict = {'ALL': median_value}
        
      else:
        index_keys = self.rdd.map(lambda row: row[index_field]).distinct()
        for k in index_keys:
          rdd_k = rdd.filter(lambda row: row[index_field]==k)
          rdd_count = rdd.count()
          median_value_k = self.rdd_k.map(lambda row: row[f])\
                               .sortBy(lambda row: row[f], False)\
                               .zipWithIndex\
                               .filter(lambda row: row[1] == int(rdd_count/2))\
                               .top(1)
          median_dict[k] = mean_value_k
        
        return median_dict

  def mode(self, field, index_field = None):
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
      mode_dict = {'ALL': mode_value}

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
    
  def std(self, field, index_field = None, threading = False):
     '''
     Calculates standard deviation value 
     :param field [int]: field index 
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
    if index_field == None:
      mean_of_sq = np.square(self.mean(self.rdd, threading = threading)[1])
      sq_rdd = self.rdd.map(lambda row: np.square(row[field]))
      sq_of_mean = np.square(self.mean(self.sq_rdd, threading = threading)[1])
      std_value = np.sqrt(mean_of_sq - sq_of_mean)
      std_dict = {'ALL': std_value}
      
    else:
      std_dict = {}
      index_keys = self.rdd.map(lambda row: row[index_field]).distinct()
      for k in index_keys:
        std_rdd_k = self.rdd.filter(map row: row[index_field] == k)
        mean_of_sq = np.square(self.mean(std_rdd_k, threading = threading)[1])
        sq_rdd = std_rdd_k.map(lambda row: np.square(row[field]))
        sq_of_mean = np.square(self.mean(sq_rdd, threading = threading)[1])
        std_value_k = np.sqrt(mean_of_sq - sq_of_mean)
        std_dict[k] = std_value_k
    
    return std_dict

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
    
  def mean(self, field, index_field = None):
     '''
     Calculates mean value of Specific RDD
     :param field [int]: field index
     :param index_field [int]: Field for performing groupby operation
     :param threading [bool]: Multithread each field on a thread
     :returns [dict]: dictionary of values with keys
     '''
      raise NotImplementedError

  def median(self, field, index_field = None):
     '''
     Calculates median value 
     :param field [int]: field index
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
      raise NotImplementedError
    
  def mode(self, field, index_field = None):
     '''
     Calculates mode value 
     :param field [int]: field index
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
      raise NotImplementedError
    
  def std(self, field, index_field = None, threading = False):
     '''
     Calculates standard deviation value 
     :param field [int]: field index
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
      raise NotImplementedError
    
  def mode(self, field, index_field = None):
     '''
     Calculates variance value 
     :param field [int]: field index
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
      raise NotImplementedError
