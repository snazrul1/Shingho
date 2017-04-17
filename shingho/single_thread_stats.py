import numpy

class rdd_stats(object):
  '''
  Calculate sample statistics on a Spark RDD. Recommended for row operations.
  '''
  def __init__(self, rdd, sampling = None, index_field = None):
    '''
    :param rdd [Spark RDD]: Spark RDD for analytics
    :param sampling [float]: Sampling rate between 0 and 1
    :param threading [bool]: Multithread each field on a thread
    '''
    self.sampling = sampling
    if sampling == None:
      self.rdd = rdd
    else:
      self.rdd = rdd.sample(sampling)
    self.index_field = index_field
    
  def mean(self, field):
     '''
     Calculates mean value
     :param fields [list of int]: list of fields
     :param index_field [int]: Field for performing groupby operation
     :returns [dict]: dictionary of values with keys
     '''
    if self.index_field == None:
      mean_value = self.rdd.map(lambda row: (row[field], 1))\
                    .reduce(lambda x,y : x+y)
                    .map(lambda x,y : x/y)
                    .top(1)
      mean_dict = {'ALL': mean_value}

    else:
      key_total = self.rdd.map(lambda row: (row[self.index_field], row[field]))\
          .reduceByKey(lambda x,y : x+y)\
          .collect()
      key_count = self.rdd.map(lambda row: (row[self.index_field], 1))\
          .reduceByKey(lambda x,y : x+y)\
          .collect()    
      mean_dict = {}
      for k in key_total.keys:
        mean_dict[k] = key_total[k] / key_count(k)
        
    return mean_dict

  def median(self, field):
     '''
     Calculates median value 
     :param field [int]: field index 
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
      if self.index_field == None:
        rdd_count = rdd.count()
        median_value = self.rdd.map(lambda row: row[f])\
                             .sortBy(lambda row: row[f], False)\
                             .zipWithIndex\
                             .filter(lambda row: row[1] == int(rdd_count/2))\
                             .top(1)
        median_dict = {'ALL': median_value}
        
      else:
        index_keys = self.rdd.map(lambda row: row[self.index_field]).distinct()
        for k in index_keys:
          rdd_k = rdd.filter(lambda row: row[self.index_field]==k)
          rdd_count = rdd.count()
          median_value_k = self.rdd_k.map(lambda row: row[f])\
                               .sortBy(lambda row: row[f], False)\
                               .zipWithIndex\
                               .filter(lambda row: row[1] == int(rdd_count/2))\
                               .top(1)
          median_dict[k] = mean_value_k
        
        return median_dict

  def mode(self, field):
     '''
     Calculates mode value 
     :param fields [list of int]: list of fields
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
    if self.index_field == None:
      mode_value = self.rdd.map(lambda row: (row[f], 1))\
                    .reduceByKey(lambda x,y : x+y)\
                    .sortBy(lambda row: row[1], False)\
                    .top(1)
      mode_dict = {'ALL': mode_value}

    else:
      mode_dict = {}
      index_keys = self.rdd.map(lambda row: row[self.index_field]).distinct()
      for k in index_keys:
        mode_value_k = self.rdd.filter(map row: row[self.index_field] == k)
                      .map(lambda row: row[f] 1)\
                      .reduceByKey(lambda x,y : x+y)\
                      .sortBy(lambda row: row[1], False)\
                      .top(1)
        mode_dict[k] = mode_value_k
    return mode_dict
    
  def std(self, field):
     '''
     Calculates standard deviation value 
     :param field [int]: field index 
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
    if self.index_field == None:
      mean_of_sq = np.square(self.mean(self.rdd, threading = threading)[1])
      sq_rdd = self.rdd.map(lambda row: np.square(row[field]))
      sq_of_mean = np.square(self.mean(self.sq_rdd, threading = threading)[1])
      std_value = np.sqrt(mean_of_sq - sq_of_mean)
      std_dict = {'ALL': std_value}
      
    else:
      std_dict = {}
      index_keys = self.rdd.map(lambda row: row[self.index_field]).distinct()
      for k in index_keys:
        std_rdd_k = self.rdd.filter(map row: row[self.index_field] == k)
        mean_of_sq = np.square(self.mean(std_rdd_k, threading = threading)[1])
        sq_rdd = std_rdd_k.map(lambda row: np.square(row[field]))
        sq_of_mean = np.square(self.mean(sq_rdd, threading = threading)[1])
        std_value_k = np.sqrt(mean_of_sq - sq_of_mean)
        std_dict[k] = std_value_k
    
    return std_dict

class df_stats(object):
  '''
  Calculate sample statistics on a Spark DataFrame. Recommended for column operations.
  '''
  def __init__(self, df, sampling = None, index_field = None):
    '''
    :param rdd [Spark DataFrame]: Spark DataFrame for analytics
    :param sampling [float]: Sampling rate between 0 and 1
    :param threading [bool]: Multithread each field on a thread
    '''
    self.sampling = sampling
    if sampling == None:
      self.df = df
    else:
      self.df = df.sample(sampling)
      self.index_field = index_field
      
    #Dataframe will be referred as "source_table" in queries
    self.df.createOrReplaceTempView("source_table")
      
    def _sql_query(query):
      '''
      Convert SQL language to Spark SQL query on DataFrame
      :param query [str]: query
      :returns [dict]: query results
      '''
      df2 = spark.sql(query)
      return df2.collect()
    
  def mean(self, field):
     '''
     Calculates mean value of Specific RDD
     :param field [int]: field index
     :param index_field [int]: Field for performing groupby operation
     :param threading [bool]: Multithread each field on a thread
     :returns [dict]: dictionary of values with keys
     '''
      if self.index_field == None:
        query = '''
        SELECT AVG(%d) 
        FROM source_table
        '''%(field)
      else:
        query = '''
        SELECT AVG(%d) 
        FROM source_table
        GROUP BY column_name(%d)
        '''%(field, index_field)
      return self._sql_query(query)

  def median(self, field):
     '''
     Calculates median value 
     :param field [int]: field index
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
      raise NotImplementedError
    
  def mode(self, field):
     '''
     Calculates mode value 
     :param field [int]: field index
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
      raise NotImplementedError
    
  def std(self, field):
     '''
     Calculates standard deviation value 
     :param field [int]: field index
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
      raise NotImplementedError
    
  def mode(self, field):
     '''
     Calculates variance value 
     :param field [int]: field index
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of values with keys
     '''
      raise NotImplementedError
