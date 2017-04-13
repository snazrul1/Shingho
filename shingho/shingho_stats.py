import os
from shingho.single_thread_stats import rdd_stats, df_stats
from shingho.utils import multiThread

class basic_stats(object):
  '''
  Calculate sample statistics
  '''
  
  def __init__(self, data, sampling = None):
    '''
    :param rdd [Spark RDD or DataFrame]: Spark RDD or DataFrame for analytics
    :param sampling [float]: Sampling rate between 0 and 1
    '''
    self.sampling = sampling
    if sampling == None:
      self.rdd = rdd
    else:
      self.rdd = rdd.sample(sampling)
    unix_output = os.system('spark-submit --version')
    self.version = int(unix_output[4].split()[-1][0])
    
  def mean(self, fields = 'ALL', index_field = None, threading = False):
     '''
     Calculates mean value of Specific RDD
     :param fields [list of int]: list of fields
     :param index_field [int]: Field for performing groupby operation
     :param threading [bool]: Multithread each field on a thread
     :returns [dict]: dictionary of mean value with keys
     '''
      #Spark DataFrame is faster for this calculation but is only available for Spark 2x
      if self.version >= 2:
        stats_object = rdd_stats(rdd = self.rdd,
                                 sampling = self.sampling)

        
      else:
        stats_object = df_stats(rdd = self.rdd,
                                sampling = self.sampling)
        
      mean_values = multiThread(fn = stats_object.mean(index_field = index_field),
                                fields = fields, 
                                threading = threading)
      return mean_values
                                
        
  def median(self, fields = 'ALL', index_field = None, threading = False):
     '''
     Calculates median value 
     :param fields [list of int]: list of fields 
     :param threading [bool]: Multithread each key on a thread
     :returns [float]: mean value
     '''
      #Spark DataFrame is faster for this calculation but is only available for Spark 2x
      if self.version >= 2:
        stats_object = rdd_stats(rdd = self.rdd,
                                 sampling = self.sampling)

        
      else:
        stats_object = df_stats(rdd = self.rdd,
                                sampling = self.sampling)
        
      median_values = multiThread(fn = stats_object.median(index_field = index_field),
                                fields = fields, 
                                threading = threading)
      return median_values
                            
    
  def mode(self, fields = 'ALL', index_field = None, threading = False):
     '''
     Calculates mode value 
     :param fields [list of int]: list of fields
     :param threading [bool]: Multithread each key on a thread
     :returns [float]: mean value
     '''
      #Spark DataFrame is faster for this calculation but is only available for Spark 2x
      if self.version >= 2:
        stats_object = rdd_stats(rdd = self.rdd,
                                 sampling = self.sampling)

        
      else:
        stats_object = df_stats(rdd = self.rdd,
                                sampling = self.sampling)
        
      mode_values = multiThread(fn = stats_object.mode(index_field = index_field),
                                fields = fields, 
                                threading = threading)
      return mode_values
                            
    
  def std(self, fields = 'ALL', index_field = None, threading = False):
     '''
     Calculates standard deviation value 
     :param fields [list of int]: list of fields
     :param threading [bool]: Multithread each key on a thread
     :returns [float]: mean value
     '''
      #Spark DataFrame is faster for this calculation but is only available for Spark 2x
      if self.version >= 2:
        stats_object = rdd_stats(rdd = self.rdd,
                                 sampling = self.sampling)

        
      else:
        stats_object = df_stats(rdd = self.rdd,
                                sampling = self.sampling)
        
      std_values = multiThread(fn = stats_object.std(index_field = index_field),
                                fields = fields, 
                                threading = threading)
      return std_values
