import os
from shingho.single_thread_stats import rdd_stats, df_stats

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
      
      if self.version >= 2:
        mean_values = multiThread(fn = df_stats.mean,
                                  index_field = index_field,
                                  fields = fields, 
                                  threading = threading)
        
      else:
        mean_values = multiThread(fn = rdd_stats.mean,
                                  index_field = index_field,
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
      raise NotImplementedError
    
  def mode(self, fields = 'ALL', index_field = None, threading = False):
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
    
  def mode(self, fields = 'ALL', index_field = None, threading = False):
     '''
     Calculates variance value 
     :param fields [list of int]: list of fields
     :param threading [bool]: Multithread each key on a thread
     :returns [float]: mean value
     '''
      raise NotImplementedError
