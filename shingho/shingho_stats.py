import os
from shingho.single_thread_stats import rdd_stats, df_stats
from shingho.utils import multithread

class basic_stats(object):
  '''
  Calculate sample statistics
  '''
  
  def __init__(self, rdd, sampling = None, index_field = None):
    '''
    :param rdd [Spark RDD or DataFrame]: Spark RDD for analytics
    :param sampling [float]: Sampling rate between 0 and 1
    :param threading [bool]: Multithread each field on a thread
    '''
    #Store input values
    self.sampling = sampling
    if sampling == None:
      self.rdd = rdd
    else:
      self.rdd = rdd.sample(sampling)
    self.index_field = index_field
    
    #Check Spark version for optimization purposes
    unix_output = os.system('spark-submit --version')
    self.version = int(unix_output[4].split()[-1][0])
    
    #Spark DataFrame is faster for this calculation but is only available for Spark 2x
    if self.version <= 2:
      self.stats_object = rdd_stats(rdd = self.rdd,
                               sampling = self.sampling,
                               index_field = self.index_field)
    else:
      self.stats_object = df_stats(rdd = self.rdd,
                              sampling = self.sampling,
                              index_field = self.index_field)
    
  def mean(self, fields = 'ALL', threading = False):
     '''
     Calculates mean value of Specific RDD
     :param fields [list of int]: list of fields
     :param index_field [int]: Field for performing groupby operation
     :param threading [bool]: Multithread each field on a thread
     :returns [dict]: dictionary of value with keys
     '''        
      mean_values = multithread(fn = self.stats_object.mean,
                                fields = fields, 
                                threading = threading)
      return mean_values
                                
        
  def median(self, fields = 'ALL', threading = False):
     '''
     Calculates median value 
     :param fields [list of int]: list of fields 
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of value with keys
     '''
      median_values = multithread(fn = self.stats_object.median,
                                fields = fields, 
                                threading = threading)
      return median_values
                            
    
  def mode(self, fields = 'ALL', threading = False):
     '''
     Calculates mode value 
     :param fields [list of int]: list of fields
     :param threading [bool]: Multithread each key on a thread
     :returns [float]: mean value
     '''
      mode_values = multithread(fn = self.stats_object.mode,
                                fields = fields, 
                                threading = threading)
      return mode_values
                            
    
  def std(self, fields = 'ALL', threading = False):
     '''
     Calculates standard deviation value 
     :param fields [list of int]: list of fields
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of value with keys
     '''        
      std_values = multithread(fn = self.stats_object.std,
                                fields = fields, 
                                threading = threading)
      return std_values
