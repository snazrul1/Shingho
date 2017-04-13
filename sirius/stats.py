import numpy
from sirius.untils import multThread

class basic_stats(object):
  '''
  Calculate sample statistics
  '''
  
  def __init__(self, rdd, sampling = None):
    '''
    :param rdd [Spark RDD]: Spark RDD for analytics
    :param sampling [float]: Sampling rate between 0 and 1
    '''
    self.sampling = sampling
    if sampling = None:
      self.rdd = rdd
    else:
      self.rdd = rdd.sample(sampling)
    
  def mean(self, fields = 'ALL', index_field = None, threading = False):
     '''
     Calculates mean value of Specific RDD
     :param fields [list of int]: list of fields
     :param index_field [int]: Field for performing groupby operation
     :param threading [bool]: Multithread each key on a thread
     :returns [dict]: dictionary of mean value with keys
     '''
      def single_thread_mean(f):
        '''Calculate mean for a single field on a single thread'''
        if index_field == None:
          mean_value = self.rdd.map(lambda row: (row[f], 1))\
                        .reduce(lambda x,y : x+y)
                        .map(lambda x,y : x/y)
                        .top(1)
          return ('All', mean_value)
        else:
          key_total = self.rdd.map(lambda row: (row[index_field], row[f]))\
              .reduceByKey(lambda x,y : x+y)\
              .collect()
          
          key_count = self.rdd.map(lambda row: (row[index_field], 1))\
              .reduceByKey(lambda x,y : x+y)\
              .collect()    
        
          mean_value = [x[1]/y[1] for x in key_total for y in key_count if x[0]==y[0]]
          return mean_value
      
      mean_values = multThread(single_thread_mean, fields, threading = threading)
          
      return mean_values
                                
        
  def median(self, fields = 'ALL', index_field = None, threading=False):
     '''
     Calculates median value 
     :param fields [list of int]: list of fields 
     :param threading [bool]: Multithread each key on a thread
     :returns [float]: mean value
     '''
      raise NotImplementedError
    
  def mode(self, fields = 'ALL', index_field = None, threading=False):
     '''
     Calculates mode value 
     :param fields [list of int]: list of fields
     :param threading [bool]: Multithread each key on a thread
     :returns [float]: mean value
     '''
      raise NotImplementedError
    
  def std(self, fields = 'ALL', index_field = None, threading=False):
     '''
     Calculates standard deviation value 
     :param fields [list of int]: list of fields
     :param threading [bool]: Multithread each key on a thread
     :returns [float]: mean value
     '''
      raise NotImplementedError
    
  def mode(self, fields = 'ALL', index_field = None, threading=False):
     '''
     Calculates variance value 
     :param fields [list of int]: list of fields
     :param threading [bool]: Multithread each key on a thread
     :returns [float]: mean value
     '''
      raise NotImplementedError
