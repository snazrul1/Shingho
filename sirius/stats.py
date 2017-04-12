import numpy
from multiprocessing.pool import ThreadPool
from sirius import utils

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
    
  def mean(self, keys = 'ALL'):
     '''
     Calculates mean value of specific RDD
     :param keys [list of int]: list of keys 
     :returns [float]: mean value
     '''
      raise NotImplementedError
        
  def median(self, keys = 'ALL'):
     '''
     Calculates median value 
     :param keys [list of int]: list of keys 
     :returns [float]: mean value
     '''
      raise NotImplementedError
    
  def mode(self, keys = 'ALL'):
     '''
     Calculates mode value 
     :param keys [list of int]: list of keys 
     :returns [float]: mean value
     '''
      raise NotImplementedError
    
  def std(self, keys = 'ALL'):
     '''
     Calculates standard deviation value 
     :param keys [list of int]: list of keys 
     :returns [float]: mean value
     '''
      raise NotImplementedError
    
  def mode(self, keys = 'ALL'):
     '''
     Calculates variance value 
     :param keys [list of int]: list of keys 
     :returns [float]: mean value
     '''
      raise NotImplementedError
