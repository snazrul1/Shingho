#To be changed to 'import shingho' only once setup.py is fixed
try:
  #This should work if setup.py ran successfully
  import shingho
except importError:
  #If setup.py fails, manually add shingho to file path (may fail for certain dependencies)
  import sys
  sys.path.insert(0, '../../../shingho')
  import shingho
  
from shingho.single_thread_stats import rdd_stats, df_stats
from pyspark import SparkContext, SparkConf
import numpy as np
import unittest

#Setup Spark Context
conf = SparkConf().setAppName('single_thread_stats unit tests')
sc = SparkContext(conf=conf)

#Test data
np_data = np.array([1, 1, 1, 1, 1, 2, 2, 2, 3, 4, 4, 5, 5, 6, 7])
pd_data = pd.DataFrame({'key_col': np_data})
pd_data_grouped = pd_data.groupby('key_col')['key_col']
rdd_data = sc.parallelize(data)

#Shingho objects for rdd_stats and df_stats
rdd_stats_no_index = single_thread_stats.rdd_stats(rdd_data, index_field = 0)
df_stats_no_index = single_thread_stats.rdd_stats(rdd_data = None)
rdd_stats_index = single_thread_stats.rdd_stats(rdd_data, index_field = 0)
df_stats_index = single_thread_stats.rdd_stats(rdd_data = None)

#This unit test object will either give a 'pass' or 'fail' for each method 
class Single_Thread_Stats_Unit_Test(unittest.TestCase):
  '''
  Unit Test Class
  '''
  def setUp(self):
      pass

  def rdd_mean_no_index(self):
      '''
      Cheack rdd_stats.mean() without index_field
      '''
      self.assertEqual(rdd_stats_no_index.mean(0)['ALL'], np.mean(np_data))

  def rdd_median_no_index(self):
      '''
      Cheack rdd_stats.median() without index_field
      '''
      self.assertEqual(rdd_stats_no_index.median(0)['ALL'], np.median(np_data))

  def rdd_mode_no_index(self):
      '''
      Cheack rdd_stats.mode() without index_field
      '''
      self.assertEqual(rdd_stats_no_index.mode(0)['ALL'], np.mode(np_data))

  def rdd_std_no_index(self):
      '''
      Cheack rdd_stats.std() without index_field
      '''
      self.assertEqual(rdd_stats_no_index.std(0)['ALL'], np.std(np_data))

  def rdd_mean_index(self):
      '''
      Cheack rdd_stats.mean() with index_field
      '''
      self.assertEqual(rdd_stats_index.mean(0), pd_data_grouped.mean().to_dict())

  def rdd_median_index(self):
      '''
      Cheack rdd_stats.median() with index_field
      '''
      self.assertEqual(rdd_stats_index.median(0), pd_data_grouped.median().to_dict())

  def rdd_mode_index(self):
      '''
      Cheack rdd_stats.mode() with index_field
      '''
      self.assertEqual(rdd_stats_index.mode(0), pd_data_grouped.mode().to_dict())

  def rdd_std_index(self):
      '''
      Cheack rdd_stats.std() with index_field
      '''
      self.assertEqual(rdd_stats_index.std(0), pd_data_grouped.std().to_dict())
      
  def df_mean_no_index(self):
      '''
      Cheack df_stats.mean() without index_field
      '''
      self.assertEqual(df_stats_no_index.mean(0)['ALL'], np.mean(np_data))

  def df_median_no_index(self):
      '''
      Cheack df_stats.median() without index_field
      '''
      self.assertEqual(df_stats_no_index.median(0)['ALL'], np.median(np_data))

  def df_mode_no_index(self):
      '''
      Cheack df_stats.mode() without index_field
      '''
      self.assertEqual(df_stats_no_index.mode(0)['ALL'], np.mode(np_data))

  def df_std_no_index(self):
      '''
      Cheack df_stats.std() without index_field
      '''
      self.assertEqual(df_stats_no_index.std(0)['ALL'], np.std(np_data))

  def df_mean_index(self):
      '''
      Cheack df_stats.mean() with index_field
      '''
      self.assertEqual(df_stats_index.mean(0), pd_data_grouped.mean().to_dict())

  def df_median_index(self):
      '''
      Cheack df_stats.median() with index_field
      '''
      self.assertEqual(df_stats_index.median(0), pd_data_grouped.median().to_dict())

  def df_mode_index(self):
      '''
      Cheack df_stats.mode() with index_field
      '''
      self.assertEqual(rdd_stats_index.mode(0), pd_data_grouped.mode().to_dict())

  def df_std_index(self):
      '''
      Cheack df_stats.std() with index_field
      '''
      self.assertEqual(df_stats_index.std(0), pd_data_grouped.std().to_dict())
      
if __name__ == '__main__':
  unittest.main()
