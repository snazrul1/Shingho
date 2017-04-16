#To be changed to 'import shingho' only once setup.py is fixed
try:
  #This should work if setup.py ran successfully
  import shingho
except importError:
  #If setup.py fails, manually add shingho to file path (may fail for certain dependencies)
  import sys
  sys.path.insert(0, '../../../shingho')
  import shingho
  
  from shingho.utils import multithreading
  
  def sum_function(first_value):
    second_value = 5
    return first_value + second_value
  
  first_values_list = [0,1,2,3,4]
  expected_result = [5,6,7,8,9]
  
  #This unit test object will either give a 'pass' or 'fail' for each method 
class Single_Thread_Stats_Unit_Test(unittest.TestCase):
  '''
  Unit Test Class
  '''
  def setUp(self):
      pass

  def multithreading_with_threading(self):
      '''
      Check utils.multithreading with threading
      '''
      function_result = multithreading(fn = sum_function, 
                                       fields = first_values_list,
                                       threading = True)
      self.assertEqual(function_result, expected_result)
      
  def multithreading_without_threading(self):
      '''
      Check utils.multithreading without threading
      '''
      function_result = multithreading(fn = sum_function, 
                                       fields = first_values_list,
                                       threading = False)
      self.assertEqual(function_result, expected_result)
