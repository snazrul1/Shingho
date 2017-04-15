#To be changed to 'import shingho' only once setup.py is fixed
try:
  #This should work if setup.py ran successfully
  import shingho
except importError:
  #If setup.py fails, manually add shingho to file path (may fail for certain dependencies)
  import sys
  sys.path.insert(0, '../../../shingho')
  import shingho
  
