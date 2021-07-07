import logging
import my_globals
from move_data_from_RVwebsite_to_destinations import move_data_from_RVwebsite_to_destinations
from class_variable_manager import VariableManager
from my_building_blocks import setup_logging

# LOGGING SETUP
setup_logging(level='info')

logging.info('******************* NON SCHEDULED run of main program starting *******************')

# warn about the environment that the program is running in
str_warn = 'Program is being executed in ' + my_globals.str_AT_base_working_on
logging.info(str_warn)

# management of variables that allow certain things to be changed during
# execution. For example, there is a variable that can be used to stop
# program execution.
logging.info('Setting-up execution variables.')
filepath_with_execution_variables = my_globals.str_fullfilepath_continuous_execution_jobs_variables
var_mgr = VariableManager(filepath_with_execution_variables)
var_mgr.var_set(my_globals.str_execution_may_go_on, True)

move_data_from_RVwebsite_to_destinations(var_mgr, int_level_of_thoroughness=1, trial_runs=False)
