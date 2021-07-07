import my_globals
from class_variable_manager import VariableManager
"""This script allows for scheduled jobs to be re-enabled if they have been
told to stop."""

# the variable manager below is used to allow/stop/interrupt the execution of
# scheduled jobs. So here, it set to True in order allow execution to resume.
filepath_with_job_execution_variables = my_globals.str_fullfilepath_continuous_execution_jobs_variables
var_mgr_jobs = VariableManager(filepath_with_job_execution_variables)
var_mgr_jobs.var_set(my_globals.str_execution_may_go_on, True)
