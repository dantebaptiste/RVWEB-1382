import my_globals
from class_variable_manager import VariableManager
"""This script allows for scheduled jobs to be stopped (and if the script is executed while
a job is running, the job will be interrupted the next time the job reaches a point where
it checks the status of the variable manager, which is generally in fairly short order.)
Note that if this script is called DURING the execution of a job, it will NOT stop future jobs
from running, because at the end of the job it sets the variable to True again (this is desired
behavior, so that if for example, a network timeout stops the current job, future jobs will
still try to run again.) On the other hand, if this script is called IN BETWEEN scheduled job
runs (in other words, when no job is running) it WILL disable future jobs from running as well.
This script allows the main loop to continue, so future scheduled jobs will still ATTEMPT
to run. They will fail to run unless the relevant variable is set back to True (which
can be done by running the script called z_hlpr_enable_jobs.py, or as stated above, if
this script is run when a job is currently being executed."""

# the variable manager below is used to allow/stop/interrupt the execution of
# scheduled jobs. So here, it set to false in order stop/interrupt execution.
filepath_with_job_execution_variables = my_globals.str_fullfilepath_continuous_execution_jobs_variables
var_mgr_jobs = VariableManager(filepath_with_job_execution_variables)
var_mgr_jobs.var_set(my_globals.str_execution_may_go_on, False)
