import my_globals
from class_variable_manager import VariableManager
"""This script allows for execution (of the main program/loop or of a manually
triggered job) to be stopped. It can be called as a function by another file, or can
be executed on its own (very useful for troubleshooting.) Either way it will
stop the main program's execution."""


def stop_program_gracefully():
    # the variable manager below is used to allow/stop/interrupt the execution of
    # scheduled jobs. So here, it set to false in order stop/interrupt execution.
    filepath_with_job_execution_variables = my_globals.str_fullfilepath_continuous_execution_jobs_variables
    var_mgr_jobs = VariableManager(filepath_with_job_execution_variables)
    var_mgr_jobs.var_set(my_globals.str_execution_may_go_on, False)

    # the variable manager below is used to allow/stop/interrupt the execution of
    # the main loop. So here, it set to false in order stop/interrupt overall execution of the program.
    filepath_with_loop_execution_variables = my_globals.str_fullfilepath_continuous_execution_mainloop_variables
    var_mgr_loop = VariableManager(filepath_with_loop_execution_variables)
    var_mgr_loop.var_set(my_globals.str_execution_may_go_on, False)


if __name__ == '__main__':
    stop_program_gracefully()
