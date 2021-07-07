import signal
import time
import logging
from apscheduler.schedulers.background import BackgroundScheduler
import my_globals
from move_data_from_RVwebsite_to_destinations import move_data_from_RVwebsite_to_destinations
from class_variable_manager import VariableManager
from my_building_blocks import setup_logging
from z_hlpr_disable_execution import stop_program_gracefully

# LOGGING SETUP
setup_logging(level='info')


def shutdown(signum, frame):
    logging.warning('Caught SIGTERM, shutting down')
    # Finish any outstanding requests, then...
    stop_program_gracefully()


if __name__ == '__main__':
    # Register handler
    signal.signal(signal.SIGTERM, shutdown)

    logging.info('*************************************************************')
    logging.info('******************* Main program starting *******************')
    logging.info('*************************************************************')

    # warn about the environment that the program is running in
    str_warn = 'Program is being executed in ' + my_globals.str_AT_base_working_on
    logging.info(str_warn)

    # management of variables that allow certain things to be changed during
    # execution. For example, there is a variable that can be used to stop
    # the execution of a running job, either manually by an operator, or by the code
    # if it runs into an exception.
    logging.debug('Setting-up scheduled-job execution variables.')
    filepath_with_job_execution_variables = my_globals.str_fullfilepath_continuous_execution_jobs_variables
    var_mgr_jobs = VariableManager(filepath_with_job_execution_variables)
    var_mgr_jobs.var_set(my_globals.str_execution_may_go_on, True)

    # in the past, the variable manager instance above was used to store variables
    # for both the scheduled jobs, as well as the main loop. This meant that if
    # an individual (scheduled) running job was stopped (for example by the code catching an exception and setting the
    # var_man execution_may_go_on variable to false), the main loop would stop too. So I'm adding
    # an additional variable manager that will allow running jobs to stop, while allowing the
    # main loop to continue running. This is useful, for example, when the airtable API is down,
    # or if the code runs into an internet call timeout; in these cases, adding this variable manager
    # will allow for the running job that encounters the error to stop executing, but it will allow
    # the main loop to continue so that the next scheduled job will try again (and hopefully by then
    # the issue will have resolved itself.)
    logging.debug('Setting-up main loop execution variables.')
    filepath_with_loop_execution_variables = my_globals.str_fullfilepath_continuous_execution_mainloop_variables
    var_mgr_loop = VariableManager(filepath_with_loop_execution_variables)
    var_mgr_loop.var_set(my_globals.str_execution_may_go_on, True)

    # define scheduling
    # for the documentation of APScheduler see
    # https://apscheduler.readthedocs.io/en/latest/index.html
    # for the specific documentation of the cron settings see
    # https://apscheduler.readthedocs.io/en/latest/modules/triggers/cron.html#module-apscheduler.triggers.cron
    logging.info('Initializing BackgroundScheduler()')
    scheduler = BackgroundScheduler()
    logging.info('Adding jobs to the scheduler.')
    scheduler.add_job(move_data_from_RVwebsite_to_destinations, 'cron', args=[var_mgr_jobs],
                      kwargs={'int_level_of_thoroughness': 1,
                              'max_multiple_vids': 48,
                              'trial_runs': False},
                      name='FREQUENT JOB',
                      hour=my_globals.str_job_sched_frequent_hours,
                      minute=my_globals.str_job_sched_frequent_minutes)
    scheduler.add_job(move_data_from_RVwebsite_to_destinations, 'cron', args=[var_mgr_jobs],
                      kwargs={'int_level_of_thoroughness': 2,
                              'trial_runs': False},
                      name='DAILY JOB',
                      day_of_week=my_globals.str_job_sched_daily_days,
                      hour=my_globals.str_job_sched_daily_hours)
    scheduler.add_job(move_data_from_RVwebsite_to_destinations, 'cron', args=[var_mgr_jobs],
                      kwargs={'int_level_of_thoroughness': 3,
                              'trial_runs': False},
                      name='WEEKLY JOB',
                      day_of_week=my_globals.str_job_sched_weekly_days,
                      hour=my_globals.str_job_sched_weekly_hours)
    logging.info('Starting the scheduler.')
    scheduler.start()
    logging.info('Scheduled jobs:')
    list_jobs = scheduler.get_jobs()
    for a_job in list_jobs:
        logging.info(str(a_job))

    # start a WHILE TRUE loop
    continue_execution = True
    logging.info('Starting the main continuous while loop.')
    while continue_execution:
        continue_execution = var_mgr_loop.var_retrieve(my_globals.str_execution_may_go_on)
        logging.debug("'Continue_execution' variable in main loop found to be: " + str(continue_execution))
        if not continue_execution:
            logging.info("'Continue_execution' variable in main loop found to be: " + str(continue_execution))
        time.sleep(5)

    logging.info('Program terminating gracefully.')
    logging.info('Currently scheduled jobs:')
    list_jobs = scheduler.get_jobs()
    for a_job in list_jobs:
        logging.info(str(a_job))
    logging.info('Shutting down jobs.')
    logging.info(
        'Please wait for all jobs to finish. This will have happened when you see the following INFO message printed'
        ' in the log: "Scheduler has been shut down". Until you see that message, there are probably still'
        ' jobs running.')
    scheduler.shutdown()
