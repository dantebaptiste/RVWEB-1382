## Description

Code to extract valuable data out of the JSON videos metadata returned by the Real Vision website api. This data is
being translated into JSON formats expected by Airtable and Algolia, and pushed to those platforms.

## How to use this code on a linux server

- Create a parent folder for the project. Two sub-folders should be created in the parent folder: code and data. The hierarchy should be as follows.

parent/

parent/code/

parent/data/

(optionally there can be a parent/venv/ folder as well for a python virtual environment.)  

- clone this git repository into the code/ folder
- two files that have sensitive information (and are therefore in the .gitignore) need to be re-created locally in the code/myToolkit/ folder (with values that the code is looking for):
code/myToolkit/my_config.py  (has passwords and API keys)
code/myToolkit/my_globals_distinct_per_environment.py (used to connect to different environments, for example, test vs. prod tables in Airtable and test vs. prod indexes in Algolia)
(Samples of these two files are provided at the end of this README.)
- add required libraries to the python interpreter (these should all be installable with pip)
  - PyPDF2
  - pdfplumber
  - textblob
  - pandas (version 1.1.3 is known to work well)
  - algoliasearch
  - APScheduler
  - selenium
  - airtable-python-wrapper (must be version 0.15.1 at least)

(note, there are other dependencies, but they tend to be libraries that come by default with most python installations.)


- add code/sub-folders to the paths the python interpreter is looking in

- add folder structure and data to the data/ folder. This consist of a specific folder structure, some mini-databases (stored as CSVs and used in memory as pandas dataframes), and files (for example, video transcripts, or video metadata as JSON.) Note that it is not advisable to run the code without the proper data existing in the data/ folder first. The code will error if it can't find certain paths, and it will take a very long time to re-create the mini-databases (in particular the ones that store topics from the videos, which are extracted using the natural language processing done by the textblob library.)

## Execution
As of this writing, running the program is done with the following command.
python code/continuous_execution/main.py &

Note that this file uses the library APScheduler to run the program continuously at intervals throughout the day, with different jobs running with different levels of thoroughness (for example, the last job of the day at 10:00pm pulls more information from the RV Website API than the jobs that run throughout the day.) APScheduler is a OS agnostic job scheduler. A future improvement may be to extract execution out of APScheduler and make this a more proper linux service/daemon.

The file
code/continuous_execution/z_hlpr_1x_manual_run_job.py
can be used to run the program just once, which is useful for testing what the program will do when executed by main.py and APScheduler without waiting for the next scheduled run time.

The file
code/continuous_execution/z_hlpr_disable_execution.py
can be used to gracefully stop execution of the main program or of the run-once version.
Depending on what portion of the code might be running at the time, this may take a few minutes. 

Note that as of this writing, authentication with the RV website is achieved using selenium driver for a Firefox graphical browser. So at this time, the code cannot be run on a headless server. The selenium browser opens a Firefox window as part of the code execution.
In order for this to work, it is probably necessary to install the GeckoDriver.

## Samples

------- SAMPLE myToolkit/my_config.py --------

airtable_api_key = 'somekeyhere'

rv_u = 'username_for_rv_website'

rv_p = '************'

algolia_app_id = 'idhere'

algolia_admin_api_key = 'algoliaapikeyhere'

----- END SAMPLE myToolkit/my_config.py ------


------- SAMPLE myToolkit/my_globals_distinct_per_environment.py --------

\# scheduling strings that can be used for APScheduler

job_sched_frequent_hours = '5-15/2'

job_sched_daily_hours = '17'

job_sched_daily_days = 'mon,tue,wed,thu,fri,sat'  # every day except Friday (Friday we will run the weekly job.)

job_sched_weekly_hours = '17'

job_sched_weekly_days = 'sun'

\# airtable bases

AT_base_testing = 'baseidhere'

AT_base_production = ''

AT_base_working_on = AT_base_testing

\# Algolia indexes and dictionaries as used in main code

\# VIDEOS index 01 is an index where we are pushing all fields, and the PSEUDO-transcript

algolia_vids_idx_01 = 't_v01'

\# VIDEOS index 02 we are using to test records with FULL transcript

algolia_vids_idx_02 = 't_v02'

\# PUBLICATIONS index 01 we are using to push the publication fields with the PSEUDO-text,
\# but NOT the full text.

algolia_pubs_idx_01 = 't_p01'
\# DISCOVER LINKS

algolia_discoverlinks_idx_01 = 't_l01'

\# Algolia indexes as used by the code that configures the index settings

idx_vids_main = 't_v01'

idx_vids_sort_date_desc = 't_v01_r01'

idx_vids_sort_date_asc = 't_v01_r02'

idx_vids_sort_likes_desc = 't_v01_r03'

idx_vids_qso = 't_v01_r04'

\# paths

path_base_project = '/home/user1/projects/rv_sync/data/'

----- END SAMPLE myToolkit/my_globals_distinct_per_environment.py ------