import os

# This file used to be ignored in .gitignore because it literally
# contained sensitive info like usernames and passwords. However
# now the file pulls these values from environment variables, so
# can now be safely included in the git repo.

rv_u = os.environ['ETLAPP_RV_USER']
rv_p = os.environ['ETLAPP_RV_PWD']

airtable_api_key = os.environ['ETLAPP_AIRTABLE_API_KEY']

algolia_admin_api_key = os.environ['ETLAPP_ALGOLIA_API_KEY']
algolia_app_id = os.environ['ETLAPP_ALGOLIA_APP_ID']

# scheduling strings that can be used for APScheduler
job_sched_frequent_hours = os.environ['ETLAPP_JOB_FREQUENT_HOURS']
job_sched_frequent_minutes = os.environ['ETLAPP_JOB_FREQUENT_MINS']
job_sched_daily_hours = os.environ['ETLAPP_JOB_DAILY_HOURS']
job_sched_daily_days = os.environ['ETLAPP_JOB_DAILY_DAYS']  # every day except Friday (Friday will be the weekly job.)
job_sched_weekly_hours = os.environ['ETLAPP_JOB_WEEKLY_HOURS']
job_sched_weekly_days = os.environ['ETLAPP_JOB_WEEKLY_DAYS']

# airtable bases
AT_base = os.environ['ETLAPP_AIRTABLE_BASE']
AT_base_working_on = os.environ['ETLAPP_ENVIRONMENT']

# Algolia indexes and dictionaries as used in main code
# VIDEOS index 01 is an index where we are pushing all fields, and the PSEUDO-transcript
algolia_vids_idx_01 = os.environ['ETLAPP_ALGOLIA_VID_IDX_01']
# VIDEOS index 02 we are using to test records with FULL transcript
algolia_vids_idx_02 = os.environ['ETLAPP_ALGOLIA_VID_IDX_02']
# PUBLICATIONS index 01 we are using to push the publication fields with the PSEUDO-text,
# but NOT the full text.
algolia_pubs_idx_01 = os.environ['ETLAPP_ALGOLIA_PUB_IDX_01']

# Algolia indexes as used by the code that configures the index settings
idx_vids_main = os.environ['ETLAPP_ALGOLIA_VID_IDX_01']
idx_vids_sort_date_desc = os.environ['ETLAPP_ALGOLIA_VID_IDX_01R01']
idx_vids_sort_date_asc = os.environ['ETLAPP_ALGOLIA_VID_IDX_01R02']
idx_vids_sort_likes_desc = os.environ['ETLAPP_ALGOLIA_VID_IDX_01R03']
idx_vids_qso = os.environ['ETLAPP_ALGOLIA_VID_IDX_01R04']
