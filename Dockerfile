FROM ubuntu:20.04

ARG arg_timezone
ARG arg_rv_user
ARG arg_rv_pwd
ARG arg_airtable_api_key
ARG arg_algolia_api_key
ARG arg_algolia_app_id
ARG arg_job_frequent_hours
ARG arg_job_frequent_mins
ARG arg_job_daily_hours
ARG arg_job_daily_days
ARG arg_job_weekly_hours
ARG arg_job_weekly_days
ARG arg_airtable_base
ARG arg_environment
ARG arg_algolia_vid_idx_01
ARG arg_algolia_vid_idx_01r01
ARG arg_algolia_vid_idx_01r02
ARG arg_algolia_vid_idx_01r03
ARG arg_algolia_vid_idx_01r04
ARG arg_algolia_vid_idx_02
ARG arg_algolia_pub_idx_01

RUN apt update -y
RUN apt install -y tzdata
ENV TZ=$arg_timezone
RUN apt install -y python3.9
RUN apt install -y python3-pip
RUN apt upgrade -y

RUN groupadd --gid 5000 realvision && \
   useradd --home-dir /home/realvision --create-home --uid 5000 --gid 5000 --shell /bin/sh realvision
RUN mkdir -p /home/realvision/rv_multi_etl/code

COPY . /home/realvision/rv_multi_etl/code

RUN /usr/bin/python3.9 -m pip install -r /home/realvision/rv_multi_etl/code/requirements.txt

USER realvision

RUN /usr/bin/python3.9 -m nltk.downloader popular
RUN /usr/bin/python3.9 -m nltk.downloader brown
ENV PYTHONPATH="/home/realvision/rv_multi_etl/code/myToolkit:$PYTHONPATH"
ENV ETLAPP_RV_USER=$arg_rv_user
ENV ETLAPP_RV_PWD=$arg_rv_pwd
ENV ETLAPP_AIRTABLE_API_KEY=$arg_airtable_api_key
ENV ETLAPP_ALGOLIA_API_KEY=$arg_algolia_api_key
ENV ETLAPP_ALGOLIA_APP_ID=$arg_algolia_app_id
ENV ETLAPP_JOB_FREQUENT_HOURS=$arg_job_frequent_hours
ENV ETLAPP_JOB_FREQUENT_MINS=$arg_job_frequent_mins
ENV ETLAPP_JOB_DAILY_HOURS=$arg_job_daily_hours
ENV ETLAPP_JOB_DAILY_DAYS=$arg_job_daily_days
ENV ETLAPP_JOB_WEEKLY_HOURS=$arg_job_weekly_hours
ENV ETLAPP_JOB_WEEKLY_DAYS=$arg_job_weekly_days
ENV ETLAPP_AIRTABLE_BASE=$arg_airtable_base
ENV ETLAPP_ENVIRONMENT=$arg_environment
ENV ETLAPP_ALGOLIA_VID_IDX_01=$arg_algolia_vid_idx_01
ENV ETLAPP_ALGOLIA_VID_IDX_01R01=$arg_algolia_vid_idx_01r01
ENV ETLAPP_ALGOLIA_VID_IDX_01R02=$arg_algolia_vid_idx_01r02
ENV ETLAPP_ALGOLIA_VID_IDX_01R03=$arg_algolia_vid_idx_01r03
ENV ETLAPP_ALGOLIA_VID_IDX_01R04=$arg_algolia_vid_idx_01r04
ENV ETLAPP_ALGOLIA_VID_IDX_02=$arg_algolia_vid_idx_02
ENV ETLAPP_ALGOLIA_PUB_IDX_01=$arg_algolia_pub_idx_01

CMD ["/usr/bin/python3.9", "/home/realvision/rv_multi_etl/code/continuous_execution/main.py"]
