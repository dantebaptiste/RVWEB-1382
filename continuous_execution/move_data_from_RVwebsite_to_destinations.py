import time
import logging
import requests
import my_globals
from class_authenticate_rv_website import AuthenticateRVwebsite
from class_transcript_universe import TranscriptAnalysis
from my_rv_website_functions import grab_from_disk_product_info, \
    pull_products_info_from_web_and_save2files, \
    refresh_vids_and_shows_from_rv_website, \
    check_if_sets_of_vids_are_error_free, \
    extractFieldsFromShowsData, \
    get_comments_stats, \
    tag_rv_website_vids_for_deletion, \
    pull_sets_of_publications_JSON_data_from_web_2disk, \
    check_if_sets_of_pubs_are_error_free, \
    tag_rv_website_pubs_for_deletion, \
    get_all_vid_transcripts, \
    get_all_publication_fulltexts
from my_data_manip_functions import update_website_videos_datastructure, \
    update_website_pubs_datastructure, \
    extract_guestsNsubjects, \
    extract_interviewers, \
    convertWebsiteJSONtoAirtableFormat
from my_airtable_functions import get_airt_guests_ordered_by_name, \
    find_website_ppl_not_in_airtable, \
    push_guestsNsubjects_delta, \
    get_airt_shows_ordered_by_rvwebname, \
    get_airt_vids_ordered_by_rvwebid, \
    push_vids
from my_algolia_functions import update_and_push_multiple_indexes


def move_data_from_RVwebsite_to_destinations(variable_manager,  # noqa: C901
                                             int_level_of_thoroughness=1,
                                             max_multiple_vids=48,
                                             trial_runs=False):
    """This function executes many workflows in succession which previously had been
    run manually one after the other. For example, first pulling info from the website,
    then processing this info into guests and subjects, then converting the info
    into an airtable record, etc. So this function puts all those workflows together in
    order to pull info from the RV website and push it to airtable, algolia, etc.
    Depending on the level of int_level_of_thoroughness requested, this function will do different
    things. For example, at the time of this writing:
    - level 1 will do basic things like refreshing/processing
    only a few most recent videos from the RV website (suitable for very frequent runs of this
    function - eg. hourly)
    - level 2 will do a few more things (so maybe suitable for a day-end job), and
    - level 3 will pull/process everything (so maybe suitable for a job once a week.)
    IMPORTANT. NOTE that the max_multiple_vids variable that is passed will not be used exactly.
    The reason for this is that the website returns videos in sets. At the time of this
    writing, the sets are of 24 videos (though this could change.) So, for example, if
    50 is the max_multiple_vids requested, the code that this function calls will most likely end
    up pulling the next multiple of 24 number of videos - in this example, it will be 72
    for each product. Counter-intuitively, if for example, 24 is passed, then 48 videos
    will be pulled because 24 is the index that the website uses to return the next set of
    24 videos. So in this example if the maximum number of videos to pull that is wanted
    is 24, then the parameter passed to this function can be any number between 0 and 23."""
    logging.info('---------------------------------------------')
    logging.info("STARTING function 'move_vid_data_fromRV_toAT'")
    logging.info('---------------------------------------------')
    start_time_overall = time.time()

    # get the number of vids the website API is returning per page (using the 'television' product)
    vids_per_page = grab_from_disk_product_info(my_globals.str_product_name_tv, 'base multiple')
    num_vids_to_process_comments_level1 = vids_per_page * 5
    num_vids_to_process_comments_level2 = num_vids_to_process_comments_level1 * 2
    num_vids_to_process_transcripts_level1 = vids_per_page * 4

    def should_continue_execution():
        # make sure execution of the program hasn't been told to stop by another module
        may_continue = variable_manager.var_retrieve(my_globals.str_execution_may_go_on)
        if not may_continue:
            logging.info('Variable: ' + my_globals.str_execution_may_go_on + ' is -> ' + str(may_continue))
        return may_continue

    def raise_exception_due_to_external_stop():
        # this function is used in the code below to raise an exception if
        # the program was 'asked' to stop executing by a VariableManager instance.
        # Raising an exception, rather than just using a simple 'return' statement
        # is useful, because it can be used as part of the try/except statement.
        # This, in turn, allows for a graceful exit of the function, including
        # a graceful logout of the RV website.
        raise Exception('Program execution asked to stop by an external source via'
                        ' the VariableManager class.')

    # make sure execution of the program hasn't been told to stop by another module
    # in all the checks further below (similar to this one) we exit using the function
    # declared above, rather than a simple 'return' statement. This is deliberate
    # because at this point in the code, we still have not logged into the RV website
    # and because we are not inside a try/except statement
    if not should_continue_execution():
        return

    # login to RV website
    logging.info('Logging in to RV Website')
    start_time = time.time()
    rv_web_auth = AuthenticateRVwebsite()
    rv_web_auth.login_rv_website()
    end_time = time.time()
    logging.info('Time it took to login to RV Website: '
                 + "{:.2f}".format(end_time - start_time) + ' seconds.')

    try:
        # we wrap the majority of the function in a try/catch loop, not so much with the intention of
        # catching errors, but rather to allow the website authentication functions to close gracefully
        # if something goes wrong.

        # set the authenticated session variables for the portions of code that need it
        if rv_web_auth.logged_in:
            sesh = requests.session()
            sesh.cookies.update(rv_web_auth.dct_cookies)
            sesh.headers.update(rv_web_auth.dct_headers)
        else:
            logging.warning('Not authenticated on the Real Vision website.')

        # --------------- PART 1: PULL DATA AND UPDATE FIRST-STAGE DATASTRUCTURES ---------------
        logging.info('-->>>>>   REFRESHING AND UPDATING DATA   <<<<<<--')
        # make sure execution of the program hasn't been told to stop by another module
        if not should_continue_execution():
            raise_exception_due_to_external_stop()

        # if we are doing a fully thorough run, then the first thing to
        # do is to re-pull the number of vids returned on each page, and
        # the max multiple that still returns data.
        if int_level_of_thoroughness >= 2:
            logging.debug(my_globals.str_logging_func_next + pull_products_info_from_web_and_save2files.__name__)
            pull_products_info_from_web_and_save2files()
            logging.debug(my_globals.str_logging_func_exited + pull_products_info_from_web_and_save2files.__name__)

        # make sure execution of the program hasn't been told to stop by another module
        if not should_continue_execution():
            raise_exception_due_to_external_stop()
        # next, we refresh data by pulling from the Real Vision production
        # website. depending on the 'int_level_of_thoroughness' requested, we either do
        # a partial pull or a full pull of videos. If it is a partial pull,
        # then, the number of videos to pull should have been given to this
        # function as a parameter.
        dict_args = {'pullShowsInfo': True, 'pullVideosInfo': True}
        # depending on the int_level_of_thoroughness level requested, the arguments
        # above are sufficient for the function call that is about to happen.
        # However, depending on the int_level_of_thoroughness, some additional arguments
        # may be required (hence the adding of arguments to the dictionary
        # in the lines below. (NOTE, in the case where absolute int_level_of_thoroughness
        # is reqeusted, the 'max_multiple' does nothing, because all
        # videos are pulled.
        if int_level_of_thoroughness == 1:  # for 'low thoroughness' only get a certain number of videos
            dict_args['max_multiple'] = max_multiple_vids
        logging.info(my_globals.str_logging_func_next + refresh_vids_and_shows_from_rv_website.__name__)
        retrieved_fresh_vid_data_without_errors = refresh_vids_and_shows_from_rv_website(**dict_args)
        logging.info(my_globals.str_logging_func_exited + refresh_vids_and_shows_from_rv_website.__name__)

        # if the function above ran without errors, we want to go on, but if it
        # had errors, we want to stop the current job (this is key, because download errors
        # could potentially result in subsequent code thinking videos/pubs have been deleted
        # at the source, which would result in a chain reaction of records being removed in
        # other workflows and instances of SimpleDS
        if not retrieved_fresh_vid_data_without_errors:
            logging.info('Retrival of fresh data about videos from the RV website had errors. Gracefully'
                         ' halting execution of the current job.')
            variable_manager.var_set(my_globals.str_execution_may_go_on, False)

        # make sure execution of the program hasn't been told to stop by another module
        if not should_continue_execution():
            raise_exception_due_to_external_stop()
        # only when  int_level_of_thoroughness is 2 or 3 do we want to attempt the workflow
        # that initiates deletions. Deletions must compare two global sets, so when the
        # int_level_of_thoroughness is 1 (low) the downloaded set of videos does not provide sufficient
        # information to know if records have been deleted on the website.
        if int_level_of_thoroughness >= 2:
            logging.debug(my_globals.str_logging_func_next + check_if_sets_of_vids_are_error_free.__name__)
            sets_of_vids_are_error_free = check_if_sets_of_vids_are_error_free()
            logging.debug(my_globals.str_logging_func_exited + check_if_sets_of_vids_are_error_free.__name__)
            if sets_of_vids_are_error_free:
                logging.debug(my_globals.str_logging_func_next + tag_rv_website_vids_for_deletion.__name__)
                tag_rv_website_vids_for_deletion(
                    tolerance_allow_max_deletions=my_globals.int_max_vid_deletions_tolerance, trial_run=trial_runs)
                logging.debug(my_globals.str_logging_func_exited + tag_rv_website_vids_for_deletion.__name__)

        # make sure execution of the program hasn't been told to stop by another module
        if not should_continue_execution():
            raise_exception_due_to_external_stop()
        # next we update the SimpleDS data structure that contains metadata pulled from the RV
        # website about individual videos. Since (at the time of this writing) this is a pretty
        # fast operation, there is no difference between int_level_of_thoroughness levels. The function that is called
        # runs through all of the videos contained in the SimpleDS
        logging.info(my_globals.str_logging_func_next + update_website_videos_datastructure.__name__)
        update_website_videos_datastructure(trial_run=trial_runs)
        logging.info(my_globals.str_logging_func_exited + update_website_videos_datastructure.__name__)

        # make sure execution of the program hasn't been told to stop by another module
        if not should_continue_execution():
            raise_exception_due_to_external_stop()
        # next, refresh data about publications. Pubs aren't added/updated as often as videos, so
        # it should be okay to only run this when a more thorough run of the function is executing,
        # so levels 2 and 3
        retrieved_fresh_pub_data_without_errors = False
        if int_level_of_thoroughness >= 2:
            start_time = time.time()
            if rv_web_auth.logged_in:
                logging.info(my_globals.str_logging_func_next +
                             pull_sets_of_publications_JSON_data_from_web_2disk.__name__)
                retrieved_fresh_pub_data_without_errors = \
                    pull_sets_of_publications_JSON_data_from_web_2disk(requests_session=sesh)
                logging.info(my_globals.str_logging_func_exited +
                             pull_sets_of_publications_JSON_data_from_web_2disk.__name__)
            end_time = time.time()
            logging.info('Time it took to refresh data about Publications from the RV website: '
                         + "{:.2f}".format(end_time - start_time) + ' seconds.')

        # make sure execution of the program hasn't been told to stop by another module
        if not should_continue_execution():
            raise_exception_due_to_external_stop()
        # next, check to see if any publications have been removed at source (the website) and
        # tag them for deletion. Again, we only do this for publications
        # if the function is running a fairly 'thorough' run (levels 2 and 3)
        if int_level_of_thoroughness >= 2:
            sets_of_pubs_are_error_free = check_if_sets_of_pubs_are_error_free()
            if retrieved_fresh_pub_data_without_errors and sets_of_pubs_are_error_free:
                logging.info(my_globals.str_logging_func_next + tag_rv_website_pubs_for_deletion.__name__)
                tag_rv_website_pubs_for_deletion(
                    tolerance_allow_max_deletions=my_globals.int_max_pub_deletions_tolerance, trial_run=trial_runs)
                logging.info(my_globals.str_logging_func_exited + tag_rv_website_pubs_for_deletion.__name__)

        # make sure execution of the program hasn't been told to stop by another module
        if not should_continue_execution():
            raise_exception_due_to_external_stop()
        # next, update the local SimpleDS (containing JSON metadata about each publication as
        # pulled from the RV website.) Again, we only do this for publications if the function is running a
        # fairly 'thorough' run (levels 2 and 3)
        if int_level_of_thoroughness >= 2:
            logging.info(my_globals.str_logging_func_next + update_website_pubs_datastructure.__name__)
            update_website_pubs_datastructure(trial_run=trial_runs)
            logging.info(my_globals.str_logging_func_exited + update_website_pubs_datastructure.__name__)

        # make sure execution of the program hasn't been told to stop by another module
        if not should_continue_execution():
            raise_exception_due_to_external_stop()
        # next, pull video comments stats from the RV website. This function needs to be
        # logged in (have an authenticated session) to work correctly
        start_time = time.time()
        if rv_web_auth.logged_in:
            num_vids = -1
            if int_level_of_thoroughness == 1:  # for low thoroughness
                num_vids = num_vids_to_process_comments_level1
            elif int_level_of_thoroughness == 2:  # for medium thoroughness
                num_vids = num_vids_to_process_comments_level2
            logging.info(my_globals.str_logging_func_next + get_comments_stats.__name__)
            get_comments_stats(variable_manager, num_vids_to_process=num_vids,
                               requests_session=sesh, trial_run=trial_runs)
            logging.info(my_globals.str_logging_func_exited + get_comments_stats.__name__)
        end_time = time.time()
        logging.info('Time it took to pull stats about Video Comments: '
                     + "{:.2f}".format(end_time - start_time) + ' seconds.')

        # make sure execution of the program hasn't been told to stop by another module
        if not should_continue_execution():
            raise_exception_due_to_external_stop()
        # next, update video transcripts
        start_time_0 = time.time()
        start_time = start_time_0
        if rv_web_auth.logged_in:
            dict_function_arguments = {'max_vids_to_process': num_vids_to_process_transcripts_level1,
                                       'trial_run': trial_runs,
                                       'try_to_fetch_even_if_tagged_as_missing': False}
            # in the dictionary above we set the options to be passed to the function below as
            # the options we want in the most frequently job run (low level of thoroughness.)
            # and now, using IF cases, we modify those options for other types of jobs
            if int_level_of_thoroughness >= 2:
                dict_function_arguments['max_vids_to_process'] = -1
            if int_level_of_thoroughness == 3:
                # at the time of this writing, the level 3 job is run once a week. So here,
                # we are saying that once a week we want to try again to get transcripts for videos
                # even where a transcript has previously not been found on the RV website.
                dict_function_arguments['try_to_fetch_even_if_tagged_as_missing'] = True
            logging.info(my_globals.str_logging_func_next + get_all_vid_transcripts.__name__)
            get_all_vid_transcripts(sesh, variable_manager, **dict_function_arguments)
            logging.info(my_globals.str_logging_func_exited + get_all_vid_transcripts.__name__)
        end_time = time.time()
        logging.info('Time it took to update Video transcripts: '
                     + "{:.2f}".format(end_time - start_time) + ' seconds.')
        # now we update data related to the transcripts, such as term-count and pseudo-transcripts
        if not should_continue_execution():
            raise_exception_due_to_external_stop()
        # first, update the term-count
        start_time = time.time()
        path_transcripts_ds = my_globals.str_dir4_vid_transcripts_ds
        path_transcript_data = my_globals.str_dir4_vid_transcripts_data
        path_tfidf_data = my_globals.str_dir4_tfidf_data
        num_vids = -1
        if int_level_of_thoroughness == 1:  # for low thoroughness
            num_vids = num_vids_to_process_transcripts_level1
        ta = TranscriptAnalysis(path_tfidf_data, path_transcripts_ds, path_transcript_data,
                                variable_manager, num_vids_to_use=num_vids)
        logging.debug(my_globals.str_logging_func_next + ta.update_term_count_dataframes.__name__)
        # this is a very time-expensive call if re-building from scratch, but quick if only new videos have been added
        ta.update_term_count_dataframes()
        logging.debug(my_globals.str_logging_func_exited + ta.update_term_count_dataframes.__name__)
        end_time = time.time()
        logging.debug('Time it took to update term-count data: '
                      + "{:.2f}".format(end_time - start_time) + ' seconds.')
        # next, update pseudo-transcript data
        start_time = time.time()
        path_web_vid_ds = my_globals.str_dir4_website_vids_ds
        path_unwanted_terms = my_globals.str_fullfilepath_pseudotranscript_unwanted_terms
        logging.debug(my_globals.str_logging_func_next + ta.update_pseudotranscript_files.__name__)
        # this is a time-expensive call if re-building from scratch, but quick if only new videos have been added
        ta.update_pseudotranscript_files(path_web_vid_ds,
                                         keep_terms_that_appear_more_than=2,
                                         force_update=False,
                                         fullpath_file_with_terms_to_not_include=path_unwanted_terms)
        logging.debug(my_globals.str_logging_func_exited + ta.update_pseudotranscript_files.__name__)
        end_time = time.time()
        logging.debug('Time it took to update pseudo-transcript data: '
                      + "{:.2f}".format(end_time - start_time) + ' seconds.')
        logging.debug('Total time it took to update all transcript data (pull, plus generate local metadata): '
                      + "{:.2f}".format(end_time - start_time_0) + ' seconds.')

        # make sure execution of the program hasn't been told to stop by another module
        if not should_continue_execution():
            raise_exception_due_to_external_stop()
        # next, update reports transcripts. At the time of this writing, we are only updating
        # publications during fairly thorough runs, hence the IF statement below
        # that encompasses the rest of the code related to updating the publications.
        if int_level_of_thoroughness >= 2:
            start_time_0 = time.time()
            start_time = start_time_0
            num_pubs = -1
            if rv_web_auth.logged_in:
                logging.info(my_globals.str_logging_func_next + get_all_publication_fulltexts.__name__)
                get_all_publication_fulltexts(sesh, variable_manager, max_pubs_to_process=num_pubs,
                                              trial_run=trial_runs)
                logging.info(my_globals.str_logging_func_exited + get_all_publication_fulltexts.__name__)
            end_time = time.time()
            logging.info('Time it took to update Publication full-texts: '
                         + "{:.2f}".format(end_time - start_time) + ' seconds.')
            # now we update data related to the full-texts, such as term-count and pseudo-fulltext
            if not should_continue_execution():
                raise_exception_due_to_external_stop()
            # first, update the term-count
            start_time = time.time()
            path_pubfulltexts_ds = my_globals.str_dir4_pubs_fulltext_ds
            path_pubfulltexts_data = my_globals.str_dir4_pubs_fulltext_data
            path_tfidf_data_pubs = my_globals.str_dir4_tfidf_pubs_data
            num_pubs = -1
            ta = TranscriptAnalysis(path_tfidf_data_pubs, path_pubfulltexts_ds, path_pubfulltexts_data,
                                    variable_manager, num_vids_to_use=num_pubs)
            logging.debug(my_globals.str_logging_func_next + ta.update_term_count_dataframes.__name__)
            # this is a very time-expensive call if re-building from scratch,
            # but quick if only new videos have been added
            ta.update_term_count_dataframes()
            logging.debug(my_globals.str_logging_func_exited + ta.update_term_count_dataframes.__name__)
            end_time = time.time()
            logging.debug('Time it took to update term-count data: '
                          + "{:.2f}".format(end_time - start_time) + ' seconds.')
            # next, update pseudo-fulltext data
            start_time = time.time()
            logging.debug(my_globals.str_logging_func_next + ta.update_pub_pseudotext_files.__name__)
            # note, we are re-using the functionality that was coded for use with video transcript
            # files, hence the 'transcript' name of the class being used.
            ta.update_pub_pseudotext_files(keep_terms_that_appear_more_than=2,
                                           fullpath_file_with_terms_to_not_include=path_unwanted_terms,
                                           force_update=False)
            logging.debug(my_globals.str_logging_func_exited + ta.update_pub_pseudotext_files.__name__)
            end_time = time.time()
            logging.debug('Time it took to update pseudo-fulltext data: '
                          + "{:.2f}".format(end_time - start_time) + ' seconds.')
            logging.debug(
                'Total time it took to update all reports fulltext data (pull, plus generate local metadata): '
                + "{:.2f}".format(end_time - start_time_0) + ' seconds.')

        # --------------- PART 2: AIRTABLE RELATED TASKS ---------------
        logging.info('-->>>>>   STARTING AIRTABLE RELATED TASKS   <<<<<<--')
        # make sure execution of the program hasn't been told to stop by another module
        if not should_continue_execution():
            raise_exception_due_to_external_stop()
        # next we update guest info
        start_time_0 = time.time()
        # first refresh guest data from Airtable
        logging.info('Updating guests and topics.')
        start_time = time.time()
        logging.debug(my_globals.str_logging_func_next + get_airt_guests_ordered_by_name.__name__)
        dict_guests_by_name = get_airt_guests_ordered_by_name(fresh_airt_pull=True)
        logging.debug(my_globals.str_logging_func_exited + get_airt_guests_ordered_by_name.__name__)
        end_time = time.time()
        logging.debug('Time it took to pull (from Airtable) guests ordered by name: '
                      + "{:.2f}".format(end_time - start_time) + ' seconds.')
        # next, recreate the relationship between guests and topics on local disk
        start_time = time.time()
        logging.debug(my_globals.str_logging_func_next + extract_guestsNsubjects.__name__)
        extract_guestsNsubjects(my_globals.odd_strings_in_website_persons_fields)
        logging.debug(my_globals.str_logging_func_exited + extract_guestsNsubjects.__name__)
        end_time = time.time()
        logging.debug('Time it took to re-build guest and topics relationships: '
                      + "{:.2f}".format(end_time - start_time) + ' seconds.')
        # next, recreate the list of interviewers.
        logging.debug(my_globals.str_logging_func_next + extract_interviewers.__name__)
        extract_interviewers(my_globals.odd_strings_in_website_persons_fields)
        logging.debug(my_globals.str_logging_func_exited + extract_interviewers.__name__)
        end_time = time.time()
        logging.debug('Time it took to recreate list of interviewers: '
                      + "{:.2f}".format(end_time - start_time) + ' seconds.')
        # next, report persons found on the website, but missing in airtable
        start_time = time.time()
        logging.debug(my_globals.str_logging_func_next + find_website_ppl_not_in_airtable.__name__)
        find_website_ppl_not_in_airtable(do_fresh_pull_of_airtable=False)
        logging.debug(my_globals.str_logging_func_exited + find_website_ppl_not_in_airtable.__name__)
        end_time = time.time()
        logging.debug('Time it took to find missing persons: '
                      + "{:.2f}".format(end_time - start_time) + ' seconds.')
        # push the guests and topics to airtable
        start_time = time.time()
        logging.debug(my_globals.str_logging_func_next + push_guestsNsubjects_delta.__name__)
        push_guestsNsubjects_delta(dict_guests_by_name, trial_run=trial_runs)
        logging.debug(my_globals.str_logging_func_exited + push_guestsNsubjects_delta.__name__)
        end_time = time.time()
        logging.debug('Time it took to push updated guest and topics to airtable: '
                      + "{:.2f}".format(end_time - start_time) + ' seconds.')
        logging.info('TOTAL time it took to pull guest/topics info from RV website and push to Airtable: '
                     + "{:.2f}".format(end_time - start_time_0) + ' seconds.')

        # make sure execution of the program hasn't been told to stop by another module
        if not should_continue_execution():
            raise_exception_due_to_external_stop()
        # next we pull a fresh set of Shows data from Airtable.
        # this isn't an expensive operation, so we do it always (we don't bother filtering
        # by int_level_of_thoroughness.)
        start_time = time.time()
        logging.info(my_globals.str_logging_func_next + get_airt_shows_ordered_by_rvwebname.__name__)
        dict_airtable_shows_by_rvwebname = get_airt_shows_ordered_by_rvwebname(fresh_airt_pull=True)
        logging.info(my_globals.str_logging_func_exited + get_airt_shows_ordered_by_rvwebname.__name__)
        end_time = time.time()
        logging.info('Time it took to pull fresh Shows info from airtable: '
                     + "{:.2f}".format(end_time - start_time) + ' seconds.')

        # make sure execution of the program hasn't been told to stop by another module
        if not should_continue_execution():
            raise_exception_due_to_external_stop()
        # next we pull a fresh copy of necessary vid info from Airtable. (At the time of
        # this writing, the necessary info is small - I think just the RV website video ID
        # as it is stored in Airtable, so we can match between website videos and airtable rows.
        # We need to have updated video information every time, because subsequent
        # functions need this info to be completely up-to-date, so we do it always
        # (we don't bother filtering by int_level_of_thoroughness.)
        start_time = time.time()
        logging.info(my_globals.str_logging_func_next + get_airt_vids_ordered_by_rvwebid.__name__)
        dict_airt_vids_by_rvwebid = get_airt_vids_ordered_by_rvwebid(fresh_airt_pull=True)
        logging.info(my_globals.str_logging_func_exited + get_airt_vids_ordered_by_rvwebid.__name__)
        end_time = time.time()
        logging.info('Time it took to pull fresh Videos info from airtable: '
                     + "{:.2f}".format(end_time - start_time) + ' seconds.')

        # make sure execution of the program hasn't been told to stop by another module
        if not should_continue_execution():
            raise_exception_due_to_external_stop()
        start_time = time.time()
        # next we convert website JSON data into JSON records in a format expected by
        # airtable. In other words, we prep the info to be pushed to Airtable.
        # do prepare to do this, we call a function that loads information about SHOWS
        # from the RV website into a dict
        dict_shows_website = extractFieldsFromShowsData()
        # call a function that translates all the videos information we have
        # in JSON files on disk, from the website JSON format, into the
        # format expected by the airtable python wrapper (fields to insert
        # Must be dictionary with Column names as Key.)
        logging.info(my_globals.str_logging_func_next + convertWebsiteJSONtoAirtableFormat.__name__)
        convertWebsiteJSONtoAirtableFormat(dict_shows_website, dict_airtable_shows_by_rvwebname, trial_runs)
        logging.info(my_globals.str_logging_func_exited + convertWebsiteJSONtoAirtableFormat.__name__)
        end_time = time.time()
        logging.info('Time it took to convert records from website JSON to Airtable JSON: '
                     + "{:.2f}".format(end_time - start_time) + ' seconds.')

        # make sure execution of the program hasn't been told to stop by another module
        if not should_continue_execution():
            raise_exception_due_to_external_stop()
        start_time = time.time()
        # next we push the converted records to airtable
        logging.info(my_globals.str_logging_func_next + push_vids.__name__)
        push_vids('Videos', dict_airt_vids_by_rvwebid, trial_runs)
        logging.info(my_globals.str_logging_func_exited + push_vids.__name__)
        end_time = time.time()
        logging.info('Time it took to push video records to Airtable: '
                     + "{:.2f}".format(end_time - start_time) + ' seconds.')

        # --------------- PART 3: ALGOLIA RELATED TASKS ---------------
        logging.info('-->>>>>   STARTING ALGOLIA RELATED TASKS   <<<<<<--')
        # make sure execution of the program hasn't been told to stop by another module
        if not should_continue_execution():
            raise_exception_due_to_external_stop()
        start_time = time.time()
        # next we call a function that updates the SimpleDSs that contain Algolia
        # records, and that also pushes the Algolia records to the interwebs
        # for a group of indexes
        dict_of_indexes = my_globals.dict_group_of_indexes_to_update_and_push_frequently.copy()
        # at the time of this writing, the IF below makes it so that if the level of thoroughness
        # is low, only indexes included in the 'update frequently' dictionary are included, but
        # for higher levels of thoroughness, the non-frequent indexes are added as well.
        if int_level_of_thoroughness >= 2:
            dict_of_indexes.update(my_globals.dict_group_of_indexes_to_update_and_push_less_frequently)
        logging.info(my_globals.str_logging_func_next + update_and_push_multiple_indexes.__name__)
        update_and_push_multiple_indexes(dict_of_indexes,
                                         my_globals.dict_algolia_fields_to_allow_to_push,
                                         variable_manager,
                                         trial_run=trial_runs)
        logging.info(my_globals.str_logging_func_exited + update_and_push_multiple_indexes.__name__)
        end_time = time.time()
        logging.info('Time it took to update Algolia SimpleDSs AND push Algolia records for multiple indexes: '
                     + "{:.2f}".format(end_time - start_time) + ' seconds.')

        # --------------- OUR JOB HERE IS DONE - JUST WRAPPING THINGS UP BELOW ----------------

    except Exception as e:
        logging.warning('Something went wrong during an automated/scheduled run of pulling'
                        ' info from RV website and pushing to airtable. The Exception was: ' + repr(e))
        # In the job above, the variable that allows for execution to continue may have
        # been set to false due to some errors (generally caused, for example, by network isues.)
        # This is useful for the current job to stop, but we don't want to halt all future jobs from
        # trying to run as well, so we set the variable to true again.
        if not should_continue_execution():
            logging.info('Re-setting the variable that allows execution of jobs to continue (to True) so that'
                         ' future jobs can try again.')
            variable_manager.var_set(my_globals.str_execution_may_go_on, True)

    # logout of RV website
    logging.info('Logging out of RV Website')
    start_time = time.time()
    rv_web_auth.logout_rv_website()
    end_time = time.time()
    logging.info('Time it took to logout of RV Website: '
                 + "{:.2f}".format(end_time - start_time) + ' seconds.')

    end_time_overall = time.time()
    logging.info("Overall time it took to run the whole function 'move_vid_data_fromRV_toAT' was: "
                 + "{:.2f}".format((end_time_overall - start_time_overall) / 60) + ' minutes.')
    logging.info('---------------------------------------------')
    logging.info("EXITING function 'move_vid_data_fromRV_toAT'")
    logging.info('---------------------------------------------')
