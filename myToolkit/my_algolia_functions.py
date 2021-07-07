import logging
from datetime import datetime
from algoliasearch.search_client import SearchClient
import my_globals
import my_config
from class_simpleDS import SimpleDS
from class_rv_website_json_vid import RVwebsiteVid
from class_rv_website_json_pub import RVwebsitePublication
from class_myalgolia_unit import AlgoliaDataUnit
from class_percent_tracker import PercentTracker
from my_airtable_functions import convert_ppl_names2another_airtable_field
from my_rv_website_functions import extractIDstringFromURLstring, extractFieldsFromShowsData
from my_building_blocks import make_now_timestamp, make_sha256_hash
from class_trancript import Transcript


def push_simple_list_of_records_to_algolia(var_manager, str_index_name,
                                           list_records_to_push, trial_run=False):
    """This function loops through the set of records passed as a list, and pushes
    them to the Algolia index also passed as a parameter.
    The function returns the result """

    # setup the Algolia API
    algolia_client = SearchClient.create(my_config.algolia_app_id, my_config.algolia_admin_api_key)
    algolia_index = algolia_client.init_index(str_index_name)

    int_records_pushed = 0
    int_records_push_issues = 0
    int_records_processed = 0

    loop_iterations = len(list_records_to_push)
    percent_tracker = PercentTracker(loop_iterations, int_output_every_x_percent=10, log_level='info')
    for a_record in list_records_to_push:
        # The following IF checks the variable manager that was passed as a paramater
        # to this function. It allows for the loop to be stopped by an external factor
        # (script, human, etc.) if a specific variable has been set to false.
        execution_should_continue = var_manager.var_retrieve(my_globals.str_execution_may_go_on)
        if (not execution_should_continue):
            break

        if not trial_run:
            try:
                # push the record
                algolia_index.partial_update_object(a_record, {'createIfNotExists': True})
                int_records_pushed += 1
            except Exception as e:
                logging.warning('Exception thrown while trying to push a record to Algolia.'
                                ' The Exception was: ' + repr(e))
                int_records_push_issues += 1

        int_records_processed += 1
        percent_tracker.update_progress(int_records_processed,
                                        show_time_remaining_estimate=True,
                                        str_description_to_include_in_logging='Pushing simple records to Algolia')

    logging.info('----- SUMMARY of Pushing records to Algolia SimpleDS ------')
    if trial_run:
        logging.info('ONLY A TRIAL RUN')
    logging.info('Videos processed: ' + str(int_records_processed))
    logging.info('Videos pushed: ' + str(int_records_pushed))
    logging.info('Videos (tried to push) did not succeed: ' + str(int_records_push_issues))


# ------------------------ END FUNCTION ------------------------ #

def update_local_algolia_video_records(path_to_algolia_simpleds, var_manager,  # noqa: C901
                                       display_name_for_logging_of_simpleds='',
                                       trial_run=False):
    """This function loops through the set of videos pulled from the Real Vision
    website (stored in an instance of SimpleDS) and adds/updates associated records
    in the algolia instance of SimpleDS."""

    int_vids_added = 0
    int_vids_updated = 0
    int_vids_tagged_for_deletion = 0
    int_vids_untouched = 0
    int_vids_processed = 0

    # load the instance of SimpleDS that contains video data from the RV website.
    # looping through this instance will be the driving force of this function.
    # by default when the 'sort' method is called, the datastructure gets sorted
    # by the date of publication to the website, with the most recent being examined
    # first in the loop.
    web_vids_ds = SimpleDS(my_globals.str_dir4_website_vids_ds, my_globals.str_name_simpleds_website_vids)
    web_vids_ds.load()
    web_vids_ds.sort()

    # load the instance of SimpleDS that stores algolia-related data
    algolia_ds = SimpleDS(path_to_algolia_simpleds, display_name_for_logging_of_simpleds)
    algolia_ds.load()
    algolia_ds.sort()

    # load the instance of SimpleDS that contains 'other info' about
    # videos, such as info about comments
    other_info_vids_ds = SimpleDS(my_globals.str_dir4_additional_vids_info_ds,
                                  my_globals.str_name_simpleds_additionalinfo_vids)
    other_info_vids_ds.load()
    other_info_vids_ds.sort()

    # load the instance of SimpleDS that contains info about
    # video transcripts
    transcripts_ds = SimpleDS(my_globals.str_dir4_vid_transcripts_ds,
                              my_globals.str_name_simpleds_transcripts)
    transcripts_ds.load()
    transcripts_ds.sort()

    # for convenience I'll use the same 'updated timestamp' for all records
    # to be updated or created.
    update_timestamp = make_now_timestamp()

    # load a dictionary to be used for converting a website SHOW
    # from its ID to its name
    dict_shows_from_website = extractFieldsFromShowsData()

    loop_iterations = len(web_vids_ds)
    percent_tracker = PercentTracker(loop_iterations, int_output_every_x_percent=5, log_level='info')
    for webvid_id in web_vids_ds:
        # The following IF checks the variable manager that was passed as a paramater
        # to this function. It allows for the loop to be stopped by an external factor
        # (script, human, etc.) if a specific variable has been set to false.
        execution_should_continue = var_manager.var_retrieve(my_globals.str_execution_may_go_on)
        if not execution_should_continue:
            break

        dct_for_updating_algolia_unit = {}
        # we're not going to do any detection method to see if things have changed
        # upstream. We are simply going to update all records always.
        rv_web_vid = RVwebsiteVid(web_vids_ds.fetch_data(webvid_id))
        algolia_unit = AlgoliaDataUnit()

        # first we populate the dictionary with values that are very straightforward to retrieve.
        # populate the objectID. Algolia requires this field, and if not provided, it
        # autogenerates it. It makes sense to use the same video ID as from the RV website.
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_id] = rv_web_vid.str_id
        # populate the Title field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_title] = rv_web_vid.str_title
        # populate the Type field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_type] = rv_web_vid.str_type
        # populate the Description field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_description] = rv_web_vid.str_description
        # populate the Thumbnail URL field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_thumbnail] = rv_web_vid.str_url_thumbnail
        # populate the Likes field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_likes] = rv_web_vid.int_likes_count
        # populate the Dislikes field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_dislikes] = rv_web_vid.int_dislikes_count
        # populate the Published On field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_publishedon] = rv_web_vid.int_published_on
        # populate the Duration field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_duration] = rv_web_vid.int_duration
        # populate the Product field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_productid] = rv_web_vid.str_product_id

        # next we populate values that are less straightforward to retrieve

        # populate the Published On field in human read-able format
        formatted_date_publishedon = (datetime.fromtimestamp(rv_web_vid.int_published_on / 1000)).strftime("%Y-%m-%d")
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_publishedon_readable] = formatted_date_publishedon

        # populate the Featuring field. This field needs to be converted from a string
        # to a list of names.
        # IMPORTANT NOTE. I'M RE-USING A FUNCTION FROM AIRTABLE FUNCTIONS HERE. So if
        # you ever stop using the Airtable functionality, it will still be required for
        # this part of the code, unless you change it.
        # we can easily access the names of the guests in the video,
        # but just as a string. We want to convert
        # them into a python list, and with some checks done like removing of pre-fixes.
        str_of_names = rv_web_vid.str_featuring_raw
        # we call a function that converts the names to whatever
        # field we request. Normally we would convert to the ID of the guest,
        # but in this case we convert to the simple name (no prefixes or suffixes)
        # as it exists in Airtable. The function
        # returns a dictionary with an entry for the people it found
        # and with a string for the people it didn't find.
        dictSearchedAndConverted = \
            convert_ppl_names2another_airtable_field(
                str_of_names, my_globals.idx_fields_airT_tbl_guests_name)
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_featuring] = \
            dictSearchedAndConverted[my_globals.str_to_use_if_something_has_been_found]

        # populate the interviewers field
        str_of_interviewers = rv_web_vid.str_interviewer_raw
        # do the same as above for the featuring field, but now for the interviewers field
        dictSearchedAndConverted = \
            convert_ppl_names2another_airtable_field(
                str_of_interviewers, my_globals.idx_fields_airT_tbl_guests_name)
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_interviewer] = \
            dictSearchedAndConverted[my_globals.str_to_use_if_something_has_been_found]

        # populate the Human Tags field. This is a combination of fields
        # from the website comprising tags, topics, and assets
        # we convert to a set, and then back to a list in order to remove possible duplicates
        set_tags = set(rv_web_vid.lst_tag_names + rv_web_vid.lst_asset_names + rv_web_vid.lst_topic_names)
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_tags] = list(set_tags)

        # populate the Show field
        # first we need to extract the show ID from the URL that we get from the raw data
        website_show_id = extractIDstringFromURLstring(rv_web_vid.str_url_show)
        # we now need use a dictionary of SHOWs (pre-loaded before the beginning of the loop)
        # to convert the ID to the name.
        show_name = ''
        if website_show_id:
            show_name = dict_shows_from_website[website_show_id][0]
            # the zero above is due to the fact that this function
            # the dictionary's keys are "show IDs",
            # and the corresponding data to each key is a list. In
            # the [0] position of the list is the show's name, which
            # is what we are looking for.
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_show] = show_name

        # populate the video url field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_vidurl] = \
            'https://www.realvision.com/tv/videos/id/' + webvid_id

        # populate the tier field
        product_vid_belongs_to = rv_web_vid.str_product_id
        rv_tier = my_globals.dict_product_mapping_to_tiers[product_vid_belongs_to]
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_tiers] = rv_tier

        # populate the number of comments field
        # First we check if the video currently being processed already
        # exists in the datastructure that store 'other video info'. If it doesn't, we simply
        # don't populate this field
        if webvid_id in other_info_vids_ds:
            dict_other_info = other_info_vids_ds.fetch_data(webvid_id)
            if my_globals.str_vid_comments in dict_other_info:
                dict_comments_info = dict_other_info[my_globals.str_vid_comments]
                dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_numcomments] = \
                    dict_comments_info[my_globals.str_vid_comments_num_total]

        # both of the following fields depend on the video existing in the
        # SimpleDS that tracks transcript metadata, so we only check once if
        # the video is in that SimpleDS
        if webvid_id in transcripts_ds:
            # populate the transcript field
            # IMPORTANT NOTE. We do not store the whole transcript in the Algolia unit
            # the idea is that we will only push the transcript at PUSH time
            # in the Algolia unit, we will only store the hash of the transcript
            dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_transcript] = \
                transcripts_ds.fetch_hash(webvid_id)

            # populate the pseudo-transcript field
            # IMPORTANT NOTE. We do not store the whole pseudo-transcript in the Algolia unit
            # the idea is that we will only push the transcript at PUSH time
            # in the Algolia unit, we will only store the hash of the pseudo-transcript
            transcript = Transcript(webvid_id)
            transcript.set_transcript_directory(my_globals.str_dir4_vid_transcripts_data)
            transcript.load_transcript_object_from_dictionary(transcripts_ds.fetch_data(webvid_id))
            pseudo_transcript_exists = transcript.is_pseudotranscript_filename_populated()
            if pseudo_transcript_exists:
                pt = transcript.get_pseudotranscript_from_disk()
                if pt:
                    dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_pseudotranscript] = \
                        make_sha256_hash(pt, sort_characters=False)

        # now we check if this video data exists yet in the Algolia SimpleDS
        # and depending on that, we add it, or we update it.
        vid_publishedon = rv_web_vid.int_published_on
        hash_incoming_data = make_sha256_hash(dct_for_updating_algolia_unit, sort_characters=True)
        if webvid_id not in algolia_ds:
            # The simple case is if it doesn't exist yet. We add it.
            algolia_unit.create_unit(dct_for_updating_algolia_unit)
            # we'll make a hash of the fresh data gathered above into a dictionary, and
            # use that for the hash stored in SimpleDS
            if not trial_run:
                dct_algolia_unit = algolia_unit.dump_algolia_unit_as_dict()
                algolia_ds.add_entry(webvid_id, update_timestamp, vid_publishedon, dct_algolia_unit, hash_incoming_data)
            int_vids_added += 1
        else:
            # otherwise, the video is already in the Algolia SimpleDS and may need to be updated
            # first we compare the stored hash, to the incoming hash. If they match, no update is
            # needed.
            vid_updated = False
            if hash_incoming_data == algolia_ds.fetch_hash(webvid_id):
                logging.debug('Hashes match - no need to update')
            else:
                # if we are here, an update is in fact needed
                # so first we fetch the data already in the SimpleDS
                dict_existing_data = algolia_ds.fetch_data(webvid_id)
                algolia_unit.load_from_dict(dict_existing_data)
                # the function that updates the algolia unit returns a dictionary of
                # changes that we can use for the change log
                dct_changes = algolia_unit.provide_updated_data(dct_for_updating_algolia_unit)
                if dct_changes:
                    vid_updated = True
                    if not trial_run:
                        dct_algolia_unit = algolia_unit.dump_algolia_unit_as_dict()
                        algolia_ds.update_entry(webvid_id, dct_algolia_unit, update_timestamp,
                                                vid_publishedon, hash_incoming_data, dct_changes,
                                                log_changes=True)
                else:
                    logging.debug('No changes detected by the update function of the Algolia unit class.')

            if vid_updated:
                int_vids_updated += 1
            else:
                int_vids_untouched += 1

        int_vids_processed += 1
        percent_tracker.update_progress(int_vids_processed,
                                        str_description_to_include_in_logging='Updating local Algolia records')

    logging.info('Checking if any videos need to be deleted.')
    # now we figure out if any videos need to be deleted. However, this case
    # for deletion is a bit different from many of the other workflows/functions.
    # If we delete the records in the SimpleDS now, the next downstream function,
    # which pushes records to Algolia, will not know which records it has to
    # delete in the Algolia index on the interwebs. Therefore, here we only
    # TAG a record for deletion. Then, once the next function has removed it
    # from Algolia, that function should also take care of deleting it from
    # SimpleDS.
    set_vids_web_ds = web_vids_ds.fetch_all_ids_as_python_set()
    set_vids_algolia_ds = algolia_ds.fetch_all_ids_as_python_set()
    set_vids_removed_at_source = set_vids_algolia_ds - set_vids_web_ds
    for entry in set_vids_removed_at_source:
        if not trial_run:
            algolia_ds.tag_add(entry, my_globals.str_tag_delete_from_algolia)
        int_vids_tagged_for_deletion += 1

    if not trial_run:
        algolia_ds.save2disk()

    logging.info('----- SUMMARY of Updating Algolia SimpleDS ------')
    if trial_run:
        logging.info('ONLY A TRIAL RUN')
    logging.info('Videos processed: ' + str(int_vids_processed))
    logging.info('Videos added: ' + str(int_vids_added))
    logging.info('Videos updated: ' + str(int_vids_updated))
    logging.info('Videos tagged for deletion: ' + str(int_vids_tagged_for_deletion))
    logging.info('Videos not touched: ' + str(int_vids_untouched))
    # ------------------------ END FUNCTION ------------------------ #


def update_local_algolia_publication_records(path_to_algolia_simpleds, var_manager,  # noqa: C901
                                             display_name_for_logging_of_simpleds='', trial_run=False):
    """This function loops through the set of publications pulled from the Real Vision
    website (stored in an instance of SimpleDS) and adds/updates associated records
    in the algolia instance of SimpleDS."""

    int_pubs_added = 0
    int_pubs_updated = 0
    int_pubs_tagged_for_deletion = 0
    int_pubs_untouched = 0
    int_pubs_processed = 0

    # load the instance of SimpleDS that contains publication data from the RV website.
    # looping through this instance will be the driving force of this function.
    # by default when the 'sort' method is called, the datastructure gets sorted
    # by the date of publication to the website, with the most recent being examined
    # first in the loop.
    web_pubs_ds = SimpleDS(my_globals.str_dir4_website_pubs_ds, my_globals.str_name_simpleds_website_pubs)
    web_pubs_ds.load()
    web_pubs_ds.sort()

    # load the instance of SimpleDS that stores algolia-related data
    algolia_ds = SimpleDS(path_to_algolia_simpleds, display_name_for_logging_of_simpleds)
    algolia_ds.load()
    algolia_ds.sort()

    # load the instance of SimpleDS that contains info about
    # publication fulltexts
    pubs_texts_ds = SimpleDS(my_globals.str_dir4_pubs_fulltext_ds,
                             my_globals.str_name_simpleds_pubsfulltext)
    pubs_texts_ds.load()
    pubs_texts_ds.sort()

    # for convenience I'll use the same 'updated timestamp' for all records
    # to be updated or created.
    update_timestamp = make_now_timestamp()

    loop_iterations = len(web_pubs_ds)
    percent_tracker = PercentTracker(loop_iterations, int_output_every_x_percent=5, log_level='info')
    for web_pub_id in web_pubs_ds:
        # The following IF checks the variable manager that was passed as a paramater
        # to this function. It allows for the loop to be stopped by an external factor
        # (script, human, etc.) if a specific variable has been set to false.
        execution_should_continue = var_manager.var_retrieve(my_globals.str_execution_may_go_on)
        if not execution_should_continue:
            break

        dct_for_updating_algolia_unit = {}
        # we're not going to do any detection method to see if things have changed
        # upstream. We are simply going to update all records always.
        rv_web_pub_obj = RVwebsitePublication(web_pubs_ds.fetch_data(web_pub_id))
        algolia_unit = AlgoliaDataUnit()

        # first we populate the dictionary with values that are very straightforward to retrieve.

        # populate the objectID. Algolia requires this field, and if not provided, it
        # autogenerates it. It makes sense to use the same publication ID as from the RV website.
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_id] = rv_web_pub_obj.str_id
        # populate the Title field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_title] = rv_web_pub_obj.str_title
        # populate the Type field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_type] = rv_web_pub_obj.str_type
        # populate the Description field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_description] = rv_web_pub_obj.str_summary
        # populate the Thumbnail URL field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_thumbnail] = rv_web_pub_obj.str_url_thumbnail
        # populate the Likes field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_likes] = rv_web_pub_obj.int_likes_count
        # populate the Dislikes field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_dislikes] = rv_web_pub_obj.int_dislikes_count
        # populate the Dislikes field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_numpages] = rv_web_pub_obj.int_page_count
        # populate the Published On field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_publishedon] = rv_web_pub_obj.int_published_on
        # populate the ProductID
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_productid] = rv_web_pub_obj.str_product_id

        # next we populate values that are less straightforward to retrieve

        # populate the Published On field in human read-able format
        formatted_date_publishedon = (datetime.fromtimestamp(rv_web_pub_obj.int_published_on / 1000)).strftime(
            "%Y-%m-%d")
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_publishedon_readable] = formatted_date_publishedon

        # populate the Human Tags field. This is a combination of fields
        # from the website comprising tags, topics, and assets
        lst_tags = rv_web_pub_obj.lst_asset_names + rv_web_pub_obj.lst_topic_names
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_tags] = lst_tags

        # populate the publication url field
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_vidurl] = \
            'https://www.realvision.com/issues/id/' + web_pub_id

        # populate the tier field
        product_pub_belongs_to = rv_web_pub_obj.str_product_id
        rv_tier = my_globals.dict_product_mapping_to_tiers[product_pub_belongs_to]
        dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_tiers] = rv_tier

        # both of the following fields depend on the publication existing in the
        # SimpleDS that tracks fulltext metadata, so we only check once if
        # the publication is in that SimpleDS
        if web_pub_id in pubs_texts_ds:
            # populate the fulltext field
            # IMPORTANT NOTE. We do not store the whole fulltext in the Algolia unit
            # the idea is that we will only push the fulltext at PUSH time.
            # in the Algolia unit, we will only store the hash of the fulltext
            dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_transcript] = \
                pubs_texts_ds.fetch_hash(web_pub_id)

            # populate the pseudo-text field
            # IMPORTANT NOTE. We do not store the whole pseudo-text in the Algolia unit
            # the idea is that we will only push the fulltext at PUSH time
            # in the Algolia unit, we will only store the hash of the pseudo-text
            fulltext = Transcript(web_pub_id)
            fulltext.set_transcript_directory(my_globals.str_dir4_pubs_fulltext_data)
            fulltext.load_transcript_object_from_dictionary(pubs_texts_ds.fetch_data(web_pub_id))
            pseudo_fulltext_exists = fulltext.is_pseudotranscript_filename_populated()
            if pseudo_fulltext_exists:
                pt = fulltext.get_pseudotranscript_from_disk()
                if pt:
                    dct_for_updating_algolia_unit[AlgoliaDataUnit.fieldname_pseudotranscript] = \
                        make_sha256_hash(pt, sort_characters=False)

        # now we check if this publication data exists yet in the Algolia SimpleDS
        # and depending on that, we add it, or we update it.
        pub_publishedon = rv_web_pub_obj.int_published_on
        hash_incoming_data = make_sha256_hash(dct_for_updating_algolia_unit, sort_characters=True)
        if web_pub_id not in algolia_ds:
            # The simple case is if it doesn't exist yet. We add it.
            algolia_unit.create_unit(dct_for_updating_algolia_unit)
            # we'll make a hash of the fresh data gathered above into a dictionary, and
            # use that for the hash stored in SimpleDS
            if not trial_run:
                dct_algolia_unit = algolia_unit.dump_algolia_unit_as_dict()
                algolia_ds.add_entry(web_pub_id, update_timestamp, pub_publishedon, dct_algolia_unit,
                                     hash_incoming_data)
            int_pubs_added += 1
        else:
            # otherwise, the publication is already in the Algolia SimpleDS and may need to be updated
            # first we compare the stored hash, to the incoming hash. If they match, no update is
            # needed.
            pub_updated = False
            if hash_incoming_data == algolia_ds.fetch_hash(web_pub_id):
                logging.debug('Hashes match - no need to update')
            else:
                # if we are here, an update is in fact needed
                # so first we fetch the data already in the SimpleDS
                dict_existing_data = algolia_ds.fetch_data(web_pub_id)
                algolia_unit.load_from_dict(dict_existing_data)
                # the function that updates the algolia unit returns a dictionary of
                # changes that we can use for the change log
                dct_changes = algolia_unit.provide_updated_data(dct_for_updating_algolia_unit)
                if dct_changes:
                    pub_updated = True
                    if not trial_run:
                        dct_algolia_unit = algolia_unit.dump_algolia_unit_as_dict()
                        algolia_ds.update_entry(web_pub_id, dct_algolia_unit, update_timestamp,
                                                pub_publishedon, hash_incoming_data, dct_changes,
                                                log_changes=True)
                else:
                    logging.debug('No changes detected by the update function of the Algolia unit class.')

            if pub_updated:
                int_pubs_updated += 1
            else:
                int_pubs_untouched += 1

        int_pubs_processed += 1
        percent_tracker.update_progress(int_pubs_processed,
                                        str_description_to_include_in_logging='Updating local Algolia records')

    logging.info('Checking if any videos need to be deleted.')
    # now we figure out if any publications need to be deleted. However, this case
    # for deletion is a bit different from many of the other workflows/functions.
    # If we delete the records in the SimpleDS now, the next downstream function,
    # which pushes records to Algolia, will not know which records it has to
    # delete in the Algolia index on the interwebs. Therefore, here we only
    # TAG a record for deletion. Then, once the next function has removed it
    # from Algolia, that function should also take care of deleting it from
    # SimpleDS.
    set_pubs_web_ds = web_pubs_ds.fetch_all_ids_as_python_set()
    set_pubs_algolia_ds = algolia_ds.fetch_all_ids_as_python_set()
    set_pubs_removed_at_source = set_pubs_algolia_ds - set_pubs_web_ds
    for entry in set_pubs_removed_at_source:
        if not trial_run:
            algolia_ds.tag_add(entry, my_globals.str_tag_delete_from_algolia)
        int_pubs_tagged_for_deletion += 1

    if not trial_run:
        algolia_ds.save2disk()

    logging.info('----- SUMMARY of Updating Algolia SimpleDS ------')
    if trial_run:
        logging.info('ONLY A TRIAL RUN')
    logging.info('publications processed: ' + str(int_pubs_processed))
    logging.info('publications added: ' + str(int_pubs_added))
    logging.info('publications tagged for deletion: ' + str(int_pubs_tagged_for_deletion))
    logging.info('publications updated: ' + str(int_pubs_updated))
    logging.info('publications not touched: ' + str(int_pubs_untouched))
    # ------------------------ END FUNCTION ------------------------ #


def push_records_to_algolia(var_manager, str_index_name, dict_index_and_fields_to_push,  # noqa: C901
                            path_to_algolia_simpleds, path_to_transcripts_simpleds,
                            path_to_transcripts_data, trial_run=False):
    """This function loops through the set of records in the Algolia instance
    of SimpleDS and creates 'delta' records to be pushed where there have
    been changes since the last push.
    A variable manager is passed to the function, in case the caller wants
    to stop execution gracefully (this is important so that the function
    can exit the loop, but still save the SimpleDS.)"""

    # setup the Algolia API
    algolia_client = SearchClient.create(my_config.algolia_app_id, my_config.algolia_admin_api_key)
    algolia_index = algolia_client.init_index(str_index_name)

    int_records_pushed = 0
    int_records_too_large = 0
    int_records_push_issues = 0
    int_records_no_changes = 0
    int_records_deleted = 0
    int_records_processed = 0

    # load the instance of SimpleDS that stores algolia-related data
    algolia_ds = SimpleDS(path_to_algolia_simpleds)
    algolia_ds.load()
    algolia_ds.sort()

    # load the instance of SimpleDS that stores transcripts metadata
    transcripts_ds = SimpleDS(path_to_transcripts_simpleds)
    transcripts_ds.load()
    transcripts_ds.sort()

    list_fields_to_push = dict_index_and_fields_to_push[str_index_name]

    loop_iterations = len(algolia_ds)
    percent_tracker = PercentTracker(loop_iterations, int_output_every_x_percent=5, log_level='info')
    for record_id in algolia_ds:
        # The following IF checks the variable manager that was passed as a paramater
        # to this function. It allows for the loop to be stopped by an external factor
        # (script, human, etc.) if a specific variable has been set to false.
        execution_should_continue = var_manager.var_retrieve(my_globals.str_execution_may_go_on)
        if (not execution_should_continue):
            break

        # an upstream function tags records if they need to be deleted from Algolia,
        # and then from the SimpleDS. So we check for that here.
        record_marked_for_removal = algolia_ds.tag_check(record_id, my_globals.str_tag_delete_from_algolia)
        if not record_marked_for_removal:
            # we enter this part of the IF/ELSE, if the record is NOT marked for deletion,
            # so we proceed with the normal adding/updating tasks.

            algolia_unit = AlgoliaDataUnit()
            existing_algolia_unit_as_dict = algolia_ds.fetch_data(record_id)
            algolia_unit.load_from_dict(existing_algolia_unit_as_dict)
            algolia_record = algolia_unit.make_algolia_record_for_pushing_delta(transcripts_ds,
                                                                                path_to_transcripts_data,
                                                                                list_fields_to_push)
            if algolia_record:
                record_size_in_bytes = len(str(algolia_record).encode('utf-8'))
                if record_size_in_bytes > 100000:
                    int_records_too_large += 1
                    logging.warning('Will not attempt to push record for ID: ' + record_id + ' because it is larger'
                                    ' than 100,000 bytes (the Algolia limit per record)')
                else:
                    if not trial_run:
                        return_val = {}
                        try:
                            # push the record
                            return_val = algolia_index.partial_update_object(algolia_record,
                                                                             {'createIfNotExists': True})
                        except Exception as e:
                            logging.warning('Exception thrown while trying to push a record to Algolia.'
                                            ' The Exception was: ' + repr(e))
                            return_val = {}
                            int_records_push_issues += 1
                        # I'm not 100% sure on this, but I THINK if the 'return_val' gets populated
                        # above by the algolia 'partial_update' function, then it means the records
                        # were pushed successfully.
                        # If that is the case - if the record was pushed successfully, then we need
                        # to update our local version of the data to tell it what has been pushed.
                        if return_val:
                            int_records_pushed += 1
                            # Merging the record into the Algolia unit that we save is fairly
                            # straighforward, with the exceptions below.

                            trscrpt_fieldname = AlgoliaDataUnit.fieldname_transcript
                            # If the transcript was pushed, the record that was pushed contains
                            # the whole transcript, whereas in the unit we save only the hash. So
                            # we need to replace the transcript with the hash in the record.
                            # However, this only needs to happen if the transcript field was in the
                            # list of fields to push, so we check for that first.
                            if trscrpt_fieldname in list_fields_to_push:
                                # some records don't have a transcript. so the IF below is used to
                                # make sure we are only trying to replace the hash for records that had the
                                # transcript field populated.
                                if trscrpt_fieldname in algolia_record:
                                    # We don't re-calculate the hash, we simply grab it from the exiting unit
                                    algolia_record[trscrpt_fieldname] = \
                                        existing_algolia_unit_as_dict[trscrpt_fieldname][
                                            AlgoliaDataUnit.key_for_values_current]

                            pseudotrscrpt_fieldname = AlgoliaDataUnit.fieldname_pseudotranscript
                            # Similarly, if the pseudo-transcript was pushed, the record that was pushed contains
                            # the whole pseudo-transcript, whereas in the unit we save only the hash. So
                            # we need to replace the pseudo-transcript with the hash in the record.
                            # However, this only needs to happen if the pseudo-transcript field was in the
                            # list of fields to push, so we check for that first.
                            if pseudotrscrpt_fieldname in list_fields_to_push:
                                # some records don't have a pseudo-transcript. so the IF below is used to
                                # make sure we are only trying to replace the hash for records that had the
                                # pseudo-transcript field populated.
                                if pseudotrscrpt_fieldname in algolia_record:
                                    # We don't re-calculate the hash, we simply grab it from the exiting unit
                                    algolia_record[pseudotrscrpt_fieldname] = \
                                        existing_algolia_unit_as_dict[pseudotrscrpt_fieldname][
                                            AlgoliaDataUnit.key_for_values_current]

                            # now update the algolia unit with the record that was pushed
                            algolia_unit.provide_pushed2algolia_data(algolia_record)
                            # once the unit is updated, then it can be saved back to the SimpleDS in dictionary form
                            # (in the SimpleDS we are storing Algolia 'units' saved as dictionaries,
                            # rather than just an Algolia 'record' as it gets pushed to Algolia.)
                            # Note that the timestamp we are keeping in the SimpleDS is the time when
                            # new data has been updated inside a unit in the SimpleDS. We will not
                            # consider a push to Algolia something that updates the stored timestamp
                            # because even though the unit is being updated, it isn't on account of
                            # new information; it is simply being updated to 'remember' what was pushed.
                            # so we'll grab the existing timestamp and use it for the update (in other words
                            # no change to the timestamp.)
                            dct_representation_of_unit = algolia_unit.dump_algolia_unit_as_dict()
                            algolia_ds.update_entry(record_id, dct_representation_of_unit,
                                                    algolia_ds.fetch_lastupdated(record_id))
            else:
                int_records_no_changes += 1
        else:
            # otherwise, we are here, if the record was marked for deletion
            if not trial_run:
                record_removed = False
                try:
                    # delete the record
                    return_val = algolia_index.delete_object(record_id)
                    record_removed = True
                except Exception as e:
                    logging.warning('Exception thrown while trying to delete a record in Algolia.'
                                    ' The Exception was: ' + repr(e))
                if record_removed:
                    # we would normally remove the tag that marked the record for some action
                    # but in this case it would be pointless, since the action is to delete
                    # the record.
                    # so now that the record was been removed from the interwebs
                    # we delete the record from the SimpleDS
                    algolia_ds.delete_entry(record_id, keep_version_of_file_in_log_directory=True)
            int_records_deleted += 1

        int_records_processed += 1
        percent_tracker.update_progress(int_records_processed,
                                        show_time_remaining_estimate=True,
                                        str_description_to_include_in_logging='Pushing records to Algolia')

    if not trial_run:
        algolia_ds.save2disk()

    logging.info('----- SUMMARY of Pushing records to Algolia SimpleDS ------')
    if trial_run:
        logging.info('ONLY A TRIAL RUN')
    logging.info('Records processed: ' + str(int_records_processed))
    logging.info('Records pushed: ' + str(int_records_pushed))
    logging.info('Records too large to push: ' + str(int_records_too_large))
    logging.info('Records (tried to push) did not succeed: ' + str(int_records_push_issues))
    logging.info('Records removed (from Algolia and local SimpleDS): ' + str(int_records_deleted))
    logging.info('Records not touched: ' + str(int_records_no_changes))
    # ------------------------ END FUNCTION ------------------------ #


def update_and_push_multiple_indexes(dict_of_indexes_and_associated_variables,
                                     dict_of_fields_to_push_per_index, variable_manager,
                                     trial_run=False):
    for idx in dict_of_indexes_and_associated_variables:
        logging.info('****** UPDATING SimpleDS of Algolia units for index --> ' + idx)
        dict_of_variables = dict_of_indexes_and_associated_variables[idx]
        idx_type = dict_of_variables[my_globals.algvarname_type]
        path_algolia_ds = dict_of_variables[my_globals.algvarname_records_dspath]
        name_algolia_ds = dict_of_variables[my_globals.algvarname_records_dsname]
        path_text_ds = dict_of_variables[my_globals.algvarname_text_dspath]
        path_text_data = dict_of_variables[my_globals.algvarname_text_datapath]
        if idx_type == my_globals.str_algolia_record_type_vid:
            update_local_algolia_video_records(path_algolia_ds, variable_manager, name_algolia_ds, trial_run=trial_run)
        elif idx_type == my_globals.str_algolia_record_type_pub:
            update_local_algolia_publication_records(path_algolia_ds, variable_manager,
                                                     name_algolia_ds, trial_run=trial_run)
        logging.info('*** Finished updating SimpleDS of Algolia units for index --> ' + idx)
        # the function called below expects a dictionary where the index given is a key
        # and the value for that key is a list of fields that should be pushed to algolia
        logging.info('****** PUSHING TO INTERWEBS Algolia records for index --> ' + idx)
        push_records_to_algolia(variable_manager, idx, dict_of_fields_to_push_per_index,
                                path_algolia_ds, path_text_ds, path_text_data, trial_run=trial_run)
        logging.info('*** Finished pushing Algolia records for index --> ' + idx)
# ------------------------ END FUNCTION ------------------------ #
