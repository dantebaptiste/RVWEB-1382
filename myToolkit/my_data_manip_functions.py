import os
import json
import time
import glob
import logging
import pandas as pd
import my_globals
from my_building_blocks import projectStandardTimestamp
from my_building_blocks import recursiveExtractFieldFromHierarchy
from my_building_blocks import extractIndividualItemsFromTextList
from my_building_blocks import make_now_timestamp
from my_building_blocks import make_sha256_hash
from my_building_blocks import cleanup_older_files_in_a_dir
from class_simpleDS import SimpleDS
from my_airtable_functions import extractVideoFieldFromWebsiteJSON
from my_airtable_functions import convert_ppl_names2another_airtable_field
from class_percent_tracker import PercentTracker
from class_rv_website_json_vid import RVwebsiteVid
from class_rv_website_json_pub import RVwebsitePublication


def load_a_single_website_vid_JSON_info_from_disk(filename):
    """Use this function to load an individual video dict from
    its individual json file (as opposed to loading videos
    from their raw dump files where a set of videos is stored.)"""
    JSONfileToParse = \
        my_globals.str_dir4_raw_website_json_single_videos \
        + filename
    with open(JSONfileToParse, 'r') as theJSONfile:
        single_vid_data = json.load(theJSONfile)
    return single_vid_data


# ------------------------ END FUNCTION ------------------------ #


def load_a_single_airT_vid_JSON_info_from_disk(filename):
    """Use this function to load an individual video dict from
    its individual json file (as opposed to loading videos
    from their raw dump files where a set of videos is stored.)"""
    JSONfileToParse = \
        my_globals.str_dir4_manipd_JSON_website_vids_converted2airtable_records \
        + filename
    with open(JSONfileToParse, 'r') as theJSONfile:
        single_vid_data = json.load(theJSONfile)
    return single_vid_data


# ------------------------ END FUNCTION ------------------------ #


def extractVidAttribs(dictContainingVidMetadata, dictOfVidAttributes2return):
    # the dictionary containing each videos data has several
    # fields. Here we only want to extract data from the
    # attributes section, which in itself is another dictionary
    dictOfVidAttributes = dictContainingVidMetadata['attributes']
    listOfAttribsInThisVid = []
    for anAttribute in dictOfVidAttributes2return:
        theAttributeName = dictOfVidAttributes2return[anAttribute]
        theAttributeValue = ''
        # first we check if the attribute exists for
        # this particular vid
        if theAttributeName in dictOfVidAttributes:
            # if it exists, then we put it in the variable that will
            # be appended to the list of attributes for this vid
            theAttributeValue = dictOfVidAttributes[theAttributeName]
        # if the IF above didn't find it, then the empty
        # string that was initialized eaerlier is what gets
        # appended instead.
        listOfAttribsInThisVid.append(theAttributeValue)
    return listOfAttribsInThisVid


# ------------------------ END FUNCTION ------------------------ #

def update_website_videos_datastructure(trial_run=True):  # noqa: C901
    """There is another function that pulls sets of videos information
    from the RV website. The website returns, at the moment, 24 vids
    at a time, and all vids get saved to disk in the same file
    to start with.
    There is also another function that creates an entry about each video
    in a SimpleDS data structure.
    This function reads in the latest files that have sets
    of videos data in them and updates the datastructure with
    any changes."""

    int_total_vids_examined = 0
    int_new_vids = 0
    int_vids_updated = 0
    int_vids_deleted = 0
    int_vids_not_touched = 0

    # load the data structure class that keeps track of videos
    # metadata downloaded from the website.
    logging.debug('Loading the SimpleDS containing vid metadata from the RV website')
    web_videos_ds = SimpleDS(my_globals.str_dir4_website_vids_ds, my_globals.str_name_simpleds_website_vids)
    web_videos_ds.load()

    # get the current timestamp. This timestamp will be used to
    # tag all videos where it is detected that the incoming
    # data is different from the data already stored in the
    # SimpleDS. This is the timestamp that will be saved in the
    # DATA-UPDATED column of the SimpleDS.
    # For simplicity, all videos where a change is detected will
    # be tagged with the same time.
    logging.debug('Setting time that will be used to stamp videos where changes are detected')
    int_time_change_detected = int(round(time.time() * 1000))

    # make a list of the files to be examined, so we can
    # know the size of the list to iterate and display percentages as
    # we go along.
    logging.debug('Making a list of the files that contain sets of videos metadata downloaded from the RV website.')
    path_pattern = my_globals.str_dir_path_raw_website_json_video_sets + '*.json'
    lst_of_filenames = glob.glob(path_pattern)

    int_progress_counter = 0
    int_num_files = len(lst_of_filenames)
    # we enclose the whole giant loop below in a try statement, because
    # if something goes wrong, it gives us a chance to still save the instance
    # of SimpleDS to disk, which may help avoid that would be caused by data
    # being saved to disk as part of the loop, but the related rows in the SimpleDS
    # not getting saved to disk at the end of the function.
    try:
        for a_file in lst_of_filenames:
            logging.info('Processing videos metadata. Percent '
                         'complete: ' + "{:.2%}".format(int_progress_counter / int_num_files))
            # each of the files we open with this for loop contains a set
            # of videos in it. So now we will look at each video
            with open(a_file, mode='r') as file_with_set_of_vids:
                logging.debug('Processing file -> ' + a_file)
                dict_json_data = json.load(file_with_set_of_vids)
                # there is a "data" structure in each file, and inside that
                # structure is the list of all the videos. So the following
                # line grabs the list of videos from inside the "data"
                # wrapper.
                list_videos = dict_json_data["data"]
                for a_vid in list_videos:
                    rv_vid_from_set = RVwebsiteVid(a_vid)
                    # variables below where I use 'from_set' in the name
                    # reference that the data is coming from the video
                    # which is being pulled from a file that has a group
                    # or a set of videos (as opposed from a file, which only
                    # contains information about one video.)
                    bool_vid_not_touched = True
                    # get the necessary attributes (such as video ID and when video was created
                    # on the website (published))
                    str_vid_from_set_id = rv_vid_from_set.str_id
                    int_vid_from_set_published = rv_vid_from_set.int_published_on
                    logging.debug('Processing video #' + str(int_total_vids_examined) + ' from SETS of videos.')
                    logging.debug('Video ID is' + str_vid_from_set_id)

                    # check if the video exists in the datastructure
                    # because if it doesn't, it will need to be added.
                    if str_vid_from_set_id in web_videos_ds:
                        logging.debug('Video ' + str_vid_from_set_id + ' exists in SimpleDS')
                        # if we are here, the video is already in the datastructure
                        # so now we will check if it has changed since the last
                        # time the function ran.
                        # the first thing we do is compare the saved hash of the video
                        # with the hash of the incoming video
                        # Because the incoming object cannot be guaranteed to always contain
                        # hierarchical data in the same order, we need to tell the hashing
                        # function to sort the data. There might some cases
                        # where sorting incoming data, might result in the
                        # same data as the data saved on disk even if they are slightly
                        # different, but for our purposes, this is deemed an acceptable
                        # risk as the probability is extremely low.
                        hash_vid_from_set = make_sha256_hash(a_vid, sort_characters=True)
                        hash_vid_in_ds = web_videos_ds.fetch_hash(str_vid_from_set_id)
                        # if the hashes of the two objects don't match, then we
                        # need to do the expensive operation of pulling the
                        # data stored on disk and comparing
                        if hash_vid_from_set != hash_vid_in_ds:
                            logging.debug('Difference detected based on hashes.')
                            # since based on the hash there is a difference, we load the existing
                            # data from disk in order to do a more specific comparison
                            rv_vid_from_ds = RVwebsiteVid(web_videos_ds.fetch_data(str_vid_from_set_id))
                            # the method that does the comparison asks for a list of attributes to compare
                            # so we get that from the values of a dictionary that maps between RV website
                            # fields and the attributes of the RVwebsiteVid class
                            lst_attributes = my_globals.dict_mapping_rv_web_vid_json_2_rv_webvidclass_attrib.values()
                            dict_results = rv_vid_from_ds.compare_with_other_version_of_same_vid(rv_vid_from_set,
                                                                                                 lst_attributes)
                            if dict_results:
                                # if dict_results is not empty, then there are changes and we need
                                # to update SimpleDS
                                bool_vid_not_touched = False
                                # now update the datastructure with the new data
                                if not trial_run:
                                    logging.debug('Updating entry in SimpleDS because changes were'
                                                  ' detected for video ' + str_vid_from_set_id)
                                    # note, we didn't used to update the 'published on' timestamp
                                    # thinking this did not change, but we found a case where it seemed
                                    # to change, so we now update this as well. In the SimpleDS this is
                                    # placed in the 'data created' column.
                                    web_videos_ds.update_entry(str_vid_from_set_id,
                                                               a_vid,
                                                               int_time_change_detected,
                                                               int_vid_from_set_published,
                                                               hash_vid_from_set,
                                                               dict_results,
                                                               log_changes=True)
                                else:
                                    logging.debug('Trial run: Would have updated entry in SimpleDS because changes were'
                                                  ' detected for video ' + str_vid_from_set_id)
                                int_vids_updated += 1
                    else:
                        logging.debug('Video ' + str_vid_from_set_id + ' does NOT exist in SimpleDS')
                        # we are here if the video is not already in the data
                        # structure in which case it needs to be added
                        bool_vid_not_touched = False
                        int_new_vids += 1
                        if not trial_run:
                            logging.debug(str_vid_from_set_id + ' is being added to SimpleDS')
                            hash_vid_from_set = make_sha256_hash(a_vid, sort_characters=True)
                            web_videos_ds.add_entry(str_vid_from_set_id, int_time_change_detected,
                                                    int_vid_from_set_published, a_vid, hash_vid_from_set)
                        else:
                            logging.debug('Trial run: ' + str_vid_from_set_id + ' would have been added to SimpleDS')

                    if bool_vid_not_touched:
                        logging.debug('Video ' + str_vid_from_set_id + 'was not touched')
                        int_vids_not_touched += 1

                    int_total_vids_examined += 1

            int_progress_counter += 1

    except Exception as e:
        logging.error('While updating the website videos datastructure there was an issue. Continuing execution'
                      ' in order to save to disk the changes made so far to the SimpleDS, thereby hopefully'
                      ' avoiding failed consistency checks (where there are a different number of file entries to'
                      ' rows in the dataframe.) The Exception was: ' + repr(e))
    if not trial_run:
        logging.debug('Saving SimpleDS (of website videos metadata) to disk')
        web_videos_ds.save2disk()

    # now we call a function that deletes all the videos tagged for deletion
    set_of_deleted_vids = \
        web_videos_ds.delete_all_items_with_specific_tag_and_save_2disk(
            my_globals.str_tag_delete_row_from_simpleds, keep_deleted_files_in_change_log=True, trial_run=trial_run)
    int_vids_deleted = len(set_of_deleted_vids)

    logging.info('--------UPDATE SimpleDS (of RV Website video data) SUMMARY--------')
    if trial_run:
        logging.info('-------TRIAL RUN-------')
    logging.info('Total videos examined: ' + str(int_total_vids_examined))
    logging.info('(New) videos: ' + str(int_new_vids))
    logging.info('Videos updated: ' + str(int_vids_updated))
    logging.info('Videos not touched: ' + str(int_vids_not_touched))
    logging.info('Videos deleted: ' + str(int_vids_deleted))


# ------------------------ END FUNCTION ------------------------ #

def update_website_pubs_datastructure(trial_run=True):  # noqa: C901
    """There is another function that pulls sets of publications (pdfs) metadata information
    from the RV website. This function reads in the latest files that have sets
    of publications data in them and updates the datastructure with
    any changes."""

    int_total_pubs_examined = 0
    int_new_pubs = 0
    int_pubs_updated = 0
    int_pubs_deleted = 0
    int_pubs_not_touched = 0

    # load the data structure class that keeps track of publications
    # metadata downloaded from the website.
    logging.debug('Loading the SimpleDS containing PUBLICATIONS metadata from the RV website')
    web_pubs_ds = SimpleDS(my_globals.str_dir4_website_pubs_ds, my_globals.str_name_simpleds_website_pubs)
    web_pubs_ds.load()

    # get the current timestamp. This timestamp will be used to
    # tag all publications where it is detected that the incoming
    # data is different from the data already stored in the
    # SimpleDS. This is the timestamp that will be saved in the
    # DATA-UPDATED column of the SimpleDS.
    # For simplicity, all publications where a change is detected will
    # be tagged with the same time.
    logging.debug('Setting time that will be used to stamp publications where changes are detected')
    int_time_change_detected = make_now_timestamp()

    # make a list of the files to be examined
    logging.debug(
        'Making a list of the files that contain sets of publications metadata downloaded from the RV website.')
    path_pattern = my_globals.str_dir_path_raw_website_json_pubs_sets + '*.json'
    lst_of_filepaths = glob.glob(path_pattern)

    # we enclose the whole giant loop below in a try statement, because
    # if something goes wrong, it gives us a chance to still save the instance
    # of SimpleDS to disk, which may help avoid that would be caused by data
    # being saved to disk as part of the loop, but the related rows in the SimpleDS
    # not getting saved to disk at the end of the function.
    try:
        for a_file in lst_of_filepaths:
            logging.info('Processing publications metadata. Working on file: ' + os.path.basename(a_file))
            # each of the files we open with this for loop contains a set
            # of publications in it. So now we will look at each publication
            list_pubs = []
            with open(a_file, mode='r') as file_with_set_of_pubs:
                logging.debug('Processing file -> ' + a_file)
                dict_json_data = json.load(file_with_set_of_pubs)
                # there is a "data" structure in each file, and inside that
                # structure is the list of all the publications. So the following
                # line grabs the list of publications from inside the "data"
                # wrapper.
                list_pubs = dict_json_data["data"]
            percent_tracker = PercentTracker(len(list_pubs), log_level='info')
            counter = 0
            for a_publication in list_pubs:
                rv_pub_from_set = RVwebsitePublication(a_publication)
                # variables below where I use 'from_set' in the name
                # reference that the data is coming from the publication
                # which is being pulled from a file that has a group
                # or a set of publications (as opposed from a file, which only
                # contains information about one publication.)
                bool_pub_not_touched = True
                # get the necessary attributes (such as publication ID and when publication was created
                # on the website (published))
                str_pub_from_set_id = rv_pub_from_set.str_id
                int_pub_from_set_published = rv_pub_from_set.int_published_on
                logging.debug('Processing publication #' + str(int_total_pubs_examined) + ' from SETS of publications.')
                logging.debug('publication ID is' + str_pub_from_set_id)

                # check if the publication exists in the datastructure
                # because if it doesn't, it will need to be added.
                if str_pub_from_set_id in web_pubs_ds:
                    logging.debug('publication ' + str_pub_from_set_id + ' exists in SimpleDS')
                    # if we are here, the publication is already in the datastructure
                    # so now we will check if it has changed since the last
                    # time the function ran.
                    # the first thing we do is compare the saved hash of the publication
                    # with the hash of the incoming publication
                    # Because the incoming object cannot be guaranteed to always contain
                    # hierarchical data in the same order, we need to tell the hashing
                    # function to sort the data. There might some cases
                    # where sorting incoming data, might result in the
                    # same data as the data saved on disk even if they are slightly
                    # different, but for our purposes, this is deemed an acceptable
                    # risk as the probability is extremely low.
                    hash_pub_from_set = make_sha256_hash(a_publication, sort_characters=True)
                    hash_pub_in_ds = web_pubs_ds.fetch_hash(str_pub_from_set_id)
                    # if the hashes of the two objects don't match, then we
                    # need to do the expensive operation of pulling the
                    # data stored on disk and comparing
                    if hash_pub_from_set != hash_pub_in_ds:
                        logging.debug('Difference detected based on hashes.')
                        # since based on the hash there is a difference, we load the existing
                        # data from disk in order to do a more specific comparison
                        rv_pub_from_ds = RVwebsitePublication(web_pubs_ds.fetch_data(str_pub_from_set_id))
                        # the method that does the comparison asks for a list of attributes to compare
                        # so we get that from the values of a dictionary that maps between RV website
                        # fields and the attributes of the RVwebsitePublication class
                        lst_attributes = my_globals.dict_mapping_rv_webpubjson_2_rv_webpubclass_attrib.values()
                        dict_results = rv_pub_from_ds.compare_with_other_version_of_same_pub(rv_pub_from_set,
                                                                                             lst_attributes)
                        if dict_results:
                            # if dict_results is not empty, then there are changes and we need
                            # to update SimpleDS
                            bool_pub_not_touched = False
                            # now update the datastructure with the new data
                            if not trial_run:
                                logging.debug('Updating entry in SimpleDS because changes were'
                                              ' detected for publication ' + str_pub_from_set_id)
                                # note, we didn't used to update the 'published on' timestamp
                                # thinking this did not change, but we found a case where it seemed
                                # to change, so we now update this as well. In the SimpleDS this is
                                # placed in the 'data created' column.
                                web_pubs_ds.update_entry(str_pub_from_set_id,
                                                         a_publication,
                                                         int_time_change_detected,
                                                         int_pub_from_set_published,
                                                         hash_pub_from_set,
                                                         dict_results,
                                                         log_changes=True)
                            else:
                                logging.debug('Trial run: Would have updated entry in SimpleDS because changes were'
                                              ' detected for publication ' + str_pub_from_set_id)
                            int_pubs_updated += 1
                    # else:
                    # if we got to here, then the publication does already
                    # exist in the dataframe, and it doesn't need to
                    # change in any way
                else:
                    logging.debug('publication ' + str_pub_from_set_id + ' does NOT exist in SimpleDS')
                    # we are here if the publication is not already in the data
                    # structure in which case it needs to be added
                    bool_pub_not_touched = False
                    int_new_pubs += 1
                    if not trial_run:
                        logging.debug(str_pub_from_set_id + ' is being added to SimpleDS')
                        hash_pub_from_set = make_sha256_hash(a_publication, sort_characters=True)
                        web_pubs_ds.add_entry(str_pub_from_set_id, int_time_change_detected,
                                              int_pub_from_set_published, a_publication, hash_pub_from_set)
                    else:
                        logging.debug('Trial run: ' + str_pub_from_set_id + ' would have been added to SimpleDS')

                if bool_pub_not_touched:
                    logging.debug('publication ' + str_pub_from_set_id + 'was not touched')
                    int_pubs_not_touched += 1

                counter += 1
                percent_tracker.update_progress(counter, str_description_to_include_in_logging='Updating pubs SimpleDS')
                int_total_pubs_examined += 1

    except Exception as e:
        logging.warning('While updating the website publications datastructure there was an issue. Continuing execution'
                        ' in order to save to disk the changes made so far to the SimpleDS, thereby hopefully'
                        ' avoiding failed consistency checks (where there are a different number of file entries to'
                        ' rows in the dataframe.) The Exception was: ' + repr(e))
    if not trial_run:
        logging.debug('Saving SimpleDS (of website publications metadata) to disk')
        web_pubs_ds.save2disk()

    # now we call a function that deletes all the publications tagged for deletion
    set_of_deleted_pubs = \
        web_pubs_ds.delete_all_items_with_specific_tag_and_save_2disk(
            my_globals.str_tag_delete_row_from_simpleds, keep_deleted_files_in_change_log=True, trial_run=trial_run)
    int_pubs_deleted = len(set_of_deleted_pubs)

    logging.info('--------UPDATE SimpleDS (of RV Website publication data) SUMMARY--------')
    if trial_run:
        logging.info('-------TRIAL RUN-------')
    logging.info('Total publications examined: ' + str(int_total_pubs_examined))
    logging.info('New publications: ' + str(int_new_pubs))
    logging.info('publications updated: ' + str(int_pubs_updated))
    logging.info('publications not touched: ' + str(int_pubs_not_touched))
    logging.info('publications deleted: ' + str(int_pubs_deleted))


# ------------------------ END FUNCTION ------------------------ #


def get_specific_fields_from_all_vids_into_list(lst_of_fields):
    """This function receives a list of fields that you want
    to extract from all videos. It uses the SimpleDS data
    structure to loop through the videos (in case in the future
    we don't want to loop through all of them, but a subset)
    and extract the info from the individual file of each video.
    It returns the information as a list of dictionaries.
    Each dictionary represents one video, and contains the
    fields requested for that video."""

    dict_vid_json_schema = my_globals.dict_vids_from_website_schema

    web_vids_ds = SimpleDS(my_globals.str_dir4_website_vids_ds)
    web_vids_ds.load()

    # a list to store the dictionaries that will store the data
    lst_of_dicts = []

    # now we iterate through the datastructure, extract the fields for each vid
    # and put them into the list of dictionaries.
    for vid_id in web_vids_ds:
        # initialize the dictionary that will store the requested
        # fields
        dict_data_in_this_vid = {}

        dict_vid = web_vids_ds.fetch_data(vid_id)

        for field in lst_of_fields:
            lst_describing_where_data_is_in_hierarchy = dict_vid_json_schema[field].copy()
            the_data = \
                recursiveExtractFieldFromHierarchy(
                    dict_vid, lst_describing_where_data_is_in_hierarchy)
            dict_data_in_this_vid[field] = the_data

        lst_of_dicts.append(dict_data_in_this_vid)
    return lst_of_dicts


# ------------------------ END FUNCTION ------------------------ #


def extract_guestsNsubjects(list_of_folks_2_ignore):  # noqa: C901
    # a dictionary that will track the overall data
    # regarding guests and the subjects they've discussed.
    dict_overall_guests_subjects = {}

    # Get a list containing a dictionary (one per video)
    # that has in it, the fields we need to extract guest
    # info and the subjects they talked about.
    lst_of_fields_with_guests_subjects = ['video_featuring',
                                          'video_topic_names',
                                          'video_tag_names',
                                          'video_asset_names'
                                          ]
    lst_data_guests_subjects = \
        get_specific_fields_from_all_vids_into_list(lst_of_fields_with_guests_subjects)

    for record in lst_data_guests_subjects:
        dict_data_in_curret_vid = record.copy()

        # first, make a list of persons that come from the
        # featuring field.
        lst_persons_in_this_record = []
        # the code expects the list of persons to be extracted
        # out of a string. If the data we have isn't a string
        # or type None (which on ocassion the website might
        # return instead of an empty string if there were no
        # persons in the featuring field) we'll stop the code.
        raw_persons_data = dict_data_in_curret_vid.pop('video_featuring')
        type_persons_data = type(raw_persons_data)
        if type_persons_data is str:
            lst_persons_in_this_record = \
                sorted(extractIndividualItemsFromTextList(raw_persons_data))
        elif type_persons_data == type(None):  # noqa: E721
            # we already initialized the list of persons
            # as the empty string, so no need to do
            # anything else here
            pass
        else:
            logging.error("Unexpected type of object in function 'extract_guestsNsubjects'.")

        # now we combine subjects that were talked about
        # which are stored in the other fields this function
        # requested, such as tags, topics, etc.
        # Since, above we popped the only part of the dictionary
        # which contained persons, only subjects that were
        # discussed remain in the dictionary. So all remaining
        # fields can be merged.
        lst_subjects_discussed_in_this_record = []
        for entry in dict_data_in_curret_vid:
            lst_subjects_discussed_in_this_record += \
                dict_data_in_curret_vid[entry]

        # now we add the guests and subjects to the overall dictionary
        for aperson in lst_persons_in_this_record:
            if aperson not in list_of_folks_2_ignore:
                if aperson not in dict_overall_guests_subjects:
                    # if the person isn't in the overall dictionary
                    # yet, we initialize their list of subjects
                    dict_overall_guests_subjects[aperson] = []
                # then we append the list of subjects in this record
                # to the person's overall list of subjects.
                dict_overall_guests_subjects[aperson].extend(
                    lst_subjects_discussed_in_this_record)

    # now do some data cleanup of the overall guest dictionary
    # first, remove duplicates in the list of subjects and sort
    # all the entries in the list of subjects as well.
    # if you try to do everything at once (as in, also immediately search
    # for the names of other guests to remove from the list of subjects)
    # in a nested loop it doesn't work other than for the first guest,
    # because subsequent guests have not been cleaned-up yet
    # (duplicates removed.)
    for person in dict_overall_guests_subjects:
        # line below removes duplicates from within the list of subjects. This needs
        # to be done first, or else the removal of names later, will only remove the first
        # instance of the name
        dict_overall_guests_subjects[person] = \
            list(dict.fromkeys(dict_overall_guests_subjects[person]))
        # sort the list of subjects
        dict_overall_guests_subjects[person].sort()

    # NOW that duplicates have been removed you can do a nested
    # loop to remove guests from each other's list of subjects
    for person in dict_overall_guests_subjects:
        # any guests name, that shows up in the list of
        # topics/tags/assets of any other guest (including itself)
        guestNameToRemoveFromOtherGuestsSubjects = person
        for aGuestLevel2 in dict_overall_guests_subjects:
            if guestNameToRemoveFromOtherGuestsSubjects in dict_overall_guests_subjects[aGuestLevel2]:
                dict_overall_guests_subjects[aGuestLevel2].remove(guestNameToRemoveFromOtherGuestsSubjects)

    # we can write the guest and subject data to a CSV file for ease of viewing/troubleshooting
    # To do so, the 'import csv' line must be added at the top of this file
    # theDirectory = my_globals.str_dir4_manipd_CSV_website_guests_subjects
    # midFileName = my_globals.str_filename_base_string4_manipd_website_guests_subjects
    # fileName = theDirectory + midFileName + projectStandardTimestamp() + '.csv'
    # with open(fileName, mode='w') as guestsAndSubjectsCSVfile:
    #     theWriter = csv.writer(guestsAndSubjectsCSVfile)
    #     theWriter.writerow(['guest', 'subjects'])
    #     for person in dict_overall_guests_subjects:
    #         stringOfSubjects = ''
    #         for aSubject in dict_overall_guests_subjects[person]:
    #             stringOfSubjects = stringOfSubjects + aSubject + ', '
    #         stringOfSubjects.strip(' ,')
    #         theWriter.writerow([person, stringOfSubjects])

    # write the guest and subject data to a JSON file
    theDirectory = my_globals.str_dir4_manipd_JSON_website_guests_subjects
    # below, we'll write the data to a file, but first we'll clean-up previously created
    # files in the directory so they don't start to consume the disk over time.
    cleanup_older_files_in_a_dir(theDirectory)
    midFileName = my_globals.str_filename_base_string4_manipd_website_guests_subjects
    fileName = theDirectory + midFileName + projectStandardTimestamp() + '.json'
    with open(fileName, mode='w') as guestsAndSubjectsJSONfile:
        json.dump(dict_overall_guests_subjects, guestsAndSubjectsJSONfile)

    return dict_overall_guests_subjects


# ------------------------ END FUNCTION ------------------------ #


def extract_interviewers(list_of_folks_2_ignore):
    # make a list with all interviewers in it
    # initially this list was used to push interviewers
    # to Airtable. This is no longer needed because the info can be queried directly now that
    # each video is pushed to Airtable as well. However, the info is still needed to ensure
    # that interviewers are added consistently - in other words to make sure that an
    # interviewer isn't added to the website that doesn't exist in airtable
    str_interviewer_field = 'video_interviewer'
    lst_fields_to_extract = [str_interviewer_field]
    lst_overall_interviewers = []

    lst_interviewers_in_all_vids = \
        get_specific_fields_from_all_vids_into_list(lst_fields_to_extract)

    for record in lst_interviewers_in_all_vids:
        # first, make a list of persons that come from the
        # interviewer field in the current record (vid)
        lst_persons_in_this_record = []
        # the code expects the list of persons to be extracted
        # out of a string. If the data we have isn't a string
        # or type None (which on occasion the website might
        # return instead of an empty string if there were no
        # persons in the interviewer field) we'll stop the code.
        raw_persons_data = record[str_interviewer_field]
        type_persons_data = type(raw_persons_data)
        if type_persons_data is str:
            lst_persons_in_this_record = \
                sorted(extractIndividualItemsFromTextList(raw_persons_data))
        elif type_persons_data == type(None):  # noqa: E721
            # we already initialized the list of persons
            # as the empty string, so no need to do
            # anything else here
            pass
        else:
            logging.debug('Unexpected type of object. Stopping execution of program.')
            exit(0)

        lst_overall_interviewers.extend(lst_persons_in_this_record)

    # now we should have all the interviewers, so we remove duplicates
    # from the list.
    lst_overall_interviewers = list(dict.fromkeys(lst_overall_interviewers))
    lst_overall_interviewers.sort()

    # for some reason the empty string is making it into the list sometimes
    # so we remove it
    if '' in lst_overall_interviewers:
        lst_overall_interviewers.remove('')

    # write the interviewers to a file
    theDirectory = my_globals.str_dir4_manipd_JSON_website_interviewers
    # below, we'll write the data to a file, but first we'll clean-up previously created
    # files in the directory so they don't start to consume the disk over time.
    cleanup_older_files_in_a_dir(theDirectory)
    midFileName = my_globals.str_filename_base_string4_manipd_website_interviewers
    fileName = theDirectory + midFileName + projectStandardTimestamp() + '.json'
    with open(fileName, mode='w') as interviewersJSONfile:
        json.dump(lst_overall_interviewers, interviewersJSONfile)

    return lst_overall_interviewers


# ------------------------ END FUNCTION ------------------------ #


def convertWebsiteJSONtoAirtableFormat(dict_shows_from_website, dict_shows_from_airtable, trial_run=True):  # noqa: C901
    """This function iterates through the datastructure of raw videos information
    returned from the Real Vision website, and translates each video entry
    into a 'record' ready to be pushed to Airtable. These records are in turn stored
    in their own instance of the SimpleDS datastructure."""

    # if this function encounters any issues with a particular data record (video)
    # then it uses the tag below to store that information in the 'TAGS' column
    # of the datastructure.
    # if other fuctions are using the same SimpleDS data strcuture, they should
    # use their own tag to track their own issues. However, here we will also use
    # a tag to create a workflow between this function and the function that pushes
    # the data to airtable. This function will tag some rows that should be
    # re-pushed up to airtable, even if the last-updated timestamps match.
    ds_tag_issues = my_globals.str_tag_web2airt_issues
    ds_tag_repush2airt = my_globals.str_tag_repush_to_airtable
    ds_tag_change_detected_upstream = my_globals.str_tag_web_vid_changed

    int_count_vids_processed = 0
    int_count_new_vids = 0
    int_count_vids_updated = 0
    int_count_vids_deleted = 0
    int_count_vids_not_touched = 0
    int_count_vids_with_no_issues = 0
    int_count_vids_with_issues = 0
    int_count_vids_with_issues_fixed = 0
    # create a pandas dataframe that will keep a log of videos
    # where issues were encountered (example, not all the
    # guests were found in Airtable)
    columns_for_issue_tracking = ['Title', 'Chronological Vid #', 'Issues']
    df_issues = pd.DataFrame(columns=columns_for_issue_tracking)

    # get some global settings that will be needed in the main loop
    dictOfFieldsWeWantInAirtable = my_globals.dict_translate_vids_fields_website2airtable
    found = my_globals.str_to_use_if_something_has_been_found
    not_found = my_globals.str_to_use_if_something_has_not_been_found
    idx_of_airT_guest_id = my_globals.idx_fields_airT_tbl_guests_id
    idx_of_airt_show_id = my_globals.idx_fields_airT_tbl_shows_airt_id

    # open the datastructure that contains metadata about each
    # video. This will allows us to iterate through the website
    # videos data with a defined size of iteration so we can
    # display percentages.
    web_vids_DS = SimpleDS(my_globals.str_dir4_website_vids_ds, my_globals.str_name_simpleds_website_vids)
    web_vids_DS.load()
    web_vids_DS.sort()

    # open the datastructure (created and maintained by this
    # function) that cointains the metadata about videos that
    # has been tranlsated (by this function) from the format that
    # is downloaded from the RV website, into a format that
    # the python airtable wrapper can use to push the info into
    # Airtable.
    airt_vids_DS = SimpleDS(my_globals.str_dir4_airt_vids_ds, my_globals.str_name_simpleds_airtable_records)
    airt_vids_DS.load()
    airt_vids_DS.sort()

    # open the datastructure that contains 'other info' about
    # videos, such as info about the transcript, or info about
    # comments made about the video
    other_info_vids_DS = SimpleDS(my_globals.str_dir4_additional_vids_info_ds,
                                  my_globals.str_name_simpleds_additionalinfo_vids)
    other_info_vids_DS.load()
    other_info_vids_DS.sort()

    # now we iterate through the datastructure that holds the
    # website videos metadata, and process each row and
    # its respective file on disk, which involves checking if
    # an entry in the airtable datastructure already exists, if not,
    # create the entry, and if it does, then see if it needs
    # to be updated or not.
    int_length_vidsDS = len(web_vids_DS)
    # a class to help with the tracking and display of percentages
    percent_tracker = PercentTracker(int_length_vidsDS, log_level='info')
    try:
        for webvid_id in web_vids_DS:
            # initialize a string to track if any issues are encountered
            # during processing of the video
            str_issues_encountered = ''
            vid_needs_updating = False
            vid_needs_adding = False
            # The booleans initialized
            # above are used to track whether an video is going to be added
            # or updated, because in either case very similar code is executed,
            # namely, the new data gets translated into Airtable record format
            # and then a method of the datastrcutre gets called to either add
            # the data or to update it.
            vid_has_pending_issue = False
            vid_tagged_as_changed_upstream = False
            vid_comments_tagged_as_changed = False
            web_vid_lastupdated = web_vids_DS.fetch_lastupdated(webvid_id)
            # check if the translated video exists already in the
            # airtable vids datastructure.
            if webvid_id in airt_vids_DS:
                # if the video does exist in the airtable datastructure
                # (in other words, the json video data from the website
                # has already been translated to airtable record format.)
                # so now that it has been confirmed the video is in
                # the datastructure, it is necessary to check whether
                # the data that comes from the website has changed as compared
                # to what is already in the airt vids datastructure.
                # in this part of the code
                airt_vid_lastupdated = airt_vids_DS.fetch_lastupdated(webvid_id)
                if web_vid_lastupdated > airt_vid_lastupdated:
                    # if the code reaches here, then the incoming data
                    # is newer than the data previously translated, so the
                    # airt datastructure needs to be updated.
                    vid_needs_updating = True
                elif web_vid_lastupdated == airt_vid_lastupdated:
                    # if the code reaches here, then the incoming data
                    # has not changed at the source, as compared to the
                    # data already in the data structure (based on the
                    # last updated timestamp at source.) So there isn't
                    # anything that needs doing.
                    pass
                else:
                    # the data in the airt vids datastructure should never
                    # be newer than the incoming data, so something has
                    # gone wrong.
                    logging.error('Data in Airtable vids datastructure is newer'
                                  ' than the incoming data from the RV website. '
                                  'This should never be the case. Stopping program'
                                  ' execution.')
                    exit(0)
                # we also want to check if the video has pending issues
                # if the data presented issues the last time
                # this function was run, then we want to try to update
                # the row again to see if the issue was fixed
                vid_has_pending_issue = airt_vids_DS.tag_check(webvid_id, ds_tag_issues)
                if vid_has_pending_issue:
                    vid_needs_updating = True
                # similarly we want to check if any other workflow has
                # tagged the video coming from the web (so in the other SimpleDS)
                # for updating due to miscellaneous reasons
                vid_tagged_as_changed_upstream = web_vids_DS.tag_check(webvid_id,
                                                                       ds_tag_change_detected_upstream)
                if vid_tagged_as_changed_upstream:
                    vid_needs_updating = True
                # we also want to check if the video has possibly changed due
                # to information stored in the 'additional video info' instance of SimpleDS.
                # Specifically we want to check if there are changes to the 'comments' statistics
                # stored in that SimpleDS. I initially tried to do this using the date of change
                # stored in the 'additional video info' SimpleDS, but because each SimpleDS only
                # has one 'last updated' column, and the Airtable records SimpleDS already uses
                # that column to compare with the SimpleDS that stores json video records downloaded
                # from the website, it was problematic. So now I'm using tags for this.
                if webvid_id in other_info_vids_DS:
                    vid_comments_tagged_as_changed = \
                        other_info_vids_DS.tag_check(webvid_id, my_globals.str_tag_comments_chngd_for_airt_convert)
                    if vid_comments_tagged_as_changed:
                        vid_needs_updating = True
                # FUTURE IMPROVEMENT
                # note that there are many test above which indicate if a video needs to be
                # updated, and currently we are just grouping them all together into one
                # boolean that tracks if the record should be updated. However, it might
                # be the case that the info coming directly from the website json SimpleDS needs to be
                # updated, but the info coming from the 'additional video info' SimpleDS does not.
                # As a result, it is SLIGHTLY inefficient to update the entire record below, when only
                # the source that actually changed needs re-processing. However, at this point
                # the processing is so fast and cheap, there is no need to implement this segregation.
            else:
                # if the code reaches here, the video is new; it is unknown
                # to the airt vids datastructure, so it needs to be added.
                vid_needs_adding = True

            # if the video was tagged above as needing to be added or updated
            # then the incoming video data needs to be translated into an
            # Airtable record.
            if vid_needs_adding or vid_needs_updating:
                dict_web_vid = web_vids_DS.fetch_data(webvid_id)
                # initialize a container to store the converted record
                dict_converted_vid_record = {}
                # Much after I had created much of the code below, I created the
                # RVwebsiteVid class, which would have been very valuable from the start, but I was just
                # re-learning things. Anyways, that's why it isn't used in the code below in many
                # places where it would have been useful, but is use in other places where I needed
                # to modify the code, and this class provided a good way to do so.
                vid_obj = RVwebsiteVid(dict_web_vid)

                for aField in dictOfFieldsWeWantInAirtable:
                    fieldNameInAirtable = aField
                    dict_converted_vid_record[fieldNameInAirtable] = \
                        extractVideoFieldFromWebsiteJSON(dict_web_vid, fieldNameInAirtable)
                    # Now that we have the field info, we need to do some
                    # specific type of work for some of the fields.
                    # If the field we are looking for is the name of the SHOW,
                    # then we need to do a little more work because at the moment
                    # what we have is an ugly ID, so we need to extract the show
                    # name from a dictionary we passed to this function.
                    if aField == my_globals.str_AT_vid_col_name_show:
                        # we currently have the show ID
                        parent_show_id = dict_converted_vid_record[fieldNameInAirtable]
                        # we use the ID to extract the show name from the dict
                        # I ran into a case where the show field was not populated.
                        # Functions up to this point are coded to handle that, but
                        # querying the show dictionary below with an empty string as the
                        # parent_show_id would fail, so we need to check
                        # if the show ID is not an empty string, and then we can query the
                        # dictionary. First, we set the parent_show_name to an empty string. Then
                        # if the show id is blank, it just stays that way, but if it isn't
                        # then the show name variable does get set.
                        parent_show_name = ''
                        airt_show_id = ''
                        if parent_show_id:
                            parent_show_name = dict_shows_from_website[parent_show_id][0]
                            # the zero above is due to the fact that this function
                            # receives a
                            # dictionary as a parameter. The dictionary's keys are "show IDs",
                            # and the corresponding data to each key is a list. In
                            # the [0] position of the list is the show's name, which
                            # is what we are looking for.
                        else:
                            # We are in this part of the code if no show id was provided by the website.

                            # It is horrible to have to "hard-code" the snippet of code below
                            # but there are something like 40 macroinsider videos that don't have
                            # the show field populated on the website. So we check for those cases
                            # and if we 'detect' this weird case, we forcefully tag the show
                            # here. We 'detect' because it seems like all these videos have the
                            # the sub-string 'Insider Talks' in their title.
                            if ('Insider Talks -' in vid_obj.str_title) or ('Insider talks -' in vid_obj.str_title):
                                parent_show_name = 'Insider Talks'
                            else:
                                str_issues_encountered += \
                                    ('No show id provided.')
                        # now we have the show name from the website, so we translate
                        # that into the Airtable ID associated with that show
                        if parent_show_name:
                            if parent_show_name in dict_shows_from_airtable:
                                airt_show_id = dict_shows_from_airtable[parent_show_name][idx_of_airt_show_id]
                            else:
                                str_issues_encountered += \
                                    ("Website show '" + parent_show_name + "' not found in Airtable shows.")
                        dict_converted_vid_record[fieldNameInAirtable] = [airt_show_id]
                    if (aField == my_globals.str_AT_vid_col_name_featuring) or \
                            (aField == my_globals.str_AT_vid_col_name_interviewers):
                        # when we reach this part of the code, the entry we have in the
                        # conversion dictionary is in fact the extracted names from the
                        # website JSON dump, but just as a string. We now want to convert
                        # them into their IDs so they can be entered as linked records
                        # in airtable.
                        str_of_names_on_website = dict_converted_vid_record[aField]
                        # we call a function that converts the names to whatever
                        # field we request, in this case the ID. The function
                        # returns a dictionary with an entry for the people it found
                        # and with a string for the people it didn't find.
                        dictSearchedAndConverted = \
                            convert_ppl_names2another_airtable_field(
                                str_of_names_on_website, idx_of_airT_guest_id)
                        if dictSearchedAndConverted[not_found]:
                            # we enter this IF, if there were people in the list
                            # that were not found in Airtable. So what I'm deciding to do
                            # in this case, is leave the field being populated in Airtable
                            # entirely empty (not even adding the people that were possibly
                            # found if there were several folks in the field.)
                            # So first, we empty the field
                            dict_converted_vid_record[aField] = []
                            # And we want to append to the string that is tracking issues for
                            # this video.
                            str_issues_encountered += \
                                ('Within "' + aField + '" ' + dictSearchedAndConverted[
                                    not_found] + ' not in Airtable. ')
                        else:
                            # we enter this part of the code, if everyone in the list
                            # of people was found.
                            # so we populate the Airtable record with the IDs
                            dict_converted_vid_record[aField] = dictSearchedAndConverted[found]

                # now we focus on populating the fields that do NOT come directly from data
                # inside the JSON pulled from the website, or which need a bit more manipulation,
                # or that are only populated sometimes but not always
                dict_converted_vid_record[my_globals.str_AT_vid_col_name_lastchanged] = web_vid_lastupdated
                dict_converted_vid_record[my_globals.str_AT_vid_col_name_linkurl] = \
                    'https://www.realvision.com/tv/videos/id/' + webvid_id
                # now we populate the field that will contain information about which RV 'tiers' (membership
                # level) the video is available to.
                product_vid_belongs_to = vid_obj.str_product_id
                rv_tier = my_globals.dict_product_mapping_to_tiers[product_vid_belongs_to]
                dict_converted_vid_record[my_globals.str_AT_vid_col_name_availableto] = rv_tier
                # now we populate some fields that are related to comments made on the RV website about
                # a video. To do so, first we check if the video currently being processed already
                # exists in the datastructure that store 'other video info'. If it doesn't, we simply
                # don't populate these fields
                if webvid_id in other_info_vids_DS:
                    dict_other_info = other_info_vids_DS.fetch_data(webvid_id)
                    if my_globals.str_vid_comments in dict_other_info:
                        dict_comments_info = dict_other_info[my_globals.str_vid_comments]
                        dict_converted_vid_record[my_globals.str_AT_vid_col_name_comments] = \
                            dict_comments_info[my_globals.str_vid_comments_num_total]
                        dict_converted_vid_record[my_globals.str_AT_vid_col_name_comments_replies] = \
                            dict_comments_info[my_globals.str_vid_comments_num_replies]
                        dict_converted_vid_record[my_globals.str_AT_vid_col_name_comments_likes] = \
                            dict_comments_info[my_globals.str_vid_comments_likes]
                        dict_converted_vid_record[my_globals.str_AT_vid_col_name_comments_dislikes] = \
                            dict_comments_info[my_globals.str_vid_comments_dislikes]
                # now we populate the column about whether the video is free or not
                # we'll only create the field/column, if the video is in fact free
                # the raw json field is a boolean value. However, if the video is free
                # we just want to pass the word 'FREE' so we do that conversion here.
                if vid_obj.bool_is_free:
                    dict_converted_vid_record[my_globals.str_AT_vid_col_name_isfree] = 'FREE'

                if vid_needs_updating and vid_needs_adding:
                    print('Something is wrong. A video should never need'
                          ' both adding AND updating. Stopping program execution.')
                    exit(0)

                # The 'date created' being stored inside the datastructure
                # is metadata. It does not refer to the date the data was first
                # inserted into the datastructure, but rather, the date when
                # when the data was created at the source. In this case, when the
                # data was published to the website.
                # get the date  inside the video metadata
                website_vid_created = vid_obj.int_published_on
                # get the date in the web videos datastructure
                webDS_vid_created = web_vids_DS.fetch_created(webvid_id)
                # compare the dates. They should be the same.
                if website_vid_created != webDS_vid_created:
                    # with this IF we are just doing a little bit of error checking
                    # because the dates should generally always agree
                    logging.warning('In function that converts website video json data into Airtable records. Video'
                                    ' with ID: ' + webvid_id + ' was found to have different creation dates in the'
                                    ' json metadata, as compared to the value stored in the column of SimpleDS.')
                if vid_needs_adding:
                    if not trial_run:
                        airt_vids_DS.add_entry(webvid_id, web_vid_lastupdated,
                                               website_vid_created, dict_converted_vid_record)
                    int_count_new_vids += 1
                elif vid_needs_updating:
                    if not trial_run:
                        # in the 'update' function below, the 'date creation' can be optionally passed to
                        # be updated, just in case it has changed. It should rarely change, but we did find
                        # a case where it had changed (very odd) so best to update it.
                        # We are also, at the moment of this writing, not detecting the changes in this
                        # instance of SimpleDS in any way, so we are not keeping a change log (which is
                        # also an optional parameter to the 'update' method below.
                        airt_vids_DS.update_entry(webvid_id, dict_converted_vid_record,
                                                  web_vid_lastupdated, website_vid_created)

                # we are still in the part of the code that gets executed if a video needs
                # either adding or updating. So regardless of which one it is, another thing
                # we want to do, is tag the row so that the next workflow (pushing to airtable)
                # knows the row should be pushed.
                if not trial_run:
                    airt_vids_DS.tag_add(webvid_id, ds_tag_repush2airt)
                # Also if the video is being updated because of a 'changed' tag,
                # the video has now been updated on account of
                # the tag, so that tag can now be cleared.
                if vid_tagged_as_changed_upstream:
                    if not trial_run:
                        web_vids_DS.tag_remove(webvid_id, ds_tag_change_detected_upstream)
                # similarly if the video was updated because of 'video comments' being
                # updated, then we can now clear that tag as well.
                if vid_comments_tagged_as_changed:
                    if not trial_run:
                        other_info_vids_DS.tag_remove(webvid_id, my_globals.str_tag_comments_chngd_for_airt_convert)

                int_count_vids_updated += 1

            if (not vid_needs_adding) and (not vid_needs_updating):
                int_count_vids_not_touched += 1

            int_count_vids_processed += 1
            # at the end of processing each video, if any issues were encountered
            # we want to add a row to the pandas dataframe for recording the issues, so
            # we create a list to add to the dataframe that matches the columns
            # given to the initial dataframe at the beginning of this function.
            if str_issues_encountered:
                # also, we tag the video to tell future runs of this function
                # that this row has a pending issue.
                if not trial_run:
                    airt_vids_DS.tag_add(webvid_id, ds_tag_issues)
                int_count_vids_with_issues += 1
                vid_title = dict_converted_vid_record['Title']
                vid_number = int_count_vids_processed
                listForIssuesDataframe = [vid_title,
                                          vid_number,
                                          str_issues_encountered]
                # the next line appends the list to the issues dataframe
                df_issues.loc[len(df_issues)] = listForIssuesDataframe
            else:
                int_count_vids_with_no_issues += 1
                # also, check if in the past the video had issues that
                # were tracked using the ds_tag_issues
                # if so, but you are now in this part of the code, you know the
                # issues have been resolved, so the flag should be cleared.
                if vid_has_pending_issue:
                    if not trial_run:
                        airt_vids_DS.tag_remove(webvid_id, ds_tag_issues)
                    int_count_vids_with_issues_fixed += 1
                    # also, we will mark the row with a tag that will tell
                    # another function to re-push the data to airtable now
                    # that the issue has been resolved.
                    if not trial_run:
                        airt_vids_DS.tag_add(webvid_id, ds_tag_repush2airt)

            # print percentages
            percent_tracker.update_progress(int_count_vids_processed)

        # now we look for any videos that may need to be deleted
        set_vids_in_web_ds = web_vids_DS.fetch_all_ids_as_python_set()
        set_vids_in_airtable_ds = airt_vids_DS.fetch_all_ids_as_python_set()
        set_vids_removed_at_source = set_vids_in_airtable_ds - set_vids_in_web_ds
        for entry in set_vids_removed_at_source:
            if not trial_run:
                airt_vids_DS.delete_entry(entry, keep_version_of_file_in_log_directory=True)
            int_count_vids_deleted += 1
            int_count_vids_not_touched -= 1

    except Exception as e:
        logging.warning('While updating the Airtable records datastructure there was an issue. Continuing execution'
                        ' at end of function in order to save to disk the changes made so far to the SimpleDS, '
                        ' thereby hopefully avoiding failed consistency checks (where there are a different number'
                        ' of file entries to rows in the dataframe.) The Exception was: ' + repr(e))

    if not trial_run:
        airt_vids_DS.save2disk()
        web_vids_DS.save2disk()
        other_info_vids_DS.save2disk()

    logging.warning('------ TABLE OF ISSUES ------')
    if trial_run:
        logging.info('------ WAS ONLY A TRIAL RUN ------')
    logging.warning(json.dumps(df_issues.to_dict('records'), indent=4))
    logging.warning('-----------------------------')
    list_of_lines_2print = ['Vids with issues: ' + str(int_count_vids_with_issues),
                            'Vids with issues fixed: ' + str(int_count_vids_with_issues_fixed),
                            'Vids without issues: ' + str(int_count_vids_with_no_issues),
                            'Vids added: ' + str(int_count_new_vids),
                            'Vids updated: ' + str(int_count_vids_updated),
                            'Vids deleted: ' + str(int_count_vids_deleted),
                            'Vids not touched: ' + str(int_count_vids_not_touched),
                            'Total vids processed: ' + str(int_count_vids_processed),
                            '------------------------------------------'
                            ]
    for a_message in list_of_lines_2print:
        logging.info(a_message)
