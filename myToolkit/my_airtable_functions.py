import json
import os
import glob
import time
import logging
from datetime import datetime
import my_globals
from class_airt_on_disk import AirtTableSubsetOnDisk
from class_myairt_wrapper import MyAirtWrapper
from class_simpleDS import SimpleDS
from my_building_blocks import convertDictKeysToList, \
    removeCommonPersonNamePrefixes, \
    extractIndividualItemsFromTextList, \
    recursiveExtractFieldFromHierarchy
from my_rv_website_functions import extractIDstringFromURLstring
from class_percent_tracker import PercentTracker


def update_a_field_deltas_only(str_table_to_update,  # noqa: C901
                               strFieldToBeUpdated,
                               dictOfReorganizedDataPulledFromAirtable,
                               indx_of_id, indx_of_data,
                               dictKeyAndUpdateData,
                               only_a_trial_run=True):
    """# this function should be passed:
        1)  a string naming the table to be updated in Airtable
        2)  a string representing the name of the field/column to be updated
        3)  a dictionary filled with data that has been pulled from the same
            table in airtable. However, this dictionary should have been reorganized
            first (we have a function that does this)
            because airtable returns the dump of its records as a list. So that
            list should be reorganized into a dictionary having the following format:
            {str_key: [list OF field_data of the relevant record]}
            AND the str_key, should represent the same information in both dictionaries
            that are passed to this function. For example, if the table is 'guests'
            then, both dictionary keys would likely be a Guest's name.
        4)  Within the list that is passed as the value of the first dictionary, one of
            the items in the list should be the 'id' of the record in Airtable.
            The INDEX of this 'id' should be passed as the next parameter.
        5)  Within the list that is passed as the value of the first dictionary, one of
            the items in the list should be the 'data' of the record in Airtable.
            The INDEX of this 'id' should be passed as the next parameter.
        6)  A dictionary in the format {str_key: data_to_update} and again the str_key
            should represent the same information in this dictionary as in the first
            dictionary, because we will use these keys to search from one dictionary
            to the other.
        7)  Whether this is only a trial run, to see what would happen or not."""
    logging.debug('Updating (deltas) a field in Airtable')
    # Create a sorted list (from the dictionary containing the data
    # to be updated) that will be used to iterate and populate airtable.
    logging.debug('Creating a sorted list that will be used for iteration of the records')
    sortedListRowsToUpdate = convertDictKeysToList(dictKeyAndUpdateData, sorted=True)
    if '' in sortedListRowsToUpdate:
        sortedListRowsToUpdate.remove('')

    airT = None
    if not only_a_trial_run:
        airT = MyAirtWrapper(str_table_to_update)
    intSizeList = len(sortedListRowsToUpdate)
    percentTracker = 0

    # start some counters to print some statistics at the end
    # they are fairly self-explanatry. they 'would have added'
    # counter is for tracking what would have been added when
    # the function is being run as trial only (without actually
    # pushing to airtable.)
    num_items_total = 0
    num_items_would_have_updated = 0
    num_items_updated_for_realz = 0
    num_items_not_updated = 0
    num_items_not_found_in_airT = 0

    # let's create a dictionary of dictionaries that will keep
    # a log of the entries that needed updating. For the top
    # level dictionary, the key will be the same as the dictionaries
    # that are passed as parameters. The value for each of those keys,
    # will in turn be a dictionary which
    # will have two entries: 'existing AirT value' and
    # 'updated AirT value'
    dict_log_of_changes = {}
    # in case the field to be updates is a list, we need a different
    # type of dictionary to log the results. Similar as above,
    # but the two entries in the sub-ddictionary are going to be slightly
    # different: 'in AirT but not in update'
    # and : 'in update but not in AirT'
    dict_log_of_changes_if_list = {}

    # now for each row check to see if an update is necessary, and
    # if so, push the updated list of subjects
    logging.debug('Looping through rows to check if an update is necessary, and if so pushing the update')
    for anItem in sortedListRowsToUpdate:
        num_items_total += 1
        # first let's confirm that the item exists both in Airtable
        # and in the dictionary that has the data to be updated
        if anItem in dictOfReorganizedDataPulledFromAirtable:
            # if it does, then we want to update the record in Airtable with
            # the data passed in the non-Airtable dictionary
            # first we find the id of the record in Airtable
            # which is passed inside the list, inside the first dictionary.
            listOfTheRecordsFields = dictOfReorganizedDataPulledFromAirtable[anItem]
            strRecordID = listOfTheRecordsFields[indx_of_id]
            # if you ever want to update more than one field, the three lines
            # below should become a loop that fills the dictionary, instead
            # of just these three lines. And instead of a single value
            # being passed in for the field to be updated as a function
            # parameter, it would need to be a list.
            field_contents_existing = listOfTheRecordsFields[indx_of_data]
            field_contents_update = dictKeyAndUpdateData[anItem]
            if type(field_contents_existing) is list:
                field_contents_existing.sort()
            if type(field_contents_update) is list:
                field_contents_update.sort()
            if (field_contents_existing == field_contents_update) or \
                    ((field_contents_existing == '' and field_contents_update == [])
                     or (field_contents_existing == [] and field_contents_update == '')):
                # the IF above is convoluted, but basically it just checks
                # whether the existing data matches the incoming data. So this
                # is either, a literal match, or a match because one of them
                # is an empty string and the other an empty list, or vice
                # versa.
                num_items_not_updated += 1
            elif field_contents_existing != field_contents_update:
                # if we are in this if (line above), then the current info, does not
                # match the info in Airtable, so an update is in order
                logging.debug('Found a difference between incoming data and existing data')
                if type(field_contents_update) is not list:
                    # this is the code we execute if the field to be
                    # updated is not a list.
                    dict_log_of_changes[anItem] = \
                        {'existing AirT value': field_contents_existing,
                         'updated AirT value': field_contents_update}
                else:
                    list_in_AirT_not_in_update = []
                    list_in_update_not_in_AirT = []
                    for itemInAirTlist in field_contents_existing:
                        if itemInAirTlist not in field_contents_update:
                            list_in_AirT_not_in_update.append(itemInAirTlist)
                    for itemInUpdatelist in field_contents_update:
                        if itemInUpdatelist not in field_contents_existing:
                            list_in_update_not_in_AirT.append(itemInUpdatelist)
                    dict_log_of_changes_if_list[anItem] = \
                        {'in AirT but not in update': list_in_AirT_not_in_update,
                         'in update but not in AirT': list_in_update_not_in_AirT}
                dictFieldsToUpdate = {strFieldToBeUpdated: field_contents_update}
                logging.info('Pushing data for ' + anItem + '. Percent complete: ' + "{:.2%}".format(
                    percentTracker / intSizeList))
                # the IF below avoids anything actually being pushed to airtable
                # if this is a trial run.
                num_items_would_have_updated += 1
                if not only_a_trial_run:
                    logging.debug('Pushing updated record to Airtable')
                    airT.update(strRecordID, dictFieldsToUpdate, typecast=True)
                    num_items_updated_for_realz += 1
                    # The Airtable API accepts a maximum of 5 requests per second,
                    # so we take a little pause here to make sure we are abiding
                    # by that
                    time.sleep(.2)
            else:
                logging.error("I don't think this code should ever be reached. Program terminating.")
                exit(0)
        else:
            num_items_not_found_in_airT += 1
        percentTracker += 1

    logging.info('\n\nTotal items:         ' + str(num_items_total))
    if only_a_trial_run:
        logging.info('Would have been pushed if run for realz: ' + str(num_items_would_have_updated))
    else:
        logging.info('Total actually pushed:                   ' + str(num_items_updated_for_realz))
    logging.info('Number items already up-to-date:               ' + str(num_items_not_updated))
    logging.info('Number not found in Airtable:               ' + str(num_items_not_found_in_airT))

    # also useful to log the dictionaries
    logging.info('-------------------------- DELTAS -----------------------------')
    logging.info(json.dumps(dict_log_of_changes, indent=4))
    logging.info('------- DELTAS IF THE FIELD TO BE UPDATED WAS A LIST ----------')
    logging.info(json.dumps(dict_log_of_changes_if_list, indent=4))


# ------------------------ END FUNCTION ------------------------ #


def remove_prefixes_from_names_in_dict(dict_with_names_as_key):
    # We want a dictionary where each name has the prefixes
    # like Dr. and Professor removed. Because in Airtable prefixes
    # are stored separately.
    # I initially tried to do this within one for loop, by adding and
    # entry with the prefixes removed and pop()ing the old entry
    # but it turns out you should not change the dict size while
    # iterating. So now I first create a list of the names to be modified
    # and then using a second loop I add/pop form the dict
    # I keep the pair of names with and without prefixes in the
    # following dict
    logging.debug('Removing prefixes from names so they can be accurately compared to names in Airtable.')
    dictOfFolksWithPrefixes = {}
    logging.debug('Looping through list of persons and making a dict of those with prefixes')
    for aGuest in dict_with_names_as_key:
        ALLCAPS_listOfPrefixes = my_globals.list_of_possible_name_prefixes
        nameWithoutPrefixes = removeCommonPersonNamePrefixes(aGuest, ALLCAPS_listOfPrefixes)
        if nameWithoutPrefixes != aGuest.strip():
            dictOfFolksWithPrefixes[aGuest] = nameWithoutPrefixes

    logging.debug('Looping through dict of names that had prefixes removed, and adding them back to '
                  'the original list, but with the prefixes removed')
    for aGuestWithPrefixedName in dictOfFolksWithPrefixes:
        nameNoPrefix = dictOfFolksWithPrefixes[aGuestWithPrefixedName]
        dict_with_names_as_key[nameNoPrefix] = dict_with_names_as_key[aGuestWithPrefixedName]
        dict_with_names_as_key.pop(aGuestWithPrefixedName)

    # For some reason, there are some videos that have nobody in the 'featuring' field
    # so in the next line of code we get rid of that entry from the dictionary
    # becasue if some entry in Airtable also have an empty name for some reason
    # we don't want those two things to match.
    if '' in dict_with_names_as_key:
        dict_with_names_as_key.pop('')

    return dict_with_names_as_key


# ------------------------ END FUNCTION ------------------------ #


def push_guestsNsubjects_delta(dictGuestsFromAirtable, trial_run=True):
    """This function doesn't do a full push of the Guests and the Subjects
    they've talked about to Airtable. Instead it builds a list of only
    the Guests for whom their list of subjects has changed, and only
    updates those."""
    logging.debug('Updating guest and topics, only for guests where their topics have changed')
    # load the most recent file that contains guests and subjects info
    filesWithTheData = my_globals.str_dir4_manipd_JSON_website_guests_subjects + '/*'
    listOfGuestSubjectFiles = glob.glob(filesWithTheData)
    mostRecentFile = max(listOfGuestSubjectFiles, key=os.path.getctime)
    with open(mostRecentFile) as websiteGuestsSubjectsFile:
        logging.debug('Opening most recent file containing guest and topics info')
        dictWebsiteGuestsAndSubjectsWithPrefixes = json.load(websiteGuestsSubjectsFile)

    dictWebsiteGuestsAndSubjects = remove_prefixes_from_names_in_dict(dictWebsiteGuestsAndSubjectsWithPrefixes)

    tableToBeUpdated = 'Guests'
    fieldToBeUpdated = 'Topics Discussed'
    update_a_field_deltas_only(tableToBeUpdated, fieldToBeUpdated, dictGuestsFromAirtable,
                               my_globals.idx_fields_airT_tbl_guests_id,
                               my_globals.idx_fields_airT_tbl_guests_subjects,
                               dictWebsiteGuestsAndSubjects, trial_run)


# ------------------------ END FUNCTION ------------------------ #


def find_website_ppl_not_in_airtable(do_fresh_pull_of_airtable=False):
    """explain the function"""
    # load a dictionary of guests from airtable-based data (either from disk
    # or freshly pulled from Airtable) that is ordered around the key
    # being the guest's name.
    logging.debug('Finding folks that are listed on the website, but not in Airtable')
    list_of_guest_airtable_fields = my_globals.lst_fields_airT_tbl_guests
    local_airt_data = AirtTableSubsetOnDisk(my_globals.str_dir4_airt_ondisk_guests_by_name,
                                            my_globals.str_name_of_airt_guests_table,
                                            list_of_guest_airtable_fields,
                                            list_of_guest_airtable_fields[my_globals.idx_fields_airT_tbl_guests_name])
    logging.debug('Loading a dictionary of guests from Airtable')
    dictGuestsFromAirtable = local_airt_data.load_data(do_fresh_pull_of_airtable)

    # load the most recent file that contains guests and subjects info
    filesWithTheData = my_globals.str_dir4_manipd_JSON_website_guests_subjects + '/*'
    listOfGuestSubjectFiles = glob.glob(filesWithTheData)
    mostRecentFile = max(listOfGuestSubjectFiles, key=os.path.getctime)
    with open(mostRecentFile) as websiteGuestsSubjectsFile:
        logging.debug('Loading a dictionary of guests from the RV website')
        dictWebsiteGuestsAndSubjects = json.load(websiteGuestsSubjectsFile)

    # load the most recent file that contains interviewers info
    filesWithTheData = my_globals.str_dir4_manipd_JSON_website_interviewers + '/*'
    listOfInteviewersFiles = glob.glob(filesWithTheData)
    mostRecentFile = max(listOfInteviewersFiles, key=os.path.getctime)
    with open(mostRecentFile) as websiteInterviewersFile:
        logging.debug('Loading a dictionary of interviewers from the RV website')
        listWebsiteInterviewers = json.load(websiteInterviewersFile)

    # create a combined list of guests and interviewers that will be
    # searched for in our airtable dictionary.
    # the statement below 'unpacks' the dictionary because it is
    # iterable, and dicts when iterator return their keys.
    logging.debug('Creating a combined list of guests and interviwers from the RV website')
    listOfPersonsFromWebsite = []
    for aPerson in dictWebsiteGuestsAndSubjects:
        listOfPersonsFromWebsite.append(
            removeCommonPersonNamePrefixes(
                aPerson, my_globals.list_of_possible_name_prefixes))
    for aPerson in listWebsiteInterviewers:
        listOfPersonsFromWebsite.append(
            removeCommonPersonNamePrefixes(
                aPerson, my_globals.list_of_possible_name_prefixes))
    # remove duplicates
    listOfPersonsFromWebsite = list(dict.fromkeys(listOfPersonsFromWebsite))
    listOfPersonsFromWebsite.sort()
    listOfPersonsFromWebsite.remove('')

    # initialize counters
    int_persons_found = 0
    int_persons_not_found = 0
    int_total_persons = 0
    listPeepsNotFound = []
    logging.debug('Starting comparison of persons in the RV website and persons in Airtable')
    for aPerson in listOfPersonsFromWebsite:
        int_total_persons += 1
        if aPerson in dictGuestsFromAirtable:
            int_persons_found += 1
        else:
            int_persons_not_found += 1
            listPeepsNotFound.append(aPerson)
    logging.info('total peeps: ' + str(int_total_persons))
    logging.info('found: ' + str(int_persons_found))
    logging.info('not found: ' + str(int_persons_not_found))
    logging.info(json.dumps(listPeepsNotFound, indent=4))


# ------------------------ END FUNCTION ------------------------ #


def convert_ppl_names2another_airtable_field(str_a_listing_of_persons_to_search,
                                             idx_of_field_airT_tbl_guests):
    """This function receives a string. The string represents a
    listing of people (not a python list).
    The function checks if these people are known in Airtable.
     It returns a dictionary with two entries:
     1) An entry where the key is 'Found'
        The associated value of this entry is a list of the
        people, translated to some field in Airtable (it could
        be just left as is (the name) or it could be translated
        to something else like the IDs.
     2) An entry where the key is 'Not found'
        The associated value of this entry is a string with
        the names of people that were not found in airtable.
     """
    logging.debug('Starting function to convert people names into a different field from the '
                  'same Airtable row')
    # load the most recent Airtable guest dictionary
    # ordered by name.
    logging.debug('Loading a dictionary of guests from Aritable ordered by name')
    list_of_guest_airtable_fields = my_globals.lst_fields_airT_tbl_guests
    local_airt_data = AirtTableSubsetOnDisk(my_globals.str_dir4_airt_ondisk_guests_by_name,
                                            my_globals.str_name_of_airt_guests_table,
                                            list_of_guest_airtable_fields,
                                            list_of_guest_airtable_fields[my_globals.idx_fields_airT_tbl_guests_name])
    dictWithAirtableGuests = local_airt_data.load_data()
    lst_names_can_be_ignored = my_globals.odd_strings_in_website_persons_fields
    # split up the string of people featured
    # in the video into a python list using a function
    # we wrote that does this.
    listOfPersons = extractIndividualItemsFromTextList(
        str_a_listing_of_persons_to_search)
    # next we iterate through the list and see if all
    # the guests in the list are already in Airtable.
    # While we iterate, we construct a list of the other
    # Airtable field that the function was asked to
    # translate to (probably the ID)
    list_of_persons_airT_converted_field = []
    # Create a string to track if anyone was not found.
    str_ppl_not_found = ''
    logging.debug('Iterating through the list that was split into distinct items (from one string) '
                  'and associating each item with an Airtable field if possible')
    for aPerson in listOfPersons:
        name_no_prefix = \
            removeCommonPersonNamePrefixes(
                aPerson, my_globals.list_of_possible_name_prefixes)
        if aPerson in lst_names_can_be_ignored:
            name_no_prefix = 'Milton'
        if name_no_prefix in dictWithAirtableGuests:
            requested_field_data = dictWithAirtableGuests[
                name_no_prefix][idx_of_field_airT_tbl_guests]
            list_of_persons_airT_converted_field.append(requested_field_data)
        else:
            str_ppl_not_found += (aPerson + ', ')
    # in case we did add some folks to the list of people not found
    # let's remove the ', ' that was added along with the last person
    if str_ppl_not_found:
        str_ppl_not_found = str_ppl_not_found[:-len(', ')]
    # Now we construct the dict to be returned
    str_found = my_globals.str_to_use_if_something_has_been_found
    str_not_found = my_globals.str_to_use_if_something_has_not_been_found
    logging.debug('Returning a dictionary with items that were found (translated to another field '
                  'if requested) and items that were not found')
    return {str_found: list_of_persons_airT_converted_field,
            str_not_found: str_ppl_not_found}


# ------------------------ END FUNCTION ------------------------ #


def extractVideoFieldFromWebsiteJSON(theStructureWithTheData, airtable_field_name):
    """This function should be passed a dictionary that represents one video
    entry as returned by the Real Vision website's JSON dumps. From this
    a corresponding entry that we want to push to airtable will be extracted.
    For the translation, a couple of dictionaries are used which are defined
    in my_globals."""
    listOfFieldsToFormatAsDates = my_globals.list_web_date_fields_2make_human_readable
    listOfFieldsWhereDataIsAtEndOfURL = my_globals.list_fields_from_website_that_are_within_url
    dictTranslateWebsite2airtableFields = my_globals.dict_translate_vids_fields_website2airtable
    website_fields = dictTranslateWebsite2airtableFields[airtable_field_name]
    # sometimes we will want to combine fields from the website into one
    # field in Airtable (like topics and tags.) Because of this the fields
    # that we want to populate in Airtable are passed as a list, even if
    # it is just one field, then it is passed as a single item in a list.
    # This allows for the loop below to deal with all cases, rather than
    # making an if statement for the case where there is only one field
    # with an else for the case where fields are going to be combined. The
    # code inside the if and else would be very similar.
    # Because of this approach of combining items, we also temporarily put
    # the data to be returned into a list, and at the end, simply return
    # the only item in the list. This allows us to put lists (for example
    # of topics) inside the list, and then append another list (for
    # example of tags) to the first list.
    listOfDataToReturn = []
    for aField in website_fields:
        listDescribingSchema = (my_globals.dict_vids_from_website_schema[aField]).copy()
        data = recursiveExtractFieldFromHierarchy(theStructureWithTheData, listDescribingSchema)
        if aField in listOfFieldsWhereDataIsAtEndOfURL:
            data = extractIDstringFromURLstring(data)
        if aField in listOfFieldsToFormatAsDates:
            data = (datetime.fromtimestamp(data / 1000)).strftime("%Y-%m-%d")
        if len(listOfDataToReturn) == 0:
            listOfDataToReturn.append(data)
        elif len(listOfDataToReturn) > 0 and type(listOfDataToReturn[0]) is list:
            listOfDataToReturn[0].extend(data)
            listOfDataToReturn[0].sort()
            # remove duplicates
            listOfDataToReturn[0] = list(dict.fromkeys(listOfDataToReturn[0]))
        else:
            logging.error('Unexpected location within if/else statement. Exiting.')
            exit(0)
    return listOfDataToReturn[0]


# ------------------------ END FUNCTION ------------------------ #


def get_airt_guests_ordered_by_name(fresh_airt_pull=False):
    list_of_guest_airtable_fields = my_globals.lst_fields_airT_tbl_guests
    local_airt_guests = AirtTableSubsetOnDisk(my_globals.str_dir4_airt_ondisk_guests_by_name,
                                              my_globals.str_name_of_airt_guests_table,
                                              list_of_guest_airtable_fields,
                                              list_of_guest_airtable_fields[my_globals.idx_fields_airT_tbl_guests_name])
    return local_airt_guests.load_data(fresh_airt_pull)


# ------------------------ END FUNCTION ------------------------ #


def get_airt_shows_ordered_by_rvwebname(fresh_airt_pull=False):
    list_of_shows_airtable_fields = my_globals.lst_fields_airT_tbl_shows
    local_airt_shows = AirtTableSubsetOnDisk(my_globals.str_dir4_airt_ondisk_shows_by_name,
                                             my_globals.str_name_of_airt_shows_table,
                                             list_of_shows_airtable_fields,
                                             list_of_shows_airtable_fields[
                                                 my_globals.idx_fields_airT_tbl_shows_namefromweb])
    return local_airt_shows.load_data(fresh_airt_pull)


# ------------------------ END FUNCTION ------------------------ #


def get_airt_vids_ordered_by_rvwebid(fresh_airt_pull=False):
    list_of_vids_airtable_fields = my_globals.lst_fields_airT_tbl_videos
    local_airt_vids = AirtTableSubsetOnDisk(my_globals.str_dir4_airt_ondisk_vids_by_webid,
                                            my_globals.str_name_of_airt_videos_table,
                                            list_of_vids_airtable_fields,
                                            list_of_vids_airtable_fields[
                                                my_globals.idx_fields_airT_tbl_videos_website_id])
    return local_airt_vids.load_data(fresh_airt_pull)


# ------------------------ END FUNCTION ------------------------ #


def push_vids(str_table_name, dict_airt_vid_data, only_a_trial_run=True):  # noqa: C901
    start_time = time.time()

    # start some counters to print some statistics at the end
    # they are fairly self-explanatory. the 'would have added'
    # counter is for tracking what would have been added when
    # the function is being run as trial only (without actually
    # pushing to airtable.)
    num_items_total = 0
    num_items_trial_pushed_new = 0
    num_items_trial_pushed_update = 0
    num_items_trial_deleted = 0
    num_items_forrealz_pushed_new = 0
    num_items_forrealz_pushed_update = 0
    num_items_forrealz_deleted = 0
    num_items_not_touched = 0

    # this function is passed a dictionary with information about
    # videos that has been downloaded from airtable. Inside the dict
    # each key is the website ID of a video, and the value of that key
    # is a list. The indexes below, represent the information stored
    # in each position of the list. Note that the information is all
    # stored in Airtable, so even though one of the indexes represents the
    # RV website ID, it is in fact the website ID, BUT AS STORED IN
    # AIRTABLE. Same for the 'last updated timestamp' - that represents
    # a unix timestamp of the last time the video was updated ON THE
    # RV WEBSITE, but the timstamp here, is that timestamp BEING STORED
    # IN AIRTABLE. Storing this info in Airtable, allows us to know if
    # a row in Airtable needs to be updated, because if the timestamp stored
    # in airtable (for last-updated time) is still the same as it is on
    # the website, then that row does not need to change.
    idx_vid_website_id = my_globals.idx_fields_airT_tbl_videos_website_id
    idx_vid_airt_id = my_globals.idx_fields_airT_tbl_videos_airt_id

    # load the datastructure that contains metadata about each
    # video. This will allows us to iterate through the videos, with a defined
    # size of iteration so we can display percentages.
    airt_vids_DS = SimpleDS(my_globals.str_dir4_airt_vids_ds, my_globals.str_name_simpleds_airtable_records)
    airt_vids_DS.load()
    airt_vids_DS.sort()

    str_vid_tagged_for_repush = my_globals.str_tag_repush_to_airtable

    airT = None
    if not only_a_trial_run:
        airT = MyAirtWrapper(str_table_name)

    # now we iterate through the datastructure, and process each row and
    # its respective file on disk
    int_length_airt_vids_DS = len(airt_vids_DS)
    percent_tracker = PercentTracker(int_length_airt_vids_DS, log_level='info')
    for vid_id in airt_vids_DS:
        # the bool below is simply used to track videos that were
        # not touched (because they don't need uploading) - ie. they
        # didn't need either updating or adding.
        vid_tagged_for_uploading = False

        pushed_record = {}
        # check if the video is in airtable
        if vid_id in dict_airt_vid_data:
            lst_vid_fields_from_airt = dict_airt_vid_data[vid_id].copy()
            website_vid_id_stored_in_airt = lst_vid_fields_from_airt[idx_vid_website_id]
            airt_vid_id = lst_vid_fields_from_airt[idx_vid_airt_id]

            # I used to check timestamps of the metadata (based on the
            # timestamp another function generates IF it detects that
            # a video has changed), and compare time of last change detection
            # of the incoming data, with the timestamp stored in Airtable
            # to see if a video needed updating. But I've changed that to
            # simply push videos to airtable if an upstream function
            # tags the simpleds row as needing to be pushed.
            vid_was_tagged_for_repush = airt_vids_DS.tag_check(vid_id, str_vid_tagged_for_repush)

            if vid_was_tagged_for_repush:
                vid_tagged_for_uploading = True
                # We will not worry about only updating the fields that have
                # changed, we will simply push all fields up again for that record.
                # First we will make sure that the video's ID (from the RV website)
                # is the same in the data pulled from Airtable, as in the datastructure's
                # index, as inside the datastructure file. If it isn't something
                # is wrong.
                dict_vid_data_in_DS = airt_vids_DS.fetch_data(vid_id)
                if vid_id == website_vid_id_stored_in_airt and \
                        vid_id == dict_vid_data_in_DS['ID on RV website']:
                    if not only_a_trial_run:
                        try:
                            pushed_record = airT.update(airt_vid_id, dict_vid_data_in_DS, typecast=True)
                        except Exception as e:
                            logging.error('Unable to update airtable record for vid: '
                                          + vid_id + ' The Exception was: ' + repr(e))
                        if pushed_record:
                            num_items_forrealz_pushed_update += 1
                            if vid_was_tagged_for_repush:
                                # if the video had been tagged for re-push
                                # it has now been re-pushed, so the tag
                                # should be cleared.
                                airt_vids_DS.tag_remove(vid_id, str_vid_tagged_for_repush)
                            # The Airtable API accepts a maximum of 5 requests per second,
                            # so we take a little pause here to make sure we are abiding
                            # by that
                            time.sleep(.2)
                    else:
                        num_items_trial_pushed_update += 1
                else:
                    # at some point, the video IDs have got messed up and
                    # out of sync, which should not happen.
                    print('Unexpected situation where video ID is not'
                          ' the same in RV website, local datastore, and '
                          'Airtable. They should be the same.\n'
                          'Stopping program execution.')
                    exit(0)
        else:
            # this part of the code is reached if the video is not found
            # in the dictionary of data that was pulled from Airtable,
            # which means the video is not yet in Airtable and needs to
            # be pushed.
            vid_tagged_for_uploading = True
            if not only_a_trial_run:
                dict_vid_data_in_DS = airt_vids_DS.fetch_data(vid_id)
                try:
                    pushed_record = airT.insert(dict_vid_data_in_DS, typecast=True)
                except Exception as e:
                    logging.error('Unable to create airtable record for vid: '
                                  + vid_id + ' The Exception was: ' + repr(e))
                if pushed_record:
                    num_items_forrealz_pushed_new += 1
                    # The Airtable API accepts a maximum of 5 requests per second,
                    # so we take a little pause here to make sure we are abiding
                    # by that
                    time.sleep(.2)
            else:
                num_items_trial_pushed_new += 1

        if not vid_tagged_for_uploading:
            num_items_not_touched += 1

        num_items_total += 1

        # print percentages every 10% or so
        percent_tracker.update_progress(num_items_total,
                                        str_description_to_include_in_logging='pushing video records to Airtable')

    # now we find out if any records need to be deleted from Airtable
    set_vids_in_local_airtable_ds = airt_vids_DS.fetch_all_ids_as_python_set()
    set_vids_in_airtable = set(dict_airt_vid_data.keys())
    set_vids_removed_at_source = set_vids_in_airtable - set_vids_in_local_airtable_ds
    for entry in set_vids_removed_at_source:
        if not only_a_trial_run:
            try:
                # I believe the method below returns the deleted record. I used to store this
                # in a variable in case we needed it at some point, but pylama complained
                # about the variable not being used, so I've removed that.
                airT.delete(dict_airt_vid_data[entry][idx_vid_airt_id])
                num_items_forrealz_deleted += 1
                num_items_not_touched -= 1
                # The Airtable API accepts a maximum of 5 requests per second,
                # so we take a little pause here to make sure we are abiding
                # by that
                time.sleep(.2)
            except Exception as e:
                logging.error('Unable to DELETE airtable record for vid (RV website ID): '
                              + entry + ' The Exception was: ' + repr(e))
        else:
            num_items_trial_deleted += 1
            num_items_not_touched -= 1

    airt_vids_DS.save2disk()
    end_time = time.time()

    list_of_lines_2print = ['Trial-pushed - new:         ' + str(num_items_trial_pushed_new),
                            'Trial-pushed - updated:     ' + str(num_items_trial_pushed_update),
                            'Trial - deleted:            ' + str(num_items_trial_deleted),
                            'Pushed for realZ - new:     ' + str(num_items_forrealz_pushed_new),
                            'Pushed for realZ - updated: ' + str(num_items_forrealz_pushed_update),
                            'Pushed for realZ - deleted: ' + str(num_items_forrealz_deleted),
                            'Not touched:                ' + str(num_items_not_touched),
                            'Total items in the list:    ' + str(int_length_airt_vids_DS),
                            'Total processed:            ' + str(num_items_total),
                            'Time it took to run this function is: '
                            + "{:.2f}".format((end_time - start_time) / 60) + ' minutes.']

    for line in list_of_lines_2print:
        logging.info(line)
    # ------------------------ END FUNCTION ------------------------ #
