import os
import glob
import json
import logging
from class_myairt_wrapper import MyAirtWrapper
from my_building_blocks import projectStandardTimestamp
from my_building_blocks import cleanup_older_files_in_a_dir


class AirtTableSubsetOnDisk:
    """A class for pulling a subset of an airtable table down to
    disk, and to re-organize that data by a different key (Airtable
    dumps the data organized by the Airtable ID of each row, which
    isn't necessarily the format it is most useful in.)
    NOTE the class expects the subdirectories and parent directory
    to be created already, before the class is initialized/instanciated."""

    subdir_raw_airt_data = 'raw_data/'
    subdir_translated_airt_data = 'translated_data/'

    def __init__(self, full_directory_path,
                 str_airt_table, lst_fields_to_store, str_field_to_make_key):
        """This function receives the directory path where
        the data for the class is going to be stored. It stores
        the raw dump of data, and the translated data in two
        subdirectories.
        The other paramenter of the function is the field
        by which the data will be reorganized."""
        logging.debug('Initializing instance of AirtTableSubsetOnDisk()')
        self.path = full_directory_path
        self.path_raw = self.path + self.subdir_raw_airt_data
        self.path_translated = self.path + self.subdir_translated_airt_data
        self.files_ext = '.json'
        self.airt_table = str_airt_table
        self.lst_fields_from_airt = lst_fields_to_store
        self.primary_key = str_field_to_make_key

    # ------------------------ END FUNCTION ------------------------ #

    def __contains__(self, item_id):
        """This dunder/magic method makes it possible to check
        if a particular """
        pass

    # ------------------------ END FUNCTION ------------------------ #

    def load_data(self, fresh_pull_from_airt=False):
        if fresh_pull_from_airt:
            # if a fresh pull of data from airtable has been requested,
            # then we do that first, which saves the freshly pulled
            # data to disk.
            self.pull_fresh_airt_data()
            # then you must also convert the freshly pulled data to
            # the translated dictionary
            self.translate_raw_data()
        # load the most recent converted data from disk
        return self.load_translated_dict()

    # ------------------------ END FUNCTION ------------------------ #

    def pull_fresh_airt_data(self):
        """a function to pull the data in a list of fields down from a table in Airtable"""
        airT = MyAirtWrapper(self.airt_table)
        # below, we remove 'id' from the list because Airtable always returns the
        # id and the date anyways.
        list_of_fields_copy = self.lst_fields_from_airt.copy()
        list_of_fields_copy.remove('id')
        logging.debug("Calling Airtable wrapper 'get_all' method.")
        listAirtableRecords = airT.get_all(fields=list_of_fields_copy)
        # now write the list of records from the Airtable table to a file
        filename = self.path_raw + projectStandardTimestamp() + self.files_ext
        with open(filename, mode='w') as airt_data_file:
            logging.debug("Writing data pulled from Airtable (using the 'get_all' method) to disk.")
            json.dump(listAirtableRecords, airt_data_file)
        # I used to keep all the files from each and every pull on disk, but naturally
        # this started to consume non-trivial amounts of disk space and these files are not
        # really needed after they are used, so I'll do some clean-up now after every fresh
        # pull of data.
        cleanup_older_files_in_a_dir(self.path_raw)

    # ------------------------ END FUNCTION ------------------------ #

    def translate_raw_data(self):
        logging.debug('Beginning function that translates downloaded Airtable data into a reordered dictionary')
        # open the raw data that was pulled from an Airtable table to disk
        listOfAirtableDataFiles = glob.glob(self.path_raw + '*')
        most_recent_file = max(listOfAirtableDataFiles, key=os.path.getctime)
        with open(most_recent_file) as airt_data_json_file:
            logging.debug('Loading from disk data that was pulled from Airtable')
            lst_airt_rawdata_rows = json.load(airt_data_json_file)
        # now convert the awkward data that gets sent back (as a list of dictionaries)
        # into a dictionary that makes more sense, where the requested field is the
        # key
        dict_reordered_by_requested_field = {}
        logging.debug('Looping through the records retrived from Airtable')
        for aRecord in lst_airt_rawdata_rows:
            # airtable returns the data in a dictionary that has an 'id'
            # key, and the data is inside another key called 'fields'
            strCurrentRecordID = aRecord['id'].strip()
            dctCurrentRecordFields = aRecord['fields']
            listOfFieldsToReturnInsideTheDict = []
            # the IF below makes sure that the current record, in its
            # dictionary of 'fields/data' has a field that contains
            # the field that is going to be the key of the dictionary
            # that will be returned, because I ran into execution once that
            # exited because the record didn't have a 'name' field for
            # example. However, in the IF below, we also need to make dispensation
            # for the primary key being requested being the 'id', because
            # that lives outside the 'fields' area of the dictionary.
            fieldname_to_make_dict_key = self.primary_key
            if (fieldname_to_make_dict_key in dctCurrentRecordFields) or \
                    (fieldname_to_make_dict_key == 'id'):
                # and if we do have it, then we do what we came here to do
                if fieldname_to_make_dict_key == 'id':
                    strCurrentRecordKey = strCurrentRecordID
                else:
                    strCurrentRecordKey = dctCurrentRecordFields[fieldname_to_make_dict_key].strip()
                list_of_fields = self.lst_fields_from_airt.copy()
                for aField in list_of_fields:
                    # the if/else below makes sure that the field being populated
                    # exists, because Airtable seems to not return a field at all if
                    # it is empty in Airtable. If it doesn't exist, then it just
                    # appends an empty string to the list of data
                    if aField in dctCurrentRecordFields or aField == 'id':
                        # the IF below checks if 'id' is the field being
                        # pupulated, and if so, because the 'id' lives outside
                        # of the 'fields' area, then slightly different code is
                        # required than for other fields.
                        if aField == 'id':
                            listOfFieldsToReturnInsideTheDict.append(strCurrentRecordID)
                        else:
                            if type(dctCurrentRecordFields[aField]) is str:
                                listOfFieldsToReturnInsideTheDict.append((dctCurrentRecordFields[aField]).strip())
                            else:
                                listOfFieldsToReturnInsideTheDict.append(dctCurrentRecordFields[aField])
                    else:
                        listOfFieldsToReturnInsideTheDict.append('')
                if strCurrentRecordKey not in dict_reordered_by_requested_field:
                    dict_reordered_by_requested_field[strCurrentRecordKey] = listOfFieldsToReturnInsideTheDict
                else:
                    logging.warning("DUPLICATE entry would be caused for key: " + strCurrentRecordKey + " while"
                                    " attempting to create a re-ordered dictionary in function 'translate_raw_data'."
                                    " The second instance is not being added to the dictionary.")
            else:
                logging.warning(
                    'fieldname_to_make_dict_key not found in dctCurrentRecordFields of record: ' + str(aRecord))
                logging.warning('Continuing program execution anyways.')

        filename = self.path_translated + projectStandardTimestamp() + self.files_ext
        with open(filename, mode='w') as airt_data_file:
            logging.debug('Saving re-ordered dictionary to disk')
            json.dump(dict_reordered_by_requested_field, airt_data_file)

        # I used to keep all the files from each and every translation on disk, but naturally
        # this started to consume non-trivial amounts of disk space and these files are not
        # really needed after they are used, so I'll do some clean-up now after every fresh
        # pull of data.
        cleanup_older_files_in_a_dir(self.path_translated)

    # ------------------------ END FUNCTION ------------------------ #

    def load_translated_dict(self):
        # open and return the most recently translated data file
        listOfAirtableDataFiles = glob.glob(self.path_translated + '*')
        most_recent_file = max(listOfAirtableDataFiles, key=os.path.getctime)
        with open(most_recent_file) as airt_data_json_file:
            logging.debug('Loading re-ordered dictionary from disk')
            return json.load(airt_data_json_file)
    # ------------------------ END FUNCTION ------------------------ #
