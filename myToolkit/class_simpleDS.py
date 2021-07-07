import os
import time
import json
import glob
import logging
import pandas as pd


class SimpleDS:
    """A class for creating a simple Data Structure stored
    as a combination of a pandas dataframe, a set of 'current' files
    (one file per row in the dataframe) containing the
    actual data, and a set of files tracking historical
    changes. The pandas dataframe will contain an ID
    for each row, a timestamp of the time the data was created, a timestamp
    of the last time the data changed, and the
    names of the two aforementioned files associated with each row.
    Then, there will be column that might be useful in some cases,
    and not in others: a column to store a hash of the data (in some
    cases this will be useful to know if incoming data has been
    updated.)
    Finally, the last column is a place for functions that are
    manipulating the data to store tags. This can be used to assit
    with workflows. For example, if a function
    is doing some conversion X to the data, and the conversion fails
    it could store the tag 'conversion_to_X' in the tags column, so
    the problem can be rectified by, maybe the same function, or
    maybe another one.
    Tags are not added at the time of adding or updating a row, instead
    they have their own function for addition.
    The pandas dataframe is stored at the root of the directory that is
    passed to the initialization function of the class.
    The data files are stored in a subdirectory (named below), and the
    change log files are stored in another sub-directory (also named
    below.)
    IMPORTANT NOTE. It is assumed that the data to be stored is a python
    dictionary."""
    ds_field_dataid = 'ID'
    ds_field_datalastupdated = 'DATA-UPDATED'
    ds_field_datacreated = 'DATA-CREATED'
    ds_field_datafilename = 'FILE-CURRENT'
    ds_field_datafilename_changes = 'FILE-CHANGES'
    ds_field_hash = 'HASH'
    ds_field_datatags = 'TAGS'
    ds_dict_columns2indexes = {ds_field_dataid: 0,
                               ds_field_datalastupdated: 1,
                               ds_field_datacreated: 2,
                               ds_field_datafilename: 3,
                               ds_field_datafilename_changes: 4,
                               ds_field_hash: 5,
                               ds_field_datatags: 6}
    str_subdir_data = 'current_data/'
    str_subdir_prev_vrsns = 'previous_versions/'
    str_to_use_when_no_change_log = 'nolog'
    files_ext = '.json'
    filename_changes_append = '_changes'
    filename_deletions_append = '_deleted'

    def __init__(self, full_directory_path, name=''):
        """This function receives the directory path where the instance of SimpleDS
        stores its files. It also receives an optimal 'name' parameter. This has
        little effect other than being used for identifying the instance of
        SimpleDS in logging."""
        logging.debug('Initializing an instance of SimpleDS')
        self.path = full_directory_path
        self.path_files = self.path + self.str_subdir_data
        self.path_files_changelog = self.path + self.str_subdir_prev_vrsns
        self.df = pd.DataFrame(columns=self.ds_dict_columns2indexes.keys())
        self.fullpath_to_df = self.path + 'pandasdf.csv'
        self.name = name

    # ------------------------ END FUNCTION ------------------------ #

    def __contains__(self, item_id):
        """This dunder/magic method makes it possible to check
        if a particular string representing the ID of a data
        row is in the data structure."""
        if item_id in self.df.index:
            return True
        else:
            return False

    # ------------------------ END FUNCTION ------------------------ #

    def __len__(self):
        return len(self.df)

    # ------------------------ END FUNCTION ------------------------ #

    def __iter__(self):
        # the iterator will iterate through all the rows
        # of the dataframe by integer index, starting
        # with the first (zero) row
        self.counter = 0
        return self

    # ------------------------ END FUNCTION ------------------------ #

    def __next__(self):
        # the iterator will iterate through all the rows
        # of the dataframe by integer location and return
        # the data 'ID' of that row
        if self.counter < len(self.df):
            data_id = \
                self.df.iloc[self.counter,
                             self.ds_dict_columns2indexes[self.ds_field_dataid]]
            self.counter += 1
            return data_id
        else:
            raise StopIteration

    # ------------------------ END FUNCTION ------------------------ #

    def load(self):
        if not os.path.isfile(self.fullpath_to_df):
            # if the file for the pandas dataframe does NOT exist
            # in the directory given by the user, then ask the user
            # if they want to create it.
            print('Currently there is no dataframe saved to disk.')
            print('If you create one, any CURRENT files in the data '
                  'subdirectory will be deleted.')
            print('Would you like to create the dataframe file?')
            user_input = input("If so, type 'yes' (any other string will "
                               "stop program execution): ")
            if user_input == 'yes':
                self.__wipe_currentfiles()
                # an empty dataframe (with correct headers) was already
                # initialized above. Here we make some adjustments to it
                # before saving it to disk for the first time.
                self.df.set_index('ID', drop=False, inplace=True)
                self.save2disk()
            else:
                # then the user did not want to create the dataframe
                # file, so execution is stopped
                print('No dataframe file available.')
                exit(0)
        # now if we reach this part of the code, the file for the
        # dataframe should exist.
        # the line below loads the datastructure's dataframe - NOTE THAT
        # it is necessary to use a converter on the 'TAGS' column because
        # otherwise it is loaded as a string, instead of a list.
        logging.debug('Loading SimpleDS dataframe from disk: ' + self.name)
        self.df = pd.read_csv(self.fullpath_to_df, sep='\t', converters={self.ds_field_datatags: eval})
        self.df.set_index('ID', drop=False, inplace=True)
        # because we keep the column containing the name of the change log empty
        # until it is needed, there can be type errors when trying to read a value that is
        # nan, so here we make the column specifically a string.
        self.df[self.ds_field_datafilename_changes] = self.df[self.ds_field_datafilename_changes].astype(str)
        self.check_status_okay()

    # ------------------------ END FUNCTION ------------------------ #

    def fetch_lastupdated(self, item_id):
        return int(self.df.loc[item_id, self.ds_field_datalastupdated])

    # ------------------------ END FUNCTION ------------------------ #

    def fetch_created(self, item_id):
        return int(self.df.loc[item_id, self.ds_field_datacreated])

    # ------------------------ END FUNCTION ------------------------ #

    def fetch_data(self, item_id):
        """Function loads (from disk) and returns the actual
        data corresponding to the ID that was passed"""
        data_filename = item_id + self.files_ext
        with open(self.path_files + data_filename, mode='r') as datafile:
            return json.load(datafile)

    # ------------------------ END FUNCTION ------------------------ #

    def fetch_hash(self, item_id):
        return self.df.loc[item_id, self.ds_field_hash]

    # ------------------------ END FUNCTION ------------------------ #

    def fetch_all_ids_as_python_set(self):
        return set(self.df.index)

    # ------------------------ END FUNCTION ------------------------ #

    def add_entry(self, item_id, timestamp_dataupdated,
                  timestamp_datacreated, the_data, data_hash=''):
        logging.debug('Adding entry to SimpleDS: ' + self.name)
        # to add an entry:
        # - create a row to be added to the dataframe, which
        # contains the correct data for each column
        # - create/save the file with the incoming data
        entry_filename = item_id + self.files_ext
        # we don't populate the change log entry until the
        # entry is updated, so we just insert a dummy string.
        # This is deliberate, rather than an empty string, because when
        # reloading the dataframe, it can cause issues because pandas
        # converts empty 'cells' to floats
        entry_filename_changelog = self.str_to_use_when_no_change_log
        # write the data to the dataframe.
        # The 'loc' syntax below is nice because if the data exists, it
        # overwrites, and if it doesn't, it adds. In this case/method, we are
        # only ever adding, but it is good to remember that this is the
        # behaviour.
        logging.debug('Adding data to the dataframe: ' + self.name)
        self.df.loc[item_id] = [item_id, timestamp_dataupdated, timestamp_datacreated,
                                entry_filename, entry_filename_changelog, data_hash, []]
        # The last item in the list above is a list corresponding to the column
        # in the dataframe for tags. Tags are stored in a list. Since, at create
        # there are no tags, an empty list is initialized.

        # write the incoming data to disk
        with open(self.path_files + entry_filename, mode='w') as datafile:
            logging.debug('Saving metadata to file as part of SimpleDS: ' + self.name)
            json.dump(the_data, datafile)

    # ------------------------ END FUNCTION ------------------------ #

    def update_entry(self, item_id, new_data, update_timestamp, created_timestamp='', new_data_hash='',
                     dict_of_the_changes={}, log_changes=False):
        """This method updates an entry in SimpleDS.
        IMPORTANT NOTE. This method does not detect changes. If changes are
        to be tracked in a log, the changes should already be detected in
        a dictionary, and passed to this function as a parameter."""
        # to update an entry, the following things are needed:
        # - update the 'last updated' field
        # - if asked to do so, append a log of the changes to the overall
        # change log
        # - update the 'current data' file with the incoming data
        # NOTE: THAT this method assumes that the 'data created' timestamp generally
        # does not change. However, for cases where this data might change, an optional
        # argument is provided to be able to update the 'data created' column.
        logging.debug('Updating an entry in SimpleDS: ' + self.name)
        # update the stored timestamp about when the data was last updated
        self.df.loc[item_id, self.ds_field_datalastupdated] = update_timestamp
        # if it was provided, update the stored hash
        if new_data_hash:
            self.df.loc[item_id, self.ds_field_hash] = new_data_hash
        # if it was provided,
        # update the stored timestamp about when the data was created AT SOURCE (not within SmipleDS)
        if created_timestamp:
            self.df.loc[item_id, self.ds_field_datacreated] = created_timestamp
        filepath = self.path_files + self.df.loc[item_id, self.ds_field_datafilename]
        if log_changes:
            # check to see if a change log file already exists for this item
            filename_changelog = self.df.loc[item_id, self.ds_field_datafilename_changes]
            # because of the way the dataframe is loaded, we can't do the usual
            # check for whether the string is empty. Instead, if the filename
            # has not been saved, the df will load a string with value 'nan'
            if filename_changelog == self.str_to_use_when_no_change_log:
                # if the change_log filename does not exist yet in SimpleDS then
                # we need to create it, and create the file with an empty dictionary
                # in it as well.
                filename_changelog = item_id + self.filename_changes_append + self.files_ext
                self.df.loc[item_id, self.ds_field_datafilename_changes] = filename_changelog
                fullpath_change_log = self.path_files_changelog + filename_changelog
                with open(fullpath_change_log, mode='w') as change_log_file:
                    change_log_file.write('{}')
            # now regardless of whether the file existed to start with, it should exist now
            # so it can be read to get the current contents, and then appended with the changes
            dict_change_log = {}
            fullpath_change_log = self.path_files_changelog + filename_changelog
            with open(fullpath_change_log, mode='r') as change_log_file:
                dict_change_log = json.load(change_log_file)
            # now we add to the existing dictionary of changes, a dictionary
            # where the key is the current timestamp, and the value is the dictionary
            # of changes passed to this method.
            timestamp_now = int(round(time.time() * 1000))
            dict_change_log[timestamp_now] = dict_of_the_changes
            with open(fullpath_change_log, mode='w') as change_log_file:
                json.dump(dict_change_log, change_log_file)

        # now overwrite the exiting data file with the incoming
        # data passed to the method
        with open(filepath, mode='w') as datafile:
            logging.debug('Saving new incoming data to disk as part of SimpleDS: ' + self.name)
            json.dump(new_data, datafile)

    # ------------------------ END FUNCTION ------------------------ #

    def delete_entry(self, item_id, keep_version_of_file_in_log_directory=True):
        """This method removes an entry from the SimpleDS. To do this one must
        remove the file, and then remove the row in the dataframe.
        There are two options for removing the file. It can be deleted completely,
        or it can simply be renamed and moved to the 'change log' directory (but
        removing it from the main 'current' directory of the instance of SimpleDS."""
        fullfilepath_current = self.path_files + self.df.loc[item_id, self.ds_field_datafilename]
        if keep_version_of_file_in_log_directory:
            # if the method was requested to keep a version of the file, then we
            # need to move/rename it
            base_filename_path = self.path_files_changelog + item_id + self.filename_deletions_append
            # NOTE. It is unlikely that the same video ID will be deleted multiple times, however, just
            # in case, we do check for this.
            file_pattern_to_match = base_filename_path + '*'
            list_of_previous_deletion_files = glob.glob(file_pattern_to_match)
            # get the number of files that already have the 'deletion' base filename for this particular ID
            num_previous_deletions = len(list_of_previous_deletion_files)
            # now construct the full path for the deletion file
            full_deletion_file_path = base_filename_path + '_' + str(num_previous_deletions) + self.files_ext
            try:
                os.rename(fullfilepath_current, full_deletion_file_path)
            except Exception as e:
                logging.warning('There was a problem while attempting to ARCHIVE a file that is part of an instance of'
                                ' of SimpleDS. The Exception is:' + repr(e))
        else:
            # we are in this part of the IF/ELSE if we don't want to keep a version of the current file,
            # we just want to delete it.
            try:
                fullfilepath_current = self.path_files + self.df.loc[item_id, self.ds_field_datafilename]
                os.remove(fullfilepath_current)
            except Exception as e:
                logging.warning('There was a problem while attempting to DELETE a file that is part of an instance of'
                                ' of SimpleDS. The Exception is:' + repr(e))
        try:
            self.df.drop(index=item_id, inplace=True)
        except Exception as e:
            logging.warning('There was a problem while attempting to remove an entry from the dataframe inside'
                            ' an instance of SimpleDS. The Exception is: ' + repr(e))

    # ------------------------ END FUNCTION ------------------------ #

    def update_hash(self, item_id, str_hash):
        self.df.loc[item_id, self.ds_field_hash] = str_hash

    # ------------------------ END FUNCTION ------------------------ #

    def tag_add(self, item_id, str_tag):
        """This function adds a tag for the data represented
        by item_id. Tags can allow workflows between different
        functions using the datastore, or different runs of
        the same function to pass messages between each other."""
        # The tag is only added if it isn't in the list already.
        if str_tag not in self.df.loc[item_id, self.ds_field_datatags]:
            logging.debug('Adding tag to row: ' + item_id + ' in SimpleDS: ' + self.name)
            self.df.loc[item_id, self.ds_field_datatags].append(str_tag)

    # ------------------------ END FUNCTION ------------------------ #

    def tag_add_all_rows(self, str_tag):
        """This function adds a tag to all of the rows in the
        dataframe. Tags can allow workflows between different
        functions using the datastore, or different runs of
        the same function to pass messages between each other."""
        # The tag is only added if it isn't in the list already.
        logging.debug('Adding a tag to all rows in SimpleDS: ' + self.name)
        for an_entry in self:
            if str_tag not in self.df.loc[an_entry, self.ds_field_datatags]:
                self.df.loc[an_entry, self.ds_field_datatags].append(str_tag)

    # ------------------------ END FUNCTION ------------------------ #

    def tag_remove(self, item_id, str_tag):
        """This function removes a tag for the data represented
        by item_id. Tags can allow workflows between different
        functions using the datastore, or different runs of
        the same function to pass messages between each other."""
        logging.debug('Removing tag from row: ' + item_id + ' in SimpleDS: ' + self.name)
        if str_tag in self.df.loc[item_id, self.ds_field_datatags]:
            self.df.loc[item_id, self.ds_field_datatags].remove(str_tag)

    # ------------------------ END FUNCTION ------------------------ #

    def tag_remove_all_rows(self, str_tag):
        """This function removes a tag from all of the rows in the
        dataframe that have the given tag. Tags can allow workflows between different
        functions using the datastore, or different runs of
        the same function to pass messages between each other."""
        logging.debug('Removing tag: ' + str_tag + ' from all rows in SimpleDS: ' + self.name)
        for an_entry in self:
            if str_tag in self.df.loc[an_entry, self.ds_field_datatags]:
                self.df.loc[an_entry, self.ds_field_datatags].remove(str_tag)

    # ------------------------ END FUNCTION ------------------------ #

    def tag_check(self, item_id, str_tag):
        """Method returns a boolean value indicating whether the tag
        exists in tags column for a particular row or not."""
        return str_tag in self.df.loc[item_id, self.ds_field_datatags]

    # ------------------------ END FUNCTION ------------------------ #

    def tag_replace(self, item_id, str_tag_existing, str_tag_new):
        """Method replaces an existing tag, with a new tag.
        The method does not fail if the given "existing" tag does not exist.
        In this case (where the 'existing' tag passed to the method is
        not present in the given row in SimpleDS), the method simply does nothing."""
        tag_is_currently_in_simpleds_row = self.tag_check(item_id, str_tag_existing)
        if tag_is_currently_in_simpleds_row:
            self.tag_remove(item_id, str_tag_existing)
            self.tag_add(item_id, str_tag_new)

    # ------------------------ END FUNCTION ------------------------ #

    def sort(self, col_name='DATA-CREATED', ascending=False):
        logging.debug('Sorting instance of SimpleDS: ' + self.name)
        self.df.sort_values(by=[col_name], ascending=ascending, inplace=True)

    # ------------------------ END FUNCTION ------------------------ #

    def check_status_okay(self):
        logging.debug('Checking status of SimpleDS: ' + self.name)
        # a function to do some error checking.
        # Check to see if the size of the dataframe
        # is the same as the number of files in the
        # data directory.
        status_okay = True
        # check if the number of files in the data subdirectory
        # and the size of the pandas dataframe are the same.
        num_files = 0
        set_of_files = set()
        for entry in os.scandir(self.path_files):
            if not entry.name.startswith('.'):
                num_files += 1
                filename_no_json_extension = entry.name
                if entry.name.endswith('.json'):
                    filename_no_json_extension = entry.name[:-5]
                set_of_files.add(filename_no_json_extension)
        if num_files != len(self.df):
            status_okay = False
        if not status_okay:
            logging.error('Issue with consistency of the datastructure! ' + self.name)
            # if the consistency is not well, we find out what the discrepancy is
            set_of_entries_ds = set(self.df.index)
            logging.error("Entries in dataframe that don't have an associated file: " +
                          str(list(set_of_entries_ds.difference(set_of_files))))
            logging.error("Files that don't have an entry in the dataframe: " +
                          str(list(set_of_files.difference(set_of_entries_ds))))

        return status_okay

    # ------------------------ END FUNCTION ------------------------ #

    def save2disk(self):
        self.check_status_okay()
        separator = '\t'
        save_index = False
        logging.debug('Saving dataframe to CSV with separator -> ' + separator + ' and saving index =' +
                      str(save_index) + '. ' + self.name)
        self.df.to_csv(self.fullpath_to_df, sep=separator, index=save_index)

    # ------------------------ END FUNCTION ------------------------ #

    def delete_all_items_with_specific_tag_and_save_2disk(self, str_tag,
                                                          keep_deleted_files_in_change_log=True, trial_run=True):
        """This method deletes all entries in the SimpleDS that have been tagged with
        a specific tag, and RETURNS a set with all the IDs (the values of the Index of the
        dataframe for each entry that was removed."""
        # first, to avoid looping through the entire dataframe, we find the subset of the df that
        # actually has data in the tags column
        # first we setup the criteria, which is that we convert the 'tags' column to a string, and
        # (still in the same line of code) make sure that the string representation isn't an empty
        # list. (I could not figure out a way to do this without converting to the string representation)
        criteria = self.df[self.ds_field_datatags].astype(str) != '[]'
        # once we have the criteria, we can use it to create a subset of the dataframe, where the
        # tags column is NOT an empty list
        df_subset = self.df[criteria]
        # And once we have this subset, we can create a set of the IDs in the index to loop through
        # and look for the tag to be deleted.
        set_of_ids_to_delete = set(df_subset.index)
        set_of_ids_deleted = set()
        # we enclose the loop in a try/except pair, so that if some
        # of the deletions succeed, and then one fails, the df
        # still gets saved to disk.
        try:
            for item in set_of_ids_to_delete:
                if self.tag_check(item, str_tag):
                    if not trial_run:
                        self.delete_entry(item, keep_version_of_file_in_log_directory=keep_deleted_files_in_change_log)
                    set_of_ids_deleted.add(item)
        except Exception as e:
            logging.error('There was a problem while deleting all rows with a particular tag.'
                          ' The Exception was: ' + repr(e))
        self.save2disk()
        return set_of_ids_deleted

    # ------------------------ END FUNCTION ------------------------ #

    def wipe(self):
        # this function deletes the existing pandas dataframe if
        # it exists, and also clears all 'current' data files. It
        # does not do anything to the files containing previous
        # versions of the data
        self.__wipe_dfcsv()
        self.__wipe_currentfiles()

    # ------------------------ END FUNCTION ------------------------ #

    def __wipe_dfcsv(self):
        # this is a 'private' function (although there isn't
        # really such a thing in python) that deletes the file
        # that stores the dataframe on disk as CSV
        logging.debug('Deleting CSV file that contains the main dataframe of a SimpleDS instance: ' + self.name)
        os.remove(self.fullpath_to_df)

    # ------------------------ END FUNCTION ------------------------ #

    def __wipe_currentfiles(self):
        # this is a 'private' function (although there isn't really
        # really such a thing in python) that deletes all the files
        # in the database directory that stores the 'current 'data
        # files.
        lst_of_filenames = []
        # make a list of the files in the directory that contains
        # 'current' data.
        for entry in os.scandir(self.path_files):
            lst_of_filenames.append(entry.path)
        # then delete each of those files. This can maybe be done
        # in the same loop, but I'm not sure if that would count
        # as changing the contents of a structure while you iterate
        # it, so I'm erring on the side of creating a list of the
        # files first, and then deleting using that list.
        logging.debug('Deleting all metadata files that form part of a SimpleDS'
                      ' instance (NOT deleting any chang log files): ' + self.name)
        for path in lst_of_filenames:
            os.remove(path)

    def __wipe_changelog_column(self):
        # this is a 'private' function (although there isn't
        # really such a thing in python) that wipes all data inside
        # the column of the dataframe that is intended to store
        # date-creation timestamps of the data
        self.df[self.ds_field_datafilename_changes] = self.str_to_use_when_no_change_log
    # ------------------------ END FUNCTION ------------------------ #
