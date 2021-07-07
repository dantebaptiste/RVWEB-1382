import os
import pandas as pd
import logging
from numpy import log
import my_globals
from class_simpleDS import SimpleDS
from class_trancript import Transcript
from class_percent_tracker import PercentTracker
from class_rv_website_json_vid import RVwebsiteVid
from my_building_blocks import convert_file_to_list_by_lines, tokenize_list_containing_people_fullnames


class TranscriptAnalysis:
    """This class uses the Transcript class to build and manage a corpus
    of document transcripts, and thereby generates the data for
    Term-Frequency-Inverse-Document-Frequency, for example.
    This class has "inside knowledge" about the data where the raw video
    information is stored. That data is stored in a class called SimpleDS
    which has several idiosincracies. For example, the transcript is stored
    inside json data, represented in python as a dictionary, and this class
    assumes the knownledge of knowing how to access the transcript from
    there."""
    str_transcript_json_field_name = my_globals.str_vid_transcript
    __str_tag_vid_data_in_doc_count = 'included_current_doc_count'
    __separator = '\t'
    __str_cheeky_document_counter = 'num_of_docs_that_have_contributed_to_the_vector'
    __column_name_terms = my_globals.str_trnscrpt_class_column_terms
    __column_name_count = my_globals.str_trnscrpt_class_column_count

    def __init__(self, str_path_for_tfidf_data, str_path_transcripts_simpleds,
                 str_path_transcript_files, variable_manger, num_vids_to_use=10):
        """This method initializes the object.
        It receives the directory
        which already stores an instance of SimpleDS, which contains video
        information, including the transcript.
        It also receives a request to process a certain number of videos
        for this particular instance of the class. This will result in
        num_vids_to_use being processed for all activities
        this class does. If ALL videos are wanted, then -1 should be
        passed as the argument."""
        logging.debug('Initializing instance of TranscriptAnalysis() class')
        self.str_path_to_transcripts_files = str_path_transcript_files
        self.fullpath_doc_count_vector_as_csv = str_path_for_tfidf_data + 'doc_count_vector.csv'
        self.transcripts_ds = SimpleDS(str_path_transcripts_simpleds)
        self.transcripts_ds.load()
        self.transcripts_ds.sort()
        self.df_doc_count_vector = pd.DataFrame()
        self.var_mgr = variable_manger
        if num_vids_to_use == -1:
            self.num_vids_to_use = len(self.transcripts_ds)
        else:
            self.num_vids_to_use = num_vids_to_use

    # ------------------------ END FUNCTION ------------------------ #

    def update_term_count_dataframes(self):
        """This method updates any missing transcript term COUNT info in the transcripts
        SimpleDS and on disk. NOTE that the info we are keeping on disk is term-COUNT and not
        term-frequency. It is very simple to convert from term-count to term-frequency, but
        not vice-versa. So later on in the process when we need term-frequency, we simply
        create it, but on disk we store the version of the data that is most granular."""
        logging.info('Starting method that updates missing Term COUNT info on disk')
        list_records_no_termcount_data_on_disk = self.__look__missing_termcount_info()
        num_missing_tc_entries = len(list_records_no_termcount_data_on_disk)
        counter = 0
        num_vids_success = 0
        percent_tracker = PercentTracker(num_missing_tc_entries,
                                         int_output_every_x_percent=1, log_level='info')
        logging.info(str(num_missing_tc_entries) + " records don't have term-COUNT data on disk.")
        for a_vid in list_records_no_termcount_data_on_disk:
            execution_should_continue = self.var_mgr.var_retrieve(my_globals.str_execution_may_go_on)
            if not execution_should_continue:
                break
            # we'll only update the timestamp in the SimpleDS if there is an actual change
            # to the transcript. So here, we'll keep the existing timestamp
            timestamp_updated = self.transcripts_ds.fetch_lastupdated(a_vid)
            logging.info('Updating term COUNT for record # ' + str(counter + 1) + ' of ' + str(num_missing_tc_entries))
            transcript = Transcript(a_vid)
            transcript.set_transcript_directory(self.str_path_to_transcripts_files)
            transcript.load_transcript_object_from_dictionary(self.transcripts_ds.fetch_data(a_vid))
            transcript.get_transcript_from_disk()
            transcript.construct_terms_count()
            saved_to_disk_successfully = transcript.save_df_terms_count_2disk()
            dict_for_ds = transcript.dump_transcript_metadata_to_dictionary()
            self.transcripts_ds.update_entry(a_vid, dict_for_ds, timestamp_updated)
            logging.debug('Added (to SimpleDS) the term-COUNT data for entry: ' + a_vid)
            if saved_to_disk_successfully:
                num_vids_success += 1
            counter += 1
            percent_tracker.update_progress(counter, show_time_remaining_estimate=True,
                                            str_description_to_include_in_logging='Updating Term Count files.')
        logging.info("Successfully saved to disk term-count data for " + str(num_vids_success) + ' records.')
        logging.info("Records processed: " + str(counter))
        self.transcripts_ds.save2disk()

    # ------------------------ END FUNCTION ------------------------ #

    def update_tfidf_dataframes(self, force_update=False):
        """This method updates any missing TranscriptAnalysis info in the transcripts
        SimpleDS and on disk"""
        logging.info('Starting method updates the TranscriptAnalysis data.')
        list_vids_no_tfidf_data_on_disk = self.__find_missing_tfidf_for_vids(force_update=force_update)
        num_missing_tfidf_vids = len(list_vids_no_tfidf_data_on_disk)
        counter = 0
        percent_tracker = PercentTracker(num_missing_tfidf_vids,
                                         int_output_every_x_percent=1, log_level='info')
        logging.info(str(num_missing_tfidf_vids) + " videos don't have TranscriptAnalysis data on disk.")

        # first we need to create a vector (series) that has the IDF of all terms
        # in the universe of documents (transcripts.)
        # when this method is called, in theory, we should already have a vector of the Document Count
        # on disk. So we retrieve this and apply some operations on it to convert it into an IDF vector.
        # so the variable below 'df_idf' is initially misleading. When we load it, it is just a
        # dataframe with document count in it, and we convert it to the IDF.
        df_idf = self.fetch_doc_count_vector_from_disk()
        if len(df_idf) == 0:
            # if the length of the dataframe loaded from disk is zero, something
            # is wrong
            logging.error('The Doc Count dataframe should not be empty. Exiting the method.')
            return
        else:
            # if we are here, then the dataframe has some content, so we proceed to convert it into
            # an IDF dataframe
            df_idf = self.convert_doc_count_to_idf(df_idf)

        for a_vid in list_vids_no_tfidf_data_on_disk:
            execution_should_continue = self.var_mgr.var_retrieve(my_globals.str_execution_may_go_on)
            if not execution_should_continue:
                break
            # we'll only update the timestamp in the SimpleDS if there is an actual change
            # to the transcript. So here, we'll keep the existing timestamp
            timestamp_updated = self.transcripts_ds.fetch_lastupdated(a_vid)
            logging.debug(
                'Updating TranscriptAnalysis for video # ' + str(counter) + ' of ' + str(num_missing_tfidf_vids))
            transcript = Transcript(a_vid)
            transcript.set_transcript_directory(self.str_path_to_transcripts_files)
            transcript.load_transcript_object_from_dictionary(self.transcripts_ds.fetch_data(a_vid))
            transcript.get_df_terms_count_from_disk()

            # in the method where we build the term-count at this point
            # we call a method of the Transcript class to build the term-count. This makes sense
            # because the Transcript object has all the information it needs to calculate
            # term-count. However, a Transcript does not 'know' the universe of documents
            # it is part of, so it does not make sense for the method that creates TranscriptAnalysis
            # to be part of the Transcript class. That is why it is a method of this class
            # instead, and then the TranscriptAnalysis info is passed to the Transcript class to be
            # stored. In fact, the cration of the term-count files probably should not have
            # been coordinated by this TranscriptAnalysis class - but it's already done that way now.
            df_tfidf = self.__create_tfidf_dataframe_for_a_vid(transcript.df_terms_count, df_idf)
            df_tfidf.index.name = my_globals.str_trnscrpt_class_column_terms
            df_tfidf.rename(
                columns={my_globals.str_trnscrpt_class_column_count: my_globals.str_trnscrpt_class_column_tfidf})
            df_tfidf.sort_values(by=self.__column_name_count, ascending=False, inplace=True)
            transcript.provide_tfidf(df_tfidf)
            transcript.save_df_tfidf_2disk()
            dict_for_ds = transcript.dump_transcript_metadata_to_dictionary()
            self.transcripts_ds.update_entry(a_vid, dict_for_ds, timestamp_updated)
            logging.debug('Added (to SimpleDS) the TranscriptAnalysis data for video: ' + a_vid)
            counter += 1
            percent_tracker.update_progress(counter, show_time_remaining_estimate=True,
                                            str_description_to_include_in_logging='Updating Term Count files.')
        self.transcripts_ds.save2disk()

    # ------------------------ END FUNCTION ------------------------ #

    def update_pseudotranscript_files(self, fullpath_to_simpleds_with_vid_json_data,
                                      keep_terms_that_appear_more_than=2,
                                      force_update=False,
                                      fullpath_file_with_terms_to_not_include=''):
        """This method updates any missing pseudo-transcript info in the transcripts
        SimpleDS and on disk.
        A pseudo-transcript is a reduced form of the transcript to be pushed to Algolia.
        It has stop-words removed, as well as terms that are only used a certain amount
        of times.
        PARAMETERS for the method. The parameters are fairly self explanatory.
        The file of terms to not include in the pseudo-transcript should be a file
        NOT in json format (as in, nor a dictionary or a list), but rather where
        each term is by itself on one line. One term per line."""
        logging.info('Starting method that updates the pseudo-transcript data.')

        num_vids_success = 0

        # we need some 'guest' information at one point in the code, so we need the
        # SimpleDS of website json videos. We normally sort the SimpleDS as soon as
        # we load it, but here it will be queried, rather than used to iterate, so
        # no need.
        web_vid_ds = SimpleDS(fullpath_to_simpleds_with_vid_json_data)
        web_vid_ds.load()

        # make a list of videos that don't have pseudo-transcript data yet
        list_vids_no_pt_data_on_disk = self.__look_for_missing_pseudotext_info(force_update=force_update)
        num_missing_pt_vids = len(list_vids_no_pt_data_on_disk)
        counter = 0
        percent_tracker = PercentTracker(num_missing_pt_vids,
                                         int_output_every_x_percent=1, log_level='info')
        logging.info(str(num_missing_pt_vids) + " videos don't have pseudo-transcript data on disk.")

        for a_vid in list_vids_no_pt_data_on_disk:
            try:
                execution_should_continue = self.var_mgr.var_retrieve(my_globals.str_execution_may_go_on)
                if not execution_should_continue:
                    break
                # we'll only update the timestamp in the SimpleDS if there is an actual change
                # to the transcript. So here, we'll keep the existing timestamp
                timestamp_updated = self.transcripts_ds.fetch_lastupdated(a_vid)
                logging.debug(
                    'Updating pseudo-transcript for video # ' + str(counter) + ' of ' + str(num_missing_pt_vids))
                transcript = Transcript(a_vid)
                transcript.set_transcript_directory(self.str_path_to_transcripts_files)
                # now we load into the object, and data that has previously been stored
                # about it to SimpleDS
                transcript.load_transcript_object_from_dictionary(self.transcripts_ds.fetch_data(a_vid))

                # call a method that makes the pseudo-transcript. However, what that method does can be improved
                # by giving it a list of terms to ignore. So we create that list.
                # we start with a basic static list.
                list_of_unwanted_terms = convert_file_to_list_by_lines(fullpath_file_with_terms_to_not_include)
                # now we want to customize the list a bit for this particular transcript, by
                # adding terms related to the people in the interview. So, for example, if the interview
                # has 'Ash Bennington' and 'Ed Harrison' in it, we want to augment the list of unwanted terms
                # with ['ash', 'bennington', 'ash beggington', 'ed', 'harrison', 'ed harrison']
                vid_obj = RVwebsiteVid(web_vid_ds.fetch_data(a_vid))
                lst_people = vid_obj.make_python_list_of_people_in_video()
                lst_people_tokens = tokenize_list_containing_people_fullnames(lst_people)
                list_of_unwanted_terms.extend(lst_people_tokens)
                # now we have the pieces in place to call the method that makes the pseudo-transcript
                transcript.make_pseudotranscript(
                    keep_terms_that_appear_more_than_x_times=keep_terms_that_appear_more_than,
                    list_of_terms_to_ignore=list_of_unwanted_terms)
                # save the pseudo-transcript to disk. This function also updates the filename metadata
                # inside the Transcript object, so that when we dump the metadata back to dictionary for saving
                # to SimpleDS, that will be included
                saved_to_disk_successfully = transcript.save_pseudotranscript_2disk()
                # dump current metadata of the Transcript object to a dictionary for saving to SimpleDS
                dict_for_ds = transcript.dump_transcript_metadata_to_dictionary()
                self.transcripts_ds.update_entry(a_vid, dict_for_ds, timestamp_updated)
                logging.debug('Added (to SimpleDS) the pseudo-transcript for video: ' + a_vid)
                if saved_to_disk_successfully:
                    num_vids_success += 1
                counter += 1
                percent_tracker.update_progress(
                    counter,
                    show_time_remaining_estimate=True,
                    str_description_to_include_in_logging='Updating pseudo-transcript files.')
            except Exception as e:
                logging.warning('While updating pseudo-transcripts there was an issue with video: ' + a_vid +
                                '\nThis try/except is inside a loop, so the method will attempt to continue'
                                ' processing other videos, and to save the SimpleDS afterwards.'
                                '\nThe Exception was: ' + repr(e))
        logging.info("Successfully saved to disk video's pseudo-transcript data: " + str(num_vids_success))
        logging.info("Videos processed: " + str(counter))
        self.transcripts_ds.save2disk()

    # ------------------------ END FUNCTION ------------------------ #

    def update_pub_pseudotext_files(self, keep_terms_that_appear_more_than=2,
                                    force_update=False, fullpath_file_with_terms_to_not_include=''):
        """This method updates any missing pseudo-text info in the fulltexts
        SimpleDS and on disk related to RV publications.
        A pseudo-text is a reduced form of the fulltext to be pushed to Algolia.
        It has stop-words removed, as well as terms that are only used a certain amount
        of times.
        PARAMETERS for the method. The parameters are fairly self explanatory.
        The file of terms to not include in the pseudo-text should be a file
        NOT in json format (as in, not a dictionary or a list), but rather where
        each term is by itself on one line. One term per line."""
        logging.info('Starting method that updates publication pseudo-text data.')

        # make a list of publications that don't have pseudo-text data yet
        list_pubs_no_pt_data_on_disk = self.__look_for_missing_pseudotext_info(force_update=force_update)
        num_missing_pt_pubs = len(list_pubs_no_pt_data_on_disk)
        counter = 0
        percent_tracker = PercentTracker(num_missing_pt_pubs,
                                         int_output_every_x_percent=1, log_level='info')
        logging.info(str(num_missing_pt_pubs) + " publications don't have pseudo-text data on disk.")

        for a_pub in list_pubs_no_pt_data_on_disk:
            try:
                execution_should_continue = self.var_mgr.var_retrieve(my_globals.str_execution_may_go_on)
                if not execution_should_continue:
                    break
                # we'll only update the timestamp in the SimpleDS if there is an actual change
                # to the fulltext. So here, we'll keep the existing timestamp
                timestamp_updated = self.transcripts_ds.fetch_lastupdated(a_pub)
                logging.debug('Updating pseudo-text for publication # '
                              + str(counter) + ' of ' + str(num_missing_pt_pubs))
                transcript = Transcript(a_pub)
                transcript.set_transcript_directory(self.str_path_to_transcripts_files)
                # now we load into the object, and data that has previously been stored
                # about it to SimpleDS
                transcript.load_transcript_object_from_dictionary(self.transcripts_ds.fetch_data(a_pub))

                # call a method that makes the pseudo-text. However, what that method does can be improved
                # by giving it a list of terms to ignore. So we create that list.
                list_of_unwanted_terms = convert_file_to_list_by_lines(fullpath_file_with_terms_to_not_include)
                # now we have the pieces in place to call the method that makes the pseudo-text
                transcript.make_pseudotranscript(
                    keep_terms_that_appear_more_than_x_times=keep_terms_that_appear_more_than,
                    list_of_terms_to_ignore=list_of_unwanted_terms)
                # save the pseudo-text to disk. This function also updates the filename metadata
                # inside the Transcript object, so that when we dump the metadata back to dictionary for saving
                # to SimpleDS, that will be included
                transcript.save_pseudotranscript_2disk()
                # dump current metadata of the Transcript object to a dictionary for saving to SimpleDS
                dict_for_ds = transcript.dump_transcript_metadata_to_dictionary()
                self.transcripts_ds.update_entry(a_pub, dict_for_ds, timestamp_updated)
                logging.debug('Added (to SimpleDS) the pseudo-text for publication: ' + a_pub)
                counter += 1
                percent_tracker.update_progress(counter, show_time_remaining_estimate=True,
                                                str_description_to_include_in_logging='Updating pseudo-text files.')
            except Exception as e:
                logging.warning('While updating pseudo-texts there was an issue with publication: ' + a_pub +
                                '\nThis try/except is inside a loop, so the method will attempt to continue'
                                ' processing other publications, and to save the SimpleDS afterwards.'
                                '\nThe Exception was: ' + repr(e))
        self.transcripts_ds.save2disk()

    # ------------------------ END FUNCTION ------------------------ #

    def update_doc_count_vector_on_disk(self, wipe_and_start_from_zero=False):
        """Method updates OR builds Document COUNT as a vector (one-column dataframe, not counting
        the index - the index contains terms, and the data-column contains the term COUNT
        in documents, which is: how many transcripts the term appears in)
        and stores it on disk to CSV. This column can then be used to generate
        the full TF-IDF for each document.
        NOTE! This vector is deliberately not IDF. In other words, there are still some
        computations necessary to convert it from just Document COUNT, into INVERSE
        DOCUMENT FREQUENCY. We are deliberately only storing the COUNT of the term in all
        documents for two reasons:
        - this is the most granular level of the information (it is very easy to go from
        here to IDF, but not vice-versa), and
        - when stored just as the COUNT of terms, it is very easy to update this dataframe
        with new transcripts as they get added."""
        logging.info('Starting method that builds term Document Count vector')

        int_vids_processed = 0
        int_vids_added_to_doc_count_vector = 0
        int_vids_not_touched = 0

        percent_increments = 10
        doc_count_file_exists = os.path.exists(self.fullpath_doc_count_vector_as_csv)

        # if a fresh start was requested, we wipe the SimpleDS clean
        # from the tags that mark a video as already included in the current
        # doc-count vector, and we delete the existing file.
        if wipe_and_start_from_zero:
            self.transcripts_ds.tag_remove_all_rows(self.__str_tag_vid_data_in_doc_count)
            percent_increments = 1
            if doc_count_file_exists:
                os.remove(self.fullpath_doc_count_vector_as_csv)
                doc_count_file_exists = False

        df_doc_count = pd.DataFrame()
        # check to see if the DC (document count) vector CSV file already exists.
        # If it does, we load a dataframe object with it, and
        # if it doesn't, we create a dataframe.
        if doc_count_file_exists:
            logging.debug('Document Count vector csv file exists. Loading dataframe from disk.')
            df_doc_count = pd.read_csv(self.fullpath_doc_count_vector_as_csv,
                                       sep=self.__separator)
            df_doc_count.set_index(self.__column_name_terms, drop=True, inplace=True)
        else:
            logging.debug('Document Count vector csv file does not exist. Will create it as part'
                          ' of the execution of the method.')
            # If we are not loading the dataframe from disk (because it does not yet exist)
            # we need to initialize this dataframe a bit more formally than usual,
            # because the loop below adds two columns together after concatenating the current
            # iteration of the loop's dataframe, with the ongoing one.
            # If we don't initialize the dataframe here with its proper columns, the sum
            # of the two columns in the first iteration of the loop fails, because in the
            # first iteration, the cumulative df declared below would not have a column to add
            # with if it were just declared as an empty dataframe.
            df_doc_count = pd.DataFrame(
                columns=[self.__column_name_terms, self.__column_name_count])
            df_doc_count.set_index(self.__column_name_terms, drop=True, inplace=True)

        # now loop through the transcripts SimpleDS, and use the term count
        # dataframe of each video that has one to create a global document count vector.
        # by vector I mean a one-dimensional dataframe, where the index is
        # populated with all of the terms, and the data-column is the number of documents
        # (transcripts) where the term is present.
        counter = 0
        max_vids_to_process = self.num_vids_to_use
        percent_trkr = PercentTracker(max_vids_to_process,
                                      int_output_every_x_percent=percent_increments, log_level='info')
        # we are going to add a cheeky term to the dataframe of each
        # video. This will allow us to basically store, as a row, in the overall document-
        # count dataframe the size of the document universe that has been
        # examined to construct the current document-count vector.
        # we do this by temporarily adding the same string (that will never be found naturally
        # in a transcript) to every video term-count dataframe. To do this, we use the
        # small, cheeky dataframe below.
        dct_for_cheeky_df = {self.__column_name_terms: [self.__str_cheeky_document_counter],
                             self.__column_name_count: [1]}
        df_cheeky = pd.DataFrame(dct_for_cheeky_df)
        df_cheeky.set_index(self.__column_name_terms, drop=True, inplace=True)
        df_cheeky.index.name = self.__column_name_terms
        for vid_id in self.transcripts_ds:
            vid_data_already_in_existing_vector = \
                self.transcripts_ds.tag_check(vid_id, self.__str_tag_vid_data_in_doc_count)
            if vid_data_already_in_existing_vector:
                int_vids_not_touched += 1
            else:
                # we are here if the video data being examined has not yet contributed to
                # the existing doc-count vector.
                execution_should_continue = self.var_mgr.var_retrieve(my_globals.str_execution_may_go_on)
                # if the instance of this class was only asked to process a certain number
                # of videos, we don't loop through the whole SimpleDS.
                # similarly, if an external event has asked for execution to stop using the
                # variable manager
                if (counter >= max_vids_to_process) or (not execution_should_continue):
                    break
                logging.debug('Builing document COUNT vector. Processing vid # ' + str(counter))

                df_vid_tc = self.__get_vid_term_count(vid_id)
                # check that the term COUNT df isn't empty
                if len(df_vid_tc) > 0:
                    # this is where we use the cheeky dataframe describe above to add to each
                    # video's term-count a specific string. This means that the overall
                    # document-count vector will always have a row that tracks how many documents
                    # have been used to construct the vector.
                    df_vid_tc = pd.concat([df_vid_tc, df_cheeky])
                    # now create a dataframe for this video that replaces all count values
                    # with True. For document count, we don't care about the actual value
                    # of the term-frequency, just whether it appears or not.
                    # Because all terms of a document obviously appear in that document,
                    # all rows will be marked as True.
                    df_vid_tc = df_vid_tc > 0
                    # now we convert the Trues into 1s so we can do math with them later
                    # this will fail if there are any nans, but we know there are not because
                    # every term exists in its own document
                    df_vid_tc[self.__column_name_count] = \
                        df_vid_tc[self.__column_name_count].astype(int)
                    # now we concatenate the cumulative single-column dataframe that is tracking
                    # the document COUNT, with the one built above.
                    df_doc_count = pd.concat([df_doc_count, df_vid_tc], join='outer', axis=1, sort=False)
                    # now we have two columns, instead of just one. In both columns there are nans
                    # because some terms existed in one df, and not in the other, and viceversa
                    # (and in for the terms that exist in both dfs, we have 1s in both.) This
                    # allows us to add the columns together below, which results in an increment of
                    # +1 in the cumulative df of all the terms (those and only those) that exist in
                    # the current video being processed by the loop.
                    # in the line below we replace the nans that got added at the concatenations with zeros
                    df_doc_count.fillna(0, inplace=True)
                    # now, as explained, we add the two columns together
                    df_doc_count = df_doc_count.iloc[:, 0] + df_doc_count.iloc[:, 1]
                    self.transcripts_ds.tag_add(vid_id, self.__str_tag_vid_data_in_doc_count)
                    int_vids_added_to_doc_count_vector += 1

            int_vids_processed += 1
            counter += 1
            percent_trkr.update_progress(counter, show_time_remaining_estimate=True)

        # we save the transcripts simpleDS to disk, because tags may have been added
        self.transcripts_ds.save2disk()
        # then save the document-count vector to disk
        df_doc_count.index.name = self.__column_name_terms
        save_index = True
        logging.debug('Saving dataframe to CSV with __separator -> ' +
                      str(self.__separator) + ' and saving index = ' + str(save_index))
        df_doc_count.to_csv(self.fullpath_doc_count_vector_as_csv, sep=self.__separator,
                            index=save_index, header=True)

        logging.info('---------- SUMMARY of updating the document count vector ----------')
        logging.info('Videos processed: ' + str(int_vids_processed))
        logging.info('Videos added to the document-count vector: ' + str(int_vids_added_to_doc_count_vector))
        logging.info('Videos not touched: ' + str(int_vids_not_touched))

    # ------------------------ END FUNCTION ------------------------ #

    def convert_doc_count_to_idf(self, df_of_dc_to_make_into_idf):
        """Method receives a dataframe that has all the terms in the universe of documents
        (transcripts) as the index, and the values in the single column are the number
        of documents that each term appears in. This dataframe is converted to IDF.
        The series should have a cheeky row in it that records how many documents
        have contributed to the creation of the vector."""
        num_transcripts = df_of_dc_to_make_into_idf.loc[self.__str_cheeky_document_counter]
        # in our case, because of the way we are constructing the set of terms
        # there should never be a term that has a document frequency of zero.
        # however, in general, if querying a new phrase using existing data,
        # in theory a term could have a document frequency of zero, so the general
        # practice is to add 1 to the document frequency, so that in the next
        # set, division by zero does not happen.
        df_of_dc_to_make_into_idf = df_of_dc_to_make_into_idf + 1
        # then we find the IDF (inverse document frequency)
        df_of_dc_to_make_into_idf = num_transcripts / df_of_dc_to_make_into_idf
        # then we find the log of that
        df_of_dc_to_make_into_idf = log(df_of_dc_to_make_into_idf)
        return df_of_dc_to_make_into_idf

    # ------------------------ END FUNCTION ------------------------ #

    def fetch_doc_count_vector_from_disk(self):
        df_to_return = pd.DataFrame
        file_exists = True
        try:
            with open(self.fullpath_doc_count_vector_as_csv, mode='r') as file_with_vars:
                logging.debug('Method for loading Doc Count vector. File DOES exist on disk.')
                # line below was added because pylama complained about file_with_vars being
                # declared but not used.
                file_with_vars.read()
        except FileNotFoundError:
            logging.error('Could not load Doc Count vector. Probably the file was not found on disk.')
            file_exists = False
        if file_exists:
            df_to_return = pd.read_csv(self.fullpath_doc_count_vector_as_csv, sep=self.__separator)
            df_to_return.set_index(self.__column_name_terms, drop=True, inplace=True)
        return df_to_return

    # ------------------------ END FUNCTION ------------------------ #

    def delete_transcripts_based_on_their_source(self, source_tag):
        # Since the transcript instance of SimpleDS does not actually store the data
        # inside the file that is associated with each entry (instead, because the data
        # can be quite large, it stores the filename of the transcript, term-count, etc.)
        # we first need to loop through the SimpleDS in order to query the files
        # associated with each entry, and remove the files. Then after that, we
        # can delete the entries in the SimpleDS itself.
        for item in self.transcripts_ds:
            if self.transcripts_ds.tag_check(item, source_tag):
                transcript = Transcript(item)
                transcript.set_transcript_directory(self.str_path_to_transcripts_files)
                transcript.load_transcript_object_from_dictionary(self.transcripts_ds.fetch_data(item))
                transcript.delete_all_transcript_related_files()
                # NOTE THAT at this point, we would probably dump the metadata back to a
                # dictionary, and save that dictionary back into the SimpleDS. However, since
                # we are immediately going to delete the entry in the SimpleDS, in this case it
                # would be pointless.

                # now we no longer need any of the data in the SimpleDS record, so we
                # can proceed to delete it.
                self.transcripts_ds.delete_entry(item)

    # ------------------------ END FUNCTION ------------------------ #

    def __create_tfidf_dataframe_for_a_vid(self, df_with_term_count_for_vid, df_with_overall_idf):
        # note, the variable below is INITIALLY a misnomer. It does not start out being the tfidf
        # but as the code proceeds it becomes the series containing the tfidf for the video
        df_tfidf = df_with_term_count_for_vid
        # first we need to convert the series that has the term-count, into
        # term-frequency instead. This is done by dividing the term-count by
        # the size of the transcript. Here, for the size of the transcript
        # we are simply going to sum all the ocurrences of all the terms in the
        # series (this is slightly different from the size of the original transcript
        # because at some point stop-words were removed, and also because in the series
        # we don't just have single words, but we have noun-phrases too, but for our purposes it
        # should be fine.)
        total_terms = df_tfidf.sum()
        df_tfidf = df_tfidf / total_terms

        # and now to arrive at TF-IDF we need to multiply the term-frequency
        # (which we now have in the series) by the IDF for each term in the series
        # so lets first get a series that is a sub-section of the IDF series (it is
        # an intersection of the indexes of the two series - in other words, it is
        # a sub-section of the IDF series, which only contains the terms we are
        # interested in for the video that is currently being processed.)
        df_sub_idf = df_with_overall_idf.loc[df_tfidf.index]
        df_tfidf = df_tfidf * df_sub_idf
        return df_tfidf

    # ------------------------ END FUNCTION ------------------------ #

    def __look__missing_termcount_info(self):
        """Makes a list of videos that have transcripts saved to disk, but
        do not have a Term-Count file saved to disk."""
        logging.debug('Starting method that looks for missing Term Count data.')
        counter = 0
        max_vids_to_process = self.num_vids_to_use
        logging.info('Examining ' + str(max_vids_to_process) + ' records.')
        list_vids_no_tc_data = []
        percent_tracker = PercentTracker(max_vids_to_process, int_output_every_x_percent=10)
        for vid_id in self.transcripts_ds:
            execution_should_continue = self.var_mgr.var_retrieve(my_globals.str_execution_may_go_on)
            if (not execution_should_continue) or (counter >= max_vids_to_process):
                break
            transcript = Transcript(vid_id)
            transcript.set_transcript_directory(self.str_path_to_transcripts_files)
            transcript.load_transcript_object_from_dictionary(self.transcripts_ds.fetch_data(vid_id))
            has_tc_data = transcript.is_termcount_filename_populated()
            if not has_tc_data:
                # we are here if the video has a transcript (it exists in the transcripts SimpleDS),
                # but the field for the filename of the Term Count file has never been populated.
                list_vids_no_tc_data.append(vid_id)
            counter += 1
            percent_tracker.update_progress(counter,
                                            str_description_to_include_in_logging='Finding missing term-count files.')
        return list_vids_no_tc_data

    # ------------------------ END FUNCTION ------------------------ #

    def __find_missing_tfidf_for_vids(self, force_update=False):
        """Makes a list of videos that have transcripts saved to disk, but
        do not have a TranscriptAnalysis file saved to disk."""
        logging.info('Starting method that looks for missing TranscriptAnalysis in all videos')
        counter = 0
        max_vids_to_process = self.num_vids_to_use
        list_vids_no_tfidf_data = []
        percent_tracker = PercentTracker(max_vids_to_process, int_output_every_x_percent=10)
        for vid_id in self.transcripts_ds:
            execution_should_continue = self.var_mgr.var_retrieve(my_globals.str_execution_may_go_on)
            if (not execution_should_continue) or (counter >= max_vids_to_process):
                break

            need_to_append_to_list = False
            if force_update:
                need_to_append_to_list = True
            if not need_to_append_to_list:
                # if we already found that the video should be appended to the list,
                # then there is no need for further checks. But if NOT, then
                # the following should still be performed
                transcript = Transcript(vid_id)
                transcript.set_transcript_directory(self.str_path_to_transcripts_files)
                transcript.load_transcript_object_from_dictionary(self.transcripts_ds.fetch_data(vid_id))
                has_tfidf_data = transcript.is_tfidf_filename_populated()
                if not has_tfidf_data:
                    # we are here if the video has a transcript (it exists in the transcripts SimpleDS),
                    # but the field for the filename of the TranscriptAnalysis file has never been populated.
                    need_to_append_to_list = True

            if need_to_append_to_list:
                list_vids_no_tfidf_data.append(vid_id)
            counter += 1
            percent_tracker.update_progress(counter,
                                            str_description_to_include_in_logging='Finding missing TFIDF files.')
        return list_vids_no_tfidf_data

    # ------------------------ END FUNCTION ------------------------ #

    def __look_for_missing_pseudotext_info(self, force_update=False):
        """Makes a list of videos that have transcripts saved to disk, but
        do not have a pseudo-transcript file saved to disk."""
        logging.debug('Starting method that looks for a missing pseudo-text info')
        counter = 0
        max_vids_to_process = self.num_vids_to_use
        logging.info('Examining ' + str(max_vids_to_process) + ' records.')
        list_vids_no_pt_data = []
        percent_tracker = PercentTracker(max_vids_to_process, int_output_every_x_percent=10)
        for vid_id in self.transcripts_ds:
            execution_should_continue = self.var_mgr.var_retrieve(my_globals.str_execution_may_go_on)
            if (not execution_should_continue) or (counter >= max_vids_to_process):
                break

            need_to_append_to_list = False
            if force_update:
                need_to_append_to_list = True
            if not need_to_append_to_list:
                # if we already found that the video should be appended to the list,
                # then there is no need for further checks. But if NOT, then
                # the following should still be performed
                transcript = Transcript(vid_id)
                transcript.set_transcript_directory(self.str_path_to_transcripts_files)
                transcript.load_transcript_object_from_dictionary(self.transcripts_ds.fetch_data(vid_id))
                has_pseudotranscript_data = transcript.is_pseudotranscript_filename_populated()
                if not has_pseudotranscript_data:
                    # we are here if the video has a transcript (it exists in the transcripts SimpleDS),
                    # but the field for the filename of the TranscriptAnalysis file has never been populated.
                    need_to_append_to_list = True

            if need_to_append_to_list:
                list_vids_no_pt_data.append(vid_id)
            counter += 1
            percent_tracker.update_progress(counter,
                                            str_description_to_include_in_logging='Finding missing pseudotext files.')
        return list_vids_no_pt_data

    # ------------------------ END FUNCTION ------------------------ #

    def __get_vid_term_count(self, vid_id):
        """Method gets a videos term-count data, and returns it as
        a dataframe. If there is not term frequency to be found, an
        empty dataframe is returned."""
        transcript = Transcript(vid_id)
        transcript.set_transcript_directory(self.str_path_to_transcripts_files)
        transcript.load_transcript_object_from_dictionary(self.transcripts_ds.fetch_data(vid_id))
        tc_file_populated = transcript.is_termcount_filename_populated()
        if tc_file_populated:
            transcript.get_df_terms_count_from_disk()
        return transcript.df_terms_count

    # ------------------------ END FUNCTION ------------------------ #

    def __save_idf_to_disk__(self, df_idf):
        """saves idf dataframe to disk."""
        df_idf.index.name = self.__column_name_terms
        separator = '\t'
        save_index = True
        logging.debug(
            'Saving IDF dataframe to CSV with separator -> ' + separator + ' and saving index =' + str(save_index))
        df_idf.to_csv(self.fullpath_doc_count_vector_as_csv, sep=separator, index=save_index)
    # ------------------------ END FUNCTION ------------------------ #
