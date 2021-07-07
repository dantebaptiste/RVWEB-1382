import os
import PyPDF2
import pdfplumber
import logging
from math import ceil
import pandas as pd
import my_globals
from textblob import TextBlob
from nltk.corpus import stopwords
from my_building_blocks import recursiveExtractFieldFromHierarchy, is_number, string_might_be_a_year


class Transcript:
    """A class for managing video Transcript data."""
    __fieldname_vid_id = 'video ID'
    fieldname_filename_rawtext = 'file with raw text'
    fieldname_filename_termcount = 'file with term-count'
    fieldname_filename_tfidf = 'file with tfidf'
    fieldname_filename_pseudotranscript = 'file with pseudo-transcript'
    __column_name_terms = my_globals.str_trnscrpt_class_column_terms
    __column_name_count = my_globals.str_trnscrpt_class_column_count
    __transcript_txt_suffix = '.txt'
    __pseudotranscript_txt_suffix = '_pseudo_.txt'
    __df_terms_count_filename_suffix = '_tc_.csv'
    __df_tfidf_filename_suffix = '_tfidf_.csv'

    __separator = '\t'

    def __init__(self, vid_id):
        self.vid_id = vid_id
        self.str_transcript_text = ''
        self.str_pseudotranscript_text = ''
        self.source = ''
        self.path_to_transcript_directory = ''
        self.df_terms_count = pd.DataFrame()
        self.df_tfidf = pd.DataFrame()
        # the filenames don't get populated until they actually get saved to disk
        self.transcript_txt_filename = ''
        self.df_terms_count_csv_filename = ''
        self.df_tfidf_csv_filename = ''
        self.pseudotranscript_txt_filename = ''

    # ------------------------ END FUNCTION ------------------------ #

    def check_if_vid_has_videoassets_and_transcripts_on_website(self, dct_of_rv_web_vid_metadata,
                                                                authenticated_requests_sesh):
        # for the video ID a dictionary will be returned where the key is the ID and there
        # will be 3 values. One for the existence of each of the following urls:
        # videoassets, json transcript, and pdf transcript

        # start with finding if there is a url for videoassets
        try:
            dct_of_rv_web_vid_metadata['relationships']['videoassets']['links']['related']
        except Exception as e:
            logging.warning('Was unable to determine if there is a videoassets url for'
                            ' record ' + self.vid_id + '. The Exception was: ' + repr(e))

        first_part_url = 'https://www.realvision.com/rv/api/videos/'
        url = first_part_url + self.vid_id
        page_content = ''
        try:
            req = authenticated_requests_sesh.get(url, timeout=my_globals.int_timeout)
            status = str(req.status_code)
            logging.debug('Status: ' + status)
        except Exception:
            logging.warning('Unable to get videoassets for video: ' + self.vid_id)

        if '200' in status:
            page_content = req.json()
            # two options exist for grabbing the transcript. Both URLs are presented, if they exist
            # in the videoassets json page of the video.
            # The first option is to construct it from
            # json data, which is probably much faster, so we try this option first.
            # In addition to likely being a faster download, the json version of the transcript is
            # most likely cleaner, becasue it won't have headers, footers, etc.
            lst_with_json_hierarchy_to_find_url = \
                my_globals.dict_fields_in_videoassets[my_globals.str_name_transcriptjson_field]
            transcriptjson_url = \
                recursiveExtractFieldFromHierarchy(page_content, lst_with_json_hierarchy_to_find_url.copy())
            if transcriptjson_url:
                self.__fetch_transcript_from_rv_website_jsonformat(transcriptjson_url, authenticated_requests_sesh)
                if self.str_transcript_text:
                    self.source = my_globals.str_tag_transcript_source_json

    # ------------------------ END FUNCTION ------------------------ #

    def get_transcript_from_rv_website(self, authenticated_requests_sesh, trial_run=False):  # noqa: C901
        """Get the transcript of a video as a string.
        This method is passed
        - an authenticated requests session."""

        # we keep a dictionary of some of the information of querying urls
        dct_results = {my_globals.str_transcripts_report_column_videoassetsurl: '',
                       my_globals.str_transcripts_report_column_jsonurl: '',
                       my_globals.str_transcripts_report_column_pdfurl: ''}

        # transcript information, if it exists, is found at the 'video assets' json
        # information webpage. So, first we try to get that page.
        first_part_url = 'https://www.realvision.com/rv/api/videos/'
        last_part_url = '/videoassets'
        url = first_part_url + self.vid_id + last_part_url
        logging.debug('Getting videoassets for vid: ' + self.vid_id)
        page_content = ''
        req = ''
        status = ''
        fetch_status_string_va = ''
        fetch_status_string_json = ''
        fetch_status_string_pdf = ''
        try:
            req = authenticated_requests_sesh.get(url, timeout=my_globals.int_timeout)
            status = str(req.status_code)
            logging.debug('Status: ' + status)
        except Exception:
            logging.warning('Unable to get videoassets for video: ' + self.vid_id)
            fetch_status_string_va += 'not-loaded'

        transcriptjson_url = ''
        transcriptpdf_url = ''
        if '200' in status:
            fetch_status_string_va += '200'
            page_content = req.json()
            fetch_status_string_va += '-loaded'
            # two options exist for grabbing the transcript. Both URLs, if they exist, are in
            # in the videoassets json page of the video.
            # First we try to populate the URL for both the json and the pdf transcripts
            # Since we already have the page data, loading both is very inexpensive and allows for a creation
            # of a report of which URLs exist.
            # Try to get the JSON transcript URL
            lst_with_json_hierarchy_to_find_url = \
                my_globals.dict_fields_in_videoassets[my_globals.str_name_transcriptjson_field]
            transcriptjson_url = \
                recursiveExtractFieldFromHierarchy(page_content, lst_with_json_hierarchy_to_find_url.copy())
            # Try to get the PDF transcript URL
            lst_with_json_hierarchy_to_find_url = \
                my_globals.dict_fields_in_videoassets[my_globals.str_name_transcriptpdf_field]
            transcriptpdf_url = \
                recursiveExtractFieldFromHierarchy(page_content, lst_with_json_hierarchy_to_find_url.copy())

            # we only try to actually get the transcript if it isn't a trial run
            if not trial_run:
                # The first option to get the transcript is to construct it from
                # json data, which is probably much faster, so we try this option first.
                # In addition to likely being a faster download, the json version of the transcript is
                # most likely cleaner, becasue it won't have headers, footers, etc.
                if transcriptjson_url:
                    fetch_status_string_json = \
                        self.__fetch_transcript_from_rv_website_jsonformat(transcriptjson_url,
                                                                           authenticated_requests_sesh)
                    if self.str_transcript_text:
                        self.source = my_globals.str_tag_transcript_source_json

                # The second option is to use the pdf format of the transcript.
                # First we check if the transcript was successfully populated by the method above. If it was, then we
                # don't need to use this second option.
                if not self.str_transcript_text:
                    if transcriptpdf_url:
                        fetch_status_string_pdf = \
                            self.__fetch_transcript_from_rv_website_pdfformat(transcriptpdf_url,
                                                                              authenticated_requests_sesh)
                        if self.str_transcript_text:
                            self.source = my_globals.str_tag_transcript_source_pdf
        else:
            fetch_status_string_va += 'non200'

        if page_content:
            dct_results[my_globals.str_transcripts_report_column_videoassetsurl] = fetch_status_string_va
        if transcriptjson_url:
            dct_results[my_globals.str_transcripts_report_column_jsonurl] = 'url-exists' + fetch_status_string_json
        if transcriptpdf_url:
            dct_results[my_globals.str_transcripts_report_column_pdfurl] = 'url-exists' + fetch_status_string_pdf

        return dct_results

    # ------------------------ END FUNCTION ------------------------ #

    def get_publication_fulltext_from_rv_website(self, authenticated_requests_sesh):  # noqa: C901
        """Get the fulltext of a publication as a string.
        This method is passed
        - an authenticated requests session."""

        # we keep a dictionary of some of the information of querying urls
        dct_results = {my_globals.str_fulltexts_report_column_url: '',
                       my_globals.str_fulltexts_report_column_pdfplumber: '',
                       my_globals.str_fulltexts_report_column_pypdf2: ''}

        first_part_url = 'https://www.realvision.com/rv/media/issue/'
        last_part_url = '/pdf'
        pdf_url = first_part_url + self.vid_id + last_part_url
        logging.info('Getting pdf of the publication from: ' + pdf_url)
        str_full_text = ''
        status = ''
        req = ''
        fetch_status_string_pdfurl = ''
        extract_status_string_plumber = ''
        extract_status_string_pypdf = ''
        try:
            req = authenticated_requests_sesh.get(pdf_url, timeout=my_globals.int_timeout)
            status = str(req.status_code)
            logging.debug('Status: ' + status)
        except Exception as e:
            logging.warning('Unable to get pdf version of transcript from: ' + pdf_url + '. The'
                                                                                         'Exception was: ' + repr(e))
            fetch_status_string_pdfurl += 'not-loaded'

        if '200' in status:
            fetch_status_string_pdfurl += '200'
            str_text_pdfplumber = ''
            str_text_pypdf2 = ''
            # temporarily write the pdf to disk
            temp_file_path = my_globals.str_path4_outputs_temp + self.vid_id + '.pdf'
            with open(temp_file_path, mode='wb') as pdffile:
                pdffile.write(req.content)
                fetch_status_string_pdfurl += '-saved2disk'
            with open(temp_file_path, mode='rb') as pdffile:
                able2open_with_pdfplumber = False
                logging.info('Attempting to open the PDF with pdfplumber.')
                try:
                    pdf_reader = pdfplumber.open(pdffile)
                    able2open_with_pdfplumber = True
                    extract_status_string_plumber += 'opened'
                except Exception as e:
                    logging.error('Problem opening the PDF with pdfplumber. The Exception was: ' + repr(e))
                    extract_status_string_plumber += 'unable2open'
                if able2open_with_pdfplumber:
                    counter = 0
                    counter_pages_success = 0
                    for page in pdf_reader.pages:
                        counter += 1
                        logging.info('WORKING ON PAGE ' + str(counter))
                        try:
                            # on one PDF I ran into a page that threw an unknown exception
                            # so rather than completely ignoring the PDF, I'm deciding to use
                            # a try/except to process all the pages that don't throw an error.
                            str_text_pdfplumber += page.extract_text()
                            counter_pages_success += 1
                        except Exception as e:
                            logging.error(
                                'While extracting text from the PDF using pdfplumber there was an issue with'
                                ' publication ' + self.vid_id + ' at page ' + str(counter) +
                                '. This try/except is inside a for loop, so the method'
                                ' will attempt to continue processing all other pages for the PDF.'
                                ' The Exception was: ' + repr(e))
                    page_percent_success_plumber = 0
                    if counter != 0:
                        page_percent_success_plumber = counter_pages_success / counter
                if (str_text_pdfplumber) and (page_percent_success_plumber > .9):
                    str_full_text = str_text_pdfplumber.strip()
                    extract_status_string_plumber += '--and-extracted'
                    if not str_full_text:
                        extract_status_string_plumber += '--no-content'

                # now we check to see if PDF plumber was able to extract the text.
                # if it was not, we try with PyPDF2
                if not str_full_text:
                    able2open_with_pypdf2 = False
                    logging.info('Attempting to open the PDF with PyPDF2.')
                    try:
                        pdf_reader2 = PyPDF2.PdfFileReader(pdffile)
                        extract_status_string_pypdf += 'opened'
                        able2open_with_pypdf2 = True
                    except Exception as e:
                        logging.error('Problem opening the PDF with PyPDF2. The Exception was: ' + repr(e))
                        extract_status_string_pypdf += 'unable2open'
                    if able2open_with_pypdf2:
                        num_pages = pdf_reader2.numPages
                        page_counter = 0
                        while page_counter < num_pages:
                            logging.info('WORKING ON PAGE ' + str(page_counter))
                            try:
                                str_text_pypdf2 += pdf_reader2.getPage(page_counter).extractText().replace('\n', ' ')
                            except Exception as e:
                                logging.error(
                                    'While extracting text from the PDF using PyPDF2 there was an issue with'
                                    ' publication ' + self.vid_id + ' at page ' + str(page_counter) +
                                    '. This try/except is inside a for loop, so the method'
                                    ' will attempt to continue processing all other pages for the PDF.'
                                    ' The Exception was: ' + repr(e))
                            page_counter += 1
                    str_text_pypdf2 = str_text_pypdf2.strip()
                    if str_text_pypdf2:
                        str_full_text = str_text_pypdf2
                        extract_status_string_pypdf += '--and-extracted'
                    else:
                        extract_status_string_pypdf += '--no-content'
            os.remove(temp_file_path)
        else:
            fetch_status_string_pdfurl += 'non200'

        self.str_transcript_text = str_full_text
        if self.str_transcript_text:
            self.source = my_globals.str_tag_transcript_source_pdf
        else:
            logging.warning('Was not able to extract text from PDF for ' + self.vid_id)

        dct_results[my_globals.str_fulltexts_report_column_url] = fetch_status_string_pdfurl
        dct_results[my_globals.str_fulltexts_report_column_pdfplumber] = extract_status_string_plumber
        dct_results[my_globals.str_fulltexts_report_column_pypdf2] = extract_status_string_pypdf
        return dct_results

    # ------------------------ END FUNCTION ------------------------ #

    def get_transcript_from_disk(self):
        if self.path_to_transcript_directory:
            full_path_to_transcript = self.path_to_transcript_directory + self.transcript_txt_filename
            try:
                with open(full_path_to_transcript, mode='r') as file_to_read:
                    self.str_transcript_text = file_to_read.read()
            except OSError as e:
                logging.error('Transcript was not loaded from disk due to an error.'
                              ' The Exception raised was: ' + repr(e))
        else:
            logging.error('Transcript was not loaded from disk because a directory has not been specified'
                          ' using the method to do so.')
        return self.str_transcript_text

    # ------------------------ END FUNCTION ------------------------ #

    def get_pseudotranscript_from_disk(self):
        if self.path_to_transcript_directory:
            full_path_to_pseudotranscript = self.path_to_transcript_directory + self.pseudotranscript_txt_filename
            try:
                with open(full_path_to_pseudotranscript, mode='r') as file_to_read:
                    self.str_pseudotranscript_text = file_to_read.read()
            except OSError as e:
                logging.error('Pseudo-transcript was not loaded from disk due to an error.'
                              ' The Exception raised was: ' + repr(e))
        else:
            logging.error('Pseudo-transcript was not loaded from disk because a directory has not been specified'
                          ' using the method to do so.')
        return self.str_pseudotranscript_text

    # ------------------------ END FUNCTION ------------------------ #

    def save_transcript_text_2disk(self):
        if self.str_transcript_text:
            if self.path_to_transcript_directory:
                self.transcript_txt_filename = self.vid_id + self.__transcript_txt_suffix
                full_path_to_transcript = self.path_to_transcript_directory + self.transcript_txt_filename
                with open(full_path_to_transcript, mode='w') as file_to_write:
                    file_to_write.write(self.str_transcript_text)
            else:
                logging.error('Transcript was not saved to disk because a directory has not been specified'
                              ' using the method to do so.')
        else:
            logging.error('Transcript was not saved to disk because no transcript has been loaded or'
                          ' downloaded for this instance.')

    # ------------------------ END FUNCTION ------------------------ #

    def save_pseudotranscript_2disk(self):
        """RETURN VALUE is true if the pseudo-transcript had some contents, and those contents were
        saved to disk."""
        return_success_value = False
        if self.str_pseudotranscript_text:
            if self.path_to_transcript_directory:
                self.pseudotranscript_txt_filename = self.vid_id + self.__pseudotranscript_txt_suffix
                full_path_to_pseudotranscript = self.path_to_transcript_directory + self.pseudotranscript_txt_filename
                with open(full_path_to_pseudotranscript, mode='w') as file_to_write:
                    file_to_write.write(self.str_pseudotranscript_text)
                return_success_value = True
            else:
                logging.error('Pseudo-transcript was not saved to disk because a directory has not been specified'
                              ' using the method to do so.')
        else:
            logging.error('Pseudo-transcript was not saved to disk because no pseudo-transcript has been loaded.')
        return return_success_value

    # ------------------------ END FUNCTION ------------------------ #

    def set_transcript_directory(self, fullpath_to_transcripts_directory):
        self.path_to_transcript_directory = fullpath_to_transcripts_directory

    # ------------------------ END FUNCTION ------------------------ #

    def dump_transcript_metadata_to_dictionary(self):
        """At the time of writing, the intention for this method is to create a dictionary
        containing transcript metadata which can be stored in the SimpleDS that will
        track Transcript metadata."""
        dict_to_return = {}
        conditions_met_to_generate_dict = False
        if self.vid_id:
            if self.path_to_transcript_directory:
                full_path_to_transcript = self.path_to_transcript_directory + self.transcript_txt_filename
                # now see if the transcript exists on disk
                if os.path.isfile(full_path_to_transcript):
                    conditions_met_to_generate_dict = True
                else:
                    # if we are here, the transcript file does not exist yet
                    logging.error('Dictionary of transcript metadata was not generated, because'
                                  ' a copy of the transcript could not be found on disk.')
            else:
                logging.error('Dictionary of transcript metadata was not generated, because'
                              ' the directory where transcripts are stored must be specified first using'
                              ' the method to do so.')
        else:
            logging.error('Dictionary of transcript metadata was not generated, because'
                          ' no video ID was specified.')

        if conditions_met_to_generate_dict:
            # more metadata can be added here over time.
            # If more metadata is created, possibly more checks should be done above
            # (but not necessarily. For example, you may not want to force the
            # existence of a particlar value in the dictionary.
            dict_to_return[self.fieldname_filename_rawtext] = self.transcript_txt_filename
            if self.df_terms_count_csv_filename:
                dict_to_return[self.fieldname_filename_termcount] = self.df_terms_count_csv_filename
            if self.df_tfidf_csv_filename:
                dict_to_return[self.fieldname_filename_tfidf] = self.df_tfidf_csv_filename
            if self.pseudotranscript_txt_filename:
                dict_to_return[self.fieldname_filename_pseudotranscript] = self.pseudotranscript_txt_filename
        return dict_to_return

    # ------------------------ END FUNCTION ------------------------ #

    def update_dict_of_transcript_metadata(self, existing_dict_of_transcript_metadata):
        """At the time of writing, the SimpleDS that stores transcript info, does not
        store a file with the transcript in it, but rather it store a dictionary of metadata
        about the transcript. For example, the filename of the file where the transcript
        is actually stored. Or for example in the future it might store Term-Count
        filename as well."""
        # NOTE THIS FUNCTION CAN BE AUGMENTED IN THE FUTURE
        # AT THE MOMENT WE ARE ONLY STORING in the dictionary, the filename where
        # the transcript lives, so there isn't really anything to do. So this method
        # is really just a placeholder for potential upgrades in the future, where
        # for example, the current dictionary (passed as a parameter) doesn't have
        # term-count metadata, and that type of data wants to be added to
        # the dict, or something like that.
        dict_to_return = existing_dict_of_transcript_metadata
        return dict_to_return

    # ------------------------ END FUNCTION ------------------------ #

    def is_termcount_filename_populated(self):
        """Return true if a term count filename has been populated."""
        tc_populated = False
        if self.df_terms_count_csv_filename:
            tc_populated = True
        return tc_populated

    # ------------------------ END FUNCTION ------------------------ #

    def is_tfidf_filename_populated(self):
        """Return true if a tfidf filename has been populated."""
        tfidf_populated = False
        if self.df_tfidf_csv_filename:
            tfidf_populated = True
        return tfidf_populated

    # ------------------------ END FUNCTION ------------------------ #

    def is_pseudotranscript_filename_populated(self):
        """Return true if a pseudotranscript filename has been populated."""
        pt_populated = False
        if self.pseudotranscript_txt_filename:
            pt_populated = True
        return pt_populated

    # ------------------------ END FUNCTION ------------------------ #

    def load_transcript_object_from_dictionary(self, transcript_object_previously_dumped_as_dict):
        if self.path_to_transcript_directory:
            dict_trans = transcript_object_previously_dumped_as_dict
            self.transcript_txt_filename = dict_trans[self.fieldname_filename_rawtext]
            if self.fieldname_filename_termcount in dict_trans:
                self.df_terms_count_csv_filename = dict_trans[self.fieldname_filename_termcount]
            if self.fieldname_filename_tfidf in dict_trans:
                self.df_tfidf_csv_filename = dict_trans[self.fieldname_filename_tfidf]
            if self.fieldname_filename_pseudotranscript in dict_trans:
                self.pseudotranscript_txt_filename = dict_trans[self.fieldname_filename_pseudotranscript]
        else:
            logging.error('Transcript was not loaded because a directory has not been specified'
                          ' using the method to do so.')

    # ------------------------ END FUNCTION ------------------------ #

    def search_transcript_for_a_string(self, the_string, int_snippet_margin_size_as_num_characters=25):
        """This method finds all instances of a phrase.
        RETURN VALUE: It returns a list. The list is empty if the_string was not found.
        If the string was found, the return value is a list containing the snippets that contain
        the_string."""
        lst_ocurrences = []
        str_transcript = self.get_transcript_from_disk()
        str_transcript = str_transcript.lower()
        the_string = the_string.lower()

        len_search_term = len(the_string)
        term_found_location = str_transcript.find(the_string)
        while (term_found_location != -1):
            len_transcript = len(str_transcript)
            margin_before = int_snippet_margin_size_as_num_characters
            margin_after = int_snippet_margin_size_as_num_characters
            if (term_found_location + 1) < int_snippet_margin_size_as_num_characters:
                margin_before = term_found_location
            if (term_found_location + len_search_term + int_snippet_margin_size_as_num_characters) > len_transcript:
                margin_after = len_transcript - (term_found_location + len_search_term)

            snippet_start = term_found_location - margin_before
            snippet_end = term_found_location + len_search_term + margin_after
            snippet = str_transcript[snippet_start:snippet_end]
            lst_ocurrences.append(snippet)

            # now we shorten the transcript string so that it no longer includes the ocurrence
            # of the term that was found. This removes the found ocurrence and allows us to
            # continue searching for other ocurrences with the rest of the loop
            str_transcript = str_transcript[(term_found_location + len_search_term):]
            term_found_location = str_transcript.find(the_string)

        return lst_ocurrences

    # ------------------------ END FUNCTION ------------------------ #

    def construct_terms_count(self):
        """This method will construct a dataframe  of terms (single words as well as nounphrases)
        present in the transcript. The terms will be the index, and the values will be
        the number of times each term is present in the transcript.
        NOTE that the transcript must first be provided to the instance using the
        relevant class method to do so."""

        # first we use a TextBlob based on the transcript as-is. This creates some odd
        # noun_phrases that have half complete strings with apostrophes, such as "'ve" or
        # "'s", which we'll take care of later.
        transcript_blob_orig = TextBlob(self.str_transcript_text)
        # now we create a dataframe of COUNT of noun-phrases in the text, based on the
        # noun-phrases provided by the TextBlob
        df_count_original = self.__make_df_of_count_nounphrases__(transcript_blob_orig)

        # now we start over, and do the same thing, but this time with the apostrophes removed FROM THE
        # ORIGINAL text (not from the blob.) Surprisingly this creates a slightly different set of noun phrases
        transcript_blob_apstrph_pre_removed = TextBlob(self.str_transcript_text.replace("'", ''))
        df_count_apstrphs_preremoved = self.__make_df_of_count_nounphrases__(transcript_blob_apstrph_pre_removed)

        # now we start again, but this time to get a dictionary of just the words (not phrases)
        # and their COUNT
        transcript_blob_for_words = TextBlob(self.str_transcript_text)
        df_count_words = self.__make_df_of_count_words__(transcript_blob_for_words)

        # now we combine all the dataframes together by concatenation, and then we remove
        # duplicate rows, by taking whichever row ranked the highest number of COUNT
        # in each dataframe. We do this by grouping the rows, and then by selecting the max.
        self.df_terms_count = \
            pd.concat([df_count_original, df_count_apstrphs_preremoved, df_count_words]).groupby(level=0).max()
        self.df_terms_count.sort_values(by=[self.__column_name_count], ascending=False, inplace=True)

    # ------------------------ END FUNCTION ------------------------ #

    def make_pseudotranscript(self, keep_terms_that_appear_more_than_x_times=2,  # noqa: C901
                              list_of_terms_to_ignore=[],
                              make_result_smaller_than_x_bytes=50000):
        """This method makes a pseudo-transcript.
        A pseudo-transcript is a reduced form of the transcript to be pushed to Algolia.
        It has stop-words removed, as well as terms that are only used a certain amount
        of times (parameter passed to the method.) However, all other terms are actually
        multiplied, so that they appear in the pseudo-transcript the same number of times
        as in the real transcript. This will allow Algolia to better gauge the relevance
        of the terms as related to the transcript."""
        # first we need to load the term-count vector if it exists. The function does
        # checks to see if the directory has been specified and what not, so we can
        # skip some checks here.
        self.get_df_terms_count_from_disk()
        length_df = len(self.df_terms_count)
        if length_df > 0:
            self.df_terms_count.sort_values(by=self.__column_name_count, ascending=False, inplace=True)
            # we loop through the dataframe in order to create the pseudo-transcript, with
            # each term on a line, and each term repeated the same number of times as it occurs
            # in the real transcript, divided by keep_terms_that_appear_more_than_x_times. This division
            # normalizes the pseudo-transcript. So, for example, if we are keeping terms that appear twice
            # or more, then all the terms taht appear the least (exactly 2 times) will show up
            # in the pseudo-transcript once, and if the maximum term appears 40 times, then in
            # the normalized pseudo-transcript it will appear 20 times.
            str_pseudotranscript = ''
            counter = 0
            while counter < length_df:
                term = self.df_terms_count.index[counter]
                term_count = self.df_terms_count.loc[term, self.__column_name_count]
                # now we only continue to add the term to the pseudo-transcript
                # if it ISN'T in the list of terms to ignore.
                if term not in list_of_terms_to_ignore:
                    if term_count > keep_terms_that_appear_more_than_x_times:
                        # the ceil() function below is from the 'math' library and rounds up
                        # this favours small numbers in the pseudo-transcript. For example, a term
                        # that shows up 3 times when we are keeping terms that appear more than
                        # 1 time, ends up in the pseudo-transcript 2 times (because ceil(3/2) is 2.)
                        # Whereas rounding (the division) up for a term that appears 51 times,
                        # favours it much less. This is fine. Because the variable specifies
                        # terms to be kept that appear MORE than x times, the numerator in the
                        # division needs to be increased by one (otherwise it would represent
                        # more than or equal to x times.)
                        normalized_term_count = ceil(term_count / (keep_terms_that_appear_more_than_x_times + 1))
                        str_pseudotranscript += ((term + '\n') * normalized_term_count)
                    else:
                        # we are here, if we reached a point in the dataframe where the frequency
                        # of a term is not greater than keep_terms_that_appear_more_than_x_times.
                        # Since the dataframe was sorted, we know all remaining terms will also
                        # be inferior, so we can now exit the loop.
                        break
                counter += 1

            size_of_pt_in_bytes = len(str_pseudotranscript.encode('utf-8'))
            if size_of_pt_in_bytes > make_result_smaller_than_x_bytes:
                # Algolia only accepts records that are 100KB in size, but they recommend (I believe)
                # that the average record be about 10KB. So we want to keep the pseudo-transcript (or pseudo-
                # text, in the case of a publication) relatively small. I'm making an arbitrary decision
                # to try to keep the pseudo-transcript or pseudo-text under 50KB (which is reflected
                # in the default value of one of the parameters to this function.)
                # So now, we check the size. Most times it will be fine off-the-bat and nothing else is needed,
                # but if it is too large, we'll progressively remove terms from the tail
                # end of the sorted dataframe of terms, and then normalize. Then we'll estimate the size of
                # re-creating the pseudo-trancript. If it is still too large, we repeate removing the least
                # and normalizing. We'll repeat until the projected size is under the desired size limit,
                # and then re-create the pseudo-transcript or pseudo-text.
                df = self.df_terms_count.copy()
                while size_of_pt_in_bytes > make_result_smaller_than_x_bytes:
                    # just dropping the most infrequent terms off the end of the dataframe isn't going to
                    # do much. The real difference to size is going to come from the normalization. Example,
                    # if the most infrequent term happens 1 time, then dropping all the terms that only happen
                    # one time leaves terms that happen 2 or more times. Then then dataframe can be normalized
                    # by dividing the term_counts by 2. At this point the terms taht used to have a term_count
                    # of 2, now appear in the df as having a term count of 1 (after the division) and the terms
                    # with a high-term count have also had their frequency halved. This is was will really
                    # reduce the size of the resulting pseudo-text.
                    # so the first thing we do in this loop is drop all terms that have a current (as of this
                    # iteration of the loop) term count of 1. If I don't use the 'copy()' in the line below
                    # pandas throws an error in the line of code further below where I normalize
                    df = df[df[self.__column_name_count] > 1].copy()
                    # now in order to normalize the dataframe, we need to find the lowest remaining term count
                    last_term = df.index[len(df) - 1]
                    lowest_term_count = df.loc[last_term, self.__column_name_count]
                    # now we can normalize
                    df[self.__column_name_count] = (df[self.__column_name_count] / lowest_term_count).apply(ceil)
                    # now we estimate if creating a new pseudo-text using the new normalized numbers in
                    # the dataframe will fit under the limit
                    counter = 0
                    length_df = len(df)
                    projected_size_in_bytes = 0
                    while counter < length_df:
                        term = df.index[counter]
                        term_count = df.loc[term, self.__column_name_count]
                        if term not in list_of_terms_to_ignore:
                            projected_size_in_bytes += len(((term + '\n') * term_count).encode('utf-8'))
                        counter += 1
                    size_of_pt_in_bytes = projected_size_in_bytes

                # after the loop above, we are left with a df that contains terms and term-counts
                # that we now KNOW can be used to construct a pseudo-text that is smaller than the limit
                # ARGUABLY WE COULD DO THE CALCULATION ABOVE at the beginning of this function every time
                # (rather than building a transcript first, and then finding out it is too big, and then
                # figuring out how to reduce the size) but I've made the deliberate choice not to do this
                # because for most transcripts and publication texts creating the pseudo-text will
                # work off-the-bat without the adjustments needed.
                str_pseudotranscript = ''
                counter = 0
                length_df = len(df)
                while counter < length_df:
                    term = df.index[counter]
                    term_count = df.loc[term, self.__column_name_count]
                    if term not in list_of_terms_to_ignore:
                        str_pseudotranscript += ((term + '\n') * term_count)
                    counter += 1

            self.str_pseudotranscript_text = str_pseudotranscript

        else:
            logging.error('Pseudo-transcript not created because the terms-count vector is not populated.')

    # ------------------------ END FUNCTION ------------------------ #

    def save_df_terms_count_2disk(self):
        """RETURN VALUE. Method returns true, if the terms-count dataframe is populated and successfully
         saved to disk."""
        return_success_value = False
        if self.path_to_transcript_directory:
            if len(self.df_terms_count) > 0:
                self.df_terms_count_csv_filename = self.vid_id + self.__df_terms_count_filename_suffix
                full_path_terms_and_count_csv = self.path_to_transcript_directory + self.df_terms_count_csv_filename
                save_index = True
                logging.debug('Saving dataframe to CSV with __separator -> ' +
                              str(self.__separator) + ' and saving index = ' + str(save_index))
                self.df_terms_count.to_csv(full_path_terms_and_count_csv, sep=self.__separator, index=save_index)
                return_success_value = True
            else:
                logging.error('NOT saving terms COUNT to disk because the Terms Count dataframe is empty.')
        else:
            logging.error('Terms Count dataframe not saved because the transcripts directory has not been specified'
                          ' using the method to do so.')
        return return_success_value

    # ------------------------ END FUNCTION ------------------------ #

    def save_df_tfidf_2disk(self):
        if self.path_to_transcript_directory:
            if len(self.df_tfidf) > 0:
                self.df_tfidf_csv_filename = self.vid_id + self.__df_tfidf_filename_suffix
                full_path_tfidf_csv = self.path_to_transcript_directory + self.df_tfidf_csv_filename
                save_index = True
                logging.debug('Saving TranscriptAnalysis dataframe to CSV with __separator -> ' +
                              str(self.__separator) + ' and saving index = ' + str(save_index))
                self.df_tfidf.to_csv(full_path_tfidf_csv, sep=self.__separator, index=save_index)
            else:
                logging.error('NOT saving TranscriptAnalysis to disk because the dataframe is empty.')
        else:
            logging.error(
                'TranscriptAnalysis dataframe not saved because the transcripts directory has not been specified'
                ' using the method to do so.')

    # ------------------------ END FUNCTION ------------------------ #

    def get_df_terms_count_from_disk(self):
        if self.path_to_transcript_directory:
            if self.df_terms_count_csv_filename:
                full_path_terms_count_csv = self.path_to_transcript_directory + self.df_terms_count_csv_filename
                self.df_terms_count = pd.read_csv(full_path_terms_count_csv, sep=self.__separator)
                # I don't know why, but I ran into an error downstream from here where some code
                # was using a dataframe loaded by this function, and the error was about not
                # being able to perform an operation when there was a NaN value. So it seems
                # that somehow on ocassion a NaN value is sneaking into the term-count data.
                # so here we drop the NaN values. We do so deliberately before setting the index
                # so as to avoid NaN values getting into the index (which is what was causing
                # the issue I encountered.)
                self.df_terms_count.dropna(inplace=True)
                self.df_terms_count.set_index(self.__column_name_terms, drop=True, inplace=True)
            else:
                logging.error(
                    'Terms Count dataframe not loaded from disk because the Terms Count filename is empty.')
        else:
            logging.error(
                'Terms Count dataframe not loaded from disk because the transcripts directory has not been specified'
                ' using the method to do so.')

    # ------------------------ END FUNCTION ------------------------ #

    def delete_all_transcript_related_files(self):
        """This function deletes all filenames and files associated with a transcript,
        for example the transcript file, the pseudo-transcript file, etc.
        NOTE that this class if agnostic of where the Transcript instance/object information is stored,
        so if the transcript object is stored in a SimpleDS, this method has no way of deleting
        that entry/file, this must be done by a source external to this class."""
        if self.path_to_transcript_directory:
            self.delete_tfidf_data()
            self.delete_termcount_data()
            self.delete_pseudo_transcript_data()
            self.delete_transcript_data()
        else:
            logging.error('Deletion of Transcript-related files was not attempted, because the transcripts'
                          ' data directory has not yet been specified.')

    # ------------------------ END FUNCTION ------------------------ #

    def delete_transcript_data(self):
        """This method independently checks for info on disk, and info in the SimpleDS.
        NOTE THAT this method is agnostic of the SimpleDS, so in order to check there, the
        dictionary info from SimpleDS must be loaded into the Transcript object first, using
        the load_transcript_object_from_dictionary method."""
        if self.path_to_transcript_directory:
            # we will independently check for the two items that represent the Transcript information
            # Item 1 - the file on disk
            # Item 2 - is the entry in the Transcript object's variables regarding
            # the filename (the most recent info must be loaded already before calling this method.)

            # First we need to check if the filename is populated, because if it isn't, then
            # there is no way to check for a file on disk, and also the variable doesn't need
            # clearing as it is already empty.
            if self.transcript_txt_filename:
                # Item 1 above involves checking for the file, and if it exists, deleting it.
                fullpath = self.path_to_transcript_directory + self.transcript_txt_filename
                file_exists = os.path.exists(fullpath)
                if file_exists:
                    os.remove(fullpath)

                # Item 2 can be taken care of now that it has been used to remove the file
                self.transcript_txt_filename = ''
        else:
            logging.error('Deletion of Transcript data was not attempted, because the transcripts'
                          ' data directory has not yet been specified.')

    # ------------------------ END FUNCTION ------------------------ #

    def delete_pseudo_transcript_data(self):
        """This method independently checks for info on disk, and info in the SimpleDS.
        NOTE THAT this method is agnostic of the SimpleDS, so in order to check there, the
        dictionary info from SimpleDS must be loaded into the Transcript object first, using
        the load_transcript_object_from_dictionary method."""
        if self.path_to_transcript_directory:
            # we will independently check for the two items that represent the pseudo-transcript information
            # Item 1 - the file on disk
            # Item 2 - is the entry in the Transcript object's variables regarding
            # the filename (the most recent info must be loaded already before calling this method.)

            # First we need to check if the filename is populated, because if it isn't, then
            # there is no way to check for a file on disk, and also the variable doesn't need
            # clearing as it is already empty.
            if self.pseudotranscript_txt_filename:
                # Item 1 above involves checking for the file, and if it exists, deleting it.
                fullpath = self.path_to_transcript_directory + self.pseudotranscript_txt_filename
                file_exists = os.path.exists(fullpath)
                if file_exists:
                    os.remove(fullpath)

                # Item 2 can be taken care of now that it has been used to remove the file
                self.pseudotranscript_txt_filename = ''
        else:
            logging.error('Deletion of pseudo-transcript data was not attempted, because the transcripts'
                          ' data directory has not yet been specified.')

    # ------------------------ END FUNCTION ------------------------ #

    def delete_termcount_data(self):
        """This method independently checks for info on disk, and info in the SimpleDS.
        NOTE THAT this method is agnostic of the SimpleDS, so in order to check there, the
        dictionary info from SimpleDS must be loaded into the Transcript object first, using
        the load_transcript_object_from_dictionary method."""
        if self.path_to_transcript_directory:
            # we will independently check for the two items that represent the Term-Count information
            # Item 1 - the file on disk
            # Item 2 - is the entry in the Transcript object's variables regarding
            # the filename (the most recent info must be loaded already before calling this method.)

            # First we need to check if the filename is populated, because if it isn't, then
            # there is no way to check for a file on disk, and also the variable doesn't need
            # clearing as it is already empty.
            if self.df_terms_count_csv_filename:
                # Item 1 above involves checking for the file, and if it exists, deleting it.
                fullpath = self.path_to_transcript_directory + self.df_terms_count_csv_filename
                file_exists = os.path.exists(fullpath)
                if file_exists:
                    os.remove(fullpath)

                # Item 2 can be taken care of now that it has been used to remove the file
                self.df_terms_count_csv_filename = ''
        else:
            logging.error('Deletion of Term Count data was not attempted, because the transcripts'
                          ' data directory has not yet been specified.')

    # ------------------------ END FUNCTION ------------------------ #

    def delete_tfidf_data(self):
        """This method independently checks for info on disk, and info in the SimpleDS.
        NOTE THAT this method is agnostic of the SimpleDS, so in order to check there, the
        dictionary info from SimpleDS must be loaded into the Transcript object first, using
        the load_transcript_object_from_dictionary method."""
        if self.path_to_transcript_directory:
            # we will independently check for the two items that represent the TranscriptAnalysis information
            # Item 1 - the file on disk
            # Item 2 - is the entry in the Transcript object's variables regarding
            # the filename (the most recent info must be loaded already before calling this method.)

            # First we need to check if the filename is populated, because if it isn't, then
            # there is no way to check for a file on disk, and also the variable doesn't need
            # clearing as it is already empty.
            if self.df_tfidf_csv_filename:
                # Item 1 above involves checking for the file, and if it exists, deleting it.
                fullpath = self.path_to_transcript_directory + self.df_tfidf_csv_filename
                file_exists = os.path.exists(fullpath)
                if file_exists:
                    os.remove(fullpath)

                # Item 2 can be taken care of now that it has been used to remove the file
                self.df_tfidf_csv_filename = ''
        else:
            logging.error('Deletion of tf-idf data was not attempted, because the transcripts'
                          ' data directory has not yet been specified.')

    # ------------------------ END FUNCTION ------------------------ #

    def provide_tfidf(self, df_with_tfidf_data):
        """Because a Transcript object cannot be aware of the universe of documents
        it is part of, it cannot generate its own TranscriptAnalysis. So TranscriptAnalysis for a transcript
        has to be generated by an external class. Once it is generated, this
        method can be used to provide the Transcript object with its TranscriptAnalysis data."""
        self.df_tfidf = df_with_tfidf_data

    # ------------------------ END FUNCTION ------------------------ #

    def __fetch_transcript_from_rv_website_jsonformat(self, transcriptjson_url, authenticated_requests_sesh):
        """This method fetches a video transcript that is returned by the server
        in json format, and re-constructs it into a string. The method is given:
         - the URL that should contain the JSON version of the transcript
         - an authenticated requests session
        After being fetched the transcript needs to be reconstructed from
        individual tokens, into a string. The transcript is stored as a list of lists.
        Below is a small example of how it is stored:
        A list called: words
        Inside words, the first item is: [an_integer, the_word]
        Inside words, the next item is: [an_integer, the_word]
        and so on and so forth. Each integer gets progressively bigger. They are
        incremental, but not sequential.
        For example:
        0
            0	"19980"
            1	"MATT ROWE:"
        1
            0	"20616"
            1	"I"
        2
            0	"20732"
            1	"am"
        3
            0	"20906"
            1	"Matt"
        """
        logging.info('Getting json-based version of video transcript from: ' + transcriptjson_url)
        str_method_status = ''
        str_full_transcript = ''
        transcript_json_data = []
        try:
            req = authenticated_requests_sesh.get(transcriptjson_url, timeout=my_globals.int_timeout)
            status = str(req.status_code)
            logging.debug('Status: ' + status)
            if '200' in status:
                # the data is stored inside a field, that at the time of this writing, is called
                # words. So in the next like we query the json response, and we extract the
                # 'words' field from it. What we should be left with, is the list of lists
                # described in the docmentation of this method.
                transcript_json_data = req.json()[my_globals.str_vid_transcriptjson_words]
                str_method_status = '-200-and-got-transcript'
            else:
                str_method_status = '-non200-web-reply'
        except Exception as e:
            logging.warning('Unable to get json version of transcript from: ' + transcriptjson_url +
                            '. The exception was: ' + repr(e))
            str_method_status = '-' + repr(e)

        if transcript_json_data:
            # I'm honestly not 100% sure that one can always count on python json to load this list, guaranteed,
            # in the correct order, but for now, the assumption that the list is downloaded into memory in
            # the correct order is sufficient.
            for word_data in transcript_json_data:
                stripped_word = word_data[1].strip()
                # for some reason, sometimes the word is empty, so if that's the case
                # we skip it.
                if stripped_word != '':
                    str_full_transcript += (stripped_word + ' ')
            # now we remove the space we tagged on to the last word.
            str_full_transcript = str_full_transcript.strip()
            str_method_status += '-and-unpacked-it'
        self.str_transcript_text = str_full_transcript
        return str_method_status

    # ------------------------ END FUNCTION ------------------------ #

    def __fetch_transcript_from_rv_website_pdfformat(self, transcriptpdf_url,  # noqa: C901
                                                     authenticated_requests_sesh):
        """This method fetches a video transcript that is returned by the server
        as a pdf. The method is given:
         - the URL that should contain the PDF version of the transcript
         - an authenticated requests session.
         After fetching the URL (which should return a PDF) the plain text is
         extracted from the pdf."""
        logging.info('Getting pdf-based version of video transcript from: ' + transcriptpdf_url)
        str_method_status = ''
        str_full_transcript = ''
        status = ''
        try:
            req = authenticated_requests_sesh.get(transcriptpdf_url, timeout=my_globals.int_timeout)
            status = str(req.status_code)
            logging.debug('Status: ' + status)
        except Exception as e:
            logging.warning('Unable to get pdf version of transcript from: ' + transcriptpdf_url +
                            '. The Exception was: ' + repr(e))
            str_method_status = '-' + repr(e)

        if '200' in status:
            str_method_status = '-200'
            str_transcript_pdfplumber = ''
            str_transcript_pypdf2 = ''
            # temporarily write the pdf to disk
            temp_file_path = my_globals.str_path4_outputs_temp + self.vid_id + '.pdf'
            with open(temp_file_path, mode='wb') as pdffile:
                pdffile.write(req.content)
                str_method_status += '-saved'
            with open(temp_file_path, mode='rb') as pdffile:
                able2open_with_pdfplumber = False
                logging.info('Attempting to open the PDF with pdfplumber.')
                try:
                    pdf_reader = pdfplumber.open(pdffile)
                    able2open_with_pdfplumber = True
                    str_method_status += '-reopened-pdfplumber'
                except Exception as e:
                    logging.error('Problem opening the PDF with pdfplumber. The Exception was: ' + repr(e))
                if able2open_with_pdfplumber:
                    counter = 0
                    counter_pages_success = 0
                    for page in pdf_reader.pages:
                        counter += 1
                        logging.info('WORKING ON PAGE ' + str(counter))
                        try:
                            # on one PDF I ran into a page that threw an unknown exception
                            # so rather than completely ignoring the PDF, I'm deciding to use
                            # a try/except to process all the pages that don't throw an error.
                            str_transcript_pdfplumber += page.extract_text()
                            counter_pages_success += 1
                        except Exception as e:
                            logging.error(
                                'While extracting text from the PDF using pdfplumber there was an issue with'
                                ' publication ' + self.vid_id + ' at page ' + str(counter) +
                                '. This try/except is inside a for loop, so the method'
                                ' will attempt to continue processing all other pages for the PDF.'
                                ' The Exception was: ' + repr(e))
                    page_percent_success_plumber = 0
                    if counter != 0:
                        page_percent_success_plumber = counter_pages_success / counter
                if (str_transcript_pdfplumber) and (page_percent_success_plumber > .9):
                    str_full_transcript = str_transcript_pdfplumber
                    str_method_status += '-constructed-pdfplumber'

                # now we check to see if PDF plumber was able to extract the text.
                # if it was not, we try with PyPDF2
                if not str_full_transcript:
                    able2open_with_pypdf2 = False
                    logging.info('Attempting to open the PDF with PyPDF2.')
                    try:
                        pdf_reader2 = PyPDF2.PdfFileReader(pdffile)
                        able2open_with_pypdf2 = True
                    except Exception as e:
                        logging.error('Problem opening the PDF with PyPDF2. The Exception was: ' + repr(e))
                    if able2open_with_pypdf2:
                        num_pages = pdf_reader2.numPages
                        page_counter = 0
                        while page_counter < num_pages:
                            logging.info('WORKING ON PAGE ' + str(page_counter))
                            try:
                                str_transcript_pypdf2 += pdf_reader2.getPage(page_counter).extractText().replace('\n',
                                                                                                                 ' ')
                            except Exception as e:
                                logging.error(
                                    'While extracting text from the PDF using PyPDF2 there was an issue with'
                                    ' publication ' + self.vid_id + ' at page ' + str(page_counter) +
                                    '. This try/except is inside a for loop, so the method'
                                    ' will attempt to continue processing all other pages for the PDF.'
                                    ' The Exception was: ' + repr(e))
                            page_counter += 1
                    if str_transcript_pypdf2:
                        str_full_transcript = str_transcript_pypdf2
                        str_method_status += '-constructed-pypdf2'
            os.remove(temp_file_path)
        else:
            str_method_status = '-non200-web-reply'
        str_full_transcript = str_full_transcript.strip()
        self.str_transcript_text = str_full_transcript
        if not self.str_transcript_text:
            logging.warning('Was not able to extract text from PDF for ' + self.vid_id)
        return str_method_status

    # ------------------------ END FUNCTION ------------------------ #

    def __make_df_of_count_nounphrases__(self, blob):
        df_columns = [self.__column_name_terms, self.__column_name_count]
        df = pd.DataFrame(columns=df_columns)
        the_nounphrases = blob.noun_phrases
        # an additional sept you can do if you want is:
        # the_nounphrases = blob.noun_phrases.singularize()

        # noun_phrases has many duplicates, so we create a set, just to use
        # as an iterable instead
        set_nounphrases = set(the_nounphrases)
        for np in set_nounphrases:
            list_to_append_to_df = [str(np).lower(), the_nounphrases.count(np)]
            # the next line appends the list to the dataframe
            df.loc[len(df)] = list_to_append_to_df
        # now we apply a function that applies some conversions to
        # unwanted strings, and in some cases the conversion results in
        # an empty string being left in the column, so then we remove
        # rows where an empty string was left.
        df[self.__column_name_terms] = df[self.__column_name_terms].apply(self.__convert_non_ideal_strings__)
        df = df[df[self.__column_name_terms] != '']
        # now before returning we set the index to be the column of terms
        # which by now should be unique, and we drop the current index which is just
        # auto-gen numbers.
        df.set_index(self.__column_name_terms, inplace=True)
        df.sort_values(by=[self.__column_name_count], ascending=False, inplace=True)
        return df

    # ------------------------ END FUNCTION ------------------------ #

    def __make_df_of_count_words__(self, blob, remove_stopwords=True):
        stop_words = set(stopwords.words('english'))
        df_columns = [self.__column_name_terms, self.__column_name_count]
        df = pd.DataFrame(columns=df_columns)
        the_words = blob.words
        # OR alternatively
        # the_words = blob.words.singularize()

        # the blob.words has many duplicates, so we create a set, just to use
        # as an iterable instead
        set_words = set(the_words)
        for word in set_words:
            word_lower = str(word).lower()
            if word_lower not in stop_words:
                list_to_append_to_df = [word_lower, the_words.count(word)]
                # the next line appends the list to the dataframe
                df.loc[len(df)] = list_to_append_to_df
        # now we apply a function that applies some conversions to
        # unwanted strings, and in some cases the conversion results in
        # an empty string being left in the column, so then we remove
        # rows where an empty string was left.
        df[self.__column_name_terms] = df[self.__column_name_terms].apply(self.__convert_non_ideal_strings__)
        df = df[df[self.__column_name_terms] != '']
        # now before returning we drop the index and sort
        df.set_index(self.__column_name_terms, inplace=True)
        df.sort_values(by=[self.__column_name_terms], ascending=False, inplace=True)
        return df

    # ------------------------ END FUNCTION ------------------------ #

    def __convert_non_ideal_strings__(self, a_string):
        new_string = a_string
        # some of the string below are very weird, but they come from inspecting data
        # that was ending up in the term COUNT for some reason
        # Also, some normal words like 'similar' or 'significant' are removed at this point, because
        # the TextBlob detects many nounphrases that start with them, but generally
        # keeping the 'similar' at the beginning of a noun-phrase isn't useful.
        # the space after SOME of the strings (but still within the string) is deliberate
        # so that we don't remove remove substrings INSIDE larger words.
        list_of_non_ideal_substrings = ["'d", "'s", "'m", "'ll", "'re", "'ve", "n't ", "shouldnt ", "doesnt ", "] ",
                                        "your ", "youre ", "youll ", "youd ", "yeah ", " [", "dont ", "cant ",
                                        "wo n't", "ca n't", "theyve ", "theyre ", "theyll ", "theres ", "didnt ",
                                        "thats ", "similar ", "significant ", "separate ", "right ", "bit ",
                                        "recent ", "potential ", "particular ", "own ", "interesting ",
                                        "important ", "good ", "different ", "certain "]
        # if a_string contains any non_ideal substrings, we want
        # to remove them
        for item in list_of_non_ideal_substrings:
            if item in a_string:
                new_string = a_string.replace(item, '')
        list_substrings_that_cancel_all_the_string = ['www.']
        # if the substrings in this list are found in a_string, then we
        # we replace the entire string with the empty string, because
        # the entire string is useless.
        for item in list_substrings_that_cancel_all_the_string:
            if item in a_string:
                new_string = ''
        # if the string is a number we don't want it
        if is_number(a_string):
            # unless it might be a year, then we still keep it
            if not string_might_be_a_year(a_string):
                new_string = ''
        # if the string is a string of just length 1, we probably
        # don't want it either
        if len(a_string) == 1:
            new_string = ''
        return new_string.strip()
    # ------------------------ END FUNCTION ------------------------ #
