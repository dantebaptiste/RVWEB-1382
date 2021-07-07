import copy
import logging
import my_globals
from class_trancript import Transcript


class AlgoliaDataUnit:
    """This class is intended to be the storage unit that will get saved to disk
    as part of storing Algolia data in a SimpleDS instance. At it's heart
    it is going to be a dictionary that will store all of the fields we want
    to push to Algolia. However, each field will also be a sub-dictionary
    which will store a before-and-after state for each field."""
    key_for_values_pushed = 'value_pushed_to_algolia'
    key_for_values_current = 'value_updated_locally'
    fieldname_id = my_globals.str_alg_fieldname_id
    fieldname_type = my_globals.str_alg_fieldname_type
    fieldname_title = my_globals.str_alg_fieldname_title
    fieldname_description = my_globals.str_alg_fieldname_description
    fieldname_thumbnail = my_globals.str_alg_fieldname_thumbnail
    fieldname_featuring = my_globals.str_alg_fieldname_featuring
    fieldname_interviewer = my_globals.str_alg_fieldname_interviewer
    fieldname_tags = my_globals.str_alg_fieldname_tags
    fieldname_likes = my_globals.str_alg_fieldname_likes
    fieldname_dislikes = my_globals.str_alg_fieldname_dislikes
    fieldname_show = my_globals.str_alg_fieldname_show
    fieldname_productid = my_globals.str_alg_fieldname_productid
    fieldname_vidurl = my_globals.str_alg_fieldname_vidurl
    fieldname_duration = my_globals.str_alg_fieldname_duration
    fieldname_publishedon = my_globals.str_alg_fieldname_publishedon
    fieldname_publishedon_readable = my_globals.str_alg_fieldname_publishedon_readable
    fieldname_tiers = my_globals.str_alg_fieldname_tiers
    fieldname_numcomments = my_globals.str_alg_fieldname_numcomments
    fieldname_transcript = my_globals.str_alg_fieldname_transcript
    fieldname_pseudotranscript = my_globals.str_alg_fieldname_pseudotranscript
    fieldname_numpages = my_globals.str_alg_fieldname_numpages
    __dct_template = {fieldname_id: {key_for_values_pushed: '', key_for_values_current: ''},
                      fieldname_type: {key_for_values_pushed: '', key_for_values_current: ''},
                      fieldname_title: {key_for_values_pushed: '', key_for_values_current: ''},
                      fieldname_description: {key_for_values_pushed: '', key_for_values_current: ''},
                      fieldname_thumbnail: {key_for_values_pushed: '', key_for_values_current: ''},
                      fieldname_featuring: {key_for_values_pushed: [], key_for_values_current: []},
                      fieldname_interviewer: {key_for_values_pushed: [], key_for_values_current: []},
                      fieldname_tags: {key_for_values_pushed: [], key_for_values_current: []},
                      fieldname_likes: {key_for_values_pushed: 0, key_for_values_current: 0},
                      fieldname_dislikes: {key_for_values_pushed: 0, key_for_values_current: 0},
                      fieldname_show: {key_for_values_pushed: '', key_for_values_current: ''},
                      fieldname_productid: {key_for_values_pushed: '', key_for_values_current: ''},
                      fieldname_vidurl: {key_for_values_pushed: '', key_for_values_current: ''},
                      fieldname_duration: {key_for_values_pushed: 0, key_for_values_current: 0},
                      fieldname_publishedon: {key_for_values_pushed: 0, key_for_values_current: 0},
                      fieldname_publishedon_readable: {key_for_values_pushed: '', key_for_values_current: ''},
                      fieldname_tiers: {key_for_values_pushed: [], key_for_values_current: []},
                      fieldname_numcomments: {key_for_values_pushed: 0, key_for_values_current: 0},
                      fieldname_transcript: {key_for_values_pushed: '', key_for_values_current: ''},
                      fieldname_pseudotranscript: {key_for_values_pushed: '', key_for_values_current: ''},
                      fieldname_numpages: {key_for_values_pushed: 0, key_for_values_current: 0},
                      }

    def __init__(self):
        """Initialize the unit."""
        self.dict_algolia_unit = {}
        self.__unit_loaded_or_created = False

    def create_unit(self, dict_with_new_data_for_creating_a_unit):
        """Use this method to create a unit for the first time."""
        errors_found = self.__errors_in_incoming_data(dict_with_new_data_for_creating_a_unit, bool_full_unit=False)
        if not errors_found:
            self.dict_algolia_unit = copy.deepcopy(self.__dct_template)
            for a_key in dict_with_new_data_for_creating_a_unit:
                self.dict_algolia_unit[a_key][self.key_for_values_current] = dict_with_new_data_for_creating_a_unit[
                    a_key]
            self.__unit_loaded_or_created = True
        else:
            logging.error('Errors were found in incoming data. The "creation" method did not complete successfully.')

    # ------------------------ END FUNCTION ------------------------ #

    def load_from_dict(self, dict_with_existing_algolia_unit):
        """Use this method to load an existing unit (most likely retrieved from
        disk using the algolia instance of SimpleDS."""
        errors_found = self.__errors_in_incoming_data(dict_with_existing_algolia_unit, bool_full_unit=True)
        if not errors_found:
            self.dict_algolia_unit = dict_with_existing_algolia_unit
            self.__unit_loaded_or_created = True
        else:
            logging.error('Errors were found in incoming data. Unable to load Algolia unit.')

    # ------------------------ END FUNCTION ------------------------ #

    def provide_updated_data(self, dict_with_new_data):
        """Use this method to update the data in a unit that has been previously
        populated into the instance of the class by using the 'create_unit' or the
        'load_from_dict' methods."""
        str_to_label_old_value = 'prior value'
        str_to_label_new_value = 'new value'
        dct_changes = {}
        if self.__unit_loaded_or_created:
            errors_found = self.__errors_in_incoming_data(dict_with_new_data, bool_full_unit=False)
            if not errors_found:
                for a_key in dict_with_new_data:
                    incoming_current_value = dict_with_new_data[a_key]
                    # sometimes, if a new field is added (an entirely new attribute to push to algolia),
                    # a situation might ocurr where the existing
                    # record does not contain the field in question. In this case, an error would
                    # be generated when trying to get the existing, current value. To avoid that
                    # we check for its existence before accessing it.

                    if a_key not in self.dict_algolia_unit:
                        # if we are at this point in the code, it is probably because a new field (attribute)
                        # is being pushed to Algolia, and the current record does not know about it. Therefore
                        # it needs to be created so that the code that references it below does not cause an error.
                        self.dict_algolia_unit[a_key] = copy.deepcopy(self.__dct_template[a_key])

                    # after the IF above, we know the field exists for sure (regardless of whether
                    # it existed when the loop iteration started), so it can now be safely queried.
                    existing_current_value = self.dict_algolia_unit[a_key][self.key_for_values_current]

                    if type(incoming_current_value) is list:
                        incoming_current_value.sort()
                    if type(existing_current_value) is list:
                        existing_current_value.sort()
                    if incoming_current_value != existing_current_value:
                        # if there is a difference, update the unit
                        self.dict_algolia_unit[a_key][self.key_for_values_current] = incoming_current_value
                        # also add the changes to the dictionary that is tracking
                        # the changes and will be returned
                        dct_changes[a_key] = {str_to_label_old_value: existing_current_value,
                                              str_to_label_new_value: incoming_current_value}
            else:
                logging.error('Unable to update the Algolia unit with new data. Errors found.')
        else:
            logging.error('Unable to update the Algolia unit with new data. Object was not previously'
                          ' created or updated.')
        return dct_changes

    # ------------------------ END FUNCTION ------------------------ #

    def provide_pushed2algolia_data(self, dict_with_pushed_data):
        """Use this method to update the data in a unit that has been pushed to
        algolia."""
        if self.__unit_loaded_or_created:
            errors_found = self.__errors_in_incoming_data(dict_with_pushed_data, bool_full_unit=False)
            if not errors_found:
                for a_key in dict_with_pushed_data:
                    self.dict_algolia_unit[a_key][self.key_for_values_pushed] = dict_with_pushed_data[a_key]
            else:
                logging.error('Unable to update the Algolia unit with PUSHED data. Errors found.')
        else:
            logging.error('Unable to update the Algolia unit with PUSHED data. Object was not previously'
                          ' created or updated.')

    # ------------------------ END FUNCTION ------------------------ #

    def make_algolia_record_for_pushing_delta(self, transcripts_simple_ds,  # noqa: C901
                                              path_to_directory_with_transcripts,
                                              list_of_fields_to_include_in_record,
                                              dont_use_pseudotext_if_fulltext_is_less_tan_x_KB=10):
        """This method creates a dictionary that is a record ready
        to be pushed to Algolia. It creates a record that only has
        changes since the last push to Algolia. It does this by comparing
        the two values held within the objects main dictionary for each key.
        This method needs to be passed the SimpleDS that stores information
        about video transcripts, so it can replace the hash with the
        full transcript.
        The parameter
        dont_use_pseudotext_if_fulltext_is_less_tan_x_kb
        tells this method to push the full text for a particular record, instead
        of the pseudo-text, IF the full text fits under the limit specified by the
        variable in kilobytes. The default is set to 10, as Algolia recommends
        an average record size of about that size. If the variable is set to 0
        then the pseudo-text will be used no matter what."""
        dict_to_be_pushed = {}
        if self.__unit_loaded_or_created:
            # first we use a loop to populate the dictionary to be pushed
            # only for the keys specified in list_of_fields_to_include_in_record
            for a_key in list_of_fields_to_include_in_record:
                val_previously_pushed = self.dict_algolia_unit[a_key][self.key_for_values_pushed]
                val_current = self.dict_algolia_unit[a_key][self.key_for_values_current]
                if type(val_previously_pushed) is list:
                    val_previously_pushed.sort()
                if type(val_current) is list:
                    val_current.sort()
                if val_previously_pushed != val_current:
                    dict_to_be_pushed[a_key] = val_current

            # and below we need to take care of some special cases

            # one special case is the ID. For algolia to know which record to update
            # the ID must always be present. The loop above would only add the ID to the
            # record if it changed (which it never does) so we need to add it
            # always (so long as a change was in fact detected.)
            if dict_to_be_pushed:
                dict_to_be_pushed[self.fieldname_id] = \
                    self.dict_algolia_unit[self.fieldname_id][self.key_for_values_current]

            # both of the next two cases are related to transcripts metadata
            # so if either one needs attention, we setup the variables first
            # and then deal with each case individually
            str_transcript_text = ''
            str_pseudotranscript_text = ''
            if (self.fieldname_transcript in dict_to_be_pushed) or \
                    (self.fieldname_pseudotranscript in dict_to_be_pushed):
                vid_id = dict_to_be_pushed[self.fieldname_id]
                transcript = Transcript(vid_id)
                transcript.set_transcript_directory(path_to_directory_with_transcripts)
                transcript.load_transcript_object_from_dictionary(transcripts_simple_ds.fetch_data(vid_id))

                # if the transcript was added above during the loop,
                # what we have at the moment is the hash of the transcript,
                # not the transcript itself, so that needs to be resolved.
                if self.fieldname_transcript in dict_to_be_pushed:
                    str_transcript_text = transcript.get_transcript_from_disk()
                    if str_transcript_text:
                        dict_to_be_pushed[self.fieldname_transcript] = str_transcript_text
                    else:
                        logging.warning('Was unable to load the transcript from disk when creating an'
                                        ' Algolia record for pushing for video ID: ' + vid_id)
                        # if we weren't able to get the transcript for some reason
                        # we don't want to push the hash, which is what was previously populated
                        # so if for some reason (which shouldn't really ever happen, because if
                        # we have a hash, we should have a transcript on disk too) we were unable
                        # to load the transcript (and therefore unable to replace the hash with the
                        # transcript) we get rid of the field entirely from the dict to be pushed.
                        dict_to_be_pushed.pop(self.fieldname_transcript)

                # another special case is the pseudo-transcript
                # similarly, if this was added above during the loop, what we currently
                # have in the dictionary is a hash, so we want to replace it with actual data
                if self.fieldname_pseudotranscript in dict_to_be_pushed:
                    # first we check to see if a parameter passed to this function specifies
                    # that the size might influence whether the pseudo-text or the full text
                    # is used.
                    use_pseudotext = True
                    if dont_use_pseudotext_if_fulltext_is_less_tan_x_KB > 0:
                        # if the variable is greater than zero, it means that a size limit has
                        # been specified, for which any record under that size will have the
                        # full-text pushed, rather than the pseudo-text.
                        # So in odrder to calculate the size, we retrieve the full-text
                        str_transcript_text = (transcript.get_transcript_from_disk()).strip()
                        if str_transcript_text:
                            # before calculating size, we'll remove all consecutive white spaces
                            # (including consecutive spaces, consecutive tabs, consecutive newlines)
                            # because this field is only for searching, not for display purposes.)
                            str_transcript_text = " ".join(str_transcript_text.split())
                            transcript_size_in_bytes = len(str_transcript_text.encode('utf-8'))
                            if transcript_size_in_bytes < (dont_use_pseudotext_if_fulltext_is_less_tan_x_KB * 1000):
                                # if the fulltext fits under the limit, we simply use the fulltext, isntead of
                                # the pseudo-text
                                use_pseudotext = False
                                dict_to_be_pushed[self.fieldname_pseudotranscript] = str_transcript_text

                    if use_pseudotext:
                        pt_file_exists = transcript.is_pseudotranscript_filename_populated()
                        if pt_file_exists:
                            str_pseudotranscript_text = transcript.get_pseudotranscript_from_disk()
                            if str_pseudotranscript_text:
                                dict_to_be_pushed[self.fieldname_pseudotranscript] = str_pseudotranscript_text
                            else:
                                logging.warning('Was unable to load the pseudo-transcript from disk when creating an'
                                                ' Algolia record for pushing for video ID: ' + vid_id)
                                # if we weren't able to get the pseudo-transcript for some reason
                                # we don't want to push the hash, which is what was previously populated
                                # so if for some reason (which shouldn't really ever happen, because if
                                # we have a hash, we should have a transcript on disk too) we were unable
                                # to load the transcript (and therefore unable to replace the hash with the
                                # transcript) we get rid of the field entirely from the dict to be pushed.
                                dict_to_be_pushed.pop(self.fieldname_pseudotranscript)

        else:
            logging.error('Unable to create record for pushing to Algolia. Object was not previously'
                          ' created or updated.')
        return dict_to_be_pushed

    # ------------------------ END FUNCTION ------------------------ #

    def dump_algolia_unit_as_dict(self):
        return self.dict_algolia_unit

    # ------------------------ END FUNCTION ------------------------ #

    def __errors_in_incoming_data(self, dict_data_to_check, bool_full_unit):
        """Use this method to make sure incoming data doesn't have any surprises
        and meets expected standards.
        The parameter distinguishes between full records which have data about
        both pushed AND updated values, and one-dimensional records that are
        simply providing new data.
        RETURN VALUES. The method returns True if errors were found, or false
        otherwise."""
        working_dict = copy.deepcopy(self.__dct_template)
        errors_found = False
        lst_of_fieldnames = working_dict.keys()
        for a_key in dict_data_to_check:
            if a_key in lst_of_fieldnames:
                if bool_full_unit:
                    sub_key_dict = dict_data_to_check[a_key]
                    if len(sub_key_dict) != 2:
                        errors_found = True
                        logging.error(
                            'When examining key -> ' + a_key + ' an unexpected number of entries were found. Two'
                                                               ' entries expected. One for pushed values, and one'
                                                               ' for current values.')
                    if self.key_for_values_pushed in sub_key_dict:
                        type_incoming = type(sub_key_dict[self.key_for_values_pushed])
                        type_should_be = type(working_dict[a_key][self.key_for_values_pushed])
                        if type_incoming != type_should_be:
                            errors_found = True
                            logging.error(
                                'When examining key -> ' + a_key + ' there was a Type mis-match between incoming'
                                                                   ' data labeled as "previously pushed",'
                                                                   ' and expected data.')
                    else:
                        errors_found = True
                        logging.error(
                            'When examining key -> ' + a_key + ' there was no entry for pushed values in a full unit.')
                    if self.key_for_values_current in sub_key_dict:
                        type_incoming = type(sub_key_dict[self.key_for_values_current])
                        type_should_be = type(working_dict[a_key][self.key_for_values_current])
                        if type_incoming != type_should_be:
                            errors_found = True
                            logging.error(
                                'When examining key -> ' + a_key + ' there was a Type mis-match between incoming'
                                                                   ' data labeled as "updated", and expected data.')
                    else:
                        errors_found = True
                        logging.error(
                            'When examining key -> ' + a_key + ' there was no entry for current values in a full unit.')
                else:
                    # we are here if the 'full-unit' parameter was passed as False.
                    type_incoming = type(dict_data_to_check[a_key])
                    type_should_be = type(working_dict[a_key][self.key_for_values_current])
                    if type_should_be != type_incoming:
                        errors_found = True
                        logging.error(
                            'The value for key ' + a_key + ' passed in a dictionary to create an Algolia Unit object'
                                                           ' was not the expected type.')
            else:
                errors_found = True
                logging.error('The key -> ' + a_key + ' passed in a dictionary to create an Algolia Unit object'
                                                      ' was not found in the list of Algolia fields.')
        return errors_found
    # ------------------------ END FUNCTION ------------------------ #
