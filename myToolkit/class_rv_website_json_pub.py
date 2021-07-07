import logging
import my_globals
from my_building_blocks import compare_two_simple_lists, \
    compare_two_simple_dictionaries


class RVwebsitePublication:
    """This class creates an object that represents information about a
    issue/publication from the Real Vision website."""

    def __init__(self, dict_publication_metadata):
        """Initialize an object that represents metadata about an issue/publication from
        the Real Vision website by passing it metadata for a single publication
        as downloaded from the RV website."""

        self.str_id = ''
        self.str_type = ''
        self.str_url_thumbnail = ''
        self.str_title = ''
        self.bool_is_published = False
        self.int_page_count = 0
        self.int_likes_count = 0
        self.int_dislikes_count = 0
        self.lst_topic_names = []
        self.lst_asset_names = []
        self.int_published_on = 0
        self.str_product_id = ''
        self.str_summary = ''

        # in case the publication is initialized as empty, we check first.
        # if it isn't empty, we load the values
        if dict_publication_metadata:
            # first we setup a dictionary that maps one-to-one the fields from the json returned
            # by the RV website, to the attributes of this class.
            dict_mapping_webpub_to_self_attribs = my_globals.dict_mapping_rv_webpubjson_2_rv_webpubclass_attrib
            # because the incoming publication data is hierarchical, we also need a dictionary that tells
            # us where in the hierarchy to find each item. This dictionary is in the format of
            # {field:[top_level_name, next_level, next_level, item]}
            # where the list that is the value inside the dictionary, is the list of names of
            # the different levels in the hieararcy.
            dict_rvwebpub_schema = my_globals.dict_pubs_from_website_schema

            # we loop through the mapping dictionary in order to set all the attributes of the class.
            # this method assumes the mapping is one-to-one, IT DOES NOT CHECK
            lst_webpub_fields = dict_mapping_webpub_to_self_attribs.keys()
            for a_field in lst_webpub_fields:
                # first we get the name of the attribute in this class that the field from the
                # website maps to
                self_attrib_name = dict_mapping_webpub_to_self_attribs[a_field]
                # next, using the schema dictionary, we pull a grab a list that describes the
                # path to traverse in the hierarchy in order to extract the data
                lst_describing_hierarchy_path = dict_rvwebpub_schema[a_field].copy()
                value = self.__recursive_extract_field_from_hierarchy(dict_publication_metadata,
                                                                      lst_describing_hierarchy_path)
                # the method used above returns an empty string if the value was not found. In that case
                # we do nothing. We only set the value to a new value, if something was found. Otherwise
                # the default value given as part of the class declaration is what remains.
                if value:
                    setattr(self, self_attrib_name, value)

    # ------------------------ END FUNCTION ------------------------ #

    def compare_with_other_version_of_same_pub(self, other_pub_version, lst_attrib_names_to_compare):  # noqa: C901
        """This method compares the (self) object, to another one. This is useful,
        for example, in the case where a version of the same publication has been re-downloaded
        and we need to see if there are any changes.
        The parameters are the other publication (same type as this class) and a list containing
        attribute (variable) names of this class. Only those variables are compared
        between the two objects.
        The object being passed in as the parameter should be the object with the most up-to-date
        information.
        RETURN value. This method returns a dictionary with any changes that were found
        between the two versions of the publication.
        This method treats the publication passed to the method as a 'new' version of the publication
        so, in the summary, the items related to the publication passed as a parameter will
        be labelled as the newer ones. Or for example, if something exists in the publication
        that is passed as a parameter, but not in self, then it is said that that
        field or value has been 'removed', because the version being compared, for the
        sake of the comparison, is considered the more up-to-date version."""

        str_to_label_old_value = 'prior value'
        str_to_label_new_value = 'new value'
        str_to_label_something_didnt_exist = 'was added'
        str_to_label_something_was_removed = 'was removed'

        # start a dictionary to track differences
        dict_of_the_changes = {}

        for attrib_name in lst_attrib_names_to_compare:
            diff_found_in_current_attrib = False
            # some of the code that gets executed is very similar in many of the
            # cases tested for below. But there are some special cases where the
            # code is slightly different. So we use the variable below to track
            # the cases where the dictionary hasn't been updated yet, so we can
            # do code for all the remaining cases at the end.
            typical_case = False
            # start  small dictionary for this particular attribute, that will track
            # before and after state.
            dict_field_changes = {}

            attrib_val_self = getattr(self, attrib_name)
            attrib_val_incoming = getattr(other_pub_version, attrib_name)

            type_attrib_self = type(attrib_val_self)
            type_attrib_incoming = type(attrib_val_incoming)
            if type_attrib_incoming == type_attrib_self:
                if type_attrib_incoming is list:
                    dict_results = compare_two_simple_lists(attrib_val_self, attrib_val_incoming)
                    if dict_results:
                        diff_found_in_current_attrib = True
                        if 'in2not1' in dict_results:
                            dict_field_changes[str_to_label_something_didnt_exist] = dict_results['in2not1']
                        if 'in1not2' in dict_results:
                            dict_field_changes[str_to_label_something_was_removed] = dict_results['in1not2']
                elif type_attrib_incoming is dict:
                    if not compare_two_simple_dictionaries(attrib_val_incoming, attrib_val_self):
                        diff_found_in_current_attrib = True
                        typical_case = True
                else:
                    # if we reach here, the types should be simple types
                    # like ints or strings and can be compared easily.
                    if attrib_val_incoming != attrib_val_self:
                        diff_found_in_current_attrib = True
                        typical_case = True
            else:
                # because of the way this class is declared (with each value declares as the correct
                # type of object it should be) this area of the code, where the compared attributes
                # have different types is unexpected. It should not really ever be reached in theory.
                logging.warning('Unexpected area of code reached in function that compares two'
                                ' objects of type RVwebsitePublication. Two attributes of the same class'
                                ' should not have different types.')
                diff_found_in_current_attrib = True
                typical_case = True

            if diff_found_in_current_attrib:
                # for non-typical cases above, the change dictionary for this field has already
                # been populated, but for the typical cases, we do it now
                if typical_case:
                    dict_field_changes[str_to_label_old_value] = attrib_val_self
                    dict_field_changes[str_to_label_new_value] = attrib_val_incoming
                dict_of_the_changes[attrib_name] = dict_field_changes
                logging.debug('Difference detected based on ' + attrib_name + ' in item ' + self.str_id)

        return dict_of_the_changes

    # ------------------------ END FUNCTION ------------------------ #

    def __recursive_extract_field_from_hierarchy(self, dictWithHierarchicalData, listDescribingHierarchy):
        """This is a recursive function to extract data out of
        a JSON hierarchical structure."""
        if len(listDescribingHierarchy) == 0:
            print('Code should not reach this point in this recursive function')
            exit(0)
        nextFieldInHierarchy = listDescribingHierarchy.pop(0)
        # I came across a situation where the expected field did not
        # exist, which is annoying in data that should be standard, but
        # such is life. So we need to check it exists first.
        if nextFieldInHierarchy in dictWithHierarchicalData:
            nextDataInHerarchy = dictWithHierarchicalData[nextFieldInHierarchy]
        else:
            # if the field doesn't exist in the expected hierarchy
            # we return an empty string.
            return ''
        if len(listDescribingHierarchy) == 0:
            return nextDataInHerarchy
        else:
            return self.__recursive_extract_field_from_hierarchy(nextDataInHerarchy, listDescribingHierarchy)
    # ------------------------ END FUNCTION ------------------------ #
