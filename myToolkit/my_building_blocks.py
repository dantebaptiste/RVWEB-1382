import os
import sys
import time
import glob
import logging
from datetime import datetime
from hashlib import sha256


def convertDictKeysToList(theDict, sorted=True):
    listToReturn = []
    for anItem in theDict:
        listToReturn.append(anItem)
    if sorted is True:
        listToReturn.sort()
    return listToReturn


# ------------------------ END FUNCTION ------------------------ #

def compare_two_simple_dictionaries(dict1, dict2):
    """A function that compares if two dictionaires are
    equivalent - in other words they have exactly the same
    keys, and each key corresponds to the same value in both
    dictionaries.
    NOTE that at the time of this writing this function
    assumes that the data inside the dictionary is quite simple,
    for example strings or integers. If the data inside the
    dictionary is something more complex like
    another dictionary or a list, then the results may be
    unexpected, because the objects may be equivalent, but
    might be sorted differently in dict1 and dict2.
    Function returns True if the dictionaries are the same,
    and False if they are different."""
    dictionaries_are_equivalent = True
    # first, check if the dicts have the same keys
    if set(dict1.keys()) != set(dict2.keys()):
        dictionaries_are_equivalent = False
    else:
        # the code reaches here if the keys are the same,
        # so now it is necessary to check if the value for
        # each key corresponds to the exact same value in
        # the other dict.
        for a_key in dict1:
            if dict1[a_key] != dict2[a_key]:
                dictionaries_are_equivalent = False
                break
    return dictionaries_are_equivalent


# ------------------------ END FUNCTION ------------------------ #

def compare_two_simple_lists(list1, list2):
    """A function that compares if two lists are
    equivalent - in other words they have exactly the same
    items. If the items are the same, but out of order, the
    lists are still considered equivalent.
    NOTE that at the time of this writing this function
    assumes that the data inside the list is quite simple,
    for example the contents are strings or integers. If the data inside the
    list is something more complex like
    another dictionary or a list, then the results may be
    unexpected.
    Function returns a dictionary. The dictionary is empty if the lists
    are the same. Otherwise it returns a dictionary with two keys:
    'in1not2'
    'in2not1'
     where each keys is associated with its corresponding (python) SET of items"""
    dict_to_return = {}
    set1 = set(list1)
    set2 = set(list2)
    items_in1_and_not_in2 = set1 - set2
    items_in2_and_not_in1 = set2 - set1
    if len(items_in1_and_not_in2) > 0:
        dict_to_return['in1not2'] = list(items_in1_and_not_in2)
    if len(items_in2_and_not_in1) > 0:
        dict_to_return['in2not1'] = list(items_in2_and_not_in1)
    return dict_to_return


# ------------------------ END FUNCTION ------------------------ #

def readFileIntoAlistAndStripWhitespace(fileName):
    with open(fileName) as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    return content


# ------------------------ END FUNCTION ------------------------ #


def projectStandardTimestamp():
    return datetime.now().strftime('%Y%m%d-%H%M')


# ------------------------ END FUNCTION ------------------------ #


def extractIndividualItemsFromTextList(someListAsString):
    """ This function extracts individual items from a text list and
        places them in a python list. For example, it could be given
        the string: "Juan Valdez, Ben Kinsley, and Joe Hammer"
        the output should be a python list that looks like
        ['Juan Valdez', 'Ben Kinsley', 'Joe Hammer']

        IMPORTANT NOTE!!
        This function will not work for lists that are SUPPOSED to contain the word
        'and' WITHIN an individual item. Only the use of the word 'and' as the last
        separator in a list is expected.
        For example, using the function on the following string:
        "rice, macaroni and cheese, tomatoes, Ben and Jerrys, lettuce, and frosting"
        will not yield the expected results because of the 'ands' that are
        PART of one item. So, macaroni and cheese will be separated into two
        distinct items, where they should really remain as only one."""
    logging.debug('Starting function that extracts (into a python list) individual '
                  'items from a string that contains comma separated values')
    # I had an issue whe a None type was passed to this function. The program
    # would error. So now I wrap the whole function in an if that checks
    # if the parameter passed is not a string
    listToReturn = []
    if type(someListAsString) is str:
        strStripped = someListAsString.strip('][,')
        listPossibleJRsubstirngs = [', Jr.;', ',Jr.;', ' Jr.;',
                                    ', Jr.,', ',Jr.,', ' Jr.,', ', Jr.',
                                    ', Jr,', ' Jr.', ' Jr,'
                                    ]
        strJRplaceholder = '--JRS--,'
        strStandardizedJR = ' Jr.'

        # in the line below, sometimes when there is a comma placed before the 'and', then
        # when you replace the ' and ' you end up with two commas in a row. That is why
        # the other part reads: .replace(',,',',')
        strRemovedANDseparators = strStripped.replace(' & ', ',').replace(' and ', ',').replace(',,', ',')

        strJRSreplaced = strRemovedANDseparators
        for aPossibility in listPossibleJRsubstirngs:
            if aPossibility in strRemovedANDseparators:
                strJRSreplaced = strJRSreplaced.replace(aPossibility, strJRplaceholder)

        # now have to account for the case where the string ', Jr' is the last substring
        # at the end of strJRSreplaced, becaues that won't have been found above (and we
        # shouldn't search for it above, in case it isn't at the end and a funny string were
        # to exist 'Smith, Jrome Doe, Jane Williams' -> that would cause 'Jrome Doe' to
        # be found are replaced with the placeholder, which would not be ideal.
        if strJRSreplaced.endswith(', Jr'):
            strJRSreplaced = strJRSreplaced.replace(', Jr', strJRplaceholder)
        if strJRSreplaced.endswith(',Jr'):
            strJRSreplaced = strJRSreplaced.replace(',Jr', strJRplaceholder)
        # note the two ifs above should probably be replaced at some point with a loop
        # because maybe there are more possibilities for Jr substrings at the end
        # of the major string, so it would make more sense to loop through them.

        listOfSubstrings = strJRSreplaced.split(',')
        # the call to split() above removed the comma, so we need to remove
        # the comma from the placeholder as well to be able to find in in the
        # resulting strings
        strJRplaceholder = strJRplaceholder.strip(',')
        for aString in listOfSubstrings:
            aString = aString.strip()
            aString = aString.replace(strJRplaceholder, strStandardizedJR)
            if aString.endswith('Jr'):
                aString = aString.replace('Jr', strStandardizedJR)
            listToReturn.append(aString.strip())
    return listToReturn


# ------------------------ END FUNCTION ------------------------ #


def removeCommonPersonNamePrefixes(str_name, all_caps_tuple_of_prefexis):
    # just in case, it is best to wrap these functions that work
    # on strings in an if statement that checks whether the
    # argument passed is in fact a string.
    logging.debug('Starting function to remove common prefixes from a name')
    str_name_to_return = ''
    if type(str_name) is str:
        str_name_to_return = str_name.strip()
        upper_case_version_of_name = str_name.strip().upper()
        # first check if the name starts with any of the prefixes.
        # if it doesn't we can return already
        if upper_case_version_of_name.startswith(all_caps_tuple_of_prefexis):
            for aPrefix in all_caps_tuple_of_prefexis:
                if upper_case_version_of_name.startswith(aPrefix):
                    slice_start_idx = len(aPrefix)
                    str_name_to_return = str_name[slice_start_idx:].strip()
                    break
    return str_name_to_return


# ------------------------ END FUNCTION ------------------------ #


def recursiveExtractFieldFromHierarchy(dictWithHierarchicalData, listDescribingHierarchy):
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
        return recursiveExtractFieldFromHierarchy(nextDataInHerarchy, listDescribingHierarchy)


# ------------------------ END FUNCTION ------------------------ #


def print_2console_and_2logfile(list_of_strings_to_print, str_pathNbase_of_filename='', print2console=True,
                                print2file=True):
    """This function prints a set of lines passed to it in a list to
    both the console and a log file. The path and base filename should be
    passed to this function as a single string."""
    fileName = ''
    if print2file:
        fileName = str_pathNbase_of_filename + '_' + projectStandardTimestamp() + '.log'
        with open(fileName, 'w') as logfile:
            for aLine in list_of_strings_to_print:
                logfile.write(aLine + '\n')
    if print2console:
        for aLine in list_of_strings_to_print:
            print(aLine)
    return fileName


# ------------------------ END FUNCTION ------------------------ #


def make_sha256_hash(an_item, sort_characters=True):
    """This function takes the paramenter passed, converts it
     to a string, sorts the characters (unless told not to sort)
     and returns a string that is the sha256 hash of
     this converted/sorted string."""
    logging.debug('Creating SHA hash. Sorting of str(item) requested is -> ' + str(sort_characters))
    item_as_string = str(an_item)
    if sort_characters:
        item_as_string = ''.join(sorted(item_as_string))
    return sha256(item_as_string.encode()).hexdigest()


# ------------------------ END FUNCTION ------------------------ #


def make_now_timestamp():
    """Make a timestamp"""
    return int(round(time.time() * 1000))


# ------------------------ END FUNCTION ------------------------ #

def is_number(str):
    try:
        float(str)
        return True
    except ValueError:
        return False


# ------------------------ END FUNCTION ------------------------ #

def string_might_be_a_year(str, boundary_min_year=1700, boundary_max_year=2100):
    # function assumes that the string has already been checked to make sure it
    # is numeric.
    # check if the number is within the given boundaries
    might_be_a_year = False
    if '.' not in str:
        float_num = float(str)
        if float_num >= boundary_min_year and float_num <= boundary_max_year:
            # now check if the number is an integer
            int_num = int(float_num)
            if int_num == float_num:
                might_be_a_year = True
    return might_be_a_year


# ------------------------ END FUNCTION ------------------------ #

def convert_file_to_list_by_lines(full_path_to_file):
    """This fucntion receives a file, and converts it into a list
    where everything on one line, is made into one line item.
    For example, a file like:
    ab
    cd
    1
    is converted into the following python list:
    ['ab', 'cd', '1']
    Carriage returns are removed."""
    the_list = []
    try:
        with open(full_path_to_file) as file_object:
            the_list = file_object.readlines()
    except FileNotFoundError:
        logging.debug('File to convert to list could not be opened.')
    # then remove whitespace characters like `\n` at the end of each line
    the_list = [x.strip() for x in the_list]
    return the_list
    # ------------------------ END FUNCTION ------------------------ #


def tokenize_list_containing_people_fullnames(a_list_containing_fullnames, make_tokens_lower_case=True):
    """For example, for the list ['Ash Bennington', 'Ed Harrison']
    we want to return
    ['ash', 'bennington', 'ash beggington', 'ed', 'harrison', 'ed harrison']
    in other words, we want a list with all possible combinations of ORDERED,
    SUCCESSIVE, NON-INTERMEDIATE, tokens.
    By ordered and successive, I mean that, for example, if the following
    item is in the original list: 'Lyn Gates Jones'
    the tokens we want created from this string are
    ['lyn', 'gates', 'jones', 'lyn gates', 'lyn gates jones']
    but we DON'T for example want
    'gates lyn'
    because that is out of order with respect to the original item
    or
    'gates jones'
    because that is an intermediate succession of tokens, and it is
    unlikely that a person will ever be referred to that way.
    Generally people will be referred to as their first name, or
    first and middle, or first, middle, and last all in a row.
    HOWEVER, ONE EXCEPTION is that people will often be referred to
    by just their last name. So we do want to include that single token
    as well."""
    set_possible_tokens = set()
    if a_list_containing_fullnames:
        # first we start a for loop that deals with each name, one per iteration.
        for an_item in a_list_containing_fullnames:
            list_split = an_item.split()
            # now that the list is split, we construct all the possible
            # successive, ordered, non-intermediate
            # tokens with a set of nested loops
            num_individual_tokens = len(list_split)
            # the counter below is deliberately started at 1, this is not
            # a mistake. The same goes for the '<=' in the outer loop definition.
            counter = 1
            while counter <= num_individual_tokens:
                str_to_add_to_set = ''
                sub_counter = 0
                while sub_counter < counter:
                    str_to_add_to_set += list_split[sub_counter] + ' '
                    sub_counter += 1
                str_to_add_to_set = str_to_add_to_set.strip()
                str_to_add_to_set = str_to_add_to_set.lower()
                set_possible_tokens.add(str_to_add_to_set)
                counter += 1
            # the last thing we want to do is add the last token
            # (the last name) to the list.
            set_possible_tokens.add(list_split[-1].lower())
    return list(set_possible_tokens)


# ------------------------ END FUNCTION ------------------------ #

def move_list_of_files_to_a_directory(list_of_full_file_paths, full_path_destination_directory):
    """The function is pretty self explanatory. The list passed is the list of files to be moved.
    Each item in the list should be a full file path. The other parameter is the full file path
    of the destination where the files should be moved to. The directory path SHOULD INCLUDE the
    '/' as the last character.
    The function returns TRUE if the move of all of the files was successful."""
    success_in_moving_the_files = False
    for path in list_of_full_file_paths:
        filename = os.path.basename(path)
        os.rename(path, full_path_destination_directory + filename)
    success_in_moving_the_files = True
    return success_in_moving_the_files


# ------------------------ END FUNCTION ------------------------ #

def cleanup_older_files_in_a_dir(fullpath_to_clean, string_to_filter_files='*.json', num_files_to_keep=48):
    """Method to clean-up a certain number of the oldest files
    in a directory. In other words, only the newest num_files_to_keep."""
    logging.debug("Beginning function that deletes older files used by the"
                  " AirTableSubsetOnDisk class.")
    # get a list of the files in the directory
    list_of_files = glob.glob(fullpath_to_clean + string_to_filter_files)
    while len(list_of_files) > num_files_to_keep:
        oldest_file = min(list_of_files, key=os.path.getctime)
        if os.path.exists(oldest_file):
            os.remove(oldest_file)
        else:
            logging.warning("Attempted to delete a file that does not exist.")
        list_of_files = glob.glob(fullpath_to_clean + string_to_filter_files)


# ------------------------ END FUNCTION ------------------------ #


def setup_logging(level='info'):
    """Setup logging"""
    # I used to send logging to a file at data/outputFiles/logs/main.log, but with
    # the move of the infrastructure into AWS ECS, I'm changing the logging to go
    # to stdout instead so that it can be picked up by a log group in AWS CloudWatch.
    if level == 'info':
        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y%m%d-%H:%M:%S',
                            level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
    if level == 'debug':
        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y%m%d-%H:%M:%S',
                            level=logging.DEBUG, handlers=[logging.StreamHandler(sys.stdout)])
# ------------------------ END FUNCTION ------------------------ #
