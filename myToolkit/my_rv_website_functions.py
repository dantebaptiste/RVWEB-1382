import os
import json
import urllib.request
import logging
import glob
import time
import my_globals
import csv
from class_trancript import Transcript
from class_simpleDS import SimpleDS
from class_percent_tracker import PercentTracker
from class_rv_website_json_vid import RVwebsiteVid
from class_rv_website_json_pub import RVwebsitePublication
from my_building_blocks import make_now_timestamp, \
    make_sha256_hash, \
    projectStandardTimestamp, \
    move_list_of_files_to_a_directory, \
    cleanup_older_files_in_a_dir


def extractIDstringFromURLstring(theURL):
    """in data returned from the API, IDs are often part of a URL, specifically
        at the end of a URL. So this function receives such a URL, and returns
        the string portion that is after the last '/'"""
    indexOfFirstCharAfterLastFwdSlash = theURL.rfind('/') + 1
    return (theURL[indexOfFirstCharAfterLastFwdSlash:len(theURL)])


# ------------------------ END FUNCTION ------------------------ #


def make_list_of_urls_with_vids_data(product_id, multiple, max_multiple):
    # explanation about max_multiple parameter
    # The website api displays videos in sets of 24 (0 to 23 in the JSON data)
    # The number max_multiple, is the last multiple of 24 (24 at the time
    # of this writing, but could change to be another multiple depending
    # on what the website api is configured to do)
    # that still displays videos. In other words, if you create a URL using
    # THE NEXT multiple of 24, when you query the URL, that page will be empty.

    # Construct the URLs that are going to be extracted and put them in a list
    # The entire URL should look something like
    # https://www.realvision.com/rv/api/videos?page%5Bskip%5D=INSERTANUMBERHERE&filter%5Bvideo_product_id%5D=television
    # with an integer being inserted in the URL above where it says INSERTANUMBERHERE
    # that URL skips all the video entries before, and starts returning data at that Number of video
    logging.debug('Building a list of URLs to pull video metadata from RV website')
    firstPartURL = 'https://www.realvision.com/rv/api/videos?page%5Bskip%5D='
    numberToInsertInURL = 0
    lastPartURL = '&filter%5Bvideo_product_id%5D=' + product_id
    listOfUrls = []
    while numberToInsertInURL <= max_multiple:
        listOfUrls.append(firstPartURL + str(numberToInsertInURL) + lastPartURL)
        numberToInsertInURL += multiple
    logging.debug('List built. Returning list of URLs to caller function.')
    return listOfUrls


# ------------------------ END FUNCTION ------------------------ #


def pull_products_info_from_web_and_save2files():
    """This function updates attributes about "products" (eg. 'television')
    from the RV website."""
    logging.info("Updating attributes about 'products' from RV website.")
    lst_product_ids = my_globals.lst_rv_website_product_ids
    high_limit_on_search = my_globals.num_upper_bound_on_search_of_highest_multiple
    for prod_id in lst_product_ids:
        logging.info('Finding info for product: ' + prod_id)
        multiple = 0
        max_multiple = 0
        info_pulled_without_errors = False
        try:
            # function called below finds how many objects the
            # website api is returning per page
            multiple = refresh_integer_multiple_from_web(prod_id)
            logging.info('Website returning ' + str(multiple) + ' items per page.')
            # function called below uses a binary search to find the highest
            # multiple of 24 that still returns data for a specific product_id
            max_multiple = refresh_max_multiple_from_web(prod_id, multiple, high_limit_on_search)
            logging.info('Maximum multiple: ' + str(max_multiple))
            info_pulled_without_errors = True
        except Exception as e:
            logging.error("Problem during function 'pull_products_info_from_web_and_save2files'."
                          " The Exception was: " + repr(e))

        # we'll only modify the currently held information on disk if we are fairly
        # confident that the code above ran without issues. If there were no problems
        # the boolean variable should have been set to true, and also the variable 'multiple' should
        # be different to zero (note that max_multiple can sometimes be zero for a
        # new product. For example, for a few weeks, max multiple was zero for the crypto product.
        if info_pulled_without_errors and (multiple != 0):
            # save the max multiple in a dictionary, in case in the future
            # we want to save other attributes related to the product id.
            dict_attribs_prodid = {'base multiple': multiple,
                                   'max multiple': max_multiple}
            # Then we save that to a file
            filename = my_globals.str_dir4_product_id_info + prod_id
            with open(filename, 'w') as fileJSONdump:
                logging.debug('Saving dictionary of attributes to file -> ' + filename)
                json.dump(dict_attribs_prodid, fileJSONdump)


# ------------------------ END FUNCTION ------------------------ #


def grab_from_disk_product_info(str_product_id, str_data_wanted):
    """There is another function that saves information about
    real vision products to file. This function retrieves the
    multiple currently being used (how many videos per product
    are returned by the website api.)"""
    with open(my_globals.str_dir4_product_id_info + str_product_id, 'r') \
            as theJSONfile:
        dictJSONdata = json.load(theJSONfile)
        return dictJSONdata[str_data_wanted]


# ------------------------ END FUNCTION ------------------------ #


def pullShowsJSONdataFromWebsiteToFile():
    """# This function pulls metadata about Real Vision SHOWS (the broad categories, not
    each individual video) down from the website and saves the info to a file.
    This is useful to have the infomration locally, rather than
    always pulling it from the website. It is data that probably does not change very
    often, so it makes sense to have a local copy."""

    # First, delete all existing files in the directory
    # (With another directory I ran into an issue where the files
    # got messed up, probably due to an execution being halted half
    # way. So it is safest, for directories that hold files that get
    # pulled form the web, to clear the directory and start over
    # every time a pull is done.)
    # To do so, make a list of the files in the directory
    logging.debug('Making a list of filenames that exist in the directory containing raw json info'
                  ' about shows on the RV website.')
    lst_of_filenames = []
    for entry in os.scandir(my_globals.str_dir_path_raw_website_json_shows):
        lst_of_filenames.append(entry.path)
    # then delete each of those files. This can maybe be done
    # in the same loop, but I'm not sure if that would count
    # as changing the contents of a structure while you iterate
    # it, so I'm erring on the side of creating a list of the
    # files first, and then deleting using that list.
    logging.debug('Deleting files containing RV website SHOWS json metadata.')
    for path in lst_of_filenames:
        os.remove(path)

    lst_product_ids = my_globals.lst_rv_website_product_ids
    str_main_url_query = 'https://www.realvision.com/rv/api/shows?filter[show_product_id]='
    logging.debug('Set base URL for querying SHOWS to: ' + str_main_url_query)
    # the website returns a dictionary where the key is "data"
    # and the value for that key is a list of shows. So we will
    # save the shows for ALL product IDs in the same format
    # so that downstream functions can simply open the data in
    # the same way. So here we initialize a dictionary that will
    # contain, associated with its 'data' key, a list of all shows.
    dict_of_all_shows = {"data": []}

    logging.debug('Creating a dictionary containing SHOWS from all products.')
    for a_product in lst_product_ids:
        url = str_main_url_query + a_product
        logging.debug('Downloading SHOW metadata for product: ' + a_product)
        response = urllib.request.urlopen(url, timeout=my_globals.int_timeout)
        dict_json_shows = json.load(response)
        list_of_shows_of_a_product = dict_json_shows["data"]
        logging.debug('Extending full list of SHOW metadata with shows from product: ' + a_product)
        dict_of_all_shows["data"].extend(list_of_shows_of_a_product)

    file_path = my_globals.str_file_raw_website_json_shows
    with open(file_path, 'w') as fileWithJSONdump:
        logging.debug('Saving dictionary containing the SHOWS json metadata of all products to: ' + file_path)
        json.dump(dict_of_all_shows, fileWithJSONdump)


# ------------------------ END FUNCTION ------------------------ #


def extractFieldsFromShowsData():
    """This function pulls previously downloaded metadata about
    Real Vision SHOWS (the broad categories of shows, not individual
    videos) from a file, and returns a dictionary with fields of
    interest, such as the show ID and the show name."""
    # Shows data once imported into a python structure
    #   json.load() the whole data      dict
    #     extracted "data" structure    list  (of shows)
    #       show                        dict
    #         id                        str
    #         type                      str
    #         relationships             dict
    #         links                     dict
    #         attributes                dict
    #           #has many, for now wer are interested in
    #           show_description        str
    #           show_id                 str
    #           show_name               str

    with open(my_globals.str_file_raw_website_json_shows, 'r') as theJSONfile:
        dictJSONdata = json.load(theJSONfile)
        # a nice way to see the json data is:
        # print(json.dumps(dictJSONdata, indent=4))

    listShowsInfo = dictJSONdata["data"]

    dictShows = {}
    for aShow in listShowsInfo:
        listShowAttributes = [(aShow['attributes'])['show_name'], (aShow['attributes'])['show_description']]
        dictShows[aShow['id']] = listShowAttributes
    # print(json.dumps(dictShows, indent=4))

    return dictShows


# ------------------------ END FUNCTION ------------------------ #

def detect_if_data_on_disk_is_full_set_of_vids():
    """This function takes a look at the set of files on disk regarding video metadata
    pulled form the RV website, and decides if the latest 'pull' was a partial pull or
    a full pull. This is done in order to assist with figuring out if any videos were deleted
    at the source. This can only be figured out when the latest pull is a 'full' pull.
    It returns TRUE from a full pull, and FALSE for anything else."""

    # What we need to do, is check (for each product) how many files there SHOULD be
    # on disk (if the entire set of files was pulled) and then compare that with how
    # many actually are on disk. Only if the right number of files is found on disk
    # will the pull be considered 'full'
    logging.debug("Detecting if sets of videos on disk represent a 'full' or a 'partial' pull"
                  " from the RV website.")
    sets_of_files_represent_full_pull = True
    list_of_rv_video_products = my_globals.lst_rv_website_product_ids
    for a_product in list_of_rv_video_products:
        multiple_used = 0
        max_multiple_on_disk = 0
        num_files_should_have_for_full_pull = 0
        # first we grab the number of files that there SHOULD be if the pull
        # is FULL pull. To calculate this, we divide the max_multiple_on_disk by the multiple_used
        # and add 1.
        # For example, if the website is returning videos in sets of 24, and the max multiple is 48
        # there there will be files tagged for that product with a 0, a 24, and 48 for a total of
        # 3 files.
        multiple_used = grab_from_disk_product_info(a_product, 'base multiple')
        max_multiple_on_disk = grab_from_disk_product_info(a_product, 'max multiple')
        # If there are new products, for some time (until they have more than 24 videos created for
        # that product) the max_multiple_on_disk will be zero, so we check here first to avoid
        # division by zero.
        if max_multiple_on_disk != 0:
            num_files_should_have_for_full_pull = int(max_multiple_on_disk / multiple_used)
        # now, after the division has been done safely we can add one to arrive at the right number
        num_files_should_have_for_full_pull += 1
        # now we find out how many files there actually are
        path_pattern = my_globals.str_dir_path_raw_website_json_video_sets + '*' + a_product + '*'
        num_files_on_disk = len(glob.glob(path_pattern))
        if num_files_on_disk != num_files_should_have_for_full_pull:
            sets_of_files_represent_full_pull = False
            # as soon as we find a product that doesn't have the right number of files, we
            # can exit the loop/function
            break
    return sets_of_files_represent_full_pull


# ------------------------ END FUNCTION ------------------------ #

def detect_if_data_on_disk_is_full_set_of_pubs():
    """AT THE MOMENT THIS FUNCTION IS REALLY A PLACEHOLDER.
    When it comes to videos, the similar function can actually do some checks, because video
    files are downloaded in sets of 24, and we save to disk the number of sets there should be
    and how many videos in each set.
    For publications we don't have this data. So all we can do is check that a file exists
    for each product being pulled, and that the json inside the file contains a non-empty
    list.
    POSSIBLY IN THE FUTURE, if the publications start being returned by the API in sets
    this function can be changed to be similar to its corresponding function for the videos,
    and check that the right number of files exist on disk."""

    sets_of_files_represent_full_pull = True

    # get a list of the products for publications
    list_pubs_products = my_globals.lst_rv_website_product_ids_publications
    # now, for each product, we make sure there is one (and exactly one) file on disk
    for a_product in list_pubs_products:
        path_pattern = my_globals.str_dir_path_raw_website_json_pubs_sets + '*' + a_product + '*'
        list_of_files_per_product = glob.glob(path_pattern)
        if len(list_of_files_per_product) != 1:
            sets_of_files_represent_full_pull = False
            # as soon as we find a product that doesn't have the right number of files, we
            # can exit the loop/function
            break
        else:
            # otherwise, if there is indeed 1 and only 1 file per product, we
            # open the file, load the json, and make sure the list inside the json data
            # is not empty.
            directory = my_globals.str_dir_path_raw_website_json_pubs_sets
            filename_base = my_globals.str_filename_base_string4_raw_website_json_pubs_sets
            filepath = directory + filename_base + a_product + '.json'
            with open(filepath, mode='r') as file_with_set_of_pubs:
                dict_json_data = json.load(file_with_set_of_pubs)
                # there is a "data" structure in each file, and inside that
                # structure is the list of all the publications. So the following
                # line grabs the list of publications from inside the "data"
                # wrapper.
                list_vids = dict_json_data["data"]
                # now, we check to make sure there is something inside the list. If there isn't
                # we set the variable that is tracking the full pull to false.
                if not list_vids:
                    sets_of_files_represent_full_pull = False

    return sets_of_files_represent_full_pull


# ------------------------ END FUNCTION ------------------------ #

def pull_sets_of_videos_JSON_data_from_web_2disk(max_multiple_to_pull=-1):  # noqa: C901
    """This function pulls data about videos from the RV website. The websites returns
    the videos in sets (currently in sets of 24 per page.) Each of these sets
    is saved to a file.
    If the variable passed to the function 'max_multiple' is -1, then this means
    that the caller would like this function to grab ALL the info from the
    website. Otherwise, if the multiple is specified, only a subset, up to and
    including the multiple passed will be downloaded."""

    # First, we will move all the existing files into a temporary backup directory,
    # so that, just in case, if anything goes wrong during the pull of fresh data
    # we can revert (put the existing files back in place.) This is important to avoid
    # missing video files, which could potentially be interpreted as videos deleted at source
    # causing a chain-reaction of videos incorrectly being deleted in downstream workflows
    logging.debug('Making a list of filenames that exist in the directory containing raw json info'
                  ' downloaded from the RV website, where each file contains metadata about'
                  ' a SET of videos.')
    path_pattern = my_globals.str_dir_path_raw_website_json_video_sets + '*.json'
    lst_of_files_fullpaths = glob.glob(path_pattern)
    success_in_making_temporary_backup_of_files = False
    logging.debug('Creating a temporary backup of the existing files with sets of videos.')
    try:
        success_in_making_temporary_backup_of_files = move_list_of_files_to_a_directory(
            lst_of_files_fullpaths, my_globals.str_dir_path_raw_web_json_vid_sets_temp_backup)
    except Exception as e:
        logging.error("Problem during function 'pull_sets_of_videos_JSON_data_from_web_2disk' while trying"
                      " to make a temporary backup of current files (with sets of videos in them.) The function"
                      " will exit without downloading new data. The Exception was: " + repr(e))

    error_while_downloading_fresh_data = False
    # we only proceed with the download if we were able to successfully create a temporary backup
    # of the existing files
    if success_in_making_temporary_backup_of_files:
        try:
            list_of_rv_video_products = my_globals.lst_rv_website_product_ids
            for a_product in list_of_rv_video_products:
                logging.debug('Getting from disk the number (multiple) of videos returned by the RV'
                              ' website for product: ' + a_product)
                multiple_used = grab_from_disk_product_info(a_product, 'base multiple')
                max_multiple_on_disk = grab_from_disk_product_info(a_product, 'max multiple')
                max_multiple = max_multiple_to_pull
                if (max_multiple == -1) or (max_multiple_on_disk < max_multiple_to_pull):
                    # if max_multiple (passed as a parameter to the function) is -1 or if it is less
                    # than max_multiple_to_pull (this happens when a product is relatively new and
                    # does not have too many videos yet) then we want to pull ALL of the info,
                    # available, so use the max multiple from disk.
                    # Otherwise, simply use the value passed as the parameter which is
                    # already set above
                    max_multiple = max_multiple_on_disk
                # explanation about max_multiple
                # The website api displays videos in sets of 24 (0 to 23 in the JSON data)
                # The number max_multiple, is the last multiple of 24 (24 at the time
                # of this writing, but could change to be another multiple depending
                # on what the website api is configured to do)
                # that still displays videos. In other words, if you create a URL using
                # THE NEXT multiple of 24, when you query the URL, that page will be empty.

                logging.debug(my_globals.str_logging_func_next + make_list_of_urls_with_vids_data.__name__)
                listOfUrls = make_list_of_urls_with_vids_data(a_product, multiple_used, max_multiple)
                logging.debug(my_globals.str_logging_func_exited + make_list_of_urls_with_vids_data.__name__)

                # Now, output the json data returned by each request into a file.
                fileNumber = 0
                logging.info('Working on product: ' + a_product)
                logging.debug(
                    'Pulling videos (for each URL constructed) from RV website and saving (one set per file) to disk.')
                for eachURL in listOfUrls:
                    if max_multiple != 0:
                        logging.info(
                            "{:.2%}".format(fileNumber / max_multiple) + " complete. Retreiving data from set: " + str(
                                fileNumber))
                    else:
                        logging.info("Retreiving data from set: " + str(fileNumber))
                    logging.debug('Downloading: ' + eachURL)
                    response = urllib.request.urlopen(eachURL, timeout=my_globals.int_timeout)
                    theDirectory = my_globals.str_dir_path_raw_website_json_video_sets
                    fileName = theDirectory + my_globals.str_filename_base_string4_raw_website_json_video_sets + \
                        a_product + str(fileNumber) + '.json'
                    with open(fileName, 'w') as fileWithJSONdump:
                        logging.debug('Saving json data from ' + eachURL + ' -> ' + fileName)
                        fileWithJSONdump.write(response.read().decode('utf-8'))
                    fileNumber += 24
        except Exception as e:
            logging.error("Problem during function 'pull_sets_of_videos_JSON_data_from_web_2disk' while trying"
                          " to download fresh sets of videos metadata. The Exception was: " + repr(e))
            error_while_downloading_fresh_data = True

        # if we ran into an error while downloading fresh data, we delete any files that did manage
        # to be created and we restore the temporary backup
        if error_while_downloading_fresh_data:
            # delete any files that were created
            path_pattern = my_globals.str_dir_path_raw_website_json_video_sets + '*.json'
            lst_of_files_fullpaths = glob.glob(path_pattern)
            for path in lst_of_files_fullpaths:
                os.remove(path)
            # move the temporary backup back from whence it came
            path_pattern = my_globals.str_dir_path_raw_web_json_vid_sets_temp_backup + '*.json'
            lst_of_files_fullpaths = glob.glob(path_pattern)
            # note, the function below returns a 'success' value, but since we currently have no need
            # for it, we are not assigning it to a variable.
            move_list_of_files_to_a_directory(
                lst_of_files_fullpaths, my_globals.str_dir_path_raw_website_json_video_sets)
        else:
            # otherwise the fresh download ran without errors, so we delete the temporary
            # backup files
            path_pattern = my_globals.str_dir_path_raw_web_json_vid_sets_temp_backup + '*.json'
            lst_of_files_fullpaths = glob.glob(path_pattern)
            for path in lst_of_files_fullpaths:
                os.remove(path)


# ------------------------ END FUNCTION ------------------------ #


def pull_sets_of_publications_JSON_data_from_web_2disk(requests_session):  # noqa: C901
    """This function pulls data about publications from the RV website. In contrast to
    the videos, the website API does not return the issues (publications) in sets of
    24. It just returns one big dump of all of them. So to try to alleviate some of
    the load on the server I'm filtering the requests by product ID.
    RETURN VALUE. The function returns 'True' if there were no errors during
    the execution of the function. Otherwise, if errors were encountered, it
    returns false."""

    # First, we will move all the existing files into a temporary backup directory,
    # so that, just in case, if anything goes wrong during the pull of fresh data
    # we can revert (put the existing files back in place.) This is important to avoid
    # missing publication files, which could potentially be interpreted as publications deleted at source
    # causing a chain-reaction of publications incorrectly being deleted in downstream workflows
    error_while_downloading_fresh_data = False
    logging.debug('Making a list of filenames that exist in the directory containing raw json info'
                  ' downloaded from the RV website, where each file contains metadata about'
                  ' a SET of publications.')
    path_pattern = my_globals.str_dir_path_raw_website_json_pubs_sets + '*.json'
    lst_of_files_fullpaths = glob.glob(path_pattern)
    success_in_making_temporary_backup_of_files = False
    logging.debug('Creating a temporary backup of the existing files with sets of publications.')
    try:
        success_in_making_temporary_backup_of_files = move_list_of_files_to_a_directory(
            lst_of_files_fullpaths, my_globals.str_dir_path_raw_web_json_pub_sets_temp_backup)
    except Exception as e:
        logging.error("Problem during function 'pull_sets_of_publications_JSON_data_from_web_2disk' while trying"
                      " to make a temporary backup of current files (with sets of publications in them.) The function"
                      " will exit without downloading new data. The Exception was: " + repr(e))

    # we only proceed with the download if we were able to successfully create a temporary backup
    # of the existing files
    if success_in_making_temporary_backup_of_files:
        try:
            list_of_rv_pubs_products = my_globals.lst_rv_website_product_ids_publications
            for a_product in list_of_rv_pubs_products:
                logging.info('Working on product: ' + a_product)
                issues_url = 'https://www.realvision.com/rv/api/issues?filter[issue_product_id]=' + a_product
                logging.debug("Opening URL of 'issues' (reports) for product: " + a_product)
                json_data = {}
                try:
                    req = requests_session.get(issues_url, timeout=my_globals.int_timeout)
                    str_status = str(req.status_code)
                    if '200' in str_status:
                        logging.debug('Status: ' + str_status)
                    else:
                        logging.warning('Status: ' + str_status)
                    json_data = req.json()
                except Exception as e:
                    logging.warning('Unable to fetch issues (publications) from website API for product: '
                                    + a_product + ' The Exception was: ' + repr(e))
                    error_while_downloading_fresh_data = True

                # now, if we got some info back from the website API we save the info to disk
                if json_data:
                    the_dir = my_globals.str_dir_path_raw_website_json_pubs_sets
                    fileName = the_dir + my_globals.str_filename_base_string4_raw_website_json_pubs_sets \
                        + a_product + '.json'
                    with open(fileName, 'w') as fileWithJSONdump:
                        logging.info('Saving json data from ' + issues_url)
                        json.dump(json_data, fileWithJSONdump)
        except Exception as e:
            logging.error("Problem during function 'pull_sets_of_publications_JSON_data_from_web_2disk' while trying"
                          " to download fresh sets of publications metadata. The Exception was: " + repr(e))
            error_while_downloading_fresh_data = True

        # if we ran into an error while downloading fresh data, we delete any files that did manage
        # to be created and we restore the temporary backup
        if error_while_downloading_fresh_data:
            # delete any files that were created
            path_pattern = my_globals.str_dir_path_raw_website_json_pubs_sets + '*.json'
            lst_of_files_fullpaths = glob.glob(path_pattern)
            for path in lst_of_files_fullpaths:
                os.remove(path)
            # move the temporary backup back from whence it came
            path_pattern = my_globals.str_dir_path_raw_web_json_pub_sets_temp_backup + '*.json'
            lst_of_files_fullpaths = glob.glob(path_pattern)
            # note, the function below returns a 'success' value, but since we currently have no need
            # for it, we are not assigning it to a variable.
            move_list_of_files_to_a_directory(
                lst_of_files_fullpaths, my_globals.str_dir_path_raw_website_json_pubs_sets)
        else:
            # otherwise the fresh download ran without errors, so we delete the temporary
            # backup files
            path_pattern = my_globals.str_dir_path_raw_web_json_pub_sets_temp_backup + '*.json'
            lst_of_files_fullpaths = glob.glob(path_pattern)
            for path in lst_of_files_fullpaths:
                os.remove(path)
    # as stated in the function's documentation comment, we want to return
    # True if the download ran error free, so we negate the error variable and return it
    return not error_while_downloading_fresh_data


# ------------------------ END FUNCTION ------------------------ #


def check_if_sets_of_vids_are_error_free():
    int_total_vids_examined = 0
    # make a list of the files to be examined, so we can
    # know the size of the list to iterate and display percentages as
    # we go along.
    logging.debug('Making list of files to be examined')
    path_pattern = my_globals.str_dir_path_raw_website_json_video_sets + '*.json'
    lst_of_filenames = glob.glob(path_pattern)

    sets_of_vids_are_error_free = True

    # need a list to store the ID of each video
    lst_all_video_ids = []

    logging.debug('Looping through list of files with SETS of videos to do some error checking')
    for a_file in lst_of_filenames:
        # each of the files we open with this for loop contains a set
        # of videos in it. So now we will look at each video
        logging.debug('Opening file -> ' + a_file)
        with open(a_file, mode='r') as file_with_set_of_vids:
            logging.debug('Loading json data from -> ' + a_file)
            dict_json_data = json.load(file_with_set_of_vids)
            # there is a "data" structure in each file, and inside that
            # structure is the list of all the videos. So the following
            # line grabs the list of videos from inside the "data"
            # wrapper.
            list_videos = dict_json_data["data"]
            for a_vid in list_videos:
                int_total_vids_examined += 1
                # the video ID, for some reason, appears in two places in the json
                # data for each video. Not sure why, but I may as well check that they
                # are they same, and exit the program if they are not.
                vid_title = a_vid['attributes']['video_title']
                logging.debug(
                    'Checking consistency for video #' + str(int_total_vids_examined) + ' with title -> ' + vid_title)
                vid_id = a_vid['id']
                vid_attributes_id = a_vid['attributes']['video_id']
                if vid_id != vid_attributes_id:
                    logging.error("Video named: " + vid_title + " has two different IDs associated"
                                  " with it. One at the top level of the json data, and one in"
                                  " the 'attributes' area of the json data.")
                    sets_of_vids_are_error_free = False
                lst_all_video_ids.append(vid_id)

    # if for some reason there were to be duplicate IDs in
    # the list of all videos (which there never should be)
    # then they would get removed when converting the list
    # to a set, and then comparing the sizes would indicate
    # an issue if they are different.
    logging.info('Checking if any video IDs are duplicates (which there should not be.)')
    set_vid_ids = set(lst_all_video_ids)
    if not (int_total_vids_examined == len(lst_all_video_ids)
            and len(lst_all_video_ids) == len(set_vid_ids)):
        logging.warning('There is some discrepancy in the raw data-set of videos.')
        sets_of_vids_are_error_free = False
    else:
        logging.info('No errors found in the counters.')
    logging.info('Total vids examined: ' + str(int_total_vids_examined))
    logging.info('Size of list of vids: ' + str(len(lst_all_video_ids)))
    logging.info('Size of set of vids: ' + str(len(set_vid_ids)))
    return sets_of_vids_are_error_free


# ------------------------ END FUNCTION ------------------------ #

def check_if_sets_of_pubs_are_error_free():
    int_total_pubs_examined = 0
    # make a list of the files to be examined, so we can
    # know the size of the list to iterate and display percentages as
    # we go along.
    logging.debug('Making list of files to be examined')
    path_pattern = my_globals.str_dir_path_raw_website_json_pubs_sets + '*.json'
    lst_of_filenames = glob.glob(path_pattern)

    sets_of_pubs_are_error_free = True

    # need a list to store the ID of each publication
    lst_all_publication_ids = []

    logging.debug('Looping through list of files with SETS of publications to do some error checking')
    for a_file in lst_of_filenames:
        # each of the files we open with this for loop contains a set
        # of publications in it. So now we will look at each publication
        logging.debug('Opening file -> ' + a_file)
        with open(a_file, mode='r') as file_with_set_of_pubs:
            logging.debug('Loading json data from -> ' + a_file)
            dict_json_data = json.load(file_with_set_of_pubs)
            # there is a "data" structure in each file, and inside that
            # structure is the list of all the publications. So the following
            # line grabs the list of publications from inside the "data"
            # wrapper.
            list_publications = dict_json_data["data"]
            for a_pub in list_publications:
                int_total_pubs_examined += 1
                # the publication ID, for some reason, appears in two places in the json
                # data for each publication. Not sure why, but I may as well check that they
                # are they same, and exit the program if they are not.
                pub_title = a_pub[my_globals.str_vid_attributes]['issue_title']
                logging.debug('Checking consistency for publication #' +
                              str(int_total_pubs_examined) + ' with title -> ' + pub_title)
                pub_id = a_pub[my_globals.str_vid_id]
                pub_attributes_id = a_pub[my_globals.str_vid_attributes]['issue_id']
                if pub_id != pub_attributes_id:
                    logging.error("publication named: " + pub_title + " has two different IDs associated"
                                  " with it. One at the top level of the json data, and one in"
                                  " the 'attributes' area of the json data.")
                    sets_of_pubs_are_error_free = False
                lst_all_publication_ids.append(pub_id)

    # if for some reason there were to be duplicate IDs in
    # the list of all publications (which there never should be)
    # then they would get removed when converting the list
    # to a set, and then comparing the sizes would indicate
    # an issue if they are different.
    logging.info('Checking if any publication IDs are duplicates (which there should not be.)')
    set_pub_ids = set(lst_all_publication_ids)
    if not (int_total_pubs_examined == len(lst_all_publication_ids)
            and len(lst_all_publication_ids) == len(set_pub_ids)):
        logging.warning('There is some discrepancy in the raw data-set of publications.')
        sets_of_pubs_are_error_free = False
    else:
        logging.info('No errors found in the COUNTERS.')
    logging.info('Total pubs examined: ' + str(int_total_pubs_examined))
    logging.info('Size of list of pubs: ' + str(len(lst_all_publication_ids)))
    logging.info('Size of set of pubs: ' + str(len(set_pub_ids)))
    return sets_of_pubs_are_error_free


# ------------------------ END FUNCTION ------------------------ #

def tag_rv_website_vids_for_deletion(tolerance_allow_max_deletions=3, trial_run=True):  # noqa: C901
    """This function loops through the sets of files that have been downloaded from
    the RV website about videos, compares to the videos stored in the SimpleDS, and tags
    the differences as videos that were deleted at source (which will allow downstream
    workflows to deal with deletions however they please.)
    This function ONLY ACTS if the files storing information about sets of videos on
    disk represent a FULL pull from the RV website, because partial pulls cannot be used to
    compare two full sets of videos (the pull and the SimpleDS.)
    PARAMETERS.
    The tolerance_allow_max_deletions is a safeguard in case something goes wrong (although
    it should not, as there are several other safeguards in place) so that a maximum of
    X videos specified by the parameter can be tagged for deletion. If more than the
    allowed tolarance is detected, a WARNING is written to the log, and ZERO rows are
    marked for deletion."""

    sets_of_videos_on_disk_are_complete = detect_if_data_on_disk_is_full_set_of_vids()
    if sets_of_videos_on_disk_are_complete:
        int_total_vids_examined = 0
        int_vids_tagged_for_deletion = 0

        # load the data structure class that keeps track of videos
        # metadata downloaded from the website.
        logging.debug('Loading the SimpleDS containing vid metadata from the RV website')
        web_videos_ds = SimpleDS(my_globals.str_dir4_website_vids_ds, my_globals.str_name_simpleds_website_vids)
        web_videos_ds.load()

        # create a python set that will be used to store the video IDs of
        # all the videos examined from the sets of json files downloaded
        # from the website. This set will be used to determine if any videos
        # have been deleted from the website, and should therefore be removed from
        # the SimpleDS
        set_videoids_from_web = set()

        # make a list of the files to be examined, so we can
        # know the size of the list to iterate and display percentages as
        # we go along.
        logging.debug('Making a list of the files that contain sets of videos metadata downloaded from the RV website.')
        path_pattern = my_globals.str_dir_path_raw_website_json_video_sets + '*.json'
        lst_of_filenames = glob.glob(path_pattern)

        int_progress_counter = 0
        int_num_files = len(lst_of_filenames)
        percent_tracker = PercentTracker(int_num_files)
        # we enclose the whole giant loop below in a try statement, because
        # if something goes wrong, it gives us a chance to still save the instance
        # of SimpleDS to disk, which may help avoid that would be caused by data
        # being saved to disk as part of the loop, but the related rows in the SimpleDS
        # not getting saved to disk at the end of the function.
        try:
            logging.info('Creating the set of all existing videos on the RV website')
            for a_file in lst_of_filenames:
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
                        vid_id = a_vid[my_globals.str_vid_id]
                        # add the video id to the set being used to compile all the IDs of the
                        # incoming videos
                        set_videoids_from_web.add(vid_id)
                        int_total_vids_examined += 1

                int_progress_counter += 1
                percent_tracker.update_progress(int_progress_counter, 'Examining sets of video files')

            # now we need to figure out if videos have been removed at source, and
            # if so, delete them from the SimpleDS
            logging.info('Detecting if any videos have been removed at source')
            set_videoids_in_ds = web_videos_ds.fetch_all_ids_as_python_set()
            set_videoids_removed_at_source = set_videoids_in_ds - set_videoids_from_web
            num_videoids_removed_at_source = len(set_videoids_removed_at_source)
            logging.info(str(num_videoids_removed_at_source) + ' videos seem to have been removed at source.')
            if num_videoids_removed_at_source <= tolerance_allow_max_deletions:
                for vid_id in set_videoids_removed_at_source:
                    vid_obj = RVwebsiteVid(web_videos_ds.fetch_data(vid_id))
                    if not trial_run:
                        web_videos_ds.tag_add(vid_id, my_globals.str_tag_delete_row_from_simpleds)
                    logging.info('Video removed at source - ' + vid_id + ' (' + vid_obj.str_title + ')')
                    int_vids_tagged_for_deletion += 1
            else:
                logging.warning('MORE videos than the allowed tolerance were detected to have'
                                ' been removed at source. In case this is an error, ZERO videos'
                                ' have been tagged for deletion. If this is not an error, the allowed'
                                ' tolerance will need to be temporarily increased for this function to'
                                ' correctly tag the videos for deletion.')

        except Exception as e:
            logging.warning('There was a problem while creating a full list of videos that exist on the'
                            ' RV website. The Exception was: ' + repr(e))
        if not trial_run:
            web_videos_ds.save2disk()

        if trial_run:
            logging.info('-------TRIAL RUN-------')
        logging.info('Total videos examined: ' + str(int_total_vids_examined))
        logging.info('Videos tagged for deletion: ' + str(int_vids_tagged_for_deletion))

    else:
        logging.info('The sets of videos on disk do not represent a FULL pulll of videos'
                     ' from the RV website, therefore an examination of videos, looking for deletion'
                     ' candidates, was not performed.')


# ------------------------ END FUNCTION ------------------------ #

def tag_rv_website_pubs_for_deletion(tolerance_allow_max_deletions=1, trial_run=True):  # noqa: C901
    """This function loops through the sets of files that have been downloaded from
    the RV website about publications, compares to the publications stored in the SimpleDS, and tags
    the differences as publications that were deleted at source (which will allow downstream
    workflows to deal with deletions however they please.)
    This function ONLY ACTS if the files storing information about sets of publications on
    disk represent a FULL pull from the RV website, because partial pulls cannot be used to
    compare two full sets of publications (the pull and the SimpleDS.)
    PARAMETERS.
    The tolerance_allow_max_deletions is a safeguard in case something goes wrong (although
    it should not, as there are several other safeguards in place) so that a maximum of
    X publications specified by the parameter can be tagged for deletion. If more than the
    allowed tolarance is detected, a WARNING is written to the log, and ZERO rows are
    marked for deletion."""

    sets_of_publications_on_disk_are_complete = detect_if_data_on_disk_is_full_set_of_pubs()
    if sets_of_publications_on_disk_are_complete:
        int_total_pubs_found_locally = 0
        int_pubs_tagged_for_deletion = 0

        # load the data structure class that keeps track of publications
        # metadata downloaded from the website.
        logging.debug('Loading the SimpleDS containing pub metadata from the RV website')
        web_pubs_ds = SimpleDS(my_globals.str_dir4_website_pubs_ds, my_globals.str_name_simpleds_website_pubs)
        web_pubs_ds.load()

        # create a python set that will be used to store the publication IDs of
        # all the publications examined from the sets of json files downloaded
        # from the website. This set will be used to determine if any publications
        # have been deleted from the website, and should therefore be removed from
        # the SimpleDS
        set_publicationids_from_web = set()

        # make a list of the files to be examined, so we can
        # know the size of the list to iterate and display percentages as
        # we go along.
        logging.debug('Making a list of the files that contain sets of publications metadata'
                      ' downloaded from the RV website.')
        path_pattern = my_globals.str_dir_path_raw_website_json_pubs_sets + '*.json'
        lst_of_filenames = glob.glob(path_pattern)

        int_progress_counter = 0
        int_num_files = len(lst_of_filenames)
        percent_tracker = PercentTracker(int_num_files)
        # we enclose the whole giant loop below in a try statement, because
        # if something goes wrong, it gives us a chance to still save the instance
        # of SimpleDS to disk, which may help avoid that would be caused by data
        # being saved to disk as part of the loop, but the related rows in the SimpleDS
        # not getting saved to disk at the end of the function.
        try:
            logging.info('Creating the set of all existing publications on the RV website')
            for a_file in lst_of_filenames:
                # each of the files we open with this for loop contains a set
                # of publications in it. So now we will look at each publication
                with open(a_file, mode='r') as file_with_set_of_pubs:
                    logging.debug('Processing file -> ' + a_file)
                    dict_json_data = json.load(file_with_set_of_pubs)
                    # there is a "data" structure in each file, and inside that
                    # structure is the list of all the publications. So the following
                    # line grabs the list of publications from inside the "data"
                    # wrapper.
                    list_publications = dict_json_data["data"]
                    for a_pub in list_publications:
                        pub_id = a_pub[my_globals.str_vid_id]
                        # add the publication id to the set being used to compile all the IDs of the
                        # incoming publications
                        set_publicationids_from_web.add(pub_id)
                        int_total_pubs_found_locally += 1

                int_progress_counter += 1
                percent_tracker.update_progress(int_progress_counter, 'Examining sets of publication files')

            # now we need to figure out if publications have been removed at source, and
            # if so, delete them from the SimpleDS
            logging.info('Detecting if any publications have been removed at source')
            set_publicationids_in_ds = web_pubs_ds.fetch_all_ids_as_python_set()
            set_publicationids_removed_at_source = set_publicationids_in_ds - set_publicationids_from_web
            num_publicationids_removed_at_source = len(set_publicationids_removed_at_source)
            logging.info(
                str(num_publicationids_removed_at_source) + ' publications seem to have been removed at source.')
            if num_publicationids_removed_at_source <= tolerance_allow_max_deletions:
                for pub_id in set_publicationids_removed_at_source:
                    pub_obj = RVwebsitePublication(web_pubs_ds.fetch_data(pub_id))
                    if not trial_run:
                        web_pubs_ds.tag_add(pub_id, my_globals.str_tag_delete_row_from_simpleds)
                    logging.info('publication removed at source - ' + pub_id + ' (' + pub_obj.str_title + ')')
                    int_pubs_tagged_for_deletion += 1
            else:
                logging.warning('MORE publications than the allowed tolerance were detected to have'
                                ' been removed at source. In case this is an error, ZERO publications'
                                ' have been tagged for deletion. If this is not an error, the allowed'
                                ' tolerance will need to be temporarily increased for this function to'
                                ' correctly tag the publications for deletion.')

        except Exception as e:
            logging.warning('There was a problem while creating a full list of publications that exist on the'
                            ' RV website. The Exception was: ' + repr(e))
        if not trial_run:
            web_pubs_ds.save2disk()

        if trial_run:
            logging.info('-------TRIAL RUN-------')
        logging.info('Total publications found on disk: ' + str(int_total_pubs_found_locally))
        logging.info('publications tagged for deletion: ' + str(int_pubs_tagged_for_deletion))

    else:
        logging.info('The sets of publications on disk do not represent a FULL pulll of publications'
                     ' from the RV website, therefore an examination of publications, looking for deletion'
                     ' candidates, was not performed.')


# ------------------------ END FUNCTION ------------------------ #

def refresh_vids_and_shows_from_rv_website(pullShowsInfo=False, pullVideosInfo=False, max_multiple=-1):
    function_ran_error_free = False
    try:
        if pullShowsInfo:
            # call a function that re-downloads information about shows
            logging.debug(my_globals.str_logging_func_next + pullShowsJSONdataFromWebsiteToFile.__name__)
            pullShowsJSONdataFromWebsiteToFile()
            logging.debug(my_globals.str_logging_func_exited + pullShowsJSONdataFromWebsiteToFile.__name__)

        if pullVideosInfo:
            # call a function that re-creates the
            # raw videos metadata files (pulling from website)
            logging.debug(my_globals.str_logging_func_next + pull_sets_of_videos_JSON_data_from_web_2disk.__name__)
            pull_sets_of_videos_JSON_data_from_web_2disk(max_multiple)
            logging.debug(my_globals.str_logging_func_exited + pull_sets_of_videos_JSON_data_from_web_2disk.__name__)
        function_ran_error_free = True
    except Exception as e:
        logging.error("Problem during function 'refresh_vids_and_shows_from_rv_website'."
                      " The Exception was: " + repr(e))
    return function_ran_error_free


# ------------------------ END FUNCTION ------------------------ #

def refresh_integer_multiple_from_web(str_product_id):
    """Initially we were finding the number of videos that a call
    to the website api returns per page, manually. This function
    finds the number of videos (the multiple) automatically.
    It is plausible that the api could be changed
    to send back more than 24 videos at at a time. This function
    checks to see what multiple is being sent back by the api."""
    logging.debug('Finding how many videos (the multiple) are returned per page by the website api.')
    first_part_url = 'https://www.realvision.com/rv/api/videos?page%5Bskip%5D='
    last_part_url = '&filter%5Bvideo_product_id%5D=' + str_product_id

    url = first_part_url + str(0) + last_part_url
    logging.debug('Downloading a page.')
    response = urllib.request.urlopen(url, timeout=my_globals.int_timeout)
    dictJSONdata = json.load(response)
    lst_of_data_objects = dictJSONdata["data"]
    if len(lst_of_data_objects) < 24:
        # it is unlikely the website api would reduce the number
        # of objects returned to less than 24, so likely there
        # is something wrong if we end up inside this IF
        logging.warning('Multiple size is unlikely to be less than 24, so'
                        ' there might be something wrong.')
    logging.debug('Returning number of videos on page.')
    return len(lst_of_data_objects)


# ------------------------ END FUNCTION ------------------------ #


def refresh_max_multiple_from_web(str_product_id, multiple_of, int_high_number):
    """Initially we were finding the maximum multiple (at time of writing of
    24) that would still return data from the website api
    manually. This function gets the max multiple
    automatically.
    The parameters the fuction receives are the product ID for the
    product that the multiple is being searched for, as well as
    the integer we want to use (at the time of this writing, the
    website returns videos in sets of 24, but that could change),
    as well as a high number that is likely to be higher than the
    max multiple. In other words, a guess of a high number that\
    will certainly be higher than the max multiple."""

    logging.debug('Finding the maximum multiple index that can be queried on the website'
                  ' and still returns videos (separately for each product.)')
    # create an array filled with multiples of the requested integer
    int_counter_integers = 0
    lst_multiples_of = [0]
    logging.debug('The high number for the binay search starting point is: ' + str(int_high_number))
    while lst_multiples_of[int_counter_integers] <= int_high_number:
        int_counter_integers += 1
        lst_multiples_of.append(int_counter_integers * multiple_of)

    # now perform a binary search on versions of the URL to be searched

    first_part_url = 'https://www.realvision.com/rv/api/videos?page%5Bskip%5D='
    last_part_url = '&filter%5Bvideo_product_id%5D=' + str_product_id

    # make sure there is actually data returned when the first URL is
    # queried (using zero as the index.) If not, something is wrong.
    url = first_part_url + str(0) + last_part_url
    response = urllib.request.urlopen(url)
    dictJSONdata = json.load(response)
    lst_of_data_objects = dictJSONdata["data"]
    if not lst_of_data_objects:
        # list is empty, and it shouldn't be with index zero
        logging.error('SOME data should be returned with index at zero,'
                      ' so something has gone wrong. Terminating program.')
        exit(0)

    logging.debug('Starting binary search')
    int_lower_index = 0
    int_upper_index = len(lst_multiples_of) - 1
    int_search_index = len(lst_multiples_of) - 1
    bool_max_multiple_found = False
    while not bool_max_multiple_found:
        # we know there is data at the beginning, so we start searching in
        # the upper half.
        url = first_part_url + str(lst_multiples_of[int_search_index]) + last_part_url
        response = urllib.request.urlopen(url)
        dictJSONdata = json.load(response)
        lst_of_data_objects = dictJSONdata["data"]
        if lst_of_data_objects:
            int_lower_index = int_search_index
        else:
            int_upper_index = int_search_index
        int_search_index = int((int_upper_index - int_lower_index) / 2) + int_lower_index

        if int_upper_index - int_lower_index <= 1:
            bool_max_multiple_found = True

    max_multiple = lst_multiples_of[int_search_index]
    logging.debug('Binary search result: ' + str(max_multiple))
    return max_multiple


# ------------------------ END FUNCTION ------------------------ #

def get_all_vid_transcripts(requests_session, var_manager, max_vids_to_process=10000, trial_run=False,  # noqa: C901
                            try_to_fetch_even_if_tagged_as_missing=False, num_of_days_to_consider_a_video_new=14,
                            refresh_even_if_already_have=False,
                            refresh_even_if_hashes_match=False):
    """This function iterates through the datastructure of raw videos information
    returned from the Real Vision website, and grabs the transcript of each
    video if it exists.
    The main parameter passed to this function is a session object that is already
    in a state of being logged-in to the RV website.
    This function will tag videos in the source SimpleDS (the one that stores json data
    about videos pulled form the RV website) with a 'transcript missing' tag if the transcript
    cannot be found. This allows the function to be called in less thorough, or more thorough modes.
    So one can tell the function to try again to get the transcript if it has been tagged
    as missing in the past, OR to ignore videos that have already been tagged with the transcript
    missing in the past. However, the function will ALWAYS try to fetch the transcript if
    the video was published recently, because as soon as videos are published it
    can take a few days for the transcript to become available. The default number of days to
    consider a video recent is 14, but this can be adjusted with the
    num_of_days_to_consider_a_video_new
    parameter.
    If the flag is set to tell the function to update all videos, even if we already
    have a transcript, then the function will attempt to do so, BUT will still not
    update if the hashes match. This behaviour can also be overridden with the other
    parameter passed to the function (which is named accordinly.)"""

    int_count_vids_processed = 0
    int_count_vids_transcript_added = 0
    int_count_vids_transcript_updated = 0
    int_count_vids_transcript_data_deleted = 0
    int_count_vids_not_touched = 0

    # create a list to track a log of the availability of
    # some transcript related URLs. The list will be a list of dictionaries
    # that can then be exported to CSV
    lst_for_logging_transcript_urls = []

    # open the datastructure that contains metadata about each
    # video. This will allows us to iterate through the website
    # videos data with a defined size of iteration so we can
    # display percentages.
    web_vids_DS = SimpleDS(my_globals.str_dir4_website_vids_ds)
    web_vids_DS.load()
    web_vids_DS.sort()

    # open the datastructure where we store transcripts
    transcripts_DS = SimpleDS(my_globals.str_dir4_vid_transcripts_ds)
    transcripts_DS.load()
    transcripts_DS.sort()

    # for convenience we'll use the same timestamp for all videos that get
    # updated or added.
    timestamp_of_update = make_now_timestamp()

    # before doing the heavy lifting work, we create two lists.
    # one list will contain video IDs of videos that may need to be added to the datastructure
    # another list will contain video IDs of videos that may need updating
    # we will only store videos in the datastructure which actually have transcripts
    # Normally, the datastructure stores a file that contains the actual data. In this case
    # because transcript files are a bit larger than normal, we are not going to store
    # the file. We are going to store the filename of the file. This is in case we want
    # to add other related transcript files. For example a Term Frequency file. Then,
    # in this datastructure, on disk, we will simply store the filenames associated with
    # different files that contain info about a transcript.
    lst_vids_to_possibly_add = []
    lst_vids_to_possibly_update = []

    # now we iterate through the datastructure that holds the
    # website videos metadata.
    int_length_vidsDS = len(web_vids_DS)
    # first we decide how many iterations of the loop are going to happen depending on what is higher
    # the number passed to the function or the length of the SimpleDS
    num_iterations = int_length_vidsDS
    if (max_vids_to_process < int_length_vidsDS) and (max_vids_to_process != -1):
        num_iterations = max_vids_to_process
    logging.info('Creating lists of videos that need to be added or updated to the SimpleDS that holds'
                 ' video transcript information.')
    if try_to_fetch_even_if_tagged_as_missing:
        logging.info("NOTE - will try to pull transcripts again even for videos that have been marked"
                     " with the 'transcript missing' tag in the past.")
    counter = 0
    for webvid_id in web_vids_DS:
        # The following IF checks the variable manager that was passed as a paramater
        # to this function. It allows for the loop to be stopped by an external factor
        # (script, human, etc.) if a specific variable has been set to false.
        execution_should_continue = var_manager.var_retrieve(my_globals.str_execution_may_go_on)
        if (not execution_should_continue) or (counter >= num_iterations):
            break
        if webvid_id in transcripts_DS:
            # if we are in this IF statement, then we know that an entry exists for this
            # video in the SimpleDS that tracks transcript info, so it does not
            # need adding, but it may need upating, which is what we check for now.

            # AS OF THIS WRITING, there is only one condition that means that a transcript
            # should be upated, but that could change in the future
            if refresh_even_if_already_have:
                lst_vids_to_possibly_update.append(webvid_id)
            else:
                # if we are here, nothing is going to be done to the video
                int_count_vids_not_touched += 1
        else:
            # if the code reaches here, the video is new; it is unknown
            # to the transcripts datastructure, so it needs to be added.
            # However, because the function can be run with different
            # levels of int_level_of_thoroughness, by default, we don't add videos if they have already been examined
            # in the past and have been tagged with the 'transcript missing' tag (no sense in continuously
            # trying to get transcripts for older videos that don't have one.) We only add videos
            # to the list of candidates to be added under certain circumstances:
            # 1) We ALWAYS add the video to the list to be added if it is a relatively new video.
            # 2) We also add videos to the list of candidates, if the function was explicitly told
            # to do so with the parameter try_to_fetch_even_if_tagged_as_missing

            add_vid_to_list_of_addition_candidates = False

            # first we check if the video has been tagged with transcript missing or not.
            vid_tagged_with_missing_transcript = web_vids_DS.tag_check(
                webvid_id, my_globals.str_tag_webvid_transcript_missing)
            if not vid_tagged_with_missing_transcript:
                # if the video does not have the 'missing' tag (AND we are also in the area of the
                # code where the vide is unknown to the Transcripts SimpleDS) then it is most likely a new video
                # (ie. we've never tried to get the transcript for it before) so we should attempt
                # to get the transcript for it.
                add_vid_to_list_of_addition_candidates = True
            else:
                # otherwise, the video has previously been tagged with the 'missing transcript' tag, so
                # we deal with special cases below.
                if try_to_fetch_even_if_tagged_as_missing:
                    # the function may have been asked to try to refresh the transcript, even
                    # if it has been tagged before. So in this case, we will simply add it to
                    # the list (further below, using this boolean.)
                    add_vid_to_list_of_addition_candidates = True
                else:
                    # we are here if the function was told to NOT try to get transcripts for
                    # videos that have already been examined in the past and have been marked
                    # with the transcript missing.

                    # However, regardless of this parameter, for the first few days that a video
                    # exists, we ALWAYS try to get its transcript, regardless of whether the
                    # transcript has been previously marked as missing or not. So, now we
                    # check if the video is relatively new or not.
                    num_secs_in_a_day = 60 * 60 * 24
                    num_secs_to_consider_a_vid_new = num_secs_in_a_day * num_of_days_to_consider_a_video_new
                    timenow = time.time()
                    time_after_which_videos_still_considered_new = timenow - num_secs_to_consider_a_vid_new
                    # the website stores timestamps in milliseconds, so below we divide by 1000 to make sure
                    # all our calculations are in seconds.
                    time_vid_published = web_vids_DS.fetch_created(webvid_id) / 1000
                    # so now we have the two key timestamps we need for the comparison to decide if
                    # the video is fairly recent (and if it is, it is always added to the list.)
                    if time_vid_published > time_after_which_videos_still_considered_new:
                        add_vid_to_list_of_addition_candidates = True

            if add_vid_to_list_of_addition_candidates:
                lst_vids_to_possibly_add.append(webvid_id)
        int_count_vids_processed += 1
        counter += 1

    # now we loop through the videos in the list of vids that need adding and see
    # if a transcript can be found for them.
    len_lst_vids_to_add = len(lst_vids_to_possibly_add)
    logging.info('There are ' + str(len_lst_vids_to_add) + ' videos that need transcripts added.'
                                                           ' Starting the addition process now.')
    percent_tracker = PercentTracker(len_lst_vids_to_add, int_output_every_x_percent=5, log_level='info')
    counter = 0
    for vid2add in lst_vids_to_possibly_add:
        vid_obj = RVwebsiteVid(web_vids_DS.fetch_data(vid2add))
        logging.info('-------- Attempting to extract transcript for video: ' + vid_obj.str_id
                     + ' (' + vid_obj.str_title + ')')
        # I came across a video that has the two transcripts backwards. The pdf is saved
        # to the json URL, and the json to the pdf URL. This obviously causes unexpected
        # errors. So I've enclosed the processing of each video in a try/except, so that
        # if there are errors, we can still keep processing other videos.
        try:
            str_transcript = ''
            # The following IF checks the variable manager that was passed as a paramater
            # to this function. It allows for the loop to be stopped by an external factor
            # (script, human, etc.) if a specific variable has been set to false.
            execution_should_continue = var_manager.var_retrieve(my_globals.str_execution_may_go_on)
            if not execution_should_continue:
                break

            transcript = Transcript(vid2add)
            dct_results = transcript.get_transcript_from_rv_website(requests_session, trial_run=trial_run)
            dct_results[my_globals.str_transcripts_report_column_videoid] = vid2add
            lst_for_logging_transcript_urls.append(dct_results)
            for key in dct_results:
                logging.info('Transcript extraction report: ' + key + '  ->  ' + dct_results[key])
            str_transcript = transcript.str_transcript_text
            if str_transcript:
                transcript.set_transcript_directory(my_globals.str_dir4_vid_transcripts_data)
                transcript.save_transcript_text_2disk()
                dict_transcript_metadata = transcript.dump_transcript_metadata_to_dictionary()
                transcript_hash = make_sha256_hash(str_transcript)
                # for the 'creation date' stored in this SimpleDS, we will use the same
                # creation date store in the web_vid_DS, which is the date of when the video
                # was published to the Real Vision website.
                timestamp_created = web_vids_DS.fetch_created(vid2add)
                if not trial_run:
                    transcripts_DS.add_entry(vid2add, timestamp_of_update,
                                             timestamp_created, dict_transcript_metadata, data_hash=transcript_hash)
                    transcripts_DS.tag_add(vid2add, transcript.source)
                    # in case the video has previously been tagged as having its 'transcript missing'
                    # we now remove that tag after successful transcript retrieval.
                    web_vids_DS.tag_remove(vid2add, my_globals.str_tag_webvid_transcript_missing)
                logging.info('Successfully retrieved transcript data.')
                int_count_vids_transcript_added += 1
            else:
                # we are in this else, if the transcript is empty (we were not able to retrieve it from
                # the website), so we tag the row with the 'missing transcript' tag
                web_vids_DS.tag_add(vid2add, my_globals.str_tag_webvid_transcript_missing)
                logging.warning('NO transcript data was retrieved.')
            counter += 1
            percent_tracker.update_progress(counter, show_time_remaining_estimate=True,
                                            str_description_to_include_in_logging='Downloading/adding transcripts.')
        except Exception as e:
            logging.warning('While updating all transcripts there was an issue with video: ' + vid2add +
                            '. This try/except is inside a loop, so the method will attempt to continue'
                            ' processing other videos. The Exception was: ' + repr(e))
        logging.info('-------- Finished the attempt to extract transcript for video: ' + vid_obj.str_id
                     + ' (' + vid_obj.str_title + ')')

    # now we loop through the videos in the list of vids that need updating
    if refresh_even_if_hashes_match:
        logging.warning('refresh_even_if_hashes_match is set to True, so an attempt'
                        ' will be made to update all videos tagged for updating.')
    len_lst_vids_to_update = len(lst_vids_to_possibly_update)
    logging.info('There are ' + str(len_lst_vids_to_update) + ' videos that may need transcripts updated.'
                                                              ' Starting the updating process now.')
    percent_tracker = PercentTracker(len_lst_vids_to_update, int_output_every_x_percent=1, log_level='info')
    counter = 0
    for vid2update in lst_vids_to_possibly_update:
        vid_obj = RVwebsiteVid(web_vids_DS.fetch_data(vid2update))
        logging.info('-------- Attempting to extract transcript for video: ' + vid_obj.str_id
                     + ' (' + vid_obj.str_title + ')')
        try:
            str_transcript = ''
            # The following IF checks the variable manager that was passed as a paramater
            # to this function. It allows for the loop to be stopped by an external factor
            # (script, human, etc.) if a specific variable has been set to false.
            execution_should_continue = var_manager.var_retrieve(my_globals.str_execution_may_go_on)
            if not execution_should_continue:
                break

            transcript = Transcript(vid2update)
            dct_results = transcript.get_transcript_from_rv_website(requests_session, trial_run=trial_run)
            dct_results[my_globals.str_transcripts_report_column_videoid] = vid2update
            lst_for_logging_transcript_urls.append(dct_results)
            str_transcript = transcript.str_transcript_text
            if str_transcript:
                incoming_transcript_hash = make_sha256_hash(str_transcript)
                saved_transcript_hash = transcripts_DS.fetch_hash(vid2update)
                should_refresh_vid_transcript = False
                if incoming_transcript_hash != saved_transcript_hash:
                    should_refresh_vid_transcript = True
                    logging.info('Icoming transcript hash is different from stored transcript hash.')
                else:
                    logging.debug('Transcript hashes match')
                    if refresh_even_if_hashes_match:
                        should_refresh_vid_transcript = True

                if should_refresh_vid_transcript:
                    transcript.set_transcript_directory(my_globals.str_dir4_vid_transcripts_data)
                    # grab the metadata we currently have about the transcript and load it into
                    # the Transcript object.
                    dict_current_transcript_metadata = transcripts_DS.fetch_data(vid2update)
                    transcript.load_transcript_object_from_dictionary(dict_current_transcript_metadata)

                    # save the updated transcript text
                    transcript.save_transcript_text_2disk()

                    # since the transcript has been updated, then things that depend on the original
                    # text should be wiped so they can later be re-created again.
                    transcript.delete_termcount_data()
                    transcript.delete_tfidf_data()

                    # now we re-dump the current info back to a dictionary that can be saved
                    # to the SimpleDS of transcripts.
                    dict_updated = transcript.dump_transcript_metadata_to_dictionary()
                    if not trial_run:
                        # NOTE that the hash we are saving in the SimpleDS is the hash of the transcript
                        # NOT THE HASH of the metadata dictionary
                        transcripts_DS.update_entry(vid2update, dict_updated,
                                                    timestamp_of_update, new_data_hash=incoming_transcript_hash)
                        # now we should remove any tags that may have been added in the past
                        # regarding the transcript's source.
                        # Because the tag removal method checks if the tag exists or not
                        # we don't need to check here, and we can just tell it to remove all
                        # possible related tags
                        transcripts_DS.tag_remove(vid2update, my_globals.str_tag_transcript_source_json)
                        transcripts_DS.tag_remove(vid2update, my_globals.str_tag_transcript_source_pdf)
                        # and we tag the row with the current source-tag
                        transcripts_DS.tag_add(vid2update, transcript.source)
                    int_count_vids_transcript_updated += 1
                else:
                    logging.debug('Transcript untouched')
            counter += 1
            percent_tracker.update_progress(counter, show_time_remaining_estimate=True,
                                            str_description_to_include_in_logging='Updating transcripts.')
        except Exception as e:
            logging.warning('While updating all transcripts there was an issue with video: ' + vid2update +
                            '. This try/except is inside a loop, so the method will attempt to continue'
                            ' processing other videos. The Exception was: ' + repr(e))
        logging.info('-------- Finished the attempt to extract transcript for video: ' + vid_obj.str_id
                     + ' (' + vid_obj.str_title + ')')

    # now we compare the two instances of SimpleDS to see if there are any
    # transcripts that need deleting.
    logging.info('Examining if there are videos that need to be deleted.')
    set_vids_in_web_ds = web_vids_DS.fetch_all_ids_as_python_set()
    set_vids_in_transcripts_ds = transcripts_DS.fetch_all_ids_as_python_set()
    set_vids_removed_at_source = set_vids_in_transcripts_ds - set_vids_in_web_ds
    for entry in set_vids_removed_at_source:
        if not trial_run:
            transcript = Transcript(entry)
            transcript.set_transcript_directory(my_globals.str_dir4_vid_transcripts_data)
            transcript.load_transcript_object_from_dictionary(transcripts_DS.fetch_data(entry))
            transcript.delete_all_transcript_related_files()
            # we don't need to do the next step, because immediately after, the SimpleDS is going to
            # be deleted. HOWEVER for our own best practice, it is always good to re-save the updated
            # transcript entry back into the SimpleDS.
            timestamp = transcripts_DS.fetch_lastupdated(entry)
            dct_updated = transcript.dump_transcript_metadata_to_dictionary()
            transcripts_DS.update_entry(entry, dct_updated, timestamp)
            # now we no longer need any of the data in the SimpleDS record, so we
            # can proceed to delete it.
            transcripts_DS.delete_entry(entry)
        int_count_vids_transcript_data_deleted += 1

    if not trial_run:
        logging.debug('Saving SimpleDS of Transcripts to disk')
        transcripts_DS.save2disk()
        web_vids_DS.save2disk()

    # save a CSV that has a log of the transcript urls and download status
    full_csv_path = my_globals.str_dir4_outputs_logs_misc + projectStandardTimestamp() + '_transcript_URLs.csv'
    with open(full_csv_path, mode='w') as csv_file:
        cols = [my_globals.str_transcripts_report_column_videoid,
                my_globals.str_transcripts_report_column_videoassetsurl,
                my_globals.str_transcripts_report_column_jsonurl,
                my_globals.str_transcripts_report_column_pdfurl]
        writer = csv.DictWriter(csv_file, fieldnames=cols)
        writer.writeheader()
        writer.writerows(lst_for_logging_transcript_urls)
    # now let's do some clean-up of older files in the directory to make sure
    # it doesn't eventually start using too much disk-space.
    cleanup_older_files_in_a_dir(my_globals.str_dir4_outputs_logs_misc,
                                 string_to_filter_files='*.csv',
                                 num_files_to_keep=256)

    logging.info('--------UPDATING OF TRANSCRIPTS SUMMARY--------')
    if trial_run:
        logging.info('-------!!TRIAL RUN!!-------')
    logging.info('Transcripts added: ' + str(int_count_vids_transcript_added))
    logging.info('Transcripts updated: ' + str(int_count_vids_transcript_updated))
    logging.info('Vids not touched: ' + str(int_count_vids_not_touched))
    logging.info('Vids processed: ' + str(int_count_vids_processed))
    logging.info('Transcripts deleted: ' + str(int_count_vids_transcript_data_deleted) +
                 ' (in total, not just in the subset being processed, because deletion must be a global operation.)')


# ------------------------ END FUNCTION ------------------------ #


def get_all_publication_fulltexts(requests_session, var_manager, max_pubs_to_process=10000,  # noqa: C901
                                  trial_run=False, refresh_even_if_already_have=False,
                                  refresh_even_if_hashes_match=False):
    """This function iterates through the datastructure of raw publications information
    returned from the Real Vision website, and attempts to extract the fulltext from
    the pdf of the pulication.
    NOTE that this function will make use of the Transcript class, which was
    originally created for videos, not for publications, but it serves a very similar
    purpose, so it can be re-used.
    The main parameter passed to this function is a session object that is already
    in a state of being logged-in to the RV website.
    If the flag is set to tell the function to update all publications, even if we already
    have a transcript, then the function will attempt to do so, BUT will still not
    update if the hashes match. This behaviour can be overriden with the other
    parameter passed to the function."""

    int_count_pubs_processed = 0
    int_count_pubs_transcript_added = 0
    int_count_pubs_transcript_updated = 0
    int_count_pubs_transcript_data_deleted = 0
    int_count_pubs_not_touched = 0

    # create a list to track a log of the availability of
    # some transcript related URLs. The list will be a list of dictionaries
    # that can then be exported to CSV
    lst_for_logging_fulltext_urls = []

    # open the datastructure that contains metadata about each
    # publication. This will allows us to iterate through the website
    # publications data with a defined size of iteration so we can
    # display percentages.
    web_pubs_DS = SimpleDS(my_globals.str_dir4_website_pubs_ds)
    web_pubs_DS.load()
    web_pubs_DS.sort()

    # open the datastructure where we store transcripts
    pubs_fulltext_DS = SimpleDS(my_globals.str_dir4_pubs_fulltext_ds)
    pubs_fulltext_DS.load()
    pubs_fulltext_DS.sort()

    # for convenience we'll use the same timestamp for all publications that get
    # updated or added.
    timestamp_of_update = make_now_timestamp()

    # before doing the heavy lifting work, we create two lists.
    # one list will contain publication IDs of publications that may need to be added to the datastructure
    # another list will contain publication IDs of publications that may need updating
    # we will only store publications in the datastructure which actually have pdfs
    # Normally, the datastructure stores a file that contains the actual data. In this case
    # because transcript files are a bit larger than normal, we are not going to store
    # the file. We are going to store the filename of the file. This is in case we want
    # to add other related transcript files. For example a Term Frequency file. Then,
    # in this datastructure, on disk, we will simply store the filenames associated with
    # different files that contain info about a transcript.
    lst_pubs_to_possibly_add = []
    lst_pubs_to_possibly_update = []

    # now we iterate through the datastructure that holds the
    # website publications metadata.
    int_length_pubsDS = len(web_pubs_DS)
    # first we decide how many iterations of the loop are going to happen depending on what is higher
    # the number passed to the function or the length of the SimpleDS
    num_iterations = int_length_pubsDS
    if (max_pubs_to_process < int_length_pubsDS) and (max_pubs_to_process != -1):
        num_iterations = max_pubs_to_process
    logging.info('Creating lists of publications that need to be added or updated to the SimpleDS that'
                 ' holds full text information.')
    counter = 0
    for webpub_id in web_pubs_DS:
        # The following IF checks the variable manager that was passed as a paramater
        # to this function. It allows for the loop to be stopped by an external factor
        # (script, human, etc.) if a specific variable has been set to false.
        execution_should_continue = var_manager.var_retrieve(my_globals.str_execution_may_go_on)
        if (not execution_should_continue) or (counter >= num_iterations):
            break
        if webpub_id in pubs_fulltext_DS:
            # if we are in this IF statement, then we know that an entry exists for this
            # publication in the SimpleDS that tracks transcript info, so it does not
            # need adding, but it may need upating, which is what we check for now.

            # AS OF THIS WRITING, there is only one condition that means that a transcript
            # should be upated, but that could change in the future
            if refresh_even_if_already_have:
                lst_pubs_to_possibly_update.append(webpub_id)
            else:
                # if we are here, nothing is going to be done to the publication
                int_count_pubs_not_touched += 1
        else:
            # if the code reaches here, the publication is new; it is unknown
            # to the transcripts datastructure, so it needs to be added.
            lst_pubs_to_possibly_add.append(webpub_id)
        int_count_pubs_processed += 1
        counter += 1

    # now we loop through the publications in the list of pubs that need adding and see
    # if a full-text can be extracted from a pdf for them.
    len_lst_pubs_to_add = len(lst_pubs_to_possibly_add)
    logging.info('There are ' + str(len_lst_pubs_to_add) + ' publications that need full-texts extracted.'
                                                           ' Starting the process now.')
    percent_tracker = PercentTracker(len_lst_pubs_to_add, int_output_every_x_percent=1, log_level='info')
    counter = 0
    for pub2add in lst_pubs_to_possibly_add:
        pub_obj = RVwebsitePublication(web_pubs_DS.fetch_data(pub2add))
        logging.info('-------- Attempting to extract fulltext from publication: ' + pub_obj.str_id
                     + ' (' + pub_obj.str_title + ')')
        try:
            str_fulltext = ''
            # The following IF checks the variable manager that was passed as a paramater
            # to this function. It allows for the loop to be stopped by an external factor
            # (script, human, etc.) if a specific variable has been set to false.
            execution_should_continue = var_manager.var_retrieve(my_globals.str_execution_may_go_on)
            if not execution_should_continue:
                break

            # the (video) Transcript class is useful for the purposes of publications as well
            transcript = Transcript(pub2add)
            dct_results = transcript.get_publication_fulltext_from_rv_website(requests_session)
            dct_results[my_globals.str_fulltexts_report_column_videoid] = pub2add
            lst_for_logging_fulltext_urls.append(dct_results)
            str_fulltext = transcript.str_transcript_text
            if str_fulltext:
                transcript.set_transcript_directory(my_globals.str_dir4_pubs_fulltext_data)
                transcript.save_transcript_text_2disk()
                dict_transcript_metadata = transcript.dump_transcript_metadata_to_dictionary()
                transcript_hash = make_sha256_hash(str_fulltext)
                # for the 'creation date' stored in this SimpleDS, we will use the same
                # creation date stored in the web_pub_DS, which is the date of when the publication
                # was published to the Real Vision website.
                timestamp_created = web_pubs_DS.fetch_created(pub2add)
                if not trial_run:
                    pubs_fulltext_DS.add_entry(pub2add, timestamp_of_update,
                                               timestamp_created, dict_transcript_metadata, data_hash=transcript_hash)
                    pubs_fulltext_DS.tag_add(pub2add, transcript.source)
                int_count_pubs_transcript_added += 1
            counter += 1
            percent_tracker.update_progress(counter, show_time_remaining_estimate=True,
                                            str_description_to_include_in_logging='Downloading/adding'
                                                                                  ' publication fulltexts.')
        except Exception as e:
            logging.error('While updating all transcripts there was an issue with publication: ' + pub2add +
                          '\nThis try/except is inside a loop, so the method will attempt to continue'
                          ' processing other publications.'
                          '\nThe Exception was: ' + repr(e))
        logging.info('-------- Finished the attempt to extract fulltext from publication: ' + pub_obj.str_id
                     + ' (' + pub_obj.str_title + ')')

    # now we loop through the publications in the list of pubs that need updating
    if refresh_even_if_hashes_match:
        logging.warning('refresh_even_if_hashes_match is set to True, so an attempt'
                        ' will be made to update all publications tagged for updating.')
    len_lst_pubs_to_update = len(lst_pubs_to_possibly_update)
    logging.info('There are ' + str(len_lst_pubs_to_update) + ' publications that may need transcripts updated.'
                                                              ' Starting the updating process now.')
    percent_tracker = PercentTracker(len_lst_pubs_to_update, int_output_every_x_percent=1, log_level='info')
    counter = 0
    for pub2update in lst_pubs_to_possibly_update:
        pub_obj = RVwebsitePublication(web_pubs_DS.fetch_data(pub2update))
        logging.info('Attempting to extract fulltext from publication: ' + pub_obj.str_id
                     + ' (' + pub_obj.str_title + ')')
        try:
            str_fulltext = ''
            # The following IF checks the variable manager that was passed as a paramater
            # to this function. It allows for the loop to be stopped by an external factor
            # (script, human, etc.) if a specific variable has been set to false.
            execution_should_continue = var_manager.var_retrieve(my_globals.str_execution_may_go_on)
            if not execution_should_continue:
                break

            # the (video) Transcript class is useful for the purposes of publications as well
            transcript = Transcript(pub2update)
            dct_results = transcript.get_publication_fulltext_from_rv_website(requests_session)
            dct_results[my_globals.str_fulltexts_report_column_videoid] = pub2update
            lst_for_logging_fulltext_urls.append(dct_results)
            str_fulltext = transcript.str_transcript_text
            if str_fulltext:
                incoming_transcript_hash = make_sha256_hash(str_fulltext)
                saved_transcript_hash = pubs_fulltext_DS.fetch_hash(pub2update)
                should_refresh_pub_transcript = False
                if incoming_transcript_hash != saved_transcript_hash:
                    should_refresh_pub_transcript = True
                    logging.info('Icoming transcript hash is different from stored transcript hash.')
                else:
                    logging.debug('Transcript hashes match')
                    if refresh_even_if_hashes_match:
                        should_refresh_pub_transcript = True

                if should_refresh_pub_transcript:
                    transcript.set_transcript_directory(my_globals.str_dir4_pubs_fulltext_data)
                    # grab the metadata we currently have about the transcript and load it into
                    # the Transcript object.
                    dict_current_fulltext_metadata = pubs_fulltext_DS.fetch_data(pub2update)
                    transcript.load_transcript_object_from_dictionary(dict_current_fulltext_metadata)

                    # save the updated transcript text
                    transcript.save_transcript_text_2disk()

                    # since the transcript has been updated, then things that depend on the original
                    # text should be wiped so they can later be re-created again.
                    transcript.delete_all_transcript_related_files()

                    # now we re-dump the current info back to a dictionary that can be saved
                    # to the SimpleDS of transcripts.
                    dict_updated = transcript.dump_transcript_metadata_to_dictionary()
                    if not trial_run:
                        # NOTE that the hash we are saving in the SimpleDS is the hash of the transcript
                        # NOT THE HASH of the metadata dictionary
                        pubs_fulltext_DS.update_entry(pub2update, dict_updated,
                                                      timestamp_of_update, new_data_hash=incoming_transcript_hash)
                        # now we should remove any tags that may have been added in the past
                        # regarding the transcript's source.
                        # Because the tag removal method checks if the tag exists or not
                        # we don't need to check here, and we can just tell it to remove all
                        # possible related tags
                        pubs_fulltext_DS.tag_remove(pub2update, my_globals.str_tag_transcript_source_pdf)
                        # and we tag the row with the current source-tag
                        pubs_fulltext_DS.tag_add(pub2update, transcript.source)
                    int_count_pubs_transcript_updated += 1
                else:
                    logging.debug('Transcript untouched')
            counter += 1
            percent_tracker.update_progress(counter, show_time_remaining_estimate=True,
                                            str_description_to_include_in_logging='Downloading/updating'
                                                                                  ' publication fulltexts.')
        except Exception as e:
            logging.warning('While updating all publications fulltexts there was an issue with publication: '
                            + pub2update + '\nThis try/except is inside a loop, so the method will attempt to continue'
                                           ' processing other publications.'
                                           '\nThe Exception was: ' + repr(e))
        logging.info('-------- Finished the attempt to extract fulltext from publication: ' + pub_obj.str_id
                     + ' (' + pub_obj.str_title + ')')

    # now we compare the two instances of SimpleDS to see if there are any
    # transcripts that need deleting.
    logging.info('Examining if there are publications that need to be deleted.')
    set_pubs_in_web_ds = web_pubs_DS.fetch_all_ids_as_python_set()
    set_pubs_in_transcripts_ds = pubs_fulltext_DS.fetch_all_ids_as_python_set()
    set_pubs_removed_at_source = set_pubs_in_transcripts_ds - set_pubs_in_web_ds
    for entry in set_pubs_removed_at_source:
        if not trial_run:
            transcript = Transcript(entry)
            transcript.set_transcript_directory(my_globals.str_dir4_pubs_fulltext_data)
            transcript.load_transcript_object_from_dictionary(pubs_fulltext_DS.fetch_data(entry))
            transcript.delete_all_transcript_related_files()
            # we don't need to do the next step, because immediately after, the SimpleDS is going to
            # be deleted. HOWEVER for our own best practice, it is always good to re-save the updated
            # transcript entry back into the SimpleDS.
            timestamp = pubs_fulltext_DS.fetch_lastupdated(entry)
            dct_updated = transcript.dump_transcript_metadata_to_dictionary()
            pubs_fulltext_DS.update_entry(entry, dct_updated, timestamp)
            # now we no longer need any of the data in the SimpleDS record, so we
            # can proceed to delete it.
            pubs_fulltext_DS.delete_entry(entry)
        int_count_pubs_transcript_data_deleted += 1

    if not trial_run:
        logging.debug('Saving SimpleDS of Transcripts to disk')
        pubs_fulltext_DS.save2disk()

    # save a CSV that has a log of the transcript urls and download status
    full_csv_path = my_globals.str_dir4_outputs_logs_misc + projectStandardTimestamp() + '_fulltext_URLs.csv'
    with open(full_csv_path, mode='w') as csv_file:
        cols = [my_globals.str_fulltexts_report_column_videoid,
                my_globals.str_fulltexts_report_column_url,
                my_globals.str_fulltexts_report_column_pdfplumber,
                my_globals.str_fulltexts_report_column_pypdf2]
        writer = csv.DictWriter(csv_file, fieldnames=cols)
        writer.writeheader()
        writer.writerows(lst_for_logging_fulltext_urls)

    logging.info('--------UPDATING OF TRANSCRIPTS SUMMARY--------')
    if trial_run:
        logging.info('-------!!TRIAL RUN!!-------')
    logging.info('Transcripts added: ' + str(int_count_pubs_transcript_added))
    logging.info('Transcripts updated: ' + str(int_count_pubs_transcript_updated))
    logging.info('pubs not touched: ' + str(int_count_pubs_not_touched))
    logging.info('pubs processed: ' + str(int_count_pubs_processed))
    logging.info('Transcripts deleted: ' + str(int_count_pubs_transcript_data_deleted) +
                 ' (in total, not just in the subset being processed, because deletion must be a global operation.)')


# ------------------------ END FUNCTION ------------------------ #


def get_comments_stats(variable_manager, num_vids_to_process, requests_session, trial_run=False,  # noqa: C901
                       update_regardless_of_changes=False):
    """This function pulls statistics about the comments made
    about a video. Like how many comments, how many replies, how many likes,
    etc. and stores it in the SimpleDS for additional video info.
    This function expects to be given a requests session that is already logged in to the
    RV website.
    If num_vids_to_process is -1, then all videos known to the SimpleDS will be processed."""

    int_count_vids_processed = 0
    int_count_vids_added = 0
    int_count_vids_updated = 0
    int_count_vids_deleted = 0
    int_count_vids_not_changed = 0

    # open the datastructure that contains metadata about each
    # video. This will allows us to iterate through the website
    # videos data with a defined size of iteration so we can
    # display percentages.
    web_vid_ds = SimpleDS(my_globals.str_dir4_website_vids_ds, my_globals.str_name_simpleds_website_vids)
    web_vid_ds.load()
    web_vid_ds.sort()

    # open the datastructure where we store additional video
    # information.
    other_vidinfo_DS = SimpleDS(my_globals.str_dir4_additional_vids_info_ds,
                                my_globals.str_name_simpleds_additionalinfo_vids)
    other_vidinfo_DS.load()
    other_vidinfo_DS.sort()

    # get the current timestamp. This timestamp will be used to
    # tag all videos where it is detected that the incoming
    # data is different from the data already stored in the
    # SimpleDS. This is the timestamp that will be saved in the
    # DATA-UPDATED column of the SimpleDS.
    # For simplicity, all videos where a change is detected will
    # be tagged with the same time.
    logging.debug("Setting time that will be used to stamp videos where "
                  " changes are detected related to 'Video Comments'")
    int_time_change_detected = make_now_timestamp()

    # now we iterate through the datastructure that holds the
    # website videos metadata.
    int_length_vidsDS = len(web_vid_ds)
    # first we decide how many iterations of the loop are going to happen depending on what is higher
    # the number passed to the function or the length of the SimpleDS. The AND portion of the IF
    # statement that has a '-1' in it makes it so that if -1 is passed as the parameter, ALL the
    # videos are processed.
    num_iterations = int_length_vidsDS
    if (num_vids_to_process < int_length_vidsDS) and (num_vids_to_process != -1):
        num_iterations = num_vids_to_process
    logging.info('Refreshing comments metadata for ' + str(num_iterations) + ' videos.')
    percent_tracker = PercentTracker(num_iterations, int_output_every_x_percent=5, log_level='info')
    # we envelop the whole loop in a try/except, so that we are able to save the SimpleDS to disk
    # at the end gracefully, and hopefully avoid inconsistencies where some data has been added
    # to disk, but the corresponding data does not end up getting added to the dataframe because
    # of an error mid-execution (the dataframe does not get saved to disk until the end of the
    # function.)
    try:
        for webvid_id in web_vid_ds:
            continue_execution = variable_manager.var_retrieve(my_globals.str_execution_may_go_on)
            # the following IF makes sure we only iterate through as many videos as
            # requested by one of the parameters passed to the function, and that the function
            # hasn't been requested to stop by an external factor that can manipulate the
            # variable manager.
            if (int_count_vids_processed >= num_iterations) or (continue_execution is False):
                break

            # set some variables we want to populate for the vid
            int_vid_total_comments = 0
            int_vid_reply_comments = 0
            int_vid_comments_likes = 0
            int_vid_comments_dislikes = 0

            vid_comments_url = 'https://www.realvision.com/rv/api/threads/' + webvid_id + '/comments'
            logging.debug("Opening URL of 'comments' for video: " + webvid_id)
            lst_comments = []
            try:
                req = requests_session.get(vid_comments_url, timeout=my_globals.int_timeout)
                str_status = str(req.status_code)
                if '200' in str_status:
                    logging.debug('Status: ' + str_status)
                else:
                    logging.info('Status: ' + str_status)
                lst_comments = req.json()[my_globals.str_vid_data]
            except Exception as e:
                logging.warning('Unable to extract comments from website API for video: '
                                + webvid_id + ' The Exception was: ' + repr(e))

            # now, if we got some info back from the website API we loop
            # through the list of comments and extract some info
            dict_results = {}
            if lst_comments:
                int_vid_total_comments = len(lst_comments)
                for a_comment in lst_comments:
                    num_comment_likes = 0
                    num_comment_dislikes = 0
                    comment_is_reply_to_another_comment = ''
                    # first we make sure the json data (which is a python dictionary at this point)
                    # has an entry called 'attributes'
                    if my_globals.str_vid_attributes in a_comment:
                        # extract the dictionary that has the information we are intersted in
                        dict_comment_attribs = a_comment[my_globals.str_vid_attributes]
                        str_comment_likes = 'comment_likes_count'
                        str_comment_dislikes = 'comment_dislikes_count'
                        str_comment_is_reply_to_other_comment = 'comment_reply_to_id'
                        # now extract from the dictionary, each piece of data we want
                        if str_comment_likes in dict_comment_attribs:
                            num_comment_likes = dict_comment_attribs[str_comment_likes]
                        if str_comment_dislikes in dict_comment_attribs:
                            num_comment_dislikes = dict_comment_attribs[str_comment_dislikes]
                        if str_comment_is_reply_to_other_comment in dict_comment_attribs:
                            comment_is_reply_to_another_comment = dict_comment_attribs[
                                str_comment_is_reply_to_other_comment]

                    # now, add the stats for this comment, to the running tally
                    int_vid_comments_likes += num_comment_likes
                    int_vid_comments_dislikes += num_comment_dislikes
                    if comment_is_reply_to_another_comment:
                        int_vid_reply_comments += 1

                # compile the results in the results dictionary
                dict_results = {my_globals.str_vid_comments_num_total: int_vid_total_comments,
                                my_globals.str_vid_comments_num_replies: int_vid_reply_comments,
                                my_globals.str_vid_comments_likes: int_vid_comments_likes,
                                my_globals.str_vid_comments_dislikes: int_vid_comments_dislikes
                                }

            # now we do stuff if we have some results in the results dictionary
            if dict_results:
                # now we check if the video is in the SimpleDS that stores 'other info'
                # because depending on that it will need to be added or updated.
                timestamp_vid_created = web_vid_ds.fetch_created(webvid_id)
                need_to_save_update = False
                need_to_add_video = False
                if webvid_id in other_vidinfo_DS:
                    # we are here if the vid already exists in the "other vid info" datastructure,
                    # so the data in the SimpleDS needs to be updated (as opposed to added.)
                    # because we store several un-related bits of data in this particular SimpleDS,
                    # they live in a dictionary. So in order to update the data, first we need to
                    # pull up the current dictionary. Then we can add the new data to the dictionary
                    # and then we add it back to the SimpleDS. However, first, out of interest (so we
                    # can report on the number of vids not changed) we compare the incoming data
                    # with the existing data. If all data is the same, then no update is needed.
                    dict_existing_data = other_vidinfo_DS.fetch_data(webvid_id)
                    if my_globals.str_vid_comments in dict_existing_data:
                        # if the function was told to update videos regardless, then we can
                        # mark the video as needing to be updated without further checks
                        if update_regardless_of_changes:
                            need_to_save_update = True
                        else:
                            # otherwise, we need to do some checks to see if the video should be
                            # tagged as needing to be updated downstream.
                            # If a 'comments' section already exists in the 'other info' data, then
                            # we do a comparison between the incoming data and the existing data.
                            for a_key in dict_results:
                                if dict_existing_data[my_globals.str_vid_comments][a_key] != dict_results[a_key]:
                                    need_to_save_update = True
                                    # as soon as a difference is found, the loop can be exited, because
                                    # at this point we are not tracking the changes, we are simply looking
                                    # to see if there are any
                                    break
                                    # POSSIBLE IMPROVEMENT. Note that somewhere around this point we could track
                                    # the changes (not just detect if there is a change) and in the 'update_entry'
                                    # method of SimpleDS, we could pass the change log as an argument.
                        if need_to_save_update:
                            int_count_vids_updated += 1
                    else:
                        # otherwise, if a 'comments' section does not yet exist in the 'other info' dict
                        # then it needs to be added
                        # note that it is a bit confusing that although we are detecting that the comments section
                        # doesn't exist (therefore, seeemingly the outcome should be an 'addition' not an 'update')
                        # we are marking it as needing to be updated (rather than added.) This is because
                        # the 'data section' where the comments are stored, already exists. If that whole section
                        # (where we store not only comments, but maybe other info in the
                        # future) doesn't exist, then it needs to be added. But here, in this case, the section
                        # exists, and it just needs to be updated with 'comments' information (that has never been
                        # added before.)
                        need_to_save_update = True
                        int_count_vids_added += 1

                    if need_to_save_update:
                        # the set of data points about comments get added to the dictionary about 'other video info'
                        # INSIDE yet another dictionary where the parent key is something like 'comments'
                        # hence the syntax below where a dictionary is being passed to the update function.
                        if not trial_run:
                            dict_existing_data.update({my_globals.str_vid_comments: dict_results})
                            other_vidinfo_DS.update_entry(webvid_id, dict_existing_data, int_time_change_detected,
                                                          timestamp_vid_created)
                else:
                    # we are here if the video does not yet exist in the datastructure, so
                    # it needs to be added.
                    # the set of data points about comments get added to the dictionary about 'other video info'
                    # INSIDE yet another dictionary where the parent key is something like 'comments'
                    # hence the syntax below where a dictionary is being passed to the add_entry function.
                    need_to_add_video = True
                    if not trial_run:
                        other_vidinfo_DS.add_entry(webvid_id, int_time_change_detected,
                                                   timestamp_vid_created, {my_globals.str_vid_comments: dict_results})
                    int_count_vids_added += 1

                # now we tag the video so that a downstream function that converts video info into
                # Airtable records knows it should be processed
                if need_to_save_update or need_to_add_video:
                    if not trial_run:
                        other_vidinfo_DS.tag_add(webvid_id, my_globals.str_tag_comments_chngd_for_airt_convert)
                else:
                    int_count_vids_not_changed += 1

            int_count_vids_processed += 1
            percent_tracker.update_progress(int_count_vids_processed,
                                            show_time_remaining_estimate=True,
                                            str_description_to_include_in_logging='Pulling Video Comments Statistics')

        # now we delete any videos that have been removed from the source (the source, in this case
        # is represented by the web_vid_ds)
        # for deletions we cannot pay attention to the num_vids_to_process variable, as in order
        # to reliably find out which videos have been removed at source, the entire universes
        # (the whole set) of each SimpleDS must be compared.
        set_vids_in_other_info_ds = other_vidinfo_DS.fetch_all_ids_as_python_set()
        set_vids_in_web_vid_ds = web_vid_ds.fetch_all_ids_as_python_set()
        set_vids_removed_at_source = set_vids_in_other_info_ds - set_vids_in_web_vid_ds
        for entry in set_vids_removed_at_source:
            if not trial_run:
                other_vidinfo_DS.delete_entry(entry, keep_version_of_file_in_log_directory=True)
            int_count_vids_deleted += 1

    except Exception as e:
        logging.warning('Something went wrong during the loop that pulls stats about video comments. This happened'
                        ' while processing video: ' + webvid_id + ' The Exception was: ' + repr(e))
    other_vidinfo_DS.save2disk()
    logging.info('Video comments-section added: ' + str(int_count_vids_added))
    logging.info('Video comments-section updated: ' + str(int_count_vids_updated))
    logging.info('Video comments-section deleted: ' + str(int_count_vids_deleted) + ' (in ALL SimpleDS, not just in the'
                 ' videos processed, because delete operation must be global.)')
    logging.info('Video comments-section unchanged: ' + str(int_count_vids_not_changed))
    logging.info('Videos processed: ' + str(int_count_vids_processed))
# ------------------------ END FUNCTION ------------------------ #
