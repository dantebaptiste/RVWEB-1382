import time
from datetime import datetime
import my_globals
import pandas as pd
from class_simpleDS import SimpleDS
from class_rv_website_json_vid import RVwebsiteVid
from class_trancript import Transcript

"""This file searches Video Transripts for a particular phrase and creates
a CSV with each video that has the phrase, and the number of times ."""

start_time = time.time()
print('Running script to find all occurrences of a phrase in Video transcripts.')

# ---- SETTINGS ---- #
int_filter_vids_newer_than = 1546318800000  # 1546318800000 is midnight 2019-01-01
int_filter_vids_older_than = 1577854800000  # 1577854800000 is midnight 2020-01-01
term_to_search = "fuck"
# -- END SETTINGS -- #


# open the datastructure that contains metadata about each
# video. This will allows us to iterate through the website
# videos data with a defined size of iteration so we can
# display percentages.
web_vids_ds = SimpleDS(my_globals.str_dir4_website_vids_ds, my_globals.str_name_simpleds_website_vids)
web_vids_ds.load()
web_vids_ds.sort()

# load the instance of SimpleDS that contains info about
# video transcripts
transcripts_ds = SimpleDS(my_globals.str_dir4_vid_transcripts_ds, my_globals.str_name_simpleds_transcripts)
transcripts_ds.load()
transcripts_ds.sort()

# create a pandas dataframe that will keep a log of the instances found for the search phrase
columns_for_search_results_table = ['Video Title', 'Date Published', 'Term Searched', 'Term Found', 'Snippet']
df_results = pd.DataFrame(columns=columns_for_search_results_table)

for webvid_id in web_vids_ds:
    lst_of_the_data = []
    vid = RVwebsiteVid(web_vids_ds.fetch_data(webvid_id))
    vid_title = vid.str_title
    vid_date_published = (datetime.fromtimestamp(vid.int_published_on / 1000)).strftime("%Y-%m-%d")

    # only do stuff for videos that meet the date filter criteria
    if (vid.int_published_on > int_filter_vids_newer_than) and (vid.int_published_on < int_filter_vids_older_than):
        if webvid_id in transcripts_ds:
            the_transcript = Transcript(webvid_id)
            the_transcript.set_transcript_directory(my_globals.str_dir4_vid_transcripts_data)
            the_transcript.load_transcript_object_from_dictionary(transcripts_ds.fetch_data(webvid_id))
            results = the_transcript.search_transcript_for_a_string(term_to_search, 35)
            if results:
                for item in results:
                    lst_of_the_data = [vid_title, vid_date_published, term_to_search, 'YES', item]
                    df_results.loc[len(df_results)] = lst_of_the_data
            else:
                lst_of_the_data = [vid_title, vid_date_published, term_to_search, '[NO - term not found]',
                                   '[n/a]']
                df_results.loc[len(df_results)] = lst_of_the_data
        else:
            lst_of_the_data = [vid_title, vid_date_published, term_to_search, '[NO - transcript unavailable]', '[n/a]']
            df_results.loc[len(df_results)] = lst_of_the_data

df_results.to_csv(my_globals.str_path4_outputs_manipd_CSVs + term_to_search + '.csv')

end_time = time.time()
print('Time it took to run this workflow is: ' + "{:.2f}".format(end_time - start_time) + ' seconds.')
