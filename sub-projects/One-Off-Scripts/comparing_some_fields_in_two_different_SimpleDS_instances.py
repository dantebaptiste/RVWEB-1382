from class_simpleDS import SimpleDS
import my_globals
from datetime import datetime
import csv
from class_rv_website_json_vid import RVwebsiteVid
from my_building_blocks import projectStandardTimestamp

old_transcripts_ds = SimpleDS('/home/mclovin/myProjects/realV/temp_restore/guestDataProject/outputFiles'
                              '/manipulatedData/SimpleDS_Instances/website_vids_transcripts_datastructure/')
old_transcripts_ds.load()
old_transcripts_ds.sort()
current_transcripts_ds = SimpleDS(my_globals.str_dir4_vid_transcripts_ds)
current_transcripts_ds.load()
current_transcripts_ds.sort()
vids_ds = SimpleDS(my_globals.str_dir4_website_vids_ds)
vids_ds.load()
vids_ds.sort()
lst_missing_entries = []

for entry in old_transcripts_ds:
    dct_results = {'Video ID': '',
                   'Video Title': '',
                   'Published On': 0,
                   'URL': ''}
    if entry not in current_transcripts_ds:
        vid_obj = RVwebsiteVid(vids_ds.fetch_data(entry))
        dct_results['Video ID'] = vid_obj.str_id
        dct_results['Video Title'] = vid_obj.str_title
        dct_results['Published On'] = (datetime.fromtimestamp(vid_obj.int_published_on / 1000)).strftime("%Y-%m-%d")
        dct_results['URL'] = 'https://www.realvision.com/tv/videos/id/' + vid_obj.str_id
        lst_missing_entries.append(dct_results)

# save a CSV that has a log of the transcript urls and download status
full_csv_path = my_globals.str_dir4_outputs_logs_misc + projectStandardTimestamp() + 'missing_transcripts.csv'
with open(full_csv_path, mode='w') as csv_file:
    cols = ['Video ID',
            'Video Title',
            'Published On',
            'URL']
    writer = csv.DictWriter(csv_file, fieldnames=cols)
    writer.writeheader()
    writer.writerows(lst_missing_entries)
