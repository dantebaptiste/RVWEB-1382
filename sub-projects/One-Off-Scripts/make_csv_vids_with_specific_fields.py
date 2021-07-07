from datetime import datetime
import csv
import my_globals
from my_building_blocks import projectStandardTimestamp
from class_simpleDS import SimpleDS
from class_rv_website_json_vid import RVwebsiteVid

path_to_simds = my_globals.str_dir4_website_vids_ds
web_vids_ds = SimpleDS(path_to_simds)
web_vids_ds.load()
web_vids_ds.sort()

lst_vids = []
fieldname_vidid = 'Video ID'
fieldname_vidtitle = 'Video Title'
fieldname_vidpublishdate = 'Published On'
fieldname_vidpublished = 'Is Published'
fieldname_vidurl = 'Prod URL'
fieldname_vidurljson = 'URL of JSON'

dct_data_to_extract = {fieldname_vidid: '',
                       fieldname_vidtitle: '',
                       fieldname_vidpublishdate: 0,
                       fieldname_vidpublished: '',
                       fieldname_vidurl: '',
                       fieldname_vidurljson: ''}

for vid_id in web_vids_ds:
    vid_obj = RVwebsiteVid(web_vids_ds.fetch_data(vid_id))
    dct_results = dct_data_to_extract.copy()
    dct_results[fieldname_vidid] = vid_obj.str_id
    dct_results[fieldname_vidtitle] = vid_obj.str_title
    dct_results[fieldname_vidpublishdate] = \
        (datetime.fromtimestamp(vid_obj.int_published_on / 1000)).strftime("%Y-%m-%d")
    dct_results[fieldname_vidpublished] = vid_obj.bool_is_published
    dct_results[fieldname_vidurl] = 'https://www.realvision.com/tv/videos/id/' + vid_obj.str_id
    dct_results[fieldname_vidurljson] = 'https://www.realvision.com/rv/api/videos/' + vid_obj.str_id
    lst_vids.append(dct_results)

# save a CSV that has a log of the transcript urls and download status
full_csv_path = my_globals.str_dir4_outputs_logs_misc + projectStandardTimestamp() + 'vids_are_published.csv'
with open(full_csv_path, mode='w') as csv_file:
    cols = [fieldname_vidid,
            fieldname_vidtitle,
            fieldname_vidpublishdate,
            fieldname_vidpublished,
            fieldname_vidurl,
            fieldname_vidurljson]
    writer = csv.DictWriter(csv_file, fieldnames=cols)
    writer.writeheader()
    writer.writerows(lst_vids)
