import my_config

# The number below is an arbitrary number to be used in a binary search
# that will find the highest multiple that still returns data. It can
# be as high as you want to guarantee that, for a particular product,
# real vision has produced less videos than that. For example, in the
# television product (currently the product with the most videos) there
# are around 2500 vids. So for the number below I am currently using
# 20,000 - a number which will be bigger than the actual number of vids
# for many years.
num_upper_bound_on_search_of_highest_multiple = 20000

# scheduling strings used by main
str_job_sched_frequent_hours = my_config.job_sched_frequent_hours
str_job_sched_frequent_minutes = my_config.job_sched_frequent_minutes
str_job_sched_daily_days = my_config.job_sched_daily_days
str_job_sched_daily_hours = my_config.job_sched_daily_hours
str_job_sched_weekly_days = my_config.job_sched_weekly_days
str_job_sched_weekly_hours = my_config.job_sched_weekly_hours

# timeout when doing web stuff
int_timeout = 10

# airtable bases
str_AT_base = my_config.AT_base
str_AT_base_working_on = my_config.AT_base_working_on

# algolia variables
# fieldnames:
str_alg_fieldname_id = 'objectID'
str_alg_fieldname_type = 'Type'
str_alg_fieldname_title = 'Title'
str_alg_fieldname_description = 'Description'
str_alg_fieldname_thumbnail = 'Thumbnail'
str_alg_fieldname_featuring = 'Featuring'
str_alg_fieldname_interviewer = 'Interviewer'
str_alg_fieldname_tags = 'Human Tags'
str_alg_fieldname_likes = 'Likes'
str_alg_fieldname_dislikes = 'Dislikes'
str_alg_fieldname_show = 'Show'
str_alg_fieldname_productid = 'Product'
str_alg_fieldname_vidurl = 'URL'
str_alg_fieldname_duration = 'Duration'
str_alg_fieldname_publishedon = 'Published On'
str_alg_fieldname_publishedon_readable = 'Published On 4Humans'
str_alg_fieldname_tiers = 'Membership Tier'
str_alg_fieldname_numcomments = 'Number of Comments'
str_alg_fieldname_transcript = 'Transcript'
str_alg_fieldname_pseudotranscript = 'Pseudo-transcript'
str_alg_fieldname_numpages = 'Page Count'
# Algolia indexes and dictionaries
# VIDEOS index 01 is an index where we are pushing all fields, and the PSEUDO-transcript
str_algolia_vids_idx_01 = my_config.algolia_vids_idx_01
# VIDEOS index 02 we are using to test records with FULL transcript
str_algolia_vids_idx_02 = my_config.algolia_vids_idx_02
# PUBLICATIONS index 01 we are using to push the publication fields with the PSEUDO-text,
# but NOT the full text.
str_algolia_pubs_idx_01 = my_config.algolia_pubs_idx_01
# also a dictionary that states which fields should be pushed to Algolia for each index.
# Think of this as a filter. It isn't the SOURCE of the list of fields; its more
# like, if the field is present in the dictionary, then the field 'is allowed'
# to be pushed. (The source of all the fields, is the declaration of field variables
# in the AlgoliaDataUnit class, which should be a mirror of the declaration of 'fields'
# above.)
dict_algolia_fields_to_allow_to_push = {str_algolia_vids_idx_01: [str_alg_fieldname_id,
                                                                  str_alg_fieldname_type,
                                                                  str_alg_fieldname_title,
                                                                  str_alg_fieldname_description,
                                                                  str_alg_fieldname_thumbnail,
                                                                  str_alg_fieldname_featuring,
                                                                  str_alg_fieldname_interviewer,
                                                                  str_alg_fieldname_tags,
                                                                  str_alg_fieldname_likes,
                                                                  str_alg_fieldname_dislikes,
                                                                  str_alg_fieldname_show,
                                                                  str_alg_fieldname_productid,
                                                                  str_alg_fieldname_vidurl,
                                                                  str_alg_fieldname_duration,
                                                                  str_alg_fieldname_publishedon,
                                                                  str_alg_fieldname_publishedon_readable,
                                                                  str_alg_fieldname_tiers,
                                                                  str_alg_fieldname_numcomments,
                                                                  str_alg_fieldname_pseudotranscript],
                                        str_algolia_vids_idx_02: [str_alg_fieldname_id,
                                                                  str_alg_fieldname_type,
                                                                  str_alg_fieldname_title,
                                                                  str_alg_fieldname_description,
                                                                  str_alg_fieldname_thumbnail,
                                                                  str_alg_fieldname_featuring,
                                                                  str_alg_fieldname_interviewer,
                                                                  str_alg_fieldname_tags,
                                                                  str_alg_fieldname_likes,
                                                                  str_alg_fieldname_dislikes,
                                                                  str_alg_fieldname_show,
                                                                  str_alg_fieldname_productid,
                                                                  str_alg_fieldname_vidurl,
                                                                  str_alg_fieldname_duration,
                                                                  str_alg_fieldname_publishedon,
                                                                  str_alg_fieldname_publishedon_readable,
                                                                  str_alg_fieldname_tiers,
                                                                  str_alg_fieldname_numcomments,
                                                                  str_alg_fieldname_transcript],
                                        str_algolia_pubs_idx_01: [str_alg_fieldname_id,
                                                                  str_alg_fieldname_type,
                                                                  str_alg_fieldname_title,
                                                                  str_alg_fieldname_description,
                                                                  str_alg_fieldname_thumbnail,
                                                                  str_alg_fieldname_numpages,
                                                                  str_alg_fieldname_tags,
                                                                  str_alg_fieldname_likes,
                                                                  str_alg_fieldname_dislikes,
                                                                  str_alg_fieldname_productid,
                                                                  str_alg_fieldname_vidurl,
                                                                  str_alg_fieldname_publishedon,
                                                                  str_alg_fieldname_publishedon_readable,
                                                                  str_alg_fieldname_tiers,
                                                                  str_alg_fieldname_pseudotranscript],
                                        }

# standard strings used in various places
str_to_use_if_something_has_been_found = 'Found'
str_to_use_if_something_has_not_been_found = 'Not found'
str_logging_func_next = '********  About to call function: '
str_logging_func_exited = '**** Exited function: '
str_execution_may_go_on = 'allowed_to_run'

str_product_name_access = 'access'
str_product_name_tv = 'television'
str_product_name_mi = 'macroinsiders'
str_product_name_tt = 'thinktank'
str_product_name_crypto = 'crypto'
# list of VIDEO products we are interested in from the Real Vision website
lst_rv_website_product_ids = [str_product_name_access, str_product_name_tv, str_product_name_mi,
                              str_product_name_crypto]
# a dictionary describing (as of this writing, but it
# may change, and this sadly cannot be pulled from the API, which would be
# great for keeping it automatically up-to-date) which products are available to which tiers
str_tier_name_pro = 'PRO'
str_tier_name_plus = 'PLUS'
str_tier_name_essential = 'ESSENTIAL'
str_tier_name_crypto = 'CRYPTO'
str_pseudotier_name_free = 'FREE'
dict_product_mapping_to_tiers = {str_product_name_mi: [str_tier_name_pro],
                                 str_product_name_access: [str_tier_name_plus],
                                 str_product_name_tv: [str_tier_name_essential],
                                 str_product_name_crypto: [str_tier_name_crypto]
                                 }
# list of ISSUES (publications) products we are interested in from the Real Vision website
# At the time of this writing, this list is used to pull Publication metadata from the RV website
# note that there are
# thinktank
# publications, but at the moment those are not wanted in Algolia
lst_rv_website_product_ids_publications = [str_product_name_access, str_product_name_mi]  # , str_product_name_tt]

# tolerances for number of videos and deletions allowed before
# considering them erroneous
int_max_vid_deletions_tolerance = 10
int_max_pub_deletions_tolerance = 5

# In the lists below, make sure you have a comma after each of the
# sub-lists. Python won't warn about the syntax if you don't, but
# the code won't run and it is an obscure error.
# Initially I was adding to this list Real Vision staff, but now
# for completeness I think it is important that everybody be matched
# to an entry in the Guests table, even staff. So the list below,
# contains strings that have been found in the 'featuring' or 'interviewer'
# fields, that should be ignored.
# Dwayne Borwick, Jim Cowden, etc. are ficticious names, so I'm not going to
# process them, as that would mean they would need to be added as rows
# in the Airtable Guests table.
odd_strings_in_website_persons_fields = ['more', 'Real Vision Editorial Team',
                                         'Jim Cowden - NOW BROUGHT TO YOU BY CRAFTY CWOFEE',
                                         'Dwayne Borwick', 'Carry Carbone']

# As of this writing, the following are your options for video attributes
#    'video_duration',
#    'video_asset_names',       (10 seems to be the max I believe. I think it is legacy)
#    'video_is_published',
#    'video_likes_count',
#    'video_tag_names',         (40 seems to be the max I believe)
#    'video_interviewer',
#    'video_topic_names',       (10 seems to be the max I believe)
#    'video_has_thumbnail',
#    'video_has_screenshot',
#    'video_dislikes_count',
#    'video_featuring',         (6 seems to be the max I believe)
#    'video_product_id',
#    'video_made_free_on',
#    'video_rating',
#    'video_published_on',
#    'video_description',
#    'video_has_4_3_image',
#    'video_title',
#    'video_content_last_updated',
#    'video_has_hero',
#    'video_guidance_rating',
#    'video_short_description',
#    'video_is_free',
#    'video_id',
#    'video_filmed_on'

# strings representing field names in JSON pulled from the
# RV website
str_vid_id = 'id'
str_vid_data = 'data'
str_vid_relationships = 'relationships'
str_vid_videoassets = 'videoassets'
str_vid_links = 'links'
str_vid_related = 'related'
str_vid_transcript = 'transcript'
str_vid_attributes = 'attributes'
str_vid_transcriptjson_words = 'words'

# variables used in conjunction with class Transcript
str_trnscrpt_class_column_terms = 'terms'
str_trnscrpt_class_column_count = 'count'
str_trnscrpt_class_column_tfidf = 'tfidf'
str_trnscrpt_class_size_varname = 'transcript size'

# variables used in conjunction with video comments
str_vid_comments = 'vid_comments'
str_vid_comments_likes = 'all_comments_likes'
str_vid_comments_dislikes = 'all_comments_dislikes'
str_vid_comments_num_total = 'total_num_comments'
str_vid_comments_num_replies = 'comments_that_are_replies'

# Names for different instances of SimpleDS
str_name_simpleds_website_vids = 'RV Website videos json SimpleDS'
str_name_simpleds_website_pubs = 'RV Website issues/publications json SimpleDS'
str_name_simpleds_airtable_records = 'Airtable video records SimpleDS'
str_name_simpleds_additionalinfo_vids = 'Additional video info SimpleDS'
str_name_simpleds_transcripts = 'Transcripts SimpleDS'
str_name_simpleds_pubsfulltext = 'Publication Texts SimpleDS'
str_name_simpleds_algolia_idx_vids_01 = 'Algolia records SimpleDS videos idx_01'
str_name_simpleds_algolia_idx_vids_02 = 'Algolia records SimpleDS videos idx_02'
str_name_simpleds_algolia_idx_pubs_01 = 'Algolia records SimpleDS publications idx_01'

# hierarchical json locations of some items we need from the RV website
str_name_transcriptjson_field = 'json_transcript'
str_name_transcriptpdf_field = 'transcript'
dict_fields_in_videoassets = {
    str_name_transcriptjson_field: [str_vid_data, str_vid_links, str_name_transcriptjson_field],
    str_name_transcriptpdf_field: [str_vid_data, str_vid_links, str_name_transcriptpdf_field]}

# The schema dictionaries below fulfill the following purpose
#    The values are a list that say where abouts in the
#    JSON data returned by the website the data actually
#    lives. The JSON data returned is a hierarchy of fields
#    for each record, so this dictionary remembers where
#    in the hierarchy everything lives. Then, a function
#    can be used to retrieve the data fairly easily.
dict_vids_from_website_schema = {'id': ['id'],
                                 'type': ['type'],
                                 'show': ['relationships', 'show', 'links', 'related'],
                                 'videoassets': ['relationships', 'videoassets', 'links', 'related'],
                                 'thumbnail': ['links', 'thumbnail'],
                                 'video_title': ['attributes', 'video_title'],
                                 'video_featuring': ['attributes', 'video_featuring'],
                                 'video_interviewer': ['attributes', 'video_interviewer'],
                                 'video_description': ['attributes', 'video_description'],
                                 'video_topic_names': ['attributes', 'video_topic_names'],
                                 'video_tag_names': ['attributes', 'video_tag_names'],
                                 'video_asset_names': ['attributes', 'video_asset_names'],
                                 'video_product_id': ['attributes', 'video_product_id'],
                                 'video_is_published': ['attributes', 'video_is_published'],
                                 'video_likes_count': ['attributes', 'video_likes_count'],
                                 'video_dislikes_count': ['attributes', 'video_dislikes_count'],
                                 'video_duration': ['attributes', 'video_duration'],
                                 'video_filmed_on': ['attributes', 'video_filmed_on'],
                                 'video_published_on': ['attributes', 'video_published_on'],
                                 'video_is_free': ['attributes', 'video_is_free'],
                                 }
dict_pubs_from_website_schema = {'id': ['id'],
                                 'type': ['type'],
                                 'thumbnail': ['links', 'thumbnail'],
                                 'issue_title': ['attributes', 'issue_title'],
                                 'issue_is_published': ['attributes', 'issue_is_published'],
                                 'issue_page_count': ['attributes', 'issue_page_count'],
                                 'issue_likes_count': ['attributes', 'issue_likes_count'],
                                 'issue_dislikes_count': ['attributes', 'issue_dislikes_count'],
                                 'issue_topic_names': ['attributes', 'issue_topic_names'],
                                 'issue_asset_names': ['attributes', 'issue_asset_names'],
                                 'issue_published_on': ['attributes', 'issue_published_on'],
                                 'issue_product_id': ['attributes', 'issue_product_id'],
                                 'issue_summary': ['attributes', 'issue_summary']
                                 }

dict_mapping_rv_web_vid_json_2_rv_webvidclass_attrib = {'id': 'str_id',
                                                        'type': 'str_type',
                                                        'show': 'str_url_show',
                                                        'videoassets': 'str_url_videoassets',
                                                        'thumbnail': 'str_url_thumbnail',
                                                        'video_title': 'str_title',
                                                        'video_featuring': 'str_featuring_raw',
                                                        'video_interviewer': 'str_interviewer_raw',
                                                        'video_description': 'str_description',
                                                        'video_topic_names': 'lst_topic_names',
                                                        'video_tag_names': 'lst_tag_names',
                                                        'video_asset_names': 'lst_asset_names',
                                                        'video_product_id': 'str_product_id',
                                                        'video_is_published': 'bool_is_published',
                                                        'video_likes_count': 'int_likes_count',
                                                        'video_dislikes_count': 'int_dislikes_count',
                                                        'video_duration': 'int_duration',
                                                        'video_filmed_on': 'int_filmed_on',
                                                        'video_published_on': 'int_published_on',
                                                        'video_is_free': 'bool_is_free',
                                                        }
dict_mapping_rv_webpubjson_2_rv_webpubclass_attrib = {'id': 'str_id',
                                                      'type': 'str_type',
                                                      'thumbnail': 'str_url_thumbnail',
                                                      'issue_title': 'str_title',
                                                      'issue_is_published': 'bool_is_published',
                                                      'issue_page_count': 'int_page_count',
                                                      'issue_likes_count': 'int_likes_count',
                                                      'issue_dislikes_count': 'int_dislikes_count',
                                                      'issue_topic_names': 'lst_topic_names',
                                                      'issue_asset_names': 'lst_asset_names',
                                                      'issue_published_on': 'int_published_on',
                                                      'issue_product_id': 'str_product_id',
                                                      'issue_summary': 'str_summary',
                                                      }

# Airtable column names
str_AT_vid_col_name_title = 'Title'
str_AT_vid_col_name_show = 'Show'
str_AT_vid_col_name_featuring = 'Featuring'
str_AT_vid_col_name_interviewers = 'Interviewer(s)'
str_AT_vid_col_name_description = 'Description'
str_AT_vid_col_name_topics = 'Topics'
str_AT_vid_col_name_likes = 'Likes'
str_AT_vid_col_name_dislikes = 'Dislikes'
str_AT_vid_col_name_product = 'Product'
str_AT_vid_col_name_publishedon = 'Published On'
str_AT_vid_col_name_filmedon = 'Filmed On'
str_AT_vid_col_name_websiteid = 'ID on RV website'
str_AT_vid_col_name_isfree = 'Is Free?'
str_AT_vid_col_name_availableto = 'Available To'
str_AT_vid_col_name_lastchanged = 'last_change_detected'
str_AT_vid_col_name_linkurl = 'Link to video'
str_AT_vid_col_name_comments = 'Total Comments'
str_AT_vid_col_name_comments_replies = 'Reply Comments'
str_AT_vid_col_name_comments_likes = 'Likes of Comments'
str_AT_vid_col_name_comments_dislikes = 'Dislikes of Comments'

# The dictionary below keeps track of the fields (columns)
# we want to push to airtable for each video, and the corresponding
# field name in the RV website JSON dumps.
# VERY IMPORTANT TO NOTE that each corresponding field is passed
# inside a list. This allows the function that uses this dictionary
# to loop through fields where in Airtable we are going to combine
# several fields from the website. So, for the most part, each
# entry in the dictionary will be a one-item list. But doing this
# allows us to have some entries, where there are multiple items
# in the list, which will get combined and put into Airtable in
# one field.
dict_translate_vids_fields_website2airtable = {str_AT_vid_col_name_title: ['video_title'],
                                               str_AT_vid_col_name_show: ['show'],
                                               str_AT_vid_col_name_featuring: ['video_featuring'],
                                               str_AT_vid_col_name_interviewers: ['video_interviewer'],
                                               str_AT_vid_col_name_description: ['video_description'],
                                               str_AT_vid_col_name_topics: ['video_topic_names', 'video_tag_names',
                                                                            'video_asset_names'],
                                               str_AT_vid_col_name_likes: ['video_likes_count'],
                                               str_AT_vid_col_name_dislikes: ['video_dislikes_count'],
                                               str_AT_vid_col_name_product: ['video_product_id'],
                                               str_AT_vid_col_name_publishedon: ['video_published_on'],
                                               str_AT_vid_col_name_filmedon: ['video_filmed_on'],
                                               str_AT_vid_col_name_websiteid: ['id'],
                                               }

list_web_date_fields_2make_human_readable = ['video_filmed_on',
                                             'video_published_on'
                                             ]

list_fields_from_website_that_are_within_url = ['show']

# The indexes below refer are metadata describing
# what each field is in each list (in the list of lists)
# in the two 'vidAttributesIwant' variables below.
idx_website_json_name = 0
idx_my_CSV_column_name = 1
idx_my_CSV_num_columns_for_field = 2
# Common configuration:
vidAttributesIwant_split = [['video_id', 'id', 1],
                            ['video_title', 'title', 1],
                            ['video_product_id', 'product ID', 1],
                            ['video_likes_count', 'likes', 1],
                            ['video_dislikes_count', 'dislikes', 1],
                            ['video_description', 'description', 1],
                            ['video_is_published', 'published', 1],
                            ['video_filmed_on', 'filmed', 1],
                            ['video_published_on', 'published on', 1],
                            ['video_interviewer', 'interviewer', 2],
                            ['video_featuring', 'featuring', 6],
                            ['video_topic_names', 'topic', 10],
                            ['video_tag_names', 'tag', 40],
                            ['video_asset_names', 'asset', 10]
                            ]
# Another common configuration:
vidAttributesIwant_nosplit = [['video_id', 'id', 1],
                              ['video_title', 'title', 1],
                              ['video_product_id', 'product ID', 1],
                              ['video_likes_count', 'likes', 1],
                              ['video_dislikes_count', 'dislikes', 1],
                              ['video_description', 'description', 1],
                              ['video_is_published', 'published', 1],
                              ['video_filmed_on', 'filmed', 1],
                              ['video_published_on', 'published on', 1],
                              ['video_interviewer', 'interviewer', 1],
                              ['video_featuring', 'featuring', 1],
                              ['video_topic_names', 'topic', 1],
                              ['video_tag_names', 'tag', 1],
                              ['video_asset_names', 'asset', 1]
                              ]

# A list to track the fields
# related to Guests that we want to query from Airtable.
# IMPORTANT
# Field0 and Field 1 SHOULD ALWAYS BE THE NAME FIELD AND THE
# ID FIELD RESPECTIVELY
str_name_of_airt_guests_table = 'Guests'
lst_fields_airT_tbl_guests = ['Name',
                              'id',
                              'Topics Discussed'
                              ]
idx_fields_airT_tbl_guests_name = 0
idx_fields_airT_tbl_guests_id = 1
idx_fields_airT_tbl_guests_subjects = 2

# A list to track the fields
# related to Videos that we want to query from Airtable.
# BELOW THE LIST are corresponding indexes which should be
# corrected manually if the list changes. The 'Title' field
# could be uncommented for troubleshooting if needed, for
# example, and then the index may need to be uncommented as well.
str_name_of_airt_videos_table = 'Videos'
lst_fields_airT_tbl_videos = ['id',
                              'ID on RV website'
                              ]
idx_fields_airT_tbl_videos_airt_id = 0
idx_fields_airT_tbl_videos_website_id = 1

# A list to track the fields
# related to SHOWS that we want to query from Airtable.
# IMPORTANT
# Field0 and Field 1 SHOULD ALWAYS BE THE NAME AND THE
# AIRTABLE-ID RESPECTIVELY
str_name_of_airt_shows_table = 'Shows'
lst_fields_airT_tbl_shows = ['Name',
                             'id',
                             'name_on_rv_website'
                             ]
idx_fields_airT_tbl_shows_name = 0
idx_fields_airT_tbl_shows_airt_id = 1
idx_fields_airT_tbl_shows_namefromweb = 2

# in the dictionary below we are storing the names of columns (as keys)
# of the custom DS (data structure) we have created for website vids. The dictionary
# translates (with the value of each key) these columns to the respective
# data fields inside the JSON info that comes from the website.
str_ds_data_id = 'ID'
str_ds_data_lastupdated = 'DATA-UPDATED'
str_ds_data_created = 'DATA-CREATED'
dict_pair_ds_columns_to_web_vid_fields = {str_ds_data_id: 'video_id',
                                          str_ds_data_lastupdated: 'video_content_last_updated',
                                          str_ds_data_created: 'video_published_on'
                                          }
idx_ds_data_id = 0
idx_ds_data_lastupdated = 1
idx_ds_data_created = 2

# the variable below is used to tag data (by the function that converts
# videos from website format to airtable format) that a different
# function should re-push to airtable even if the timestamps match.
str_tag_repush_to_airtable = 'repush2airtable'
# another tag is used to mark a video that has issues (something is
# not quite right with the data on the website) - for example, a guest
# was added to 'featuring' that isn't in airtable, or for example, show
# names don't match between website and airtable.
str_tag_web2airt_issues = 'web2airt-issues'
# similarly the tag below can be used to tag the
# simpleDS data structure that holds info pulled from the website
# to tag videos as changed. This normally is not necessary because
# a hash of changed videos is stored. However, in some cases it might
# be useful. For example, in the case that a new field is being
# populated, then you'd want to mark ALL the videos with this tag
# so they all get pushed again.
str_tag_web_vid_changed = 'something_changed'
# below is a tag used by the method that pulls stats about comments
# about videos, to mark a video so that another method that converts info
# into airtable records, knows the record should be processed.
str_tag_comments_chngd_for_airt_convert = 'airtable_conversion_should_reprocess_comments'
# below, tags for the transcripts SmipleDS to label the source
# of the transcript
str_tag_transcript_source_json = 'source_json'
str_tag_transcript_source_pdf = 'source_pdf'
# below is a tag used to mark videos for which, during the last attempt
# a video transcript could not be found on the RV website.
str_tag_webvid_transcript_missing = 'transcript_missing'
# below is a generic tag that can be used to mark a row in SimpleDS
# for removal.
str_tag_delete_row_from_simpleds = 'remove_row'
# below is the tag that marks a record for removal from Algolia,
# which also marks it as needing to be deleted from the AlgoliaSimpleDS
str_tag_delete_from_algolia = 'remove_from_algolia_and_locally'

# REMEMBER. Anything you add to the list below must be in ALL CAPS
# for the function that uses this list to work
list_of_possible_name_prefixes = ('DR.', 'PROFESSOR', 'SENATOR', 'LORD', 'BRIG. GENERAL (RET)', 'MAYOR')

str_transcripts_report_column_videoid = 'Video'
str_transcripts_report_column_videoassetsurl = 'VideoAssets URL'
str_transcripts_report_column_jsonurl = 'JSON Transcript URL'
str_transcripts_report_column_pdfurl = 'PDF Transcript URL'
str_fulltexts_report_column_videoid = 'Publication'
str_fulltexts_report_column_url = 'Transcript URL'
str_fulltexts_report_column_pdfplumber = 'PDF plumber results'
str_fulltexts_report_column_pypdf2 = 'PyPDF results'

# below are paths to different data directories used by the code
str_path_data = '/home/realvision/rv_multi_etl/data/'
str_path4_outputs = str_path_data + 'outputFiles/'
str_dir4_execution_related = str_path_data + 'execution_related/'
str_path4_subprojects = str_path_data + 'sub-projects/'
str_path4_outputs_raw_from_web = str_path4_outputs + 'rawJSONfromWebFiles/'
str_path4_outputs_manipd = str_path4_outputs + 'manipulatedData/'
str_path4_outputs_manipd_simpledsinstances = str_path4_outputs_manipd + 'SimpleDS_Instances/'
str_path4_outputs_manipd_CSVs = str_path4_outputs + 'manipulatedData/CSVs/'
str_path4_outputs_manipd_JSON = str_path4_outputs + 'manipulatedData/JSON/'
str_path4_outputs_logs = str_path4_outputs + 'logs/'
str_path4_outputs_temp = str_path4_outputs + 'temp/'

str_dir4_outputs_logs_misc = str_path4_outputs_logs + 'misc/'

str_fullfilepath_main_log = str_path4_outputs_logs + 'main.log'

str_dir4_execution_related_generic = str_dir4_execution_related + 'generic/'
str_fullfilepath_generic_execution_variables = str_dir4_execution_related_generic + 'execution_variables.txt'

str_dir4_execution_related_rvwebsite = str_dir4_execution_related + 'rv_website/'
str_dir4_product_id_info = str_dir4_execution_related_rvwebsite + 'product_ids/'
str_fullfilepath_rv_website_authentication_data = \
    str_dir4_execution_related_rvwebsite + 'Authentication/auth_data.json'
str_fullfilepath_rv_website_authentication_vars = \
    str_dir4_execution_related_rvwebsite + 'Authentication/authenticate_workflow_vars.txt'

str_dir4_execution_related_algolia = str_dir4_execution_related + 'algolia/'
str_fullfilepath_algolia_execution_variables = str_dir4_execution_related_algolia + 'execution_variables.txt'
str_fullfilepath_pseudotranscript_unwanted_terms = str_dir4_execution_related_algolia + 'unwanted_terms.txt'

str_dir4_execution_related_continuous_exec = str_dir4_execution_related + 'continuous_execution/'
str_fullfilepath_continuous_execution_jobs_variables = str_dir4_execution_related_continuous_exec +\
                                                       'jobs_execution_variables.txt'
str_fullfilepath_continuous_execution_mainloop_variables = str_dir4_execution_related_continuous_exec +\
                                                       'main_loop_variables.txt'

str_midpath4_outputs_website_guests_subjects = 'website_guestsNsubjectsData/'
str_midpath4_outputs_website_interviewers = 'website_interviewers/'

str_dir_path_raw_website_json_shows = str_path4_outputs_raw_from_web + 'rv_website_infoShows/'
str_file_raw_website_json_shows = str_dir_path_raw_website_json_shows + 'showsInfo.json'

str_dir_path_raw_website_json_video_sets = str_path4_outputs_raw_from_web + 'rv_website_infoVideoSets/'
str_dir_path_raw_web_json_vid_sets_temp_backup = str_dir_path_raw_website_json_video_sets + 'temporary_backup/'
str_filename_base_string4_raw_website_json_video_sets = 'raw_json_sets_of_videos_'

str_dir_path_raw_website_json_pubs_sets = str_path4_outputs_raw_from_web + 'rv_website_infoPublicationSets/'
str_dir_path_raw_web_json_pub_sets_temp_backup = str_dir_path_raw_website_json_pubs_sets + 'temporary_backup/'
str_filename_base_string4_raw_website_json_pubs_sets = 'raw_json_sets_of_pubs_'

str_dir_path_raw_airtable_json_guests = str_path4_outputs_raw_from_web + 'airtable_table_guests/'
str_filename_base_string4_raw_airtable_json_guests = 'air_table_Guests'

str_dir_path_raw_airtable_json_vids = str_path4_outputs_raw_from_web + 'airtable_table_videos/'
str_filename_base_string4_raw_airtable_json_vids = 'air_table_videos'

str_dir4_manipd_CSV_website_videos = str_path4_outputs_manipd_CSVs + 'website_vidsData/'
str_filename_base_string4_manipd_website_CSV_videos = 'websiteVidsData'

str_dir4_manipd_CSV_website_guests_subjects =\
    str_path4_outputs_manipd_CSVs + str_midpath4_outputs_website_guests_subjects
str_dir4_manipd_JSON_website_guests_subjects =\
    str_path4_outputs_manipd_JSON + str_midpath4_outputs_website_guests_subjects
str_filename_base_string4_manipd_website_guests_subjects = 'guestsSubjectsData'

str_dir4_manipd_JSON_website_interviewers = str_path4_outputs_manipd_JSON + str_midpath4_outputs_website_interviewers
str_filename_base_string4_manipd_website_interviewers = 'interviewers'

str_dir4_website_vids_ds = str_path4_outputs_raw_from_web + 'rv_website_videos_datastructure/'
str_dir4_website_pubs_ds = str_path4_outputs_raw_from_web + 'rv_website_pubs_datastructure/'

str_dir4_airt_vids_ds = str_path4_outputs_manipd_simpledsinstances + 'website_vids_as_airT_format/'

str_dir4_manipd_JSON_airT_guests_name_as_key = str_path4_outputs_manipd_JSON + 'airT_guests_w_name_as_key/'
str_filename_base_string4_manipd_airT_guests_name_as_key = 'airTguestsNameAsKey'

str_dir4_manipd_JSON_airT_vids_webid_as_key = str_path4_outputs_manipd_JSON + 'airT_videos_w_webid_as_key/'
str_filename_base_string4_manipd_airT_vids_webid_as_key = 'airTvideosWebidAsKey'

str_dir4_airt_ondisk_class = str_path4_outputs + 'class_AirT_onDisk_files/'
str_dir4_airt_ondisk_guests_by_name = str_dir4_airt_ondisk_class + 'guests_by_name/'
str_dir4_airt_ondisk_shows_by_name = str_dir4_airt_ondisk_class + 'shows_by_name/'
str_dir4_airt_ondisk_vids_by_webid = str_dir4_airt_ondisk_class + 'videos_by_websiteID/'

str_dir4_additional_vids_info_ds = \
    str_path4_outputs_manipd_simpledsinstances + 'website_vids_additional_info/'

str_dir4_vid_transcripts_ds = str_path4_outputs_manipd_simpledsinstances + 'website_vids_transcripts/'
str_dir4_pubs_fulltext_ds = str_path4_outputs_manipd_simpledsinstances + 'website_pubs_fulltext/'

str_dir4_algolia_ds_idx_vids_01 = str_path4_outputs_manipd_simpledsinstances + 'website_vids_as_algolia_for_idx_01/'
str_dir4_algolia_ds_idx_vids_02 = str_path4_outputs_manipd_simpledsinstances + 'website_vids_as_algolia_for_idx_02/'
str_dir4_algolia_ds_idx_pubs_01 = str_path4_outputs_manipd_simpledsinstances + 'website_pubs_as_algolia_for_idx_01/'

str_dir4_vid_transcripts_data = str_path4_outputs_manipd + 'Transcripts/'
str_dir4_pubs_fulltext_data = str_path4_outputs_manipd + 'Publication_texts/'
str_dir4_tfidf_data = str_path4_outputs_manipd + 'TF-IDF_Data/'
str_dir4_tfidf_pubs_data = str_path4_outputs_manipd + 'TF-IDF_Data_pubs/'

# dictionary representing the Algolia indexes and related variables that can
# be used in a loop, to update the respective SimpleDSs and push the records, for
# multiple indexes
str_algolia_record_type_vid = 'video'
str_algolia_record_type_pub = 'publication'
algvarname_type = 'type'
algvarname_records_dspath = 'Algolia SimpleDS path'
algvarname_records_dsname = 'Algolia SimpleDS name'
algvarname_text_dspath = 'Text SimpleDS path'
algvarname_text_datapath = 'Text data path'
dict_group_of_indexes_to_update_and_push_frequently = {
    str_algolia_vids_idx_01: {algvarname_type: str_algolia_record_type_vid,
                              algvarname_records_dspath: str_dir4_algolia_ds_idx_vids_01,
                              algvarname_records_dsname: str_name_simpleds_algolia_idx_vids_01,
                              algvarname_text_dspath: str_dir4_vid_transcripts_ds,
                              algvarname_text_datapath: str_dir4_vid_transcripts_data}
}
dict_group_of_indexes_to_update_and_push_less_frequently = {
    str_algolia_pubs_idx_01: {algvarname_type: str_algolia_record_type_pub,
                              algvarname_records_dspath: str_dir4_algolia_ds_idx_pubs_01,
                              algvarname_records_dsname: str_name_simpleds_algolia_idx_pubs_01,
                              algvarname_text_dspath: str_dir4_pubs_fulltext_ds,
                              algvarname_text_datapath: str_dir4_pubs_fulltext_data}
}
