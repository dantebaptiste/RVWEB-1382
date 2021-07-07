import my_config
import my_globals_distinct_per_environment
from algoliasearch.search_client import SearchClient

# setup the Algolia API
algolia_client = SearchClient.create(my_config.algolia_app_id, my_config.algolia_admin_api_key)

# --------- SETTINGS FOR VIDEOS INDEXES ---------
# Set the settings for the main videos index which can also be replicated to all replicas
index_vids_main = my_globals_distinct_per_environment.idx_vids_main
algolia_index = algolia_client.init_index(index_vids_main)
dict_settings = {'searchableAttributes': ['Pseudo-transcript',
                                          'Description',
                                          'Title',
                                          'Featuring',
                                          'Interviewer',
                                          'Human Tags'],
                 'typoTolerance': False,
                 'minProximity': 2,
                 'attributesForFaceting': ['searchable(Featuring)',
                                           'searchable(Interviewer)',
                                           'searchable(Show)',
                                           'searchable(Published On)',
                                           'Membership Tier'],
                 'attributesToRetrieve': ['objectID',
                                          'Thumbnail',
                                          'Title',
                                          'Description',
                                          'Featuring',
                                          'Interviewer',
                                          'Membership Tier',
                                          'Show',
                                          'URL',
                                          'Duration',
                                          'Published On'],
                 'unretrievableAttributes': ['Type',
                                             'Human Tags',
                                             'Likes',
                                             'Dislikes',
                                             'Product',
                                             'Published On 4Humans',
                                             'Number of Comments',
                                             'Pseudo-transcript']
                 }
return_val = algolia_index.set_settings(dict_settings, {'forwardToReplicas': True})
# now, for the same main index as above, set the ranking, but this time without
# forwarding the settings to replicas.
dict_settings = {'ranking': ['typo',
                             'geo',
                             'words',
                             'filters',
                             'proximity',
                             'attribute',
                             'exact'],
                 'customRanking': ['desc(Published On)']
                 }
return_val = algolia_index.set_settings(dict_settings)

# now set the ranking for the videos index that is sorted by date descending
index_vids_sorted_date_descending = my_globals_distinct_per_environment.idx_vids_sort_date_desc
algolia_index = algolia_client.init_index(index_vids_sorted_date_descending)
dict_settings = {'ranking': ['desc(Published On)',
                             'typo',
                             'geo',
                             'words',
                             'filters',
                             'proximity',
                             'attribute',
                             'exact']
                 }
return_val = algolia_index.set_settings(dict_settings)

# now set the ranking for the videos index that is sorted by date ascending
index_vids_sorted_date_ascending = my_globals_distinct_per_environment.idx_vids_sort_date_asc
algolia_index = algolia_client.init_index(index_vids_sorted_date_ascending)
dict_settings = {'ranking': ['asc(Published On)',
                             'typo',
                             'geo',
                             'words',
                             'filters',
                             'proximity',
                             'attribute',
                             'exact']
                 }
return_val = algolia_index.set_settings(dict_settings)

# now set the ranking for the videos index that is sorted by likes descending
index_vids_sorted_likes_descending = my_globals_distinct_per_environment.idx_vids_sort_likes_desc
algolia_index = algolia_client.init_index(index_vids_sorted_likes_descending)
dict_settings = {'ranking': ['desc(Likes)',
                             'typo',
                             'geo',
                             'words',
                             'filters',
                             'proximity',
                             'attribute',
                             'exact']
                 }
return_val = algolia_index.set_settings(dict_settings)

# now set the ranking for the videos index that is used in the qso
index_vids_for_qso = my_globals_distinct_per_environment.idx_vids_qso
algolia_index = algolia_client.init_index(index_vids_for_qso)
dict_settings = {'ranking': ['desc(Published On)',
                             'typo',
                             'geo',
                             'words',
                             'filters',
                             'proximity',
                             'attribute',
                             'exact']
                 }
return_val = algolia_index.set_settings(dict_settings)
