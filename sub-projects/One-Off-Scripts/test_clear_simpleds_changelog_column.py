from class_simpleDS import SimpleDS

fullpath_to_simpleds = \
    '/home/mclovin/myProjects/realV/guestDataProject/outputFiles/rawJSONfromWebFiles/rv_website_videos_datastructure/'
web_vid_ds = SimpleDS(fullpath_to_simpleds)
web_vid_ds.load()
web_vid_ds._SimpleDS__wipe_changelog_column()
web_vid_ds.save2disk()
