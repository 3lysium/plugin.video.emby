#################################################################################################
# LibrarySync
#################################################################################################

import xbmc
import xbmcgui
import xbmcaddon
import xbmcvfs
import json
import sqlite3
import inspect
import threading
import urllib
from datetime import datetime, timedelta, time
import urllib2
import os

from API import API
import Utils as utils
from DownloadUtils import DownloadUtils
from ReadEmbyDB import ReadEmbyDB
from ReadKodiDB import ReadKodiDB
from WriteKodiDB import WriteKodiDB

addondir = xbmc.translatePath(xbmcaddon.Addon(id='plugin.video.emby').getAddonInfo('profile'))
dataPath = os.path.join(addondir,"library")
movieLibrary = os.path.join(dataPath,'movies')
tvLibrary = os.path.join(dataPath,'tvshows')

class LibrarySync():   
        
    def syncDatabase(self):
        
        #set some variable to check if this is the first run
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        WINDOW = xbmcgui.Window( 10000 )

        startupDone = WINDOW.getProperty("startup") == "done"
        syncInstallRunDone = addon.getSetting("SyncInstallRunDone") == "true"
        WINDOW.setProperty("SyncDatabaseRunning", "true")
        
        if(WINDOW.getProperty("SyncDatabaseShouldStop") ==  "true"):
            utils.logMsg("Sync Database", "Can not start SyncDatabaseShouldStop=True", 0)
            return True

        try:
            completed = True
            connection = utils.KodiSQL()
            cursor = connection.cursor()
            # sync movies
            if(syncInstallRunDone == False): # on first install run do a full sync with model progress dialog
                completed = completed and self.TvShowsSync(connection, cursor,True, True)
                completed = completed and self.MoviesSync(connection, cursor,True, True)
                completed = completed and self.MusicVideosSync(True, True,connection , cursor)
            elif(startupDone == False): # on first run after startup do a inc then a full sync
                self.TvShowsSync(connection, cursor,False, False)
                self.MoviesSync(connection, cursor,False, False)
                self.MusicVideosSync(False, False, connection,cursor)
                self.TvShowsSync(connection, cursor,True, False)
                self.MoviesSync(connection, cursor,True, False)
                self.MusicVideosSync(True, False,connection,cursor)
            else: # on scheduled sync do a full sync
                self.TvShowsSync(connection, cursor,True, False)
                self.MoviesSync(connection, cursor,True, False)
                self.MusicVideosSync(True, False,connection,cursor)
           
            # set the install done setting
            if(syncInstallRunDone == False and completed):
                addon = xbmcaddon.Addon(id='plugin.video.emby') #force a new instance of the addon
                addon.setSetting("SyncInstallRunDone", "true")        
            
            # set prop to show we have run for the first time
            WINDOW.setProperty("startup", "done")
            
        finally:
            WINDOW.setProperty("SyncDatabaseRunning", "false")
            utils.logMsg("Sync DB", "syncDatabase Exiting", 0)
            cursor.close()
            
        return True      
    
    def MoviesSync(self,connection, cursor, fullsync, installFirstRun,itemList = []):

        WINDOW = xbmcgui.Window( 10000 )
        pDialog = None
        startedSync = datetime.today()
        
        try:
            addon = xbmcaddon.Addon(id='plugin.video.emby')
            dbSyncIndication = addon.getSetting("dbSyncIndication")
                
            if(installFirstRun or dbSyncIndication == "Dialog Progress"):
                pDialog = xbmcgui.DialogProgress()
            elif(dbSyncIndication == "BG Progress"):
                pDialog = xbmcgui.DialogProgressBG()
            
            if(pDialog != None):
                pDialog.create('Sync DB', 'Sync DB')
                
            totalItemsAdded = 0
            totalItemsUpdated = 0
            totalItemsDeleted = 0
            
            allEmbyMovieIds = list()
                
            views = ReadEmbyDB().getCollections("movies")
            viewCount = len(views)
            viewCurrent = 1
            progressTitle = ""
            
            for view in views:
                
                #process new movies
                allMB3Movies = ReadEmbyDB().getMovies(id = view.get('id'), fullinfo=True, fullSync = fullsync, itemList = itemList)
                allKodiIds = set(ReadKodiDB().getKodiMoviesIds(True))
            
                if(self.ShouldStop(pDialog)):
                    return False            
            
                if(allMB3Movies == None):
                    return False
            
                if(pDialog != None):
                    progressTitle = "Sync DB : Processing " + view.get('title') + " " + str(viewCurrent) + " of " + str(viewCount)
                    pDialog.update(0, progressTitle)
                    total = len(allMB3Movies) + 1
                    count = 1
                
                for item in allMB3Movies:
                    
                    if not item.get('IsFolder'):                    
                        allEmbyMovieIds.append(item["Id"])
                        item['Tag'] = []
                        item['Tag'].append(view.get('title'))
                        
                        if item["Id"] not in allKodiIds:
                            WriteKodiDB().addMovieToKodiLibrary(item,connection, cursor)
                            totalItemsAdded += 1
                        
                        if(self.ShouldStop(pDialog)):
                            return False
                    
                        # update progress bar
                        if(pDialog != None):
                            percentage = int(((float(count) / float(total)) * 100))
                            pDialog.update(percentage, progressTitle, "Adding Movie: " + str(count))
                            count += 1
                
                if(self.ShouldStop(pDialog)):
                    return False

                if(pDialog != None):
                    progressTitle = "Sync DB : Processing " + view.get('title') + " " + str(viewCurrent) + " of " + str(viewCount)
                    pDialog.update(0, progressTitle, "")
                    total = len(allMB3Movies) + 1
                    count = 1                    

                #process updates
                allKodiMovies = ReadKodiDB().getKodiMovies(True)
                for item in allMB3Movies:
                    
                    if not item.get('IsFolder'):
                        item['Tag'] = []
                        item['Tag'].append(view.get('title'))
                        
                        if allKodiMovies != None:
                            kodimovie = allKodiMovies.get(item["Id"], None)
                        else:
                            kodimovie = None
                            
                        userData = API().getUserData(item)                       
                        
                        if(kodimovie != None):
                            updated = WriteKodiDB().updateMovieToKodiLibrary_Batched(item, kodimovie, connection, cursor)
                            if(updated):
                                totalItemsUpdated += 1
                        
                        if(self.ShouldStop(pDialog)):
                            return False
                    
                        # update progress bar
                        if(pDialog != None):
                            percentage = int(((float(count) / float(total)) * 100))
                            pDialog.update(percentage, progressTitle, "Updating Movie: " + str(count))
                            count += 1
                
                viewCurrent += 1
                
            # process box sets - TODO cope with movies removed from a set
            if fullsync:

                if(pDialog != None):
                    progressTitle = "Sync DB : BoxSets"
                    pDialog.update(0, progressTitle, "Retrieving Boxset List")
                            
                utils.logMsg("Sync Movies", "BoxSet Sync Started", 1)
                boxsets = ReadEmbyDB().getBoxSets()
                
                if(pDialog != None):
                    total = len(boxsets) + 1
                    count = 1                

                for boxset in boxsets:
                    if(pDialog != None):
                        percentage = int(((float(count) / float(total)) * 100))
                        pDialog.update(percentage, progressTitle, "Updating BoxSet: " + str(count) + " of " + str(total))
                        count += 1
                    if(self.ShouldStop(pDialog)):
                        return False                
                    boxsetMovies = ReadEmbyDB().getMoviesInBoxSet(boxset["Id"])
                    WriteKodiDB().addBoxsetToKodiLibrary(boxset,connection, cursor)
                    
                    for boxsetMovie in boxsetMovies:
                        if(self.ShouldStop(pDialog)):
                            return False
                        WriteKodiDB().updateBoxsetToKodiLibrary(boxsetMovie,boxset)
                        
                utils.logMsg("Sync Movies", "BoxSet Sync Finished", 1)                
                
            if(pDialog != None):
                progressTitle = "Removing Deleted Items"
                pDialog.update(0, progressTitle, "")
            
            if(self.ShouldStop(pDialog)):
                return False            
            
            # process any deletes only at fullsync
            if fullsync:
                allKodiIds = ReadKodiDB().getKodiMoviesIds(True)
                allEmbyMovieIds = set(allEmbyMovieIds)
                for kodiId in allKodiIds:
                    if not kodiId in allEmbyMovieIds:
                        WINDOW.setProperty(kodiId,"deleted")
                        WriteKodiDB().deleteMovieFromKodiLibrary(kodiId)
                        totalItemsDeleted += 1
            
            if(self.ShouldStop(pDialog)):
                return False            
            
            # display notification if set up
            notificationString = ""
            if(totalItemsAdded > 0):
                notificationString += "Added:" + str(totalItemsAdded) + " "
            if(totalItemsUpdated > 0):
                notificationString += "Updated:" + str(totalItemsUpdated) + " "
            if(totalItemsDeleted > 0):
                notificationString += "Deleted:" + str(totalItemsDeleted) + " "
                
            timeTaken = datetime.today() - startedSync
            timeTakenString = str(int(timeTaken.seconds / 60)) + ":" + str(timeTaken.seconds % 60)
            utils.logMsg("Sync Movies", "Finished " + timeTakenString + " " + notificationString, 0)
            
            if(dbSyncIndication == "Notify OnChange" and notificationString != ""):
                notificationString = "(" + timeTakenString + ") " + notificationString
                xbmc.executebuiltin("XBMC.Notification(Movie Sync: " + notificationString + ",)")
            elif(dbSyncIndication == "Notify OnFinish"):
                if(notificationString == ""):
                    notificationString = "Done"
                notificationString = "(" + timeTakenString + ") " + notificationString
                xbmc.executebuiltin("XBMC.Notification(Movie Sync: " + notificationString + ",)")

        finally:
            if(pDialog != None):
                pDialog.close()
        
        return True
        
    def TvShowsSync(self, connection, cursor ,fullsync, installFirstRun, itemList = []):

        addon = xbmcaddon.Addon(id='plugin.video.emby')
        WINDOW = xbmcgui.Window( 10000 )
        pDialog = None
        startedSync = datetime.today()
        
        try:
            dbSyncIndication = addon.getSetting("dbSyncIndication")
                
            if(installFirstRun or dbSyncIndication == "Dialog Progress"):
                pDialog = xbmcgui.DialogProgress()
            elif(dbSyncIndication == "BG Progress"):
                pDialog = xbmcgui.DialogProgressBG()
                
            if(pDialog != None):
                pDialog.create('Sync DB', 'Sync DB')
                
            totalItemsAdded = 0
            totalItemsUpdated = 0
            totalItemsDeleted = 0                
            allTVShows = list()
            allMB3EpisodeIds = list() #for use with deletions
            allKodiEpisodeIds = [] # for use with deletions            
            
            views = ReadEmbyDB().getCollections("tvshows")
            viewCount = len(views)
            viewCurrent = 1
            progressTitle = ""

            for view in views:
            
                progressTitle = "Sync DB : Processing " + view.get('title') + " " + str(viewCurrent) + " of " + str(viewCount)
                
                # incremental sync --> new episodes only
                if fullsync == False:
                    
                    latestMBEpisodes = ReadEmbyDB().getLatestEpisodes(fullinfo = True, itemList = itemList)
                    utils.logMsg("Sync TV", "Inc Sync Started on : " + str(len(latestMBEpisodes)) + " : " + str(itemList), 1)
                    
                    if latestMBEpisodes != None:
                        allKodiTvShowsIds = set(ReadKodiDB().getKodiTvShowsIds(True))
                        
                        # get included TV Shows
                        showList = []
                        for episode in latestMBEpisodes:
                            if(episode["SeriesId"] not in showList):
                                showList.append(episode["SeriesId"])
                        
                        utils.logMsg("Incremental TV Sync", "Included TV Show List : " + str(showList), 0)
                        
                        if(pDialog != None):
                            pDialog.update(0, progressTitle)
                            total = len(showList) + 1
                            count = 1                           
                        
                        # process included TV Shows
                        for showID in showList:
                        
                            embyTvShow = ReadEmbyDB().getFullItem(showID)
                        
                            if(showID not in allKodiTvShowsIds):
                                utils.logMsg("Incremental TV Sync", "Adding TV Show : " + embyTvShow.get("Name"), 1)
                                WriteKodiDB().addTVShowToKodiLibrary(embyTvShow, connection, cursor)
                            
                            kodiTvShow = ReadKodiDB().getKodiTVShow(showID)
                            utils.logMsg("Incremental TV Sync", "Updating  TV Show : " + embyTvShow.get("Name"), 1)
                            WriteKodiDB().updateTVShowToKodiLibrary(embyTvShow, kodiTvShow, connection, cursor)

                            # update progress bar
                            if(pDialog != None):
                                percentage = int(((float(count) / float(total)) * 100))
                                pDialog.update(percentage, progressTitle, "Processing TV Shows : " + str(count))
                                count += 1
                        
                        if(pDialog != None):
                            pDialog.update(0, progressTitle)
                            total = len(latestMBEpisodes) + 1
                            count = 1
                            
                        # process new episodes
                        for episode in latestMBEpisodes:                               
                            if(self.ShouldStop(pDialog)):
                                return False                

                            WriteKodiDB().addEpisodeToKodiLibrary(episode, connection, cursor)
                            progressAction = "Adding"
                            totalItemsAdded += 1                 
                            
                            # update progress bar
                            if(pDialog != None):
                                percentage = int(((float(count) / float(total)) * 100))
                                pDialog.update(percentage, progressTitle, progressAction + " Episode: " + str(count))
                                count += 1    
                        
                        #process updates
                        if(pDialog != None):
                            progressTitle = "Sync DB : Processing Episodes"
                            pDialog.update(0, progressTitle)
                            total = len(latestMBEpisodes) + 1
                            count = 1
                                    
                        for episode in latestMBEpisodes:
                            if(self.ShouldStop(pDialog)):
                                return False                           
                            allKodiTVShows = ReadKodiDB().getKodiTvShows(False)
                            kodishow = allKodiTVShows.get(episode["SeriesId"],None)
                            kodiEpisodes = ReadKodiDB().getKodiEpisodes(kodishow["tvshowid"],True,True)
                            
                            if(self.ShouldStop(pDialog)):
                                return False   

                            userData = API().getUserData(episode)

                            if kodiEpisodes != None:
                                KodiItem = kodiEpisodes.get(episode.get("Id"), None)
                                if(KodiItem != None): 
                                    WriteKodiDB().updateEpisodeToKodiLibrary(episode, KodiItem, connection, cursor)
                                        
                            if(self.ShouldStop(pDialog)):
                                return False                        

                            # update progress bar
                            if(pDialog != None):
                                percentage = int(((float(count) / float(total)) * 100))
                                pDialog.update(percentage, progressTitle, "Updating Episode: " + str(count))
                                count += 1    
                        
                
                # full sync --> Tv shows and Episodes
                if fullsync:
                    viewTVShows = list()
                    tvShowData = ReadEmbyDB().getTVShows(id = view.get('id') , fullinfo = True, fullSync = True)
                    allKodiIds = set(ReadKodiDB().getKodiTvShowsIds(True))
                    
                    if(self.ShouldStop(pDialog)):
                        return False            
                    
                    if (tvShowData == None):
                        return False
                        
                    if(pDialog != None):
                        progressTitle = "Sync DB : Processing TV Shows"
                        pDialog.update(0, progressTitle)
                        total = len(tvShowData) + 1
                        count = 1
                        
                    # add TV Shows
                    for item in tvShowData:
                        if item.get('IsFolder') and item.get('RecursiveItemCount') != 0:
                            allTVShows.append(item["Id"])
                            viewTVShows.append(item["Id"])
                            item['Tag'] = []
                            item['Tag'].append(view.get('title'))                            
                            progMessage = "Processing"
                            if item["Id"] not in allKodiIds:
                                WriteKodiDB().addTVShowToKodiLibrary(item,connection, cursor)
                                totalItemsAdded += 1
                                
                            if(self.ShouldStop(pDialog)):
                                return False
                                
                            # update progress bar
                            if(pDialog != None):
                                percentage = int(((float(count) / float(total)) * 100))
                                pDialog.update(percentage, progressTitle, "Adding Tv Show: " + str(count))
                                count += 1                        
                            
                    if(pDialog != None):
                        progressTitle = "Sync DB : Processing TV Shows"
                        pDialog.update(0, progressTitle, "")
                        total = len(viewTVShows) + 1
                        count = 1                    
                    
                    # update TV Shows
                    allKodiTVShows = ReadKodiDB().getKodiTvShows(True)
                    for item in tvShowData:
                        if item.get('IsFolder'):
                            item['Tag'] = []
                            item['Tag'].append(view.get('title'))
                            if allKodiTVShows != None:
                                kodishow = allKodiTVShows.get(item["Id"],None)
                            else:
                                kodishow = None
                            
                            if(kodishow != None):
                                updated = WriteKodiDB().updateTVShowToKodiLibrary(item,kodishow,connection, cursor)
                                if(updated):
                                    totalItemsUpdated += 1
                                
                            if(self.ShouldStop(pDialog)):
                                return False
                                
                            # update progress bar
                            if(pDialog != None):
                                percentage = int(((float(count) / float(total)) * 100))
                                pDialog.update(percentage, progressTitle, "Updating Tv Show: " + str(count))
                                count += 1                              

                                
                    # do episode adds
                    allEpisodes = list()
                    showTotal = len(viewTVShows)
                    showCurrent = 1                    
                    for tvshow in viewTVShows:
                        
                        episodeData = ReadEmbyDB().getEpisodes(tvshow,True)                       
                        if episodeData != None:
                            
                            if(self.ShouldStop(pDialog)):
                                return False                
                            
                            if(pDialog != None):
                                progressTitle = "Sync DB : Processing Tv Show " + str(showCurrent) + " of " + str(showTotal)
                                pDialog.update(0, progressTitle)
                                total = len(episodeData) + 1
                                count = 0         
    
                            for item in episodeData:
                            
                                if(self.ShouldStop(pDialog)):
                                    return False        
                                    
                                progressAction = "Adding"
                                WriteKodiDB().addEpisodeToKodiLibrary(item, connection, cursor)
                                
                                # update progress bar
                                if(pDialog != None):
                                    percentage = int(((float(count) / float(total)) * 100))
                                    pDialog.update(percentage, progressTitle, progressAction + " Episode: " + str(count))
                                    count += 1
                                    
                            showCurrent += 1
                                            
                    # do episode updates
                    showCurrent = 1
                    for tvshow in viewTVShows:
                        episodeData = ReadEmbyDB().getEpisodes(tvshow,True)
                        
                        kodiEpisodes = None
                        allKodiTVShows = ReadKodiDB().getKodiTvShows(False)
                        if allKodiTVShows != None:
                            kodishow = allKodiTVShows.get(tvshow,None)
                            if kodishow != None:
                                kodiEpisodes = ReadKodiDB().getKodiEpisodes(kodishow["tvshowid"],True,True)
                        
                        if(self.ShouldStop(pDialog)):
                            return False                
                        
                        if(pDialog != None):
                            progressTitle = "Sync DB : Processing Tv Show " + str(showCurrent) + " of " + str(showTotal)
                            pDialog.update(0, progressTitle)
                            total = len(episodeData) + 1
                            count = 0         
    
                        #we have to compare the lists somehow
                        for item in episodeData:
                            #add episodeId to the list of all episodes for use later on the deletes
                            allMB3EpisodeIds.append(item["Id"])
                            
                            matchFound = False
    
                            userData = API().getUserData(item)
                            
                            if kodiEpisodes != None:
                                KodiItem = kodiEpisodes.get(item.get("Id"), None)
                                if(KodiItem != None):
                                    updated = WriteKodiDB().updateEpisodeToKodiLibrary(item, KodiItem, connection, cursor)
                                    if(updated):
                                        totalItemsUpdated += 1                                
                            
                            if(self.ShouldStop(pDialog)):
                                return False                        
                                
                            # update progress bar
                            if(pDialog != None):
                                percentage = int(((float(count) / float(total)) * 100))
                                pDialog.update(percentage, progressTitle, "Updating Episode: " + str(count))
                                count += 1
                        
                        
                        #add all kodi episodes to a list with episodes for use later on to delete episodes
                        #the mediabrowser ID is set as uniqueID in the NFO... for some reason this has key 'unknown' in the json response
                        if kodishow != None:
                            show = ReadKodiDB().getKodiEpisodes(kodishow["tvshowid"],False,False)
                            if show != None:
                                for episode in show:
                                    dict = {'episodeid': str(episode["uniqueid"]["unknown"]),'tvshowid': tvshow}
                                    allKodiEpisodeIds.append(dict)
                        
                        showCurrent += 1                  
                    
            if(pDialog != None):
                progressTitle = "Removing Deleted Items"
                pDialog.update(0, progressTitle)
               
            if(self.ShouldStop(pDialog)):
                return False            
                
            # DELETES -- EPISODES
            # process any deletes only at fullsync
            allMB3EpisodeIdsSet = set(allMB3EpisodeIds)
            for episode in allKodiEpisodeIds:
                if episode.get('episodeid') not in allMB3EpisodeIdsSet:
                    WINDOW.setProperty("embyid" + str(episode.get('episodeid')),"deleted")
                    WriteKodiDB().deleteEpisodeFromKodiLibrary(episode.get('episodeid'),episode.get('tvshowid'))
                    totalItemsDeleted += 1
            
            # DELETES -- TV SHOWS
            if fullsync:
                allKodiShows = ReadKodiDB().getKodiTvShowsIds(True)
                allMB3TVShows = set(allTVShows)
                for show in allKodiShows:
                    if not show in allMB3TVShows:
                        WriteKodiDB().deleteTVShowFromKodiLibrary(show)
                        totalItemsDeleted += 1
            
            if(self.ShouldStop(pDialog)):
                return False            
    
            # display notification if set up
            notificationString = ""
            if(totalItemsAdded > 0):
                notificationString += "Added:" + str(totalItemsAdded) + " "
            if(totalItemsUpdated > 0):
                notificationString += "Updated:" + str(totalItemsUpdated) + " "
            if(totalItemsDeleted > 0):
                notificationString += "Deleted:" + str(totalItemsDeleted) + " "
                
            timeTaken = datetime.today() - startedSync
            timeTakenString = str(int(timeTaken.seconds / 60)) + ":" + str(timeTaken.seconds % 60)
            utils.logMsg("Sync Episodes", "Finished " + timeTakenString + " " + notificationString, 0)
            
            if(dbSyncIndication == "Notify OnChange" and notificationString != ""):
                notificationString = "(" + timeTakenString + ") " + notificationString
                xbmc.executebuiltin("XBMC.Notification(Episode Sync: " + notificationString + ",)")
            elif(dbSyncIndication == "Notify OnFinish"):
                if(notificationString == ""):
                    notificationString = "Done"
                notificationString = "(" + timeTakenString + ") " + notificationString
                xbmc.executebuiltin("XBMC.Notification(Episode Sync: " + notificationString + ",)")

        finally:
            if(pDialog != None):
                pDialog.close()
        
        return True
    
    def MusicVideosSync(self, fullsync, installFirstRun,connection, cursor):
        
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        WINDOW = xbmcgui.Window( 10000 )
        pDialog = None
        
        try:
            dbSyncIndication = addon.getSetting("dbSyncIndication")
                
            if(installFirstRun or dbSyncIndication == "Dialog Progress"):
                pDialog = xbmcgui.DialogProgress()
            elif(dbSyncIndication == "BG Progress"):
                pDialog = xbmcgui.DialogProgressBG()
            
            if(pDialog != None):
                pDialog.create('Sync DB', 'Sync DB')
                
            allEmbyMusicVideoIds = list()

            progressTitle = ""
            
            #process new musicvideos
            allMB3MusicVideos = ReadEmbyDB().getMusicVideos(True, fullsync)
            allKodiIds = set(ReadKodiDB().getKodiMusicVideoIds(True))
        
            if(self.ShouldStop(pDialog)):
                return False            
        
            if(allMB3MusicVideos == None):
                return False
        
            if(pDialog != None):
                progressTitle = "Sync DB : Processing Musicvideos"
                pDialog.update(0, progressTitle)
                total = len(allMB3MusicVideos) + 1
                count = 1
            
            for item in allMB3MusicVideos:
                
                if not item.get('IsFolder'):
                    allEmbyMusicVideoIds.append(item["Id"])
                    
                    if item["Id"] not in allKodiIds:
                        WriteKodiDB().addMusicVideoToKodiLibrary(item, connection, cursor)
                    
                    if(self.ShouldStop(pDialog)):
                        return False
                
                    # update progress bar
                    if(pDialog != None):
                        percentage = int(((float(count) / float(total)) * 100))
                        pDialog.update(percentage, progressTitle, "Adding Musicvideo: " + str(count))
                        count += 1
            
            if(self.ShouldStop(pDialog)):
                return False

            if(pDialog != None):
                progressTitle = "Sync DB : Processing musicvideos"
                pDialog.update(0, progressTitle, "")
                total = len(allMB3MusicVideos) + 1
                count = 1                    
            
            #process updates
            allKodiMusicVideos = ReadKodiDB().getKodiMusicVideos(True)
            for item in allMB3MusicVideos:
                
                if not item.get('IsFolder'):
                    
                    if allKodiMusicVideos != None:
                        kodimusicvideo = allKodiMusicVideos.get(item["Id"], None)
                    else:
                        kodimusicvideo = None
                    
                    if(kodimusicvideo != None):
                        WriteKodiDB().updateMusicVideoToKodiLibrary_Batched(item, kodimusicvideo)
                    
                    if(self.ShouldStop(pDialog)):
                        return False
                
                    # update progress bar
                    if(pDialog != None):
                        percentage = int(((float(count) / float(total)) * 100))
                        pDialog.update(percentage, progressTitle, "Updating MusicVideo: " + str(count))
                        count += 1

                
            if(pDialog != None):
                progressTitle = "Removing Deleted Items"
                pDialog.update(0, progressTitle, "")
            
            if(self.ShouldStop(pDialog)):
                return False            
            
            # process any deletes only at fullsync
            if fullsync:
                allKodiIds = ReadKodiDB().getKodiMusicVideoIds(True)
                allEmbyMusicVideoIds = set(allEmbyMusicVideoIds)
                for kodiId in allKodiIds:
                    if not kodiId in allEmbyMusicVideoIds:
                        WriteKodiDB().deleteMusicVideoFromKodiLibrary(kodiId)
            
            if(self.ShouldStop(pDialog)):
                return False            
            
        finally:
            if(pDialog != None):
                pDialog.close()
        
        return True  

    def updatePlayCounts(self):
        #update all playcounts from MB3 to Kodi library
        
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        WINDOW = xbmcgui.Window( 10000 )
        pDialog = None
        startedSync = datetime.today()
        processMovies = True
        processTvShows = True
        
        if(WINDOW.getProperty("SyncDatabaseShouldStop") ==  "true"):
            utils.logMsg("Sync PlayCount", "Can not start SyncDatabaseShouldStop=True", 0)
            return True        
        
        if(WINDOW.getProperty("updatePlayCounts_Running") == "true"):
            utils.logMsg("Sync PlayCount", "updatePlayCounts Already Running", 0)
            return False
            
        WINDOW.setProperty("updatePlayCounts_Running", "true")
            
        try:
            playCountSyncIndication = addon.getSetting("playCountSyncIndication")
            playCountSyncFirstRun = addon.getSetting("SyncFirstCountsRunDone")
                
            if(playCountSyncFirstRun != "true" or playCountSyncIndication == "Dialog Progress"):
                pDialog = xbmcgui.DialogProgress()
            elif(playCountSyncIndication == "BG Progress"):
                pDialog = xbmcgui.DialogProgressBG()
                
            if(pDialog != None):
                pDialog.create('Sync PlayCounts', 'Sync PlayCounts')        
        
            totalCountsUpdated = 0
            totalPositionsUpdated = 0
            
            #process movies
            if processMovies:
                if(pDialog != None):
                    pDialog.update(0, "Processing Movies", "")
                    
                views = ReadEmbyDB().getCollections("movies")
                viewCount = len(views)
                viewCurrent = 1
                for view in views:
                    allMB3Movies = ReadEmbyDB().getMovies(view.get('id'), fullinfo = False, fullSync = True)
                    allKodiMovies = ReadKodiDB().getKodiMovies(False)
                    
                    if(self.ShouldStop(pDialog)):
                        return False
                            
                    if(allMB3Movies != None and allKodiMovies != None):
                        
                        if(pDialog != None):
                            progressTitle = "Sync PlayCounts: Processing " + view.get('title') + " " + str(viewCurrent) + " of " + str(viewCount)
                            pDialog.update(0, progressTitle)
                            totalCount = len(allMB3Movies) + 1
                            count = 1
                    
                        for item in allMB3Movies:
                            
                            if not item.get('IsFolder'):                           
                                kodiItem = allKodiMovies.get(item["Id"], None)
                                
                                userData = API().getUserData(item)
                                timeInfo = API().getTimeInfo(item)
                                
                                if kodiItem != None:
                                    kodiresume = int(round(kodiItem['resume'].get("position")))
                                    resume = int(round(float(timeInfo.get("ResumeTime"))))*60
                                    total = int(round(float(timeInfo.get("TotalTime"))))*60
                                    if kodiresume != resume:
                                        WriteKodiDB().setKodiResumePoint(kodiItem['movieid'],resume,total,"movie")
                                        totalPositionsUpdated += 1
                                    updated = WriteKodiDB().updateProperty(kodiItem,"playcount",int(userData.get("PlayCount")), "movie")
                                    updated |= WriteKodiDB().updateProperty(kodiItem,"lastplayed",userData.get("LastPlayedDate"), "movie")
                                    if(updated):
                                        totalCountsUpdated += 1
                                        
                                if(self.ShouldStop(pDialog)):
                                    return False
                                
                                # update progress bar
                                if(pDialog != None):
                                    percentage = int(((float(count) / float(totalCount)) * 100))
                                    pDialog.update(percentage, progressTitle, "Updating Movie: " + str(count))
                                    count += 1   
                                
                    viewCurrent += 1
                    
            #process Tv shows
            if processTvShows:
                if(pDialog != None):
                    pDialog.update(0, "Processing TV Episodes", "")
                views = ReadEmbyDB().getCollections("tvshows")
                viewCount = len(views)
                viewCurrent = 1
                progressTitle = ""
                for view in views:            
            
                    tvshowData = ReadEmbyDB().getTVShows(id = view.get('id'), fullinfo = False, fullSync = True)
                    
                    if(self.ShouldStop(pDialog)):
                        return False
                                
                    if (tvshowData != None):
                        
                        showTotal = len(tvshowData)
                        showCurrent = 1                    
                        
                        for item in tvshowData:
                            
                            episodeData = ReadEmbyDB().getEpisodes(item["Id"], False)
                            allKodiTVShows = ReadKodiDB().getKodiTvShows(False)
                            kodishow = allKodiTVShows.get(item["Id"],None)
                            if kodishow != None:
                                kodiEpisodes = ReadKodiDB().getKodiEpisodes(kodishow["tvshowid"],False,True)
                            else:
                                kodiEpisodes = None
                            
                            if (episodeData != None):
                                if(pDialog != None):
                                    progressTitle = "Sync PlayCounts: Processing TV Show " + str(showCurrent) + " of " + str(showTotal)
                                    pDialog.update(0, progressTitle)
                                    totalCount = len(episodeData) + 1
                                    count = 1                  
                            
                                for episode in episodeData:
    
                                    kodiItem = None
                                    matchFound = False
                                    if kodiEpisodes != None:
                                        kodiItem = kodiEpisodes.get(episode.get("Id"), None)
    
                                    userData=API().getUserData(episode)
                                    timeInfo = API().getTimeInfo(episode)
                                    
                                    
                                    if kodiItem != None:
                                        WINDOW = xbmcgui.Window( 10000 )
                                        WINDOW.setProperty("episodeid" + str(kodiItem['episodeid']), episode.get('Name') + ";;" + episode.get('Id'))
                                        WINDOW.setProperty(episode.get('Id'), "episode;;" + str(kodishow["tvshowid"]) + ";;" +str(kodiItem['episodeid']))
                                        kodiresume = int(round(kodiItem['resume'].get("position")))
                                        resume = int(round(float(timeInfo.get("ResumeTime"))))*60
                                        total = int(round(float(timeInfo.get("TotalTime"))))*60
                                        if kodiresume != resume:
                                            WriteKodiDB().setKodiResumePoint(kodiItem['episodeid'],resume,total,"episode")
                                            totalPositionsUpdated += 1
                                        
                                        updated = WriteKodiDB().updateProperty(kodiItem,"playcount",int(userData.get("PlayCount")),"episode")
                                        updated |= WriteKodiDB().updateProperty(kodiItem,"lastplayed",userData.get("LastPlayedDate"), "episode")
                                        if(updated):
                                            totalCountsUpdated += 1 
                                            
                                    if(self.ShouldStop(pDialog)):
                                        return False
                                    
                                    # update progress bar
                                    if(pDialog != None):
                                        percentage = int(((float(count) / float(totalCount)) * 100))
                                        pDialog.update(percentage, progressTitle, "Updating Episode: " + str(count))
                                        count += 1
                                        
                                showCurrent += 1
             
            if(playCountSyncFirstRun != "true"):
                addon = xbmcaddon.Addon(id='plugin.video.emby')                  
                addon.setSetting("SyncFirstCountsRunDone", "true")
                
            # display notification if set up
            notificationString = ""
            if(totalPositionsUpdated > 0):
                notificationString += "Pos:" + str(totalPositionsUpdated) + " "
            if(totalCountsUpdated > 0):
                notificationString += "Counts:" + str(totalCountsUpdated) + " "
                
            timeTaken = datetime.today() - startedSync
            timeTakenString = str(int(timeTaken.seconds / 60)) + ":" + str(timeTaken.seconds % 60)
            utils.logMsg("Sync PlayCount", "Finished " + timeTakenString + " " + notificationString, 0)
            
            if(playCountSyncIndication == "Notify OnChange" and notificationString != ""):
                notificationString = "(" + timeTakenString + ") " + notificationString
                xbmc.executebuiltin("XBMC.Notification(PlayCount Sync: " + notificationString + ",)")
            elif(playCountSyncIndication == "Notify OnFinish"):
                if(notificationString == ""):
                    notificationString = "Done"
                notificationString = "(" + timeTakenString + ") " + notificationString
                xbmc.executebuiltin("XBMC.Notification(PlayCount Sync: " + notificationString + ",)")

        finally:
            WINDOW.setProperty("updatePlayCounts_Running", "false")
            if(pDialog != None):
                pDialog.close()            
        
        return True
    
    def updatePlayCount(self, itemID):
        #update playcount of the itemID from MB3 to Kodi library
        
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        WINDOW = xbmcgui.Window( 10000 )
        
        embyItem = ReadEmbyDB().getItem(itemID)
        if(embyItem == None):
            return False
        
        type = embyItem.get("Type")
        
        #process movie
        if type == 'Movie':
            kodiItem = ReadKodiDB().getKodiMovie(itemID)     

            if(kodiItem == None):
                return False
                
            if(self.ShouldStop(None)):
                return False
 
            userData = API().getUserData(embyItem)
            timeInfo = API().getTimeInfo(embyItem)
                
            kodiresume = int(round(kodiItem['resume'].get("position")))
            resume = int(round(float(timeInfo.get("ResumeTime"))))*60
            total = int(round(float(timeInfo.get("TotalTime"))))*60
            if kodiresume != resume:
                WriteKodiDB().setKodiResumePoint(kodiItem['movieid'],resume,total,"movie")
            #write property forced will refresh the item in the list so playcount change is immediately visible
            WriteKodiDB().updateProperty(kodiItem,"playcount",int(userData.get("PlayCount")),"movie",True)
            WriteKodiDB().updateProperty(kodiItem,"lastplayed",userData.get("LastPlayedDate"), "movie")
                
            if(self.ShouldStop(None)):
                return False 
                    
        #process episode
        elif type == 'Episode':
            if(self.ShouldStop(None)):
                return False                   
                    
            kodiItem = ReadKodiDB().getKodiEpisodeByMbItem(embyItem["Id"], embyItem["SeriesId"])

            userData = API().getUserData(embyItem)
            timeInfo = API().getTimeInfo(embyItem)
            
            if kodiItem != None:
                kodiresume = int(round(kodiItem['resume'].get("position")))
                resume = int(round(float(timeInfo.get("ResumeTime"))))*60
                total = int(round(float(timeInfo.get("TotalTime"))))*60
                if kodiresume != resume:
                    WriteKodiDB().setKodiResumePoint(kodiItem['episodeid'],resume,total,"episode")
                #write property forced will refresh the item in the list so playcount change is immediately visible
                WriteKodiDB().updateProperty(kodiItem,"playcount",int(userData.get("PlayCount")),"episode",True)
                WriteKodiDB().updateProperty(kodiItem,"lastplayed",userData.get("LastPlayedDate"), "episode")       
        
        return True
    
    def ShouldStop(self, prog):
        
        if(prog != None and type(prog) == xbmcgui.DialogProgress):
            if(prog.iscanceled() == True):
                return True
    
        if(xbmc.Player().isPlaying() or xbmc.abortRequested):
            return True

        WINDOW = xbmcgui.Window( 10000 )
        if(WINDOW.getProperty("SyncDatabaseShouldStop") == "true"):
            return True

        return False

        
        
        