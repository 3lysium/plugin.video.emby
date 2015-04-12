#################################################################################################
# ReadEmbyDB
#################################################################################################

import xbmc
import xbmcgui
import xbmcaddon
import json

from DownloadUtils import DownloadUtils

addon = xbmcaddon.Addon(id='plugin.video.emby')

class ReadEmbyDB():   
    def getMovies(self, id, fullinfo = False, fullSync = True, itemList = []):
        result = None
        
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        port = addon.getSetting('port')
        host = addon.getSetting('ipaddress')
        server = host + ":" + port
        
        downloadUtils = DownloadUtils()
        userid = downloadUtils.getUserId()

        if fullSync:
            sortstring = "&SortBy=SortName"
        else:
            if(len(itemList) > 0): # if we want a certain list specify it
                #sortstring = "&Ids=" + ",".join(itemList)
                sortstring = "" # work around for now until ParetnId and Id work together
            else: # just get the last 20 created items
                sortstring = "&Limit=20&SortBy=DateCreated"
            
        if fullinfo:
            url = server + '/mediabrowser/Users/' + userid + '/items?ParentId=' + id + sortstring + '&Fields=Path,Genres,SortName,Studios,Writer,ProductionYear,Taglines,CommunityRating,OfficialRating,CumulativeRunTimeTicks,Metascore,AirTime,DateCreated,MediaStreams,People,Overview&Recursive=true&SortOrder=Descending&IncludeItemTypes=Movie&CollapseBoxSetItems=false&format=json&ImageTypeLimit=1'
        else:
            url = server + '/mediabrowser/Users/' + userid + '/items?ParentId=' + id + sortstring + '&Fields=CumulativeRunTimeTicks&Recursive=true&SortOrder=Descending&IncludeItemTypes=Movie&CollapseBoxSetItems=false&format=json&ImageTypeLimit=1'

        jsonData = downloadUtils.downloadUrl(url, suppress=False, popup=0)
        if jsonData != None and jsonData != "":
            result = json.loads(jsonData)
            if(result.has_key('Items')):
                result = result['Items']

        # work around for now until ParetnId and Id work together
        if(result != None and len(result) > 0 and len(itemList) > 0):
            newResult = []
            for item in result:
                if(item.get("Id") in itemList):
                    newResult.append(item)
            result = newResult
            
        return result

    def getMusicVideos(self, fullinfo = False, fullSync = True):
        result = None
        
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        port = addon.getSetting('port')
        host = addon.getSetting('ipaddress')
        server = host + ":" + port
        
        downloadUtils = DownloadUtils()
        userid = downloadUtils.getUserId()

        if not fullSync:
            sortstring = "&Limit=20&SortBy=DateCreated"
        else:
            sortstring = "&SortBy=SortName"
        
        if fullinfo:
            url = server + '/mediabrowser/Users/' + userid + '/items?' + sortstring + '&Fields=Path,Genres,SortName,Studios,Writer,ProductionYear,Taglines,CommunityRating,OfficialRating,CumulativeRunTimeTicks,Metascore,AirTime,DateCreated,MediaStreams,People,Overview&Recursive=true&SortOrder=Descending&IncludeItemTypes=MusicVideo&format=json&ImageTypeLimit=1'
        else:
            url = server + '/mediabrowser/Users/' + userid + '/items?' + sortstring + '&Fields=CumulativeRunTimeTicks&Recursive=true&SortOrder=Descending&IncludeItemTypes=MusicVideo&CollapseBoxSetItems=false&format=json&ImageTypeLimit=1'
        
        jsonData = downloadUtils.downloadUrl(url, suppress=False, popup=0)
        if jsonData != None and jsonData != "":
            result = json.loads(jsonData)
            if(result.has_key('Items')):
                result = result['Items']

        return result
        
    def getItem(self, id):
        result = None
        
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        port = addon.getSetting('port')
        host = addon.getSetting('ipaddress')
        server = host + ":" + port
        
        downloadUtils = DownloadUtils()
        userid = downloadUtils.getUserId()

        jsonData = downloadUtils.downloadUrl("http://" + server + "/mediabrowser/Users/" + userid + "/Items/" + id + "?format=json&ImageTypeLimit=1", suppress=False, popup=1 )     
        if jsonData != None and jsonData != "":
            result = json.loads(jsonData)

        return result
    
    def getFullItem(self, id):
        result = None
        
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        port = addon.getSetting('port')
        host = addon.getSetting('ipaddress')
        server = host + ":" + port
        
        downloadUtils = DownloadUtils()
        userid = downloadUtils.getUserId()

        jsonData = downloadUtils.downloadUrl("http://" + server + "/mediabrowser/Users/" + userid + "/Items/" + id + "?format=json", suppress=False, popup=1 )     
        if jsonData != None and jsonData != "":
            result = json.loads(jsonData)

        return result
    
    def getTVShows(self, id, fullinfo = False, fullSync = False):
        result = None
        
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        port = addon.getSetting('port')
        host = addon.getSetting('ipaddress')
        server = host + ":" + port
        
        downloadUtils = DownloadUtils()
        userid = downloadUtils.getUserId()

        if not fullSync:
            sortstring = "&Limit=20&SortBy=DateCreated"
        else:
            sortstring = "&SortBy=SortName"
        
        
        if fullinfo:
            url = server + '/mediabrowser/Users/' + userid + '/Items?ParentId=' + id  + sortstring + '&Fields=Path,Genres,SortName,Studios,Writer,ProductionYear,Taglines,CommunityRating,OfficialRating,CumulativeRunTimeTicks,Metascore,AirTime,DateCreated,MediaStreams,People,Overview&Recursive=true&SortOrder=Descending&IncludeItemTypes=Series&format=json&ImageTypeLimit=1'
        else:
            url = server + '/mediabrowser/Users/' + userid + '/Items?ParentId=' + id  + sortstring + '&Fields=CumulativeRunTimeTicks&Recursive=true&SortOrder=Descending&IncludeItemTypes=Series&format=json&ImageTypeLimit=1'
        
        jsonData = downloadUtils.downloadUrl(url, suppress=False, popup=0)
        if jsonData != None and jsonData != "":
            result = json.loads(jsonData)
            if(result.has_key('Items')):
                result = result['Items']

        return result
    
    def getTVShowSeasons(self, tvShowId):
        result = None
        
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        port = addon.getSetting('port')
        host = addon.getSetting('ipaddress')
        server = host + ":" + port
        
        downloadUtils = DownloadUtils()
        userid = downloadUtils.getUserId()

        url = 'http://' + server + '/Shows/' + tvShowId + '/Seasons?UserId=' + userid + '&format=json&ImageTypeLimit=1'
        
        jsonData = downloadUtils.downloadUrl(url, suppress=False, popup=0)
        if jsonData != None and jsonData != "":
            result = json.loads(jsonData)
            if(result.has_key('Items')):
                result = result['Items']
                result = self.changeSeasonSpecialToSeason100(result)

        return result

    def changeSeasonSpecialToSeason100(self, result):
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        if(addon.getSetting("useSeason100ForSpecials") != "true"):
            return result
            
        for item in result:
            if(item != None and item.get("IndexNumber") != None and item.get("IndexNumber") == 0):
                item["IndexNumber"] = 100
        return result
    
    def changeEpisodeSpecialToSeason100(self, result):
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        if(addon.getSetting("useSeason100ForSpecials") != "true"):
            return result
            
        for item in result:
            if(item != None and item.get("ParentIndexNumber") != None and item.get("ParentIndexNumber") == 0):
                item["ParentIndexNumber"] = 100
        return result
    
    def getEpisodes(self, showId, fullinfo = False):
        result = None
        
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        port = addon.getSetting('port')
        host = addon.getSetting('ipaddress')
        server = host + ":" + port
        
        downloadUtils = DownloadUtils()
        userid = downloadUtils.getUserId()   
        
        if fullinfo:
            url = server + '/mediabrowser/Users/' + userid + '/Items?ParentId=' + showId + '&IsVirtualUnaired=false&IsMissing=False&SortBy=SortName&Fields=Path,Genres,SortName,Studios,Writer,ProductionYear,Taglines,CommunityRating,OfficialRating,CumulativeRunTimeTicks,Metascore,AirTime,DateCreated,MediaStreams,People,Overview&Recursive=true&SortOrder=Ascending&IncludeItemTypes=Episode&format=json&ImageTypeLimit=1'
        else:
            url = server + '/mediabrowser/Users/' + userid + '/Items?ParentId=' + showId + '&IsVirtualUnaired=false&IsMissing=False&SortBy=SortName&Fields=Name,SortName,CumulativeRunTimeTicks&Recursive=true&SortOrder=Ascending&IncludeItemTypes=Episode&format=json&ImageTypeLimit=1'
        
        jsonData = downloadUtils.downloadUrl(url, suppress=False, popup=0)
        
        if jsonData != None and jsonData != "":
            result = json.loads(jsonData)
            if(result.has_key('Items')):
                result = result['Items']
                result = self.changeEpisodeSpecialToSeason100(result)
                
        return result
    
    def getLatestEpisodes(self, fullinfo = False, itemList = []):
        result = None
        
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        port = addon.getSetting('port')
        host = addon.getSetting('ipaddress')
        server = host + ":" + port
        
        downloadUtils = DownloadUtils()
        userid = downloadUtils.getUserId()   
        
        limitString = "Limit=20&SortBy=DateCreated&"
        if(len(itemList) > 0): # if we want a certain list specify it
            limitString = "Ids=" + ",".join(itemList) + "&"
        
        if fullinfo:
            url = server + '/mediabrowser/Users/' + userid + '/Items?' + limitString + 'IsVirtualUnaired=false&IsMissing=False&Fields=ParentId,Path,Genres,SortName,Studios,Writer,ProductionYear,Taglines,CommunityRating,OfficialRating,CumulativeRunTimeTicks,Metascore,AirTime,DateCreated,MediaStreams,People,Overview&Recursive=true&SortOrder=Descending&IncludeItemTypes=Episode&format=json&ImageTypeLimit=1'
        else:
            url = server + '/mediabrowser/Users/' + userid + '/Items?' + limitString + 'IsVirtualUnaired=false&IsMissing=False&Fields=ParentId,Name,SortName,CumulativeRunTimeTicks&Recursive=true&SortOrder=Descending&IncludeItemTypes=Episode&format=json&ImageTypeLimit=1'
        
        jsonData = downloadUtils.downloadUrl(url, suppress=False, popup=0)
        
        if jsonData != None and jsonData != "":
            result = json.loads(jsonData)
            if(result.has_key('Items')):
                result = result['Items']
                result = self.changeEpisodeSpecialToSeason100(result)
                
        return result
    
    def getCollections(self, type):
        #Build a list of the user views
        userid = DownloadUtils().getUserId()  
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        port = addon.getSetting('port')
        host = addon.getSetting('ipaddress')
        server = host + ":" + port
        downloadUtils = DownloadUtils()
        
        try:
            jsonData = downloadUtils.downloadUrl("http://" + server + "/mediabrowser/Users/" + userid + "/Items/Root?format=json")
        except Exception, msg:
            error = "Get connect : " + str(msg)
            xbmc.log (error)
            return []
        
        if(jsonData == ""):
            return []
            
        result = json.loads(jsonData)
        
        parentid = result.get("Id")
        
        htmlpath = ("http://%s/mediabrowser/Users/" % server)
        jsonData = downloadUtils.downloadUrl(htmlpath + userid + "/items?ParentId=" + parentid + "&Sortby=SortName&format=json")
        collections=[]
        
        if(jsonData == ""):
            return []
        
        result = json.loads(jsonData)
        result = result.get("Items")
        
        for item in result:
            if(item.get("RecursiveItemCount") != 0):
                Temp = item.get("Name")
                Name = Temp.encode('utf-8')
                section = item.get("CollectionType")
                itemtype = item.get("CollectionType")
                if itemtype == None or itemtype == "":
                    itemtype = "movies" # User may not have declared the type
                if itemtype == type and item.get("Name") != "Collections":
                    collections.append( {'title'      : item.get("Name"),
                            'type'           : itemtype,
                            'id'             : item.get("Id")})
        return collections
    
    def getViewCollections(self, type):
        #Build a list of the user views
        userid = DownloadUtils().getUserId()  
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        port = addon.getSetting('port')
        host = addon.getSetting('ipaddress')
        server = host + ":" + port
        
        viewsUrl = server + "/mediabrowser/Users/" + userid + "/Views?format=json&ImageTypeLimit=1"
        jsonData = DownloadUtils().downloadUrl(viewsUrl, suppress=False, popup=0 )
        collections=[]
        
        if(jsonData != ""):
            views = json.loads(jsonData)
            views = views.get("Items")

            for view in views:
                if view.get("Type") == 'UserView': # Need to grab the real main node
                    newViewsUrl = server + '/mediabrowser/Users/' + userid + '/items?ParentId=' + view.get("Id") + '&SortBy=SortName&SortOrder=Ascending&format=json&ImageTypeLimit=1'
                    jsonData = DownloadUtils().downloadUrl(newViewsUrl, suppress=False, popup=0 )
                    if(jsonData != ""):
                        newViews = json.loads(jsonData)
                        newViews = newViews.get("Items")
                        for newView in newViews:
                            # There are multiple nodes in here like 'Latest', 'NextUp' - below we grab the full node.
                            if newView.get("CollectionType") == "MovieMovies" or newView.get("CollectionType") == "TvShowSeries":
                                view=newView
                if(view.get("ChildCount") != 0):
                    Name =(view.get("Name")) 
                    
                total = str(view.get("ChildCount"))
                type = view.get("CollectionType")
                if type == None:
                    type = "None" # User may not have declared the type
                if type == type:
                    collections.append( {'title'      : Name,
                            'type'           : type,
                            'id'             : view.get("Id")})
        return collections
    
    def getBoxSets(self):
        result = None
        
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        port = addon.getSetting('port')
        host = addon.getSetting('ipaddress')
        server = host + ":" + port
        
        downloadUtils = DownloadUtils()
        userid = downloadUtils.getUserId()   
        
        url = server + '/mediabrowser/Users/' + userid + '/Items?SortBy=SortName&IsVirtualUnaired=false&IsMissing=False&Fields=Name,SortName,CumulativeRunTimeTicks&Recursive=true&SortOrder=Ascending&IncludeItemTypes=BoxSet&format=json&ImageTypeLimit=1'
        
        jsonData = downloadUtils.downloadUrl(url, suppress=False, popup=0)
        
        if jsonData != None and jsonData != "":
            result = json.loads(jsonData)
            if(result.has_key('Items')):
                result = result['Items']
        return result
    
    def getMoviesInBoxSet(self,boxsetId):
        result = None
        
        addon = xbmcaddon.Addon(id='plugin.video.emby')
        port = addon.getSetting('port')
        host = addon.getSetting('ipaddress')
        server = host + ":" + port
        
        downloadUtils = DownloadUtils()
        userid = downloadUtils.getUserId()   
        
        url = server + '/mediabrowser/Users/' + userid + '/Items?ParentId=' + boxsetId + '&Fields=ItemCounts&format=json&ImageTypeLimit=1'
        
        jsonData = downloadUtils.downloadUrl(url, suppress=False, popup=0)
        
        if jsonData != None and jsonData != "":
            result = json.loads(jsonData)
            if(result.has_key('Items')):
                result = result['Items']
        return result
        
