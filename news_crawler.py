#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 19 10:13:37 2021

@author: k.liu
"""
from datetime import datetime

class news_crawler():
    
    def __init__(self, api):
        self.api = api
        
        
    def crawl_playlistItem(self, username):
        idList = []
        id_ = self.api.uploadsId(username)
        response = self.api.playlistItems(id_)
        for dic in response:
            for item in dic['items']:                
                idList.append(item['contentDetails']['videoId'])
        return idList
    
    def crawl_video(self, idList):
        data = []
        key_list = ['publishedAt', 'channelId', 'title', 'description', 'channelTitle', 'categoryId']
        videos = self.api.videos(idList)
        for dic in videos:
            items = dic['items']
            for item in items:
                snippet = item['snippet']
                statistics = item['statistics']
                datum = {key:snippet[key] for key in key_list}
                datum.update(statistics)
                datum['videoId'] = item['id']
                datum['publishedAt'] = datetime.strptime(datum['publishedAt'], '%Y-%m-%dT%H:%M:%SZ')
                data.append(datum)
        return data

    def crawl_comment(self, idList):
        
        def parse(item):
            snippet_keys = ['videoId', 'textOriginal', 'authorDisplayName', 'likeCount', 'publishedAt']
            datum = {}
            datum.update({i: item['snippet'][i] for i in snippet_keys})
            datum.update({'commentId' : item['id']})
            if 'authorChannelId' in item['snippet'].keys():
                datum.update({'authorChannelId': item['snippet']['authorChannelId']['value']})
            # if 'authorChannelId' in datum.keys():
            #     datum.update({'authorChannelId': datum['authorChannelId']['value']})
            else: datum.update({'authorChannelId': None})
            if 'parentId' in item['snippet'].keys():
                datum['parentId'] = item['snippet']['parentId']
            else: datum['parentId'] = datum['commentId']
            datum['publishedAt'] = datetime.strptime(datum['publishedAt'], '%Y-%m-%dT%H:%M:%SZ')
            return datum            

        data = []
        comments = self.api.comments(idList)
        for dic in comments:
            items = dic['items']
            for item in items:
                data.append(parse(item['snippet']['topLevelComment']))
                if 'replies' in item.keys(): 
                    replies = item['replies']['comments']
                    data += [parse(reply) for reply in replies]
        return data
                