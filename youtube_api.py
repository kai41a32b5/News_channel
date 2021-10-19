#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 19 10:11:49 2021

@author: k.liu
"""

from googleapiclient.discovery import build

class youtube_api():
    
    def __init__(self, api_key):
        self.api_key = api_key
        self.con = build('youtube', 'v3', developerKey=api_key)

    def uploadsId(self, username):
        response = self.con.channels().list(
            part='contentDetails',
            forUsername=username
            ).execute()
        uploads_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        return uploads_id

    def playlistItems(self, playlistId, pageToken=None, part='contentDetails'):
        uploads = self.con.playlistItems().list(
            playlistId= playlistId,
            part= part,
            maxResults=50,
            pageToken = pageToken
            ).execute()
        if 'nextPageToken' not in uploads.keys():
            return [uploads]
        else: return [uploads]+ self.playlistItems(playlistId, uploads['nextPageToken'])
    
    def videos(self, idList, part='snippet, statistics'):
        if len(idList)<51:
            videos = self.con.videos().list(
                id = ','.join(idList),
                part = part,
                maxResults=50).execute()
            return [videos]
        else: return self.videos(idList[:len(idList)//2]) + self.videos(idList[len(idList)//2:])
        
    def comments(self, idList):
        
        def commentThreads(videoId, part='snippet, replies', pageToken=None):
            comments = self.con.commentThreads().list(
                videoId = videoId,
                part = part,
                pageToken = pageToken,
                maxResults=50).execute()
            if 'nextPageToken' not in comments.keys():
                return [comments]
            else: return [comments] + commentThreads(videoId, pageToken=comments['nextPageToken'])
        
        results = []
        for videoId in idList:
            try: results = results + commentThreads(videoId)
            except: pass
        return results