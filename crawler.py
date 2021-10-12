#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  5 11:54:19 2021

@author: k.liu
"""
from googleapiclient.discovery import build
from sqlalchemy import create_engine, Table, Column, Text, Integer, String, MetaData, DateTime
from sqlalchemy.dialects.mysql import insert
from datetime import datetime

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
            except: break
        return results
            
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
                
class mysql():
    
    def __init__(self, connection, metadata):
        
        self.con = connection
        self.metadata = metadata
        self.tables = metadata.tables
    
    def insert(self, table, data):
        ins = table.insert()
        self.con.execute(ins, data)
        
    def upsert(self, table, data):
        n = len(data)
        count = 1
        for i in data:
            ins = insert(table).values(i).on_duplicate_key_update(i)
            self.con.execute(ins)
            if count%10000==0:
                print(count,'/',n)
            count += 1
            
    def close(self):
        self.con.close()
        
username, password, ip, database = 'kai', 'Ss3745120', 'localhost', 'news'
path = 'mysql+pymysql://{}:{}@{}/{}'.format(username, password, ip, database)
engine = create_engine(path)
con = engine.connect()
metadata = MetaData()

videos = Table('videos', metadata,
               Column('videoId', String(50), primary_key=True),
               Column('title', Text),
               Column('description', Text),
               Column('channelId', String(50)),
               Column('channelTitle', Text),
               Column('categoryId', String(50)),
               Column('viewCount', Integer),
               Column('likeCount', Integer),
               Column('dislikeCount', Integer),
               Column('publishedAt', DateTime),
               Column('favoriteCount', Integer),
               Column('commentCount', Integer))

comments = Table('comments', metadata,
                 Column('commentId', String(50), primary_key=True),
                 Column('textOriginal', Text),
                 Column('authorDisplayName', Text),
                 Column('authorChannelId', String(50)),
                 Column('parentId', String(50)),
                 Column('likeCount', Integer),
                 Column('videoId', String(50)),
                 Column('publishedAt', DateTime))

metadata.create_all(engine)
sql = mysql(con, metadata)

def update_videos(username, crawler):
    global sql
    global videos 
    idList = crawler.crawl_playlistItem(username)
    video_list = crawler.crawl_video(idList)
    print('idLIst: ',len(idList),'\n updated: ',len(video_list))
    sql.upsert(videos, video_list)

def update_comments(username, crawler_1, crawler_2):
    global sql
    global comments
    idList = crawler_1.crawl_playlistItem(username)
    comment_list = crawler_1.crawl_comment(idList[:10000]) + crawler_2.crawl_comment(idList[10000:])
    print('idLIst: ',len(idList),'\n','updated: ',len(comment_list))
    sql.upsert(comments, comment_list)
    
def main():
    global sql
    crawler_1 = news_crawler((youtube_api('AIzaSyCrlFsn0hgELyxt2xKxSb7hRX_uT_wwB8I')))
    crawler_2 = news_crawler(youtube_api('AIzaSyD_6LW8_Xh7PV5E8fqV4b4qYL1ShpdbwZ4'))
    crawler_3 = news_crawler(youtube_api('AIzaSyD0tLLCgYs9VoyTYp5vgRbB_zrLLEqD6ok'))
    channel_list = ['ctitv', 'setnews159', 'newsebc', 'TBSCTS', 'TVBS', 'eranewsupload', 'PNNPTS']
    username = channel_list[datetime.today().weekday()]
    update_videos(username, crawler_1)
    update_comments(username, crawler_2, crawler_3)
    sql.close()

if __name__ == '__main__':
    main()
