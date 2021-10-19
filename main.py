#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  5 11:54:19 2021

@author: k.liu
"""
from sqlalchemy import create_engine, Table, Column, Text, Integer, String, MetaData, DateTime
from datetime import datetime
from news_crawler import news_crawler
from youtube_api import youtube_api
from mysql import mysql

def main():
    
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

        idList = crawler.crawl_playlistItem(username)
        video_list = crawler.crawl_video(idList)
        print('idLIst: ',len(idList),'\n updated: ',len(video_list))
        sql.upsert(videos, video_list)
    
    def update_comments(username, crawler_1, crawler_2):

        idList = crawler_1.crawl_playlistItem(username)
        comment_list = crawler_1.crawl_comment(idList[:10000]) + crawler_2.crawl_comment(idList[10000:])
        print('idLIst: ',len(idList),'\n','updated: ',len(comment_list))
        sql.upsert(comments, comment_list)

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
