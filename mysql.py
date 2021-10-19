#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 19 10:15:04 2021

@author: k.liu
"""
from sqlalchemy.dialects.mysql import insert

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