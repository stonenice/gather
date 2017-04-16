#!/usr/bin/python
#Filename: db.ArticleModel.py
#coding:utf-8

from model import model
import time
import MySQLdb

class ArticleModel(model):
    def getMaxId(self):
        return self.maxid('importnew_article')
    
    def exist(self,entry):
        entryId=-1
        flag=False
        if isinstance(entry, dict):
            entryId=entry['article_id'] if 'article_id' in entry else -1
        else:
            entryId=entry
            
        if entryId>0 :
            sql='select article_id from importnew_article where article_id=%s'%(entryId)
            self.open()
            cur=self._connect.cursor(MySQLdb.cursors.DictCursor)
            count=cur.execute(sql)
            if count>0:
                one=cur.fetchone()
                flag=('article_id' in one and int(one['article_id'])==int(entryId))
            self.close()
        
        return flag
    
    def latesttime(self):
        lt=None
            
        sql='select max(article_publish) as lt from importnew_article'
        self.open()
        cur=self._connect.cursor(MySQLdb.cursors.DictCursor)
        count=cur.execute(sql)
        if count>0:
            one=cur.fetchone()
            lt=one['lt'] if 'lt' in one else None
        self.close()

        return lt

    def checkRecords(self,records):
        result=[]
        if len(records)<=0:
            return result
        for item in records:
            if not self.exist(item):
                result.append(item)
        return result        
    
    def save(self,records):
        checked=self.checkRecords(records)
        maxid=self.getMaxId()
        count=self.insert(checked,'importnew_article', primaryKeyValue=maxid)
        return count
    
    