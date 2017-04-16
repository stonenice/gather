#!/usr/bin/python
#Filename: db.TophotModel.py

from model import model

import MySQLdb

class TophotModel(model):
    def __init__(self):
        model.__init__(self)
    
    def getMaxId(self):
        return self.maxid('importnew_tophot')
    
    def exist(self,entry):
        entryId=-1
        flag=False
        if isinstance(entry, dict):
            entryId=entry['entry_id'] if 'entry_id' in entry else -1
        else:
            entryId=entry
            
        if entryId>0 :
            sql='select entry_id from importnew_tophot where entry_id=%s'%(entryId)
            self.open()
            cur=self._connect.cursor(MySQLdb.cursors.DictCursor)
            count=cur.execute(sql)
            if count>0:
                one=cur.fetchone()
                flag=('entry_id' in one and int(one['entry_id'])==int(entryId))
            self.close()
        
        return flag
    
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
        count=self.insert(checked,'importnew_tophot', primaryKeyValue=maxid)
        return count
    
    def assignPublishTime(self):
        sql='SELECT a.entry_id,b.article_publish FROM importnew_tophot a INNER JOIN importnew_article b '+ \
            'ON a.entry_id=b.article_id WHERE a.entry_publish IS NULL'
        self.open()
        cur=self._connect.cursor(MySQLdb.cursors.DictCursor)
        count=cur.execute(sql)
        if count>0:
            result=cur.fetchall()
            
            for x in result:
                usql="update importnew_tophot set entry_publish='%s' where entry_id=%s"%(x['article_publish'],x['entry_id'])
                cur.execute(usql)    
        cur.close()
        self._connect.commit()
        self.close()
        return count
        