#!/usr/bin/python
#Filename: db.Model.py
#coding:utf-8

import config
import MySQLdb
from biz import utils

class model(object):
    
    def __init__(self):
        conf=config.DBConfig
        keys=conf.keys();
        
        self.__host=conf['db_host'] if 'db_host' in keys else 'localhost'
        self.__port=conf['db_port'] if 'db_port' in keys else 3306
        self.__user=conf['db_user'] if 'db_user' in keys else 'root'
        self.__pwd=conf['db_pwd'] if 'db_pwd' in keys else '***'
        self.__name=conf['db_name'] if 'db_name' in keys else 'test'
        self.__charset=conf['db_charset'] if 'db_charset' in keys else 'utf8'
        
        self._connect=None
        
    def open(self):
        
        if self._connect!=None:
            self._connect.close()
            self._connect=None
        
        self._connect=MySQLdb.connect(host=self.__host,port=self.__port,user=self.__user,
                                       passwd=self.__pwd,db=self.__name,use_unicode=True,
                                       charset=self.__charset)
        
        
    def close(self):
        if self._connect!=None:
            self._connect.close()
        self._connect=None
        
    def maxid(self,table,col='id'):
        maxid=0
        sql='select max(`%s`) as maxid from `%s`'%(col,table)
        self.open()
        cur=self._connect.cursor(MySQLdb.cursors.DictCursor)
        count=cur.execute(sql)
        if count>0:
            one=cur.fetchone()
            maxid=one['maxid'] if 'maxid' in one and one['maxid']>0 else 0
        cur.close()
        self.close()
        
        return maxid

    def insert(self,records,table,primaryKeyValue=-1,primaryKey=None):
        if records==None:
            return -1
        
        pk=primaryKey if primaryKey!=None else 'id'
        
        if isinstance(records, list) and len(records)>0:
            kv=records[0]
            cols=[]
            vals=[]
            sql='insert into `%s` '%(table)
            for key in kv.keys():
                cols.append('`%s`'%(key))
                vals.append('%s')
                
            if primaryKeyValue>=0:
                cols.append('`%s`'%(pk))
                vals.append('%s')
            nrecords=[]
            maxid=primaryKeyValue+1
            for item in records:
                nitem=[]
                for key in cols:
                    index=key.strip('`')
                    if index in item:
                        nr=item[index]
                        nitem.append(nr)
                    
                if primaryKeyValue>=0:
                    nitem.append(maxid)
                    maxid+=1
                nrecords.append(nitem)
                
            sql+=' (%s) values(%s)'%(','.join(cols),','.join(vals))
            
            self.open()
            cur=self._connect.cursor()
            count=cur.executemany(sql,nrecords)
            cur.close()
            if count==len(nrecords):
                self._connect.commit()
                cur.close()
                self.close()
                return count
            else:
                self._connect.rollback()
                cur.close()
                self.close()
                return -1
            
        elif isinstance(records, dict):
            if primaryKeyValue>=0:
                records[pk]=primaryKeyValue+1
            sql='insert into `%s` '%(table)
            cols=[]
            vals=[]
            
            for key in records.keys():
                val=records[key]
                
                cols.append('`%s`'%(key))
                vals.append(utils.sqlstr(val))
                
            sql+='(%s) values(%s)'%(','.join(cols),','.join(vals))
            
            self.open()
            cur=self._connect.cursor()
            count=cur.execute(sql)
            
            if count==1:
                self._connect.commit()
                cur.close()
                self.close()
                return 1
            else:
                self._connect.rollback()
                cur.close()
                self.close()
                return -1
            
        else:
            return -1