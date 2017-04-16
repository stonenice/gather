#!/usr/bin/python
#Filename: biz.utils.py
#coding:utf-8

import time,datetime

class utils(object):
    @staticmethod
    def now(format='%Y-%m-%d %X'):
        ltime=time.localtime(time.time())
        return time.strftime(format,ltime)
    
    @staticmethod
    def time2str(date,format='%Y-%m-%d %H:%M:%S'):
        if isinstance(date,(datetime.datetime)):
            tstr=date.strftime(format)
        else:
            tstr=date
        return tstr
    
    @staticmethod
    def tcmp(timea,timeb,formart='%Y-%m-%d %H:%M:%S'):
        
        if isinstance(timea,(datetime.datetime)):
            timea=timeb.strftime('%Y-%m-%d %H:%M:%S')
            
        if isinstance(timeb,(datetime.datetime)):
            timeb=timeb.strftime('%Y-%m-%d %H:%M:%S')
        
        timea= timea if timea.find(':')>=0 else '%s 00:00:00'%(timea)
        timeb= timeb if timeb.find(':')>=0 else '%s 00:00:00'%(timeb)
        
        
        a=time.strptime(timea, '%Y-%m-%d %X')
        b=time.strptime(timeb, '%Y-%m-%d %X')
        
        retval=(1 if a>b else (0 if a==b else -1))
        
        return retval
    
    @staticmethod
    def sqlstr(var):
        if isinstance(var, (str,unicode)):
            return "'%s'"%(var)
        else:
            return '%s'%(var)
        
        
            
