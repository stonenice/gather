#!/usr/bin/python
#Filename: biz.tophot.py
#coding:utf-8

import re,urllib,os
import time,datetime
from pyquery import PyQuery

class tophot(object):
    '''
    tophot(**kargs)
        url    website
        cache_time    page cache time, the unit is second. default 0s
    '''
    
    ENTRY_TYPE_WEEK='HOT_IN_WEEK'
    ENTRY_TYPE_MONTH='HOT_IN_MONTH'
    ENTRY_TYPE_NONE='NONE'
    
    def __init__(self,**args):
            
        keys=args.keys()
        self.__url=args['url'] if 'url' in keys else 'http://www.importnew.com/'
        self.__cacheTime=args['cache_time'] if 'cache_time' in keys else 0
        
    def week(self):
        topweek=[]

        query=PyQuery(url=self.__url)
        
        result=query('#tab-most-discussed .post-meta a')
        items=result.items()
        for item in items:
            href=item.attr('href')
            title=item.text()
            m=re.search(r'(?P<entryId>[0-9]+)', href)
            if m:
                entryId=m.group('entryId')
            else:
                entryId=-1
            
            if entryId<=0:
                continue
            data={'entry_id':entryId,'entry_title':title,'entry_url':href,'entry_type':tophot.ENTRY_TYPE_WEEK}
            topweek.append(data)
        return topweek
    
    
    
    def month(self):
        topmonth=[]
       
        query=PyQuery(url=self.__url)
        result=query('#tab-latest-comments .post-meta a')
        items=result.items()
        for item in items:
            href=item.attr('href')
            title=item.text()
            m=re.search(r'(?P<entryId>[0-9]+)', href)
            if m:
                entryId=m.group('entryId')
            else:
                entryId=-1
            
            if entryId<=0:
                continue
            data={'entry_id':entryId,'entry_title':title,'entry_url':href,'entry_type':tophot.ENTRY_TYPE_MONTH}
            topmonth.append(data)
        return topmonth
        