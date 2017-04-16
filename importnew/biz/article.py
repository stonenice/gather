#!/usr/bin/python
#Filename: biz.article.py
#coding:utf-8

import re
from pyquery import PyQuery

class article(object):
    def __init__(self,**kargs):
        keys=kargs.keys()
        self.__url=kargs['url'] if 'url' in keys else 'http://www.importnew.com/all-posts'
        self.__cacheTime=kargs['cache_time'] if 'cache_time' in keys else 0
        self.__firstpage=None
    
    def getPages(self):
        doc=PyQuery(url=self.__url)
        pages=doc('.navigation a')
        pageNums=[int(x.text()) for x in pages.items() if re.match('\d+', x.text())]
        maxPage=max(pageNums)
        return maxPage
    
    def parsePage(self,url=None):
        url=url if url!=None else self.__url
        doc=PyQuery(url=url)
        posts=doc('#archive .post .post-meta')

        articles=[]

        for post in posts.items():
            p=post('p:first').text()
            info=p.split('|')
            article_tag=info[1]
            m=re.search(r'(?P<publish>\d+/\d+/\d+)', p)
            if not m:
                continue
            ap=m.group('publish')
            article_publish=ap.replace('/','-')
            title=post('.meta-title')
            article_url=title.attr('href')
            m=re.search('(?P<id>\d+)', article_url)
            if not m:
                continue
            article_id=m.group('id')
            
            article_title=title.text()
            article_abstract=post('.excerpt:first').text()
            
            article={'article_id':article_id,'article_title':article_title,'article_abstract':article_abstract,
                     'article_url':article_url,'article_publish':article_publish,'article_tag':article_tag}
            
            articles.append(article)
            
        return articles

        
    
    