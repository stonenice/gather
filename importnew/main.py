#!/usr/bin/python
#coding:utf-8
import biz
import db

if __name__ != '__main__':
    exit()

m=db.TophotModel()
now=biz.utils.now();

print 'Importnew'

tophot=biz.tophot(cache_time=3600)
week=tophot.week()
month=tophot.month()


for i in range(len(week)):
    week[i]['create_time']=now
    week[i]['update_time']=now

for i in range(len(month)):
    month[i]['create_time']=now
    month[i]['update_time']=now

m.save(week)
m.save(month) 
print 'finish to parse tophot'

    
am=db.ArticleModel()
ltime=am.latesttime()

article=biz.article()
size=article.getPages()

count=1
while count<=size:
    url='http://www.importnew.com/all-posts/page/%d'%(count)
    articles=article.parsePage(url)
    
    if ltime!=None:
        articles=[x for x in articles if biz.utils.tcmp(x['article_publish'], ltime)>0]
    
    if len(articles)<=0:
        break
    for i in range(len(articles)):
        articles[i]['create_time']=now
        articles[i]['update_time']=now
    
    am.save(articles)
    
    print 'parse page ',count

    articles=[]
    count+=1
print 'finish to parse article'

m.assignPublishTime();
print 'finish to assign publish time'

print 'complete'