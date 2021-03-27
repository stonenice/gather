#!/usr/bin/python
# encoding:utf-8
from pyquery import PyQuery
import time
import random
import re
import json

import sys
import os
import base64
import pyhocon
import arc4
from loguru import logger
import pymysql
import pymysql.cursors

cdfx = 'https://www.cdfangxie.com'
cdfx_infor = '%s/Infor/type/typeid/36.htm' % cdfx

sql_tpl = '''
insert into cdfx_loupan (uniqekey,certno,project,region,purpose,phone,selldate,infos,files,gmt_create,gmt_modified)
values('{uniqekey}','{certno}','{project}','{region}','{purpose}','{phone}','{selldate}','{infos}','{files}','{gmt_created}','{gmt_modified}');
'''
timestr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def auto_connect_mysql(db, charset='utf8mb4'):
    # read system variables
    path = os.getenv('SYS_CFG_PATH')
    pwd = os.getenv('SYS_CFG_PWD')
    # load and parse conf
    conf = pyhocon.ConfigFactory.parse_file(path)
    # encrypt password
    encrypt_pwd = conf.get_string('database.mysql.password')
    pwd = arc4.ARC4(pwd.encode('utf-8')).decrypt(base64.b64decode(encrypt_pwd))
    # create connect
    return pymysql.connect(host=conf.get_string('database.mysql.host'),
                           port=conf.get_int('database.mysql.port'),
                           user=conf.get_string('database.mysql.user'),
                           password=pwd,
                           db=db,
                           charset=charset,
                           cursorclass=pymysql.cursors.DictCursor)


connect = auto_connect_mysql('gather')


def get_total_page():
    pq = PyQuery(cdfx_infor)
    text = pq('.pages2').text()
    k = re.findall(r'([\d/\s]+)', text)
    if len(k) <= 0: return -1
    split = str(k[0]).split('/')
    return int(split[1]) if len(split) == 2 else -1


def parse_page(page):
    url = '%s?p=%d' % (cdfx_infor, page)
    doc = PyQuery(url)
    data = []

    for x in doc.items('.ul_list li'):
        a = x.find('a')
        if not a: continue
        line = str(a.text())
        split = line.split('|')
        if len(split) != 2: continue
        href = a.attr('href')
        item = {}
        item['region'] = str(split[0]).strip()
        item['project'] = str(split[1]).strip()

        time.sleep(random.random())
        doc_details = PyQuery(cdfx + href)
        infos = []
        resources = []
        for e in doc_details.items('.MsoNormal'):
            txt = str(e.text())
            kv = txt.split(':')
            if len(kv) != 2: continue
            ik = str(kv[0]).strip()
            iv = str(kv[1]).strip()
            if ik.find('电话') >= 0:
                item['phone'] = iv
            if ik.find('房屋用途') >= 0:
                item['purpose'] = iv
            if ik.find('上市时间') >= 0:
                item['selldate'] = iv
            if ik.find('证号') >= 0:
                item['certno'] = iv
            infos.append({'key': ik, 'value': iv})
        for ae in doc_details.items('.MsoNormal a'):
            resources.append({'key': str(ae.text()).strip(), 'value': str(ae.attr('href')).strip()})

        item['infos'] = json.dumps(infos, ensure_ascii=False)
        item['files'] = json.dumps(resources, ensure_ascii=False)
        data.append(item)
    return data


if __name__ != '__main__':
    sys.exit()

certno_list = []
try:
    cur = connect.cursor()
    cur.execute('select uniqekey from cdfx_loupan')
    certno_list = [x['uniqekey'] for x in cur.fetchall()]
    cur.close()

    pages = int(get_total_page())

    advance = True
    p = 1
    while advance and p <= pages:
        items = parse_page(p)
        for x in items:
            uniqekey = '{region}###{project}###{selldate}'.format(**x)
            if uniqekey in certno_list:
                advance = False
                break
            x['uniqekey'] = uniqekey
            x['gmt_created'] = timestr
            x['gmt_modified'] = timestr
            try:
                cur = connect.cursor()
                cur.execute(sql_tpl.format(**x))
                connect.commit()
                cur.close()
            except Exception as sqlex:
                logger.error(sqlex)
        logger.info('page:%d' % p)
        p += 1

except Exception as ex:
    logger.error(ex)
    connect.close()
