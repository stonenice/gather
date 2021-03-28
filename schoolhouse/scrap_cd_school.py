#!/usr/bin/python
# encoding:utf-8

import os
import sys
import pyhocon
import base64
import random
import time
import logging
import arc4
import pymysql
from pyquery import PyQuery

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(filename)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    )
logger = logging.getLogger()

INSERT_SQL_TPL = '''
replace  into edu_school (school_name,school_type,school_level,school_nature,school_phone,city_name,area_name,
                   address_detail,outer_link,outer_id,school_region)
values('{school_name}','{school_type}','{school_level}','{school_nature}','{school_phone}','{city_name}','{area_name}',
                   '{address_detail}','{outer_link}','{outer_id}','{school_region}')
'''


def auto_connect_mysql(db, charset='utf8mb4'):
    """
    自动读取数据库链接配置并建立起数据库链接
    :param db: 数据库名称
    :param charset: 字符集编码
    :return: 返回值
    """

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


class ScrapSchool:
    """
    爬取成都学校信息对象类
    """

    # 学校类型枚举定义
    SCHOOL_TYPE = {
        'infant_school': '幼儿园',
        'primary_school': '小学',
        'junior_school': '初中',
        'senior_school': '高中',
        'university_school': '大学'
    }

    # 成都教育局根目录地址
    SCRAP_ROOT_URL = 'http://infomap.cdedu.com'

    # 成都市教育局学校信息首页爬取地址
    SCRAP_BASE_URL = SCRAP_ROOT_URL + '/Home/Index'

    @staticmethod
    def url_param_to_map(line):
        """
        URL参数行转换成MAP的工具方法
        :param line: URL地址
        :return: 返回值
        """
        kv_map = {}
        if not line:
            return kv_map
        find_pos = str(line).rfind('?')
        line_str = line[find_pos + 1:] if find_pos >= 0 else line
        for kv in line_str.split('&'):
            kv_split = kv.split('=')
            if len(kv_split) != 2:
                continue
            key = kv_split[0]
            value = kv_split[1]
            kv_map[key] = value
        return kv_map

    @staticmethod
    def url_param_exclude(line, exclude_param_key=[]):
        """
        URL参数行转换成MAP的工具方法，同时移除指定的参数键
        :param line: URL地址
        :param exclude_param_key: 需要排除的参数键
        :return: 返回值
        """
        if not line or len(exclude_param_key) <= 0:
            return line
        kv_list = []
        for kv in str(line).split('&'):
            kv_split = kv.split('=')
            if len(kv_split) != 2:
                continue
            if kv_split[0] not in exclude_param_key:
                kv_list.append(kv)
        return '&'.join(kv_list)

    @staticmethod
    def extract_school_info(key, all_info_list):
        if not all_info_list or not key:
            return None
        for info in all_info_list:
            if key in info:
                pos = str(info).find('】')
                return info[pos + 1:] if pos >= 0 else info
        return None

    @staticmethod
    def create_pyquery(url):
        """
        根据指定URL地址创建网页解析对象
        :param url: URL地址
        :return:  返回值
        """
        return PyQuery(
            url=url,
            headers={
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Referer': url,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36 Edg/89.0.774.57'
            }
        )

    def __init__(self, school_type):
        """
        构造方法
        :param school_type: 学校类型
        """
        self._school_type = school_type
        if self._school_type not in self.SCHOOL_TYPE:
            raise Exception('do not recognize the school type:%s' % (self._school_type))
        self._scrap_url_map = {}
        self._total_pages = None
        self._current_page = None

    def get_scrap_url(self):
        """
        获取当前对象爬取数据的URL地址
        :return: 返回值
        """
        return self._scrap_url_map[self._school_type]

    def get_school_type(self):
        """
        获取当前对象爬取的学校类型
        :return:  返回值
        """
        return self._school_type

    def parse_page_to_school_list(self, page):
        data_list = []
        for ul in page('div>ul').items():
            for li in ul('li').items():
                outer_link = li('a').attr('href')
                url_map = self.url_param_to_map(outer_link)
                all_info_list = [p.text() for p in li('.text_div p').items() if '】' in p.text()]
                school_region = self.extract_school_info('区域', all_info_list)
                school = {
                    'outer_link': self.SCRAP_ROOT_URL + outer_link,
                    'outer_id': url_map['Id'],
                    'school_name': li('a h1').text(),
                    'school_level': self.extract_school_info('学段', all_info_list),
                    'school_region': school_region,
                    'school_nature': self.extract_school_info('性质', all_info_list),
                    'school_phone': self.extract_school_info('电话', all_info_list),
                    'address_detail': self.extract_school_info('地址', all_info_list),
                }

                if school_region and '教育局' in school_region:
                    school['area_name'] = school_region.rstrip('教育局')
                elif school_region and '区' in school_region:
                    school['area_name'] = school_region.lstrip('成都')
                data_list.append(school)
        return data_list

    def init_scrap(self):
        pq = self.create_pyquery(self.SCRAP_BASE_URL)
        school_level_a = pq('#filter .filter-item:first-child dd a')

        # parsing dynamic url
        for a in school_level_a.items():
            href = str(a.attr('href'))
            school_type = None
            for key, value in self.SCHOOL_TYPE.items():
                if str(a.text()).strip() == value:
                    school_type = key
                    break
            if not school_type:
                continue
            pos = href.find('?')
            url_param = href[pos + 1:] if pos >= 0 else href
            new_url_param = self.url_param_exclude(url_param, ['dep'])
            # add to list
            self._scrap_url_map[school_type] = self.SCRAP_BASE_URL + '?' + new_url_param
        self._total_pages = None
        self._current_page = None

    def fetch(self):
        if not self._total_pages:
            first_page = self.create_pyquery(self.get_scrap_url())
            self._total_pages = max([int(a.text()) for a in first_page('div a').items() if str(a.text()).isnumeric()])
            self._current_page = 1
            for x in self.parse_page_to_school_list(first_page):
                yield x

        while self._current_page + 1 <= self._total_pages:
            page_index = self._current_page + 1
            sleep = int(random.random() * 10)
            next_page_url = self.get_scrap_url() + '&pages=' + str(page_index)
            print('after sleep', sleep, 's continue to scrap the next page:', next_page_url)
            time.sleep(sleep)
            result_list = self.parse_page_to_school_list(PyQuery(next_page_url))
            self._current_page = self._current_page + 1
            for x in result_list:
                yield x


if __name__ != '__main__':
    logger.error('not a library')
    sys.exit()

scrap_school_type = sys.argv[1] if len(sys.argv) >= 2 else 'junior_school'
logger.info('start to scrap:%s' % (scrap_school_type))
scrap = ScrapSchool(school_type=scrap_school_type)
scrap.init_scrap()

connect = auto_connect_mysql('gather')
try:
    for x in scrap.fetch():
        try:
            sql = INSERT_SQL_TPL.format(
                school_name=x['school_name'],  # required
                school_type=scrap.get_school_type(),  # required
                school_level=x['school_level'] if 'school_level' in x else '',
                school_nature=x['school_nature'] if 'school_nature' in x else '',
                school_phone=x['school_phone'] if 'school_phone' in x else '',
                city_name='成都市',
                area_name=x['area_name'] if 'area_name' in x else '',
                address_detail=x['address_detail'] if 'address_detail' in x else '',
                outer_link=x['outer_link'] if 'outer_link' in x else '',
                outer_id=x['outer_id'] if 'outer_id' in x else '',
                school_region=x['school_region'] if 'school_region' in x else ''
            )
            logger.info(sql)
            cursor = connect.cursor()
            cursor.execute(sql)
            connect.commit()
            cursor.close()
        except BaseException as e:
            logger.error(e)
            pass
finally:
    connect.close()
