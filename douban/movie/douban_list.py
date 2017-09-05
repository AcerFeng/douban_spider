#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2017-09-05 16:52:10
# Project: douban_movies_time

from pyspider.libs.base_handler import *
import pymysql

class Handler(BaseHandler):
    crawl_config = {
        'itag': 'v002'
    }

    def __init__(self):
        try:
            self.connect = pymysql.connect(host='localhost', port=3306, user='root', passwd='123456', db='douban', charset='utf8mb4')
            
        except Exception as e:
            print('Cannot Connect To Mysql!/n', e)
            raise e

        

    def save_data(self, tableName=None, **kw):
        #tagMoviesData = kw['data']
        #normalMoviesData = kw['subjects']
        movieData = kw['subjects']
        if not movieData:
            return

        for item in movieData:
            try:
                cursor = self.connect.cursor()
                cursor.execute('select count(*) from douban_data where douban_id=%s', (item['id'],))
                result = cursor.fetchone()
                if result[0]:
                    # todo: 更新操作
                    if 'directors' in item:
                        pass
                    else:
                        pass
                else:
                    # todo: 插入操作
                    if 'directors' in item:
                        sql = 'insert into douban_data(douban_id, title, url, image_large, rate, subtype) values (%s, %s, %s, %s, %s, %s)'
                        cursor.execute(sql, (item['id'], item['title'], item['url'], item['cover'], item['rate'], 'movie'))
                    else:
                        sql = 'insert into douban_data(douban_id, title, url, image_large, rate, is_new, playable, subtype) values (%s, %s, %s, %s, %s, %s, %s, %s)'
                        cursor.execute(sql, (item['id'], item['title'], item['url'], item['cover'], item['rate'], item['is_new'], item['playable'], 'movie'))
                self.connect.commit()

            except Exception as e:
                raise e

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('http://movie.douban.com/j/search_subjects?type=movie&tag=%E7%83%AD%E9%97%A8&sort=time', callback=self.detail_page, params={'page_limit' : 20, 'page_start' : 0}, save={'page_limit' : 20, 'page_start' : 0})

    @config(priority=2)
    def detail_page(self, response):
        if len(response.json['subjects']) != 0:
            self.crawl('http://movie.douban.com/j/search_subjects?type=movie&tag=%E7%83%AD%E9%97%A8&sort=time', callback=self.detail_page, params={'page_limit' : response.save['page_limit'] + 20, 'page_start' : response.save['page_start'] + 20}, save={'page_limit' : response.save['page_limit'] + 20, 'page_start' : response.save['page_start'] + 20})
            return response.json

    def on_result(self,result):
        if not result:
            return
        self.save_data('douban_data',**result)