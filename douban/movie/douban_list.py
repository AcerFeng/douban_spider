#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2017-09-05 16:52:10
# Project: douban_movies_time

from pyspider.libs.base_handler import *
import pymysql

class Handler(BaseHandler):
    crawl_config = {
        'itag': 'v009'
    }

    def __init__(self):
        self.DOU_BAN_MOVIE_HOT = 'http://movie.douban.com/j/search_subjects?type=movie&tag=%E7%83%AD%E9%97%A8&sort=time'
        try:
            self.connect = pymysql.connect(host='localhost', port=3306, user='root', passwd='123456', db='douban', charset='utf8mb4')
            
        except Exception as e:
            print('Cannot Connect To Mysql!/n', e)
            raise e

    def save_data(self, **kw):
        movieData = None
        if 'data' in kw:
            movieData = kw
        elif 'subjects' in kw:
            movieData = kw['subjects']
        else:
            return
    

        if not movieData:
            return

        for item in movieData:
            try:
                cursor = self.connect.cursor()
                cursor.execute('select count(*) from douban_data where douban_id=%s', (item['id'],))
                result = cursor.fetchone()
                if result[0]:
                    # 更新操作
                    if 'directors' in item:
                        sql = 'update douban_data set title=%s, url=%s, image_large=%s, rate=%s where douban_id=%s'
                        cursor.execute(sql, (item['title'], item['url'], item['cover'], item['rate'], item['id']))
                    else:
                        sql = 'update douban_data set title=%s, url=%s, image_large=%s, rate=%s, is_new=%s, playable=%s where douban_id=%s'
                        cursor.execute(sql, (item['title'], item['url'], item['cover'], item['rate'], item['is_new'], item['playable'], item['id']))
                else:
                    # 插入操作
                    if 'directors' in item:
                        sql = 'insert into douban_data(douban_id, title, url, image_large, rate, subtype) values (%s, %s, %s, %s, %s, %s)'
                        cursor.execute(sql, (item['id'], item['title'], item['url'], item['cover'], item['rate'], 1))
                    else:
                        sql = 'insert into douban_data(douban_id, title, url, image_large, rate, is_new, playable, subtype) values (%s, %s, %s, %s, %s, %s, %s, %s)'
                        cursor.execute(sql, (item['id'], item['title'], item['url'], item['cover'], item['rate'], item['is_new'], item['playable'], 1))
                self.connect.commit()

            except Exception as e:
                self.connect.rollback()
                raise e

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl(self.DOU_BAN_MOVIE_HOT, callback=self.detail_page, params={'page_limit' : 20, 'page_start' : 0}, save={'page_limit' : 20, 'page_start' : 0})

    @config(priority=2)
    def detail_page(self, response):
        if len(response.json['subjects']) != 0:
            self.crawl(self.DOU_BAN_MOVIE_HOT, callback=self.detail_page, params={'page_limit' : response.save['page_limit'] + 20, 'page_start' : response.save['page_start'] + 20}, save={'page_limit' : response.save['page_limit'] + 20, 'page_start' : response.save['page_start'] + 20})
            return response.json

    def on_result(self,result):
        if not result:
            return
        self.save_data(**result)
