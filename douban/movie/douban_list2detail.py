#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2017-09-05 16:52:10
# Project: douban_movies_time

from pyspider.libs.base_handler import *
import pymysql
import re

class Handler(BaseHandler):
    crawl_config = {
        'itag': 'v003'
    }
    
    def __init__(self):
        self.DOU_BAN_MOVIE_HOT = 'http://movie.douban.com/j/search_subjects?type=movie&tag=%E7%83%AD%E9%97%A8&sort=time'
        try:
            self.connect = pymysql.connect(host='localhost', port=3306, user='root', passwd='123456', db='douban', charset='utf8mb4')
            
        except Exception as e:
            print('Cannot Connect To Mysql!/n', e)
            raise e

    def save_comments(self, comments, video_id):
        if comments and len(comments)>0 and video_id:
            for comment in comments:
                    cursor = self.connect.cursor()
                    cursor.execute('select count(*) from douban_comments where user_id=%s and video_id=%s', (comment['user_id'],video_id))
                    result = cursor.fetchone()
                    if result[0]:
                        # 更新操作
                        sql = '''update douban_comments set 
                            status=%s, 
                            time=%s, 
                            like_count=%s,
                            content=%s
                            where user_id=%s and video_id=%s'''

                        cursor.execute(sql, (item['status'], 
                                    item['time'], 
                                    item['like_count'], 
                                    item['content'], 
                                    item['user_id'], 
                                    video_id))
                    else:
                        # 插入
                        sql = '''
                            insert into douban_comments
                            (user_id, 
                            status, 
                            time, 
                            like_count, 
                            content, 
                            video_id) 
                            values (%s, %s, %s, %s, %s, %s)
                        '''

    def save_data(self, **kw):
        
        try:
            cursor = self.connect.cursor()
            cursor.execute('select count(*) from douban_video where video_id=%s', (item['douban_id'],))
            result = cursor.fetchone()
            if result[0]:
                # 更新操作
                sql = '''update douban_video set 
                    comments_count=%s, 
                    directors_name=%s, 
                    genres=%s,
                    images=%s,
                    is_free=%s,
                    language=%s,
                    mins=%s,
                    play_platforms=%s,
                    premiere=%s,
                    same_like_ids=%s,
                    score_count=%s,
                    summary=%s,
                    want_to_watch_count=%s,
                    watched_count=%s,
                    watching_count=%s,
                    writers=%s
                    where video_id=%s'''
                cursor.execute(sql, (item['comments_count'], 
                                    item['directors_name'], 
                                    item['genres'], 
                                    item['images'], 
                                    item['is_free'], 
                                    item['language'],
                                    item['mins'],
                                    item['play_platforms'],
                                    item['premiere'],
                                    item['same_like_ids'],
                                    item['score_count'],
                                    item['summary'],
                                    item['want_to_watch_count'],
                                    item['watched_count'],
                                    item['watching_count'],
                                    item['writers'],
                                    item['douban_id']))

                self.save_comments(item['hot_comments'], item['douban_id'])
                
                
                
            else:
                # 插入操作
                if 'directors' in item:
                    sql = 'insert into douban_video(video_id, title, url, image_large, rate, subtype) values (%s, %s, %s, %s, %s, %s)'
                    cursor.execute(sql, (item['id'], item['title'], item['url'], item['cover'], item['rate'], 1))
                else:
                    sql = 'insert into douban_video(video_id, title, url, image_large, rate, is_new, playable, subtype) values (%s, %s, %s, %s, %s, %s, %s, %s)'
                    cursor.execute(sql, (item['id'], item['title'], item['url'], item['cover'], item['rate'], item['is_new'], item['playable'], 1))
            self.connect.commit()

        except Exception as e:
            self.connect.rollback()
            raise e

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl(self.DOU_BAN_MOVIE_HOT, callback=self.index_page, params={'page_limit' : 20, 'page_start' : 0}, save={'page_limit' : 20, 'page_start' : 0})

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        data_list = []
        if 'subjects' in response.json:
            data_list = response.json['subjects']
        elif 'data' in response.json:
            data_list = response.json['data']

        if len(data_list) != 0:
            for item in data_list:
                self.crawl(item['url'], callback=self.detail_page)
            self.crawl(self.DOU_BAN_MOVIE_HOT, callback=self.index_page, params={'page_limit' : response.save['page_limit'] + 20, 'page_start' : response.save['page_start'] + 20}, save={'page_limit' : response.save['page_limit'] + 20, 'page_start' : response.save['page_start'] + 20})


    @config(priority=2)
    def detail_page(self, response):
        same_likes = []
        for x in response.doc('#recommendations dt a').items():
            re_same = re.match(r'.+\/(\d+)\/', x.attr('href'))
            if re_same:
                same_likes.append(re_same.group(1))
            
            
        comments = []
        for item in response.doc('#hot-comments .comment-item .comment').items():
            re_id = re.match(r'.+\/(\w+)\/', item.find('.comment-info a').attr('href'))
            user_comment = {
                'like_count' : item.find('span.comment-vote span.votes').text(),
                'name' : item.find('.comment-info a').text(),
                'url': item.find('.comment-info a').attr('href'),
                'user_id': re_id.group(1) if re_id else None,
                'status': item.find('.comment-info span:eq(0)').text().strip(),
                'score': item.find('.comment-info span:eq(1)').attr('title').strip(),
                'time' : item.find('.comment-info span:eq(2)').text().strip(),
                'content': item.find('p').text().strip(),
            }
            comments.append(user_comment)
     
        re_mins = re.match(r'.+片长: (\w+[\u4e00-\u9fa5]+) ',response.doc('#info').text())
        mins = re_mins.group(1) if re_mins else response.doc('#info span[property|="v:runtime"]').text().strip()
        re_language = re.match(r'.+语言: ([\u4e00-\u9fa5 \/]+) ',response.doc('#info').text())
        language = re_language.group(1).strip() if re_language else None
        re_douban_id = re.match(r'.+\/(\d+)\/', response.url)
        re_watching = re.match(r'.*?(\d+)人在看',response.doc('#subject-others-interests .subject-others-interests-ft a').text().strip())
        re_watched = re.match(r'.*?(\d+)人看过',response.doc('#subject-others-interests .subject-others-interests-ft a').text().strip())
        re_want_to_watch = re.match(r'.*?(\d+)人想看',response.doc('#subject-others-interests .subject-others-interests-ft a').text().strip())
        re_comments_count = re.match(r'[\u4e00-\u9fa5 ]+(\d+)[\u4e00-\u9fa5 ]+', response.doc('#comments-section span.pl a').text().strip())
        re_title = re.match(r'(.+)的短评', response.doc('#comments-section .mod-hd i').text().strip())
        return {
            "douban_id": re_douban_id.group(1) if re_douban_id else None,
            "url": response.url,
            "directors_name": [x.text() for x in response.doc('a[rel="v:directedBy"]').items()],
            "writers": [x.text().strip() for x in response.doc('#info span:eq(1) span.attrs a').items()],
            #"casts": [x.text().strip() for x in response.doc('#info span.actor span.attrs a').items()],
            "genres": [x.text().strip() for x in response.doc('#info span[property|="v:genre"]').items()],
            "premiere": [x.text().strip() for x in response.doc('#info span[property|="v:initialReleaseDate"]').items()],
            "mins": mins,
            "language": language,
            "score_count": response.doc('#interest_sectl span[property|="v:votes"]').text().strip(),
            "rate": response.doc('#interest_sectl strong').text().strip(),
            "summary": response.doc('#link-report span[property|="v:summary"]').text().strip(),
            "watching_count": re_watching.group(1) if re_watching else None,
            "watched_count": re_watched.group(1) if re_watched else None,
            "want_to_watch_count": re_want_to_watch.group(1) if re_want_to_watch else None,
            "images": [x.attr('src') for x in response.doc('#related-pic img').items()],
            "same_like_ids": same_likes,
            "play_platforms": [x.text().strip() for x in response.doc('.gray_ad ul.bs li a.playBtn').items()],
            "is_free": [x.text().strip() for x in response.doc('.gray_ad ul.bs li span.buylink-price span').items()],
            "comments_count": re_comments_count.group(1) if re_comments_count.group(1) else '0',
            "hot_comments": comments,
            "poster": response.doc('#mainpic img').attr('src').strip(),
            "title": re_title.group(1) if re_title else None,
        }

    def on_result(self,result):
        if not result:
            return
        self.save_data(**result)
