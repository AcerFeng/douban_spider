#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2017-09-05 16:52:10
# Project: douban_movies_time

from pyspider.libs.base_handler import *
import re

class Handler(BaseHandler):
    crawl_config = {
        'itag': 'v003'
    }
    
    def __init__(self):
        self.DOU_BAN_MOVIE_HOT = 'http://movie.douban.com/j/search_subjects?type=movie&tag=%E7%83%AD%E9%97%A8&sort=time'

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('https://movie.douban.com/subject/26776350/?tag=%E7%83%AD%E9%97%A8&from=gaia_video', callback=self.index_page, params={'page_limit' : 20, 'page_start' : 0}, save={'page_limit' : 20, 'page_start' : 0})

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        self.crawl('https://movie.douban.com/subject/26776350/?tag=%E7%83%AD%E9%97%A8&from=gaia_video', callback=self.detail_page, params={'page_limit' : 20, 'page_start' : 0}, save={'page_limit' : 20, 'page_start' : 0})
        return
        if len(response.json['subjects']) != 0:
            for item in response.json['subjects']:
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
                'id': re_id.group(1) if re_id else None,
                'status': item.find('.comment-info span:eq(0)').text().strip(),
                'score': item.find('.comment-info span:eq(1)').attr('title').strip(),
                'time' : item.find('.comment-info span:eq(2)').text().strip(),
            }
            comments.append(user_comment)
     
        re_mins = re.match(r'.+片长: (\w+[\u4e00-\u9fa5]+) ',response.doc('#info').text())
        mins = re_mins.group(1) if re_mins else None
        re_language = re.match(r'.+语言: ([\u4e00-\u9fa5 \/]+) ',response.doc('#info').text())
        language = re_language.group(1).strip() if re_language else None
        re_douban_id = re.match(r'.+\/(\d+)\/', response.url)
        re_watching = re.match(r'.*?(\d+)人在看',response.doc('#subject-others-interests .subject-others-interests-ft a').text().strip())
        re_watched = re.match(r'.*?(\d+)人看过',response.doc('#subject-others-interests .subject-others-interests-ft a').text().strip())
        re_want_to_watch = re.match(r'.*?(\d+)人想看',response.doc('#subject-others-interests .subject-others-interests-ft a').text().strip())
        re_comments_count = re.match(r'[\u4e00-\u9fa5 ]+(\d+)[\u4e00-\u9fa5 ]+', response.doc('#comments-section span.pl a').text().strip())
        
        return {
            "douban_id": re_douban_id.group(1) if re_douban_id else None,
            "url": response.url,
            "directors": [x.text() for x in response.doc('a[rel="v:directedBy"]').items()],
            "writers": [x.text().strip() for x in response.doc('#info span:eq(1) span.attrs a').items()],
            #"casts": [x.text().strip() for x in response.doc('#info span.actor span.attrs a').items()],
            "genres": [x.text().strip() for x in response.doc('#info span[property|="v:genre"]').items()],
            "premiere": [x.text().strip() for x in response.doc('#info span[property|="v:initialReleaseDate"]').items()],
            "mins": mins,
            "language": language,
            "score_count": response.doc('#interest_sectl span[property|="v:votes"]').text().strip(),
            "rate": response.doc('#interest_sectl strong').text().strip(),
            "summary": response.doc('#link-report span[property|="v:summary"]').text().strip(),
            "watching": re_watching.group(1) if re_watching else None,
            "watched": re_watched.group(1) if re_watched else None,
            "want_to_watch": re_want_to_watch.group(1) if re_want_to_watch else None,
            "images": [x.attr('src') for x in response.doc('#related-pic img').items()],
            "same_likes": same_likes,
            "play_platform": [x.text().strip() for x in response.doc('.gray_ad ul.bs li a.playBtn').items()],
            "is_free": [x.text().strip() for x in response.doc('.gray_ad ul.bs li span.buylink-price span').items()],
            "comments_count": re_comments_count.group(1) if re_comments_count.group(1) else '0',
            "hot_comments": comments,
        }



    # def on_result(self,result):
    #     if not result:
    #         return
    #     self.save_data(**result)
