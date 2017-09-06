#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2017-09-05 16:52:10
# Project: douban_movies_time

from pyspider.libs.base_handler import *
import re
from lxml import etree

class Handler(BaseHandler):
    crawl_config = {
        'itag': 'v002'
    }
    
    def __init__(self):
        self.DOU_BAN_MOVIE_HOT = 'http://movie.douban.com/j/search_subjects?type=movie&tag=%E7%83%AD%E9%97%A8&sort=time'

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('https://movie.douban.com/subject/27037043/?tag=%E7%BB%BC%E8%89%BA&from=gaia', callback=self.index_page, params={'page_limit' : 20, 'page_start' : 0}, save={'page_limit' : 20, 'page_start' : 0})

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        self.crawl('https://movie.douban.com/subject/27037043/?tag=%E7%BB%BC%E8%89%BA&from=gaia', callback=self.index_page, params={'page_limit' : 20, 'page_start' : 0}, save={'page_limit' : 20, 'page_start' : 0})
        return
        if len(response.json['subjects']) != 0:
            for item in response.json['subjects']:
                self.crawl(item['url'], callback=self.detail_page)
            
            self.crawl(self.DOU_BAN_MOVIE_HOT, callback=self.index_page, params={'page_limit' : response.save['page_limit'] + 20, 'page_start' : response.save['page_start'] + 20}, save={'page_limit' : response.save['page_limit'] + 20, 'page_start' : response.save['page_start'] + 20})


    @config(priority=2)
    def detail_page(self, response):
        html = etree.HTML(response.content)
        #result = etree.tostring(html)
        same_likes = []
        for x in response.doc('#recommendations dt a').items():
            print(x.attr('href'))
            str = x.attr('href')
            same_likes.append(re.match(r'(http|ftp|https):\/\/[\w+\-_]+(\.[\w\-_]+)+\/[a-zA-Z]+\/(\d+)', x.attr('href')).group(3))
        return {
            "url": response.url,
            "title3": [x.text() for x in response.doc('a[rel="v:directedBy"]').items()],
            "title2": response.doc('#info a[rel|="v:directedBy"]').text(),
            "title": '/'.join(name.text for name in html.xpath('//*[@id="info"]/span[1]/span[2]/a[@rel="v:directedBy"]')).strip(),
            "bianju": [x.text().strip() for x in response.doc('#info span:eq(1) span.attrs a').items()],
            "zhuyan": [x.text().strip() for x in response.doc('#info span.actor span.attrs a').items()],
            "genre": [x.text().strip() for x in response.doc('#info span[property|="v:genre"]').items()],
            "shoubo": [x.text().strip() for x in response.doc('#info span[property|="v:initialReleaseDate"]').items()],
            "pingfen_count": response.doc('#interest_sectl span[property|="v:votes"]').text().strip(),
            "rate": response.doc('#interest_sectl strong').text().strip(),
            "summary": response.doc('#link-report span[property|="v:summary"]').text().strip(),
            "onwatch": response.doc('#subject-others-interests .subject-others-interests-ft a:eq(0)').text().strip(),
            "watched": response.doc('#subject-others-interests .subject-others-interests-ft a:eq(1)').text().strip(),
            "wantwatch": response.doc('#subject-others-interests .subject-others-interests-ft a:eq(2)').text().strip(),
            "detail_images": [x.attr('src') for x in response.doc('#related-pic img').items()],
            "same_likes": same_likes,
        }


    # def on_result(self,result):
    #     if not result:
    #         return
    #     self.save_data(**result)
