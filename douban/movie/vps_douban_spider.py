#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2017-09-12 17:24:16
# Project: douban_tagList2detail

from pyspider.libs.base_handler import *
import pymysql
import re

class Handler(BaseHandler):
    headers= {
    "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding":"gzip, deflate, sdch",
    "Accept-Language":"zh-CN,zh;q=0.8",
    "Cache-Control":"max-age=0",
    "Connection":"keep-alive",
    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36"
    }
    crawl_config = {
        'itag': 'v002',
        'headers': headers,
        'proxy': 'https://61.160.208.222:8080',
        'cookies': {
          '__utmb':'30149280.0.10.1505289762',
          'dbcl2': '162930320:6IWEbIKitho'
        },
    }
    
    def __init__(self):
        self.DOU_BAN_MOVIE_HOT = 'http://movie.douban.com/j/search_subjects?type=movie&tag=%E7%83%AD%E9%97%A8&sort=time'
        
        self.pattern_current_season = re.compile(r'.+?季数:</span>(.+?)<')
        self.pattern_episodes_count = re.compile(r'.+?集数:</span>(.+?)<')
        self.pattern_countries = re.compile(r'.+地区:<\/span>(.+?)<')
        self.pattern_writers = re.compile(r'.编剧</span>: <span class="attrs">(.+?)</span>')
        self.pattern_aka = re.compile(r'.+?又名:</span>(.+)<')
        self.pattern_mins = re.compile(r'.+片长: (\w+[\u4e00-\u9fa5]+) ')
        self.pattern_language = re.compile(r'.+语言:<\/span>(.+?)<')
        self.pattern_douban_id = re.compile(r'.+/(\d+)/')
        self.pattern_watching = re.compile(r'.*?(\d+)人在看')
        self.pattern_watched = re.compile(r'.*?(\d+)人看过')
        self.pattern_want_to_watch = re.compile(r'.*?(\d+)人想看')
        self.pattern_comments_count = re.compile(r'全部 (\d+) 条')
        self.pattern_title = re.compile(r'(.+?) \(豆瓣\)')
        self.pattern_subtype = re.compile(r'.+的影评 ·.+')
        
        self.pattern_cele_image = re.compile(r'.+\((.+)\)')
        self.pattern_cele_id = re.compile(r'.+\/(.+)\/$')

        self.pattern_casts_id = re.compile(r'.+?/(\d+)/')

        self.pattern_same_like_id = re.compile(r'.+\/(\d+)\/')
        self.pattern_same_like_movie_url = re.compile(r'(.+?)\?')

        self.pattern_comments_id = re.compile(r'.+\/(\w+)\/')

        try:
            self.connect = pymysql.connect(host='localhost', port=3306, user='root', passwd='QINLANfeng2!', db='yingdong', charset='utf8mb4')
            self.cursor = self.connect.cursor()
        except Exception as e:
            print('Cannot Connect To Mysql!/n', e)
            raise e

    

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl(self.DOU_BAN_MOVIE_HOT, callback=self.index_page, params={'page_limit' : 20, 'page_start' : 0}, save={'page_limit' : 20, 'page_start' : 0})

    @config(age=60 * 24 * 60 * 60)
    def index_page(self, response):
        data_list = None
        if 'subjects' in response.json:
            data_list = response.json['subjects']
        elif 'data' in response.json:
            data_list = response.json['data']

        
        if data_list and len(data_list) != 0:
            for item in data_list:
                re_douban_id = self.pattern_same_like_id.match(item['url'])
                if re_douban_id and self.is_exist(re_douban_id.group(1)):
                    continue
                self.crawl(item['url'], callback=self.detail_page)
            self.crawl(self.DOU_BAN_MOVIE_HOT, callback=self.index_page, params={'page_limit' : response.save['page_limit'] + 20, 
                                                                                'page_start' : response.save['page_start'] + 20}, 
                                                                        save={'page_limit' : response.save['page_limit'] + 20, 
                                                                                'page_start' : response.save['page_start'] + 20})


    @config(priority=2)
    def detail_page(self, response):
        same_likes = []
        for x in response.doc('#recommendations dt a').items():
            re_same = self.pattern_same_like_id.match(x.attr('href'))
            re_url = self.pattern_same_like_movie_url.match(x.attr('href'))
            if re_same:
                same_likes.append(re_same.group(1))
            if re_url and re_same and not self.is_exist(re_same.group(1)):
                self.crawl(re_url.group(1), callback=self.detail_page)
            
            
        comments = []
        for item in response.doc('#hot-comments .comment-item .comment').items():
            re_id = self.pattern_comments_id.match(item.find('.comment-info a').attr('href'))
            user_comment = {
                'like_count' : item.find('span.comment-vote span.votes').text(),
                'user_name' : item.find('.comment-info a').text(),
                'url': item.find('.comment-info a').attr('href'),
                'user_id': re_id.group(1) if re_id else None,
                'status': item.find('.comment-info span:eq(0)').text(),
                'score': item.find('.comment-info span.rating').attr('title'),
                'time' : item.find('.comment-info span.comment-time').attr('title'),
                'content': item.find('p').text().strip(),
            }
            comments.append(user_comment)
        
        re_countries = self.pattern_countries.search(response.doc('#info').html().encode('utf-8'))
        re_aka = self.pattern_aka.search(response.doc('#info').html().strip().encode('utf-8'))
        re_mins = self.pattern_mins.match(response.doc('#info').text())
        mins = re_mins.group(1) if re_mins else response.doc('#info span[property|="v:runtime"]').text().strip()
        re_language = self.pattern_language.search(response.doc('#info').html().encode('utf-8'))
        language = re_language.group(1).strip() if re_language else None
        re_douban_id = self.pattern_douban_id.match(response.url)
        re_watching = self.pattern_watching.match(response.doc('#subject-others-interests .subject-others-interests-ft a').text().strip().encode('utf-8'))
        re_watched = self.pattern_watched.match(response.doc('#subject-others-interests .subject-others-interests-ft a').text().strip().encode('utf-8'))
        re_want_to_watch = self.pattern_want_to_watch.match(response.doc('#subject-others-interests .subject-others-interests-ft a').text().strip().encode('utf-8'))
        re_comments_count = self.pattern_comments_count.match(response.doc('#comments-section span.pl a').text().strip().encode('utf-8'))
        re_subtype = self.pattern_subtype.match(response.doc('section.reviews header h2').text().strip().encode('utf-8'))
        current_season = None
        episodes_count = None
        
        if not re_subtype:
            re_current_season = self.pattern_current_season.search(response.doc('#info').html().encode('utf-8'))
            re_episodes_count = self.pattern_episodes_count.search(response.doc('#info').html().encode('utf-8'))
            episodes_count = re_episodes_count.group(1) if re_episodes_count else None
            
            if len(response.doc('#season option[selected="selected"]').text().strip()):
                current_season = response.doc('#season option[selected="selected"]').text()
            elif re_current_season:
                current_season = re_current_season.group(1)
        
        celebrities_images = []
        celebrities_name = []
        celebrities_url = []
        celebrities_id = []
        celebrities_role = []
        for celebrity in response.doc('#celebrities ul li').items():
            re_cele_image = self.pattern_cele_image.match(celebrity.find('a .avatar').attr('style') if celebrity.find('a .avatar').attr('style') else '')
            re_cele_id = self.pattern_cele_id.match(celebrity.find('.info .name a').attr('href') if celebrity.find('.info .name a').attr('href') else '')
            celebrities_images.append(re_cele_image.group(1) if re_cele_image else '')
            celebrities_name.append(celebrity.find('.info .name a').text().strip() if celebrity.find('.info .name a').text().strip() else '')
            celebrities_url.append(celebrity.find('.info .name a').attr('href').strip() if celebrity.find('.info .name a').attr('href') else '')
            celebrities_id.append(re_cele_id.group(1) if re_cele_id else '')
            celebrities_role.append(celebrity.find('.info .role').text().strip() if celebrity.find('.info .role').text().strip() else '')

        play_platforms = [x.text().strip() for x in response.doc('.gray_ad ul.bs li a.playBtn').items()]
        casts_ids = []
        for item in response.doc('#info span.actor span.attrs a').items():
            re_casts_id = self.pattern_casts_id.match(item.attr('href'))
            casts_ids.append(re_casts_id.group(1) if re_casts_id else '')
        title = response.doc('title').text().strip().encode('utf-8').replace(' (豆瓣)', '')
        re_writers = self.pattern_writers.search(response.doc('#info').html().encode('utf-8'))
        writers = None
        if re_writers:
            writers_doc = pq(unicode(re_writers.group(1),"utf-8"))
            writers = writers_doc('a').text().replace(" ", ",")
        
        
        return {
            "douban_id": re_douban_id.group(1) if re_douban_id else None,
            "url": response.url,
            "directors_name": [x.text() for x in response.doc('a[rel="v:directedBy"]').items()],
            "writers": writers,
            "casts": [x.text().strip() for x in response.doc('#info span.actor span.attrs a').items()],
            "casts_url": [x.attr('href') for x in response.doc('#info span.actor span.attrs a').items()],
            "casts_ids" : casts_ids,
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
            "play_platforms": play_platforms,
            "is_free": [x.text().strip() for x in response.doc('.gray_ad ul.bs li span.buylink-price span').items()],
            "comments_count": re_comments_count.group(1) if re_comments_count else '0',
            "hot_comments": comments,
            "image_large": response.doc('#mainpic img').attr('src').strip(),
            "title": title if title else None,
            "subtype": 1 if re_subtype else 2,
            "celebrities_images": celebrities_images,
            "celebrities_name": celebrities_name,
            "celebrities_url": celebrities_url,
            "celebrities_id": celebrities_id,
            "celebrities_role": celebrities_role,
            "playable": len(play_platforms)>0,
            "aka" : re_aka.group(1).strip().split(' / ') if re_aka else [],
            "rating_per5": response.doc('#interest_sectl .ratings-on-weight .item:eq(0) .rating_per').text().strip(),
            "rating_per4": response.doc('#interest_sectl .ratings-on-weight .item:eq(1) .rating_per').text().strip(),
            "rating_per3": response.doc('#interest_sectl .ratings-on-weight .item:eq(2) .rating_per').text().strip(),
            "rating_per2": response.doc('#interest_sectl .ratings-on-weight .item:eq(3) .rating_per').text().strip(),
            "rating_per1": response.doc('#interest_sectl .ratings-on-weight .item:eq(4) .rating_per').text().strip(),
            "current_season": current_season,
            "episodes_count": episodes_count,
            "countries": re_countries.group(1).strip().split(' / ') if re_countries else [],
            "douban_tags": [x.text() for x in response.doc('.tags .tags-body a').items()],
            "rating_betterthan": response.doc('#interest_sectl .rating_betterthan').text().strip(),
        }
    
    def on_result(self,result):
        if not result:
            return
        self.save_data(**result)

    
    def is_exist(self, douban_id=None):
        if not douban_id:
            return True
        return self.cursor.execute('select id from douban_video where douban_id=%s', douban_id)
    
    def save_comments(self, comments, douban_id):
        if comments and len(comments)>0 and douban_id:
            for comment in comments:
                    try:
                        cursor = self.connect.cursor()
                        cursor.execute('select count(*) from douban_comment where user_id=%s and douban_id=%s', (comment['user_id'],douban_id))
                        result = cursor.fetchone()
                        if result[0]:
                            # 更新操作
                            sql = '''
                                update douban_comment set 
                                status=%s, 
                                time=%s, 
                                like_count=%s,
                                content=%s,
                                score=%s
                                where user_id=%s and douban_id=%s'''

                            cursor.execute(sql, (comment['status'], 
                                        comment['time'], 
                                        comment['like_count'], 
                                        comment['content'], 
                                        comment['score'],
                                        comment['user_id'], 
                                        douban_id))
                        else:
                            # 插入
                            sql = '''
                                insert into douban_comment
                                (user_id, 
                                status,
                                score, 
                                time, 
                                like_count, 
                                content, 
                                douban_id,
                                user_name) 
                                values (%s, %s, %s, %s, %s, %s, %s, %s)
                            '''
                            cursor.execute(sql, (comment['user_id'], 
                                                comment['status'], 
                                                comment['score'], 
                                                comment['time'], 
                                                comment['like_count'], 
                                                comment['content'], 
                                                douban_id,
                                                comment['user_name']))

                        self.connect.commit()
                    except Exception as e:
                        self.connect.rollback()
                        raise e

    def save_data(self, **kw):
        
        try:
            cursor = self.connect.cursor()
            cursor.execute('select count(*) from douban_video where douban_id=%s', (kw['douban_id'],))
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
                    writers=%s,
                    casts=%s,
                    casts_ids=%s,
                    celebrities_images=%s,
                    celebrities_name=%s,
                    celebrities_id=%s,
                    celebrities_role=%s,
                    aka=%s,
                    rating_per5=%s,
                    rating_per4=%s,
                    rating_per3=%s,
                    rating_per2=%s,
                    rating_per1=%s,
                    playable=%s,
                    current_season=%s,
                    episodes_count=%s,
                    countries=%s,
                    douban_tags=%s,
                    rating_betterthan=%s 
                    where douban_id=%s'''
                cursor.execute(sql, (kw['comments_count'], 
                                    ','.join(kw['directors_name']), 
                                    ','.join(kw['genres']), 
                                    ','.join(kw['images']), 
                                    ','.join(kw['is_free']), 
                                    kw['language'],
                                    kw['mins'],
                                    ','.join(kw['play_platforms']),
                                    ','.join(kw['premiere']),
                                    ','.join(kw['same_like_ids']),
                                    kw['score_count'],
                                    kw['summary'],
                                    kw['want_to_watch_count'],
                                    kw['watched_count'],
                                    kw['watching_count'],
                                    kw['writers'],
                                    ','.join(kw['casts']),
                                    ','.join(kw['casts_ids']),
                                    ','.join(kw['celebrities_images']),
                                    ','.join(kw['celebrities_name']),
                                    ','.join(kw['celebrities_id']),
                                    ','.join(kw['celebrities_role']),
                                    ','.join(kw['aka']),
                                    kw['rating_per5'],
                                    kw['rating_per4'],
                                    kw['rating_per3'],
                                    kw['rating_per2'],
                                    kw['rating_per1'],
                                    kw['playable'],
                                    kw['current_season'],
                                    kw['episodes_count'],
                                    ','.join(kw['countries']),
                                    ','.join(kw['douban_tags']),
                                    kw['rating_betterthan'],
                                    kw['douban_id']))

                self.save_comments(kw['hot_comments'], kw['douban_id'])
                
                
                
            else:
                # 插入
                sql = '''
                    insert into douban_video
                    (comments_count, 
                    directors_name, 
                    douban_id,
                    genres,
                    images,
                    is_free,
                    language,
                    mins,
                    play_platforms,
                    image_large,
                    premiere,
                    rate,
                    same_like_ids,
                    score_count,
                    summary,
                    name,
                    url,
                    want_to_watch_count,
                    watched_count,
                    watching_count,
                    writers,
                    subtype,
                    casts,
                    casts_ids,
                    celebrities_images,
                    celebrities_name,
                    celebrities_id,
                    celebrities_role,
                    aka,
                    rating_per5,
                    rating_per4,
                    rating_per3,
                    rating_per2,
                    rating_per1,
                    playable,
                    current_season,
                    episodes_count,
                    countries,
                    douban_tags,
                    rating_betterthan) 
                    values (%s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s)
                '''
                cursor.execute(sql, (kw['comments_count'], 
                                    ','.join(kw['directors_name']), 
                                    kw['douban_id'], 
                                    ','.join(kw['genres']), 
                                    ','.join(kw['images']), 
                                    ','.join(kw['is_free']), 
                                    kw['language'],
                                    kw['mins'],
                                    ','.join(kw['play_platforms']),
                                    kw['image_large'],
                                    ','.join(kw['premiere']),
                                    kw['rate'],
                                    ','.join(kw['same_like_ids']),
                                    kw['score_count'],
                                    kw['summary'],
                                    kw['title'],
                                    kw['url'],
                                    kw['want_to_watch_count'],
                                    kw['watched_count'],
                                    kw['watching_count'],
                                    kw['writers'],
                                    kw['subtype'],
                                    ','.join(kw['casts']),
                                    ','.join(kw['casts_ids']),
                                    ','.join(kw['celebrities_images']),
                                    ','.join(kw['celebrities_name']),
                                    ','.join(kw['celebrities_id']),
                                    ','.join(kw['celebrities_role']),
                                    ','.join(kw['aka']),
                                    kw['rating_per5'],
                                    kw['rating_per4'],
                                    kw['rating_per3'],
                                    kw['rating_per2'],
                                    kw['rating_per1'],
                                    kw['playable'],
                                    kw['current_season'],
                                    kw['episodes_count'],
                                    ','.join(kw['countries']),
                                    ','.join(kw['douban_tags']),
                                    kw['rating_betterthan']))
                self.save_comments(kw['hot_comments'], kw['douban_id'])

            self.connect.commit()

        except Exception as e:
            self.connect.rollback()
            raise e
            