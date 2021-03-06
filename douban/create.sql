CREATE TABLE `douban_video` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `douban_id` varchar(15) NOT NULL DEFAULT '',
  `name` varchar(100) NOT NULL DEFAULT '',
  `url` varchar(100) NOT NULL DEFAULT '',
  `image_large` varchar(150) DEFAULT '',
  `rate` varchar(4) DEFAULT NULL,
  `is_new` tinyint(1) NOT NULL DEFAULT '0',
  `summary` varchar(2000) DEFAULT NULL,
  `playable` tinyint(1) DEFAULT NULL,
  `star` varchar(5) DEFAULT NULL,
  `subtype` int(3) DEFAULT '1',
  `json_source` json DEFAULT NULL,
  `comments_count` varchar(10) DEFAULT NULL,
  `directors_name` varchar(1000) DEFAULT NULL,
  `genres` varchar(30) DEFAULT NULL,
  `images` varchar(550) DEFAULT NULL,
  `is_free` varchar(150) DEFAULT NULL,
  `language` varchar(50) DEFAULT NULL,
  `mins` varchar(100) DEFAULT NULL,
  `play_platforms` varchar(90) DEFAULT NULL,
  `premiere` varchar(500) DEFAULT NULL,
  `same_like_ids` varchar(150) DEFAULT NULL,
  `score_count` varchar(10) DEFAULT NULL,
  `want_to_watch_count` varchar(10) DEFAULT NULL,
  `watched_count` varchar(10) DEFAULT NULL,
  `watching_count` varchar(10) DEFAULT NULL,
  `writers` varchar(500) DEFAULT NULL,
  `casts` varchar(1000) DEFAULT NULL,
  `casts_ids` varchar(500) DEFAULT NULL,
  `celebrities_images` varchar(900) DEFAULT NULL,
  `celebrities_name` varchar(180) DEFAULT NULL,
  `celebrities_id` varchar(100) DEFAULT NULL,
  `celebrities_role` varchar(300) DEFAULT NULL,
  `aka` varchar(300) DEFAULT NULL,
  `create_time` datetime DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  `rating_per5` varchar(7) DEFAULT NULL,
  `rating_per4` varchar(7) DEFAULT NULL,
  `rating_per3` varchar(7) DEFAULT NULL,
  `rating_per2` varchar(7) DEFAULT NULL,
  `rating_per1` varchar(7) DEFAULT NULL,
  `current_season` varchar(4) DEFAULT NULL,
  `episodes_count` varchar(5) DEFAULT NULL,
  `countries` varchar(50) DEFAULT NULL,
  `douban_tags` varchar(300) DEFAULT NULL,
  `rating_betterthan` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `douban_id` (`douban_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `douban_user` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `douban_user_id` varchar(20) DEFAULT NULL,
  `douban_url` varchar(100) DEFAULT NULL,
  `douban_name` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `douban_comment` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` varchar(20) NOT NULL DEFAULT '',
  `status` varchar(5) DEFAULT NULL,
  `time` varchar(30) DEFAULT NULL,
  `like_count` varchar(10) DEFAULT NULL,
  `content` varchar(300) DEFAULT NULL,
  `douban_id` varchar(15) DEFAULT NULL,
  `score` varchar(10) DEFAULT NULL,
  `user_name` varchar(15) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;