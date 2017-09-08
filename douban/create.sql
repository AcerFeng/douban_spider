CREATE TABLE `douban_video` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `video_id` varchar(15) NOT NULL DEFAULT '',
  `title` varchar(30) NOT NULL DEFAULT '',
  `url` varchar(100) NOT NULL DEFAULT '',
  `image_large` varchar(150) DEFAULT '',
  `rate` varchar(4) DEFAULT NULL,
  `is_new` tinyint(1) DEFAULT NULL,
  `summary` varchar(500) DEFAULT NULL,
  `playable` tinyint(1) DEFAULT NULL,
  `star` varchar(5) DEFAULT NULL,
  `subtype` int(3) NOT NULL DEFAULT '1',
  `json_source` json DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `douban_id` (`video_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `douban_user` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `douban_user_id` varchar(20) DEFAULT NULL,
  `douban_url` varchar(100) DEFAULT NULL,
  `douban_name` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `douban_comments` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` varchar(20) NOT NULL DEFAULT '',
  `status` varchar(5) DEFAULT NULL,
  `time` varchar(18) DEFAULT NULL,
  `like_count` varchar(10) DEFAULT NULL,
  `content` varchar(150) DEFAULT NULL,
  `video_id` varchar(15) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;