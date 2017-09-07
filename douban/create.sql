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