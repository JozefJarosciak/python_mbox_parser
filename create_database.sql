

CREATE TABLE IF NOT EXISTS `all_files` (
                                           `file_name` varchar(255) DEFAULT NULL,
                                           `current` int(11) DEFAULT NULL,
                                           `total` int(11) DEFAULT NULL,
                                           `processing` tinyint(1) unsigned zerofill NOT NULL,
                                           UNIQUE KEY `file_name` (`file_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS `from_contacts` (
                                               `id` int(11) NOT NULL AUTO_INCREMENT,
                                               `from_name` varchar(255) DEFAULT NULL,
                                               `from_email` varchar(255) DEFAULT NULL,
                                               PRIMARY KEY (`id`),
                                               UNIQUE KEY `from_email` (`from_email`),
                                               KEY `from_contacts_idx_id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;



CREATE TABLE IF NOT EXISTS `message_body` (
                                              `id` int(11) NOT NULL AUTO_INCREMENT,
                                              `body` mediumtext COLLATE utf8mb4_bin DEFAULT NULL,
                                              PRIMARY KEY (`id`),
                                              KEY `message_body_idx_id` (`id`),
                                              FULLTEXT KEY `body` (`body`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;



CREATE TABLE IF NOT EXISTS `message_ids` (
                                             `id` int(11) NOT NULL AUTO_INCREMENT,
                                             `messageid` varchar(255) NOT NULL,
                                             PRIMARY KEY (`id`),
                                             UNIQUE KEY `messageid` (`messageid`),
                                             KEY `messageid-index` (`messageid`),
                                             KEY `message_ids_idx_id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS `message_subject_lines` (
                                                       `id` int(11) NOT NULL AUTO_INCREMENT,
                                                       `subject` mediumtext NOT NULL,
                                                       PRIMARY KEY (`id`),
                                                       KEY `message_subject_line_idx_id` (`id`),
                                                       FULLTEXT KEY `subject` (`subject`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



CREATE TABLE IF NOT EXISTS `newsgroup_ids` (
                                               `id` int(11) NOT NULL AUTO_INCREMENT,
                                               `newsgroupname` varchar(255) NOT NULL,
                                               PRIMARY KEY (`id`),
                                               UNIQUE KEY `newsgroupname` (`newsgroupname`),
                                               KEY `newsgroupname-index` (`newsgroupname`),
                                               KEY `newsgroup_ids_idx_id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;

CREATE TABLE IF NOT EXISTS `message_references` (
                                                    `messageid` int(11) NOT NULL,
                                                    `reference` int(11) NOT NULL,
                                                    KEY `FK_message_references_message_ids_2` (`reference`),
                                                    KEY `messageid_reference` (`messageid`,`reference`),
                                                    KEY `message_references_idx_reference` (`reference`),
                                                    KEY `message_references_idx_messageid` (`messageid`),
                                                    CONSTRAINT `FK_message_references_message_ids` FOREIGN KEY (`messageid`) REFERENCES `message_ids` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
                                                    CONSTRAINT `FK_message_references_message_ids_2` FOREIGN KEY (`reference`) REFERENCES `message_ids` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;




CREATE TABLE IF NOT EXISTS `all_messages` (
                                              `messageid` int(11) NOT NULL,
                                              `from_contact` int(11) NOT NULL,
                                              `date_time` datetime DEFAULT NULL,
                                              `has_reference` tinyint(1) DEFAULT NULL,
                                              `subject` int(11) NOT NULL,
                                              `body` int(11) NOT NULL,
                                              `processed` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
                                              UNIQUE KEY `messageid` (`messageid`,`from_contact`),
                                              KEY `messageid2` (`messageid`),
                                              KEY `from_contact` (`from_contact`),
                                              KEY `date_time` (`date_time`),
                                              KEY `FK_all_messages_message_subject_lines` (`subject`),
                                              KEY `FK_all_messages_message_body` (`body`),
                                              KEY `has_reference` (`has_reference`),
                                              CONSTRAINT `FK.from_contact2` FOREIGN KEY (`from_contact`) REFERENCES `from_contacts` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
                                              CONSTRAINT `FK.messageid2` FOREIGN KEY (`messageid`) REFERENCES `message_ids` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
                                              CONSTRAINT `FK_all_messages_message_body` FOREIGN KEY (`body`) REFERENCES `message_body` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;
