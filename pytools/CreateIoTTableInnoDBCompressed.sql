SET GLOBAL innodb_file_per_table=1;
SET GLOBAL innodb_file_format=Barracuda;
DROP TABLE if EXISTS `IoTMessages`;
CREATE TABLE `IoTMessages` (
  `record_id` int(11) NOT NULL AUTO_INCREMENT,
  `timestamp` bigint NOT NULL,
  `source` binary(16) NOT NULL,
  `message` varchar(255) NOT NULL,
  PRIMARY KEY `record_id` (`record_id`),
  KEY `timestamp` (`timestamp`),
  KEY `source` (`source`)
) ROW_FORMAT=COMPRESSED ENGINE=InnoDB;
