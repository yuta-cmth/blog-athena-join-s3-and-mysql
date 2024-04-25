DROP TABLE IF EXISTS `blog_athena_join_s3_mysql`.`master_dimension`;

CREATE TABLE IF NOT EXISTS `blog_athena_join_s3_mysql`.`master_dimension` (
  `key` VARCHAR(256) NOT NULL,
  `name` VARCHAR(256) NOT NULL,
  PRIMARY KEY (`key`));

INSERT INTO `blog_athena_join_s3_mysql`.`master_dimension` (`key`, `name`) 
VALUES ('tokyo', '東京'), ('osaka', '大阪'), ('nagoya', '名古屋'), ('fukuoka', '福岡');
