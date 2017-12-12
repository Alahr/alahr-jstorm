/*
Navicat MySQL Data Transfer

Source Server         : sunyan
Source Server Version : 50517
Source Host           : localhost:3306
Source Database       : jstorm

Target Server Type    : MYSQL
Target Server Version : 50517
File Encoding         : 65001

Date: 2017-12-12 11:39:24
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for `animal`
-- ----------------------------
DROP TABLE IF EXISTS `animal`;
CREATE TABLE `animal` (
`a_no`  varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`p_no`  varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`a_name`  varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`a_type`  varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`a_age`  tinyint(2) NULL DEFAULT NULL 
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci

;

-- ----------------------------
-- Records of animal
-- ----------------------------
BEGIN;
INSERT INTO `animal` VALUES ('a_001', 'p_001', 'Caty', 'cat', '2'), ('a_002', 'p_001', 'Donge', 'dog', '1'), ('a_003', 'p_003', 'Sharl', 'horse', '5'), ('a_004', 'p_004', 'Pero', 'pig', '3');
COMMIT;

-- ----------------------------
-- Table structure for `person`
-- ----------------------------
DROP TABLE IF EXISTS `person`;
CREATE TABLE `person` (
`p_no`  varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`p_name`  varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`p_age`  tinyint(3) NULL DEFAULT NULL ,
`p_address`  varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL 
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci

;

-- ----------------------------
-- Records of person
-- ----------------------------
BEGIN;
INSERT INTO `person` VALUES ('p_001', 'Tom', '23', 'shanghai'), ('p_002', 'Dave', '25', 'beijing'), ('p_003', 'Jim', '21', 'Landon'), ('p_004', 'Sun', '19', 'NewYork');
COMMIT;
