CREATE TABLE `purchaser_dim` (
    `purchaser_id`  int(4) unsigned NOT NULL,
    `name` varchar(64) NOT NULL  COMMENT '姓名',
    `gender` varchar(32) NOT NULL COMMENT '性别',
    `age` int(4) NOT NULL COMMENT '年龄',
    PRIMARY KEY (`purchaser_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='购买者_维表';

-- 插入10条数据
INSERT INTO `purchaser_dim` VALUES (1,'tom','male','16');
INSERT INTO `purchaser_dim` VALUES (2,'mack','male','17');
INSERT INTO `purchaser_dim` VALUES (3,'jack','male','16');
INSERT INTO `purchaser_dim` VALUES (4,'marry','female','15');
INSERT INTO `purchaser_dim` VALUES (5,'bob','male','18');
INSERT INTO `purchaser_dim` VALUES (6,'john','male','16');
INSERT INTO `purchaser_dim` VALUES (7,'lucy','female','15');
INSERT INTO `purchaser_dim` VALUES (8,'eva','female','18');
INSERT INTO `purchaser_dim` VALUES (9,'grace','female','17');
INSERT INTO `purchaser_dim` VALUES (10,'anna','female','16');


