CREATE PROCEDURE `showself_statistics_channel_non_added_proc`()
    COMMENT '渠道非新增用户状态统计'
BEGIN
  DECLARE v_done 	  			  			INT DEFAULT 0;	#变量
  DECLARE p_id 								INT DEFAULT 0; 	#主键
  DECLARE p_start_time 						INT DEFAULT 0; 	#开始时间
  DECLARE p_end_time 						INT DEFAULT 0;	#结束时间
  DECLARE p_period 	  						INT DEFAULT 0;	#执行周期
  DECLARE p_target_table    				VARCHAR(250) DEFAULT '';  #生成数据的表
  DECLARE p_target_new_table    			VARCHAR(250) DEFAULT '';  #分表表
  DECLARE p_sql    							VARCHAR(1024) DEFAULT ''; #sql

	#声明游标
	DECLARE v_new_statistics_cursor CURSOR FOR
	SELECT id,start_time,end_time,period,target_table FROM showself_statistics_task_def 
	WHERE proc_name='showself_statistics_channel_non_added_proc' AND UNIX_TIMESTAMP() >=end_time AND `status`=1;


  #############################################################################
  #                           生成渠道非新增用户状态数据
  #############################################################################
  -- 声明游标的异常处理，设置一个终止标记 
  DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET v_done=1; 
	
  SET v_done = 0; ## 
	
  OPEN v_new_statistics_cursor;
	
  REPEAT
    FETCH v_new_statistics_cursor INTO p_id,p_start_time,p_end_time,p_period,p_target_table ;
    IF NOT v_done THEN
			
			#####清空临时表
			truncate  showself_statistics_non_added_user_sum_tmp;
			truncate  showself_statistics_non_added_pure_visitor_sum_tmp;
			truncate  showself_statistics_non_added_register_sum_tmp;
			truncate  showself_statistics_non_added_pure_visitor_register_sum_tmp;
			truncate  showself_statistics_non_added_pure_visitor_qq_sum_tmp;
			truncate  showself_statistics_non_added_visitor_sum_tmp;
			truncate  showself_statistics_non_added_visitor_register_sum_tmp;
			truncate  showself_statistics_non_added_visitor_qq_sum_tmp;
			truncate  showself_statistics_non_added_recharge_user_sum_tmp;
			truncate  showself_statistics_non_added_chat_user_sum_tmp;
			truncate  showself_statistics_non_added_user_chat_sum_tmp;
			truncate  showself_statistics_non_added_flower_user_sum_tmp;
			truncate  showself_statistics_non_added_user_flower_sum_tmp;
			truncate  showself_statistics_non_added_md5_pass_num_tmp;	## Added 2015.03.27 for MD5 verification.
			truncate  showself_statistics_non_added_md5_fail_num_tmp;	## Added 2015.03.27 for MD5 verification.
			
			#####准备在线游客临时表  showself_statistics_wild_online_visitor_
			SET @p_sql := CONCAT("DROP TABLE IF EXISTS `showself_statistics_wild_online_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"`;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

			SET @p_sql := CONCAT("CREATE TABLE `showself_statistics_wild_online_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"` (
				  `id` int(10) NOT NULL AUTO_INCREMENT COMMENT '主键id',
				  `idfa` varchar(50) NOT NULL COMMENT '设备token',
				  `channel_id` varchar(50) NOT NULL COMMENT '渠道id',
				  `device_type` int(10) NOT NULL DEFAULT '0' COMMENT '设备类型',
				  PRIMARY KEY (`id`),
				  KEY `idfa` (`idfa`),
				  KEY `channel_id` (`channel_id`)
				) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='用户在线游客数据临时表';");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

			SET @p_sql := CONCAT("INSERT INTO showself_statistics_wild_online_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," 
				SELECT NULL,idfa,channel_id,device_type FROM showself_statistics_wild_online_",FROM_UNIXTIME(p_start_time,'%Y%m')," 
				WHERE dateline between ",p_start_time," AND ",p_end_time," AND uid = 3000006 
				GROUP BY idfa;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			
			#####准备在线注册临时表  showself_statistics_wild_online_register_
			SET @p_sql := CONCAT("DROP TABLE IF EXISTS `showself_statistics_wild_online_register_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"`;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

			SET @p_sql := CONCAT("CREATE TABLE `showself_statistics_wild_online_register_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"` (
				  `id` int(10) NOT NULL AUTO_INCREMENT COMMENT '主键id',
				  `uid` int(10) NOT NULL DEFAULT '0' COMMENT '用户uid',
				  `idfa` varchar(50) NOT NULL COMMENT '设备token',
				  `channel_id` varchar(50) NOT NULL COMMENT '渠道id',
				  `device_type` int(10) NOT NULL DEFAULT '0' COMMENT '设备类型',
				  PRIMARY KEY (`id`),
				  KEY `uid` (`uid`),
				  KEY `idfa` (`idfa`),
				  KEY `channel_id` (`channel_id`)
				) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='用户在线注册数据临时表';");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

			SET @p_sql := CONCAT("INSERT INTO showself_statistics_wild_online_register_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," 
				SELECT NULL,uid,idfa,channel_id,device_type FROM showself_statistics_wild_online_",FROM_UNIXTIME(p_start_time,'%Y%m')," 
				WHERE dateline between ",p_start_time," AND ",p_end_time," AND uid <> 3000006 
				GROUP BY uid;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			
			#####准备首次联网临时表  showself_statistics_wild_first_visitor_
			SET @p_sql := CONCAT("DROP TABLE IF EXISTS `showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"`;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

			SET @p_sql := CONCAT("CREATE TABLE `showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"` (
				  `id` int(10) NOT NULL AUTO_INCREMENT COMMENT '主键id',
				  `idfa` varchar(50) NOT NULL DEFAULT '' COMMENT '设备token',
				  `channel_id` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道id',
				  `device_type` int(10) NOT NULL DEFAULT '0' COMMENT '设备类型',
				  PRIMARY KEY (`id`),
				  KEY `idfa` (`idfa`),
				  KEY `channel_id` (`channel_id`)
				) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='用户首次联网临时表';");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

			SET @p_sql := CONCAT("INSERT INTO showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," 
				SELECT NULL,idfa,channel_id,device_type FROM showself_statistics_wild_first_visitor 
				WHERE dateline between ",p_start_time," AND ",p_end_time,";");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			#####准备注册临时表  showself_statistics_wild_register_detail_
			SET @p_sql := CONCAT("DROP TABLE IF EXISTS `showself_statistics_wild_register_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"`;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

			SET @p_sql := CONCAT("CREATE TABLE `showself_statistics_wild_register_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"` (
				  `id` int(10) NOT NULL AUTO_INCREMENT COMMENT '主键id',
				  `uid` int(10) NOT NULL DEFAULT '0' COMMENT '用户uid',
				  `idfa` varchar(50) NOT NULL DEFAULT '' COMMENT '设备token',
				  `channel_id` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道id',
				  `device_type` int(10) NOT NULL DEFAULT '0' COMMENT '设备类型',
				  `qqstatus` int(4) NOT NULL DEFAULT '0' COMMENT '=1为QQ用户',
				  PRIMARY KEY (`id`),
				  KEY `uid` (`uid`),
				  KEY `idfa` (`idfa`),
				  KEY `channel_id` (`channel_id`)
				) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='用户注册临时表';");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

			SET @p_sql := CONCAT("INSERT INTO showself_statistics_wild_register_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," 
				SELECT NULL,uid,idfa,channel_id,device_type,qqstatus FROM showself_statistics_wild_register_detail 
				WHERE dateline between ",p_start_time," AND ",p_end_time,";");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			#####准备心跳临时表  showself_statistics_wild_heartbeat_by_day_
			SET @p_sql := CONCAT("DROP TABLE IF EXISTS `showself_statistics_wild_heartbeat_by_day_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"`;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

			SET @p_sql := CONCAT("CREATE TABLE `showself_statistics_wild_heartbeat_by_day_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"` (
				  `id` int(10) NOT NULL AUTO_INCREMENT COMMENT '主键id',
				  `uid` int(10) NOT NULL DEFAULT '0' COMMENT '用户uid',
				  `roomid` int(10) NOT NULL DEFAULT '0' COMMENT '房间id',
				  `dateline` int(10) NOT NULL DEFAULT '0' COMMENT '时间戳',
				  `idfa` varchar(50) NOT NULL COMMENT '设备token',
				  `channel_id` varchar(50) NOT NULL COMMENT '渠道id',
				  `device_type` int(10) NOT NULL DEFAULT '0' COMMENT '设备类型',
				  `heart_num` int(10) NOT NULL DEFAULT '0' COMMENT '心跳数量',
				  PRIMARY KEY (`id`),
				  KEY `uid` (`uid`),
				  KEY `dateline` (`dateline`),
				  KEY `idfa` (`idfa`),
				  KEY `channel_id` (`channel_id`)
				) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='用户心跳数据临时表';");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

			SET @p_sql := CONCAT("INSERT INTO showself_statistics_wild_heartbeat_by_day_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," 
				SELECT NULL,uid,roomid,(dateline - 12*60*60 - ",p_start_time,") dateline,idfa,channel_id,device_type,heart_num FROM showself_statistics_wild_heartbeat_by_day_",FROM_UNIXTIME(p_start_time,'%Y%m')," 
				WHERE dateline between ",p_start_time," AND ",p_end_time,";");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			#####准备消费临时表  showself_statistics_wild_user_consume_
			SET @p_sql := CONCAT("DROP TABLE IF EXISTS `showself_statistics_wild_user_consume_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"`;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

			SET @p_sql := CONCAT("CREATE TABLE `showself_statistics_wild_user_consume_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"` (
				  `id` int(10) NOT NULL AUTO_INCREMENT COMMENT '主键id',
				  `uid` int(10) NOT NULL DEFAULT '0' COMMENT '用户uid',
				  PRIMARY KEY (`id`),
				  KEY `uid` (`uid`)
				) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='用户消费临时表';");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

			SET @p_sql := CONCAT("INSERT INTO showself_statistics_wild_user_consume_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," 
				SELECT NULL,uid FROM showself_statistics_wild_user_consume_",FROM_UNIXTIME(p_start_time,'%Y%m')," 
				WHERE dateline between ",p_start_time," AND ",p_end_time," 
				GROUP BY uid;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			#####准备充值临时表  showself_statistics_wild_recharge_detail_
			SET @p_sql := CONCAT("DROP TABLE IF EXISTS `showself_statistics_wild_recharge_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"`;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

			SET @p_sql := CONCAT("CREATE TABLE `showself_statistics_wild_recharge_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"` (
				  `id` int(10) NOT NULL AUTO_INCREMENT COMMENT '主键id',
				  `uid` int(10) NOT NULL DEFAULT '0' COMMENT '用户uid',
					`money` int(10) NOT NULL DEFAULT '0' COMMENT '充值金额',
				  PRIMARY KEY (`id`),
				  KEY `uid` (`uid`)
				) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='用户充值临时表';");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

			SET @p_sql := CONCAT("INSERT INTO showself_statistics_wild_recharge_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," 
				SELECT NULL,uid, SUM(money) FROM showself_statistics_wild_recharge_detail_",FROM_UNIXTIME(p_start_time,'%Y%m'),"
				WHERE rechange_status = 30 AND rechange_channel < 20 AND dateline between ",p_start_time," AND ",p_end_time," 
				GROUP BY uid;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			#####非新增用户  non_added_user_sum   =非新增游客(当天联网的游客，且不是新增，且当天没有注册)+非新增注册用户(当天联网的注册用户，且不是新增的)
			#####增加： 新设备老帐号 部分，日期：2015-05-12（以前的算法会漏掉新设备老帐号的数据）
			SET @p_sql := CONCAT("INSERT INTO showself_statistics_non_added_user_sum_tmp (channel_id, device_type, non_added_user_sum) 
			SELECT t.channel_id, t.device_type, SUM(t.n) FROM (
			SELECT o.channel_id, o.device_type, COUNT(o.idfa) n from showself_statistics_wild_online_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o
			LEFT JOIN showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," fv ON fv.idfa = o.idfa 
			LEFT JOIN showself_statistics_wild_register_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," r ON r.idfa = o.idfa 
			WHERE fv.idfa IS NULL AND r.idfa IS NULL 
			GROUP BY o.channel_id, o.device_type   
			UNION ALL
			SELECT o.channel_id, o.device_type, COUNT(o.uid) n from showself_statistics_wild_online_register_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o
			LEFT JOIN showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," fv ON fv.idfa = o.idfa 
			WHERE fv.idfa IS NULL 
			GROUP BY o.channel_id, o.device_type 
			UNION ALL
			SELECT o.channel_id, o.device_type, COUNT(o.uid) n from showself_statistics_wild_online_register_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o
			INNER JOIN showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," fv ON fv.idfa = o.idfa 
			LEFT JOIN showself_statistics_wild_register_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," r ON r.uid = o.uid
			WHERE r.uid IS NULL 
			GROUP BY o.channel_id, o.device_type
			) t GROUP BY t.channel_id, t.device_type;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

			#####非新增用户通过MD5校验 Added 2015.03.27 Android only.
			#####增加： 新设备老帐号 部分，日期：2015-05-12（以前的算法会漏掉新设备老帐号的数据）
			SET @p_sql := CONCAT("INSERT INTO showself_statistics_non_added_md5_pass_num_tmp (channel_id, device_type, md5_pass_num) 
			SELECT t.channel_id, t.device_type, SUM(t.n) FROM (
			SELECT o.channel_id, o.device_type, SUM(IF(s.md5 = 1, 1, 0)) n from showself_statistics_wild_online_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o
			INNER JOIN showself_md5_checksum AS s ON s.idfa = o.idfa AND s.dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
			LEFT JOIN showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," fv ON fv.idfa = o.idfa 
			LEFT JOIN showself_statistics_wild_register_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," r ON r.idfa = o.idfa 
			WHERE fv.idfa IS NULL AND r.idfa IS NULL 
			GROUP BY o.channel_id, o.device_type   
			UNION ALL
			SELECT o.channel_id, o.device_type, SUM(IF(s.md5 = 1, 1, 0)) n from showself_statistics_wild_online_register_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o
			INNER JOIN showself_md5_checksum AS s ON s.idfa = o.idfa AND s.dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
			LEFT JOIN showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," fv ON fv.idfa = o.idfa 
			WHERE fv.idfa IS NULL 
			GROUP BY o.channel_id, o.device_type 
			UNION ALL
			SELECT o.channel_id, o.device_type, SUM(IF(s.md5 = 1, 1, 0)) n from showself_statistics_wild_online_register_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o
			INNER JOIN showself_md5_checksum AS s ON s.idfa = o.idfa AND s.dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
			INNER JOIN showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," fv ON fv.idfa = o.idfa 
			LEFT JOIN showself_statistics_wild_register_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," r ON r.uid = o.uid
			WHERE r.uid IS NULL 
			GROUP BY o.channel_id, o.device_type 
			) t GROUP BY t.channel_id, t.device_type;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			

			#####非新增用户未通过MD5校验 Added 2015.03.27 Android only.
			#####增加： 新设备老帐号 部分，日期：2015-05-12（以前的算法会漏掉新设备老帐号的数据）
			SET @p_sql := CONCAT("INSERT INTO showself_statistics_non_added_md5_fail_num_tmp (channel_id, device_type, md5_fail_num) 
			SELECT t.channel_id, t.device_type, SUM(t.n) FROM (
			SELECT o.channel_id, o.device_type, SUM(IF(s.md5 = 0, 1, 0)) n from showself_statistics_wild_online_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o
			INNER JOIN showself_md5_checksum AS s ON s.idfa = o.idfa AND s.dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
			LEFT JOIN showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," fv ON fv.idfa = o.idfa 
			LEFT JOIN showself_statistics_wild_register_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," r ON r.idfa = o.idfa 
			WHERE fv.idfa IS NULL AND r.idfa IS NULL
			GROUP BY o.channel_id, o.device_type   
			UNION ALL
			SELECT o.channel_id, o.device_type, SUM(IF(s.md5 = 0, 1, 0)) n from showself_statistics_wild_online_register_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o
			INNER JOIN showself_md5_checksum AS s ON s.idfa = o.idfa AND s.dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
			LEFT JOIN showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," fv ON fv.idfa = o.idfa 
			WHERE fv.idfa IS NULL
			GROUP BY o.channel_id, o.device_type 
			UNION ALL
			SELECT o.channel_id, o.device_type, SUM(IF(s.md5 = 0, 1, 0)) n from showself_statistics_wild_online_register_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o
			INNER JOIN showself_md5_checksum AS s ON s.idfa = o.idfa AND s.dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
			INNER JOIN showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," fv ON fv.idfa = o.idfa 
			LEFT JOIN showself_statistics_wild_register_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," r ON r.uid = o.uid
			WHERE r.uid IS NULL 
			GROUP BY o.channel_id, o.device_type 
			) t GROUP BY t.channel_id, t.device_type;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			
			#####非新增纯游客  non_added_pure_visitor_sum   = 当天联网的游客，且不是新增，且该设备在这次联网之前没有注册过用户
			## 修改说明: 采用新的算法, 加上当天注册的部分, 并清洗所有历史数据
			## 修改时间: 2015.12.05
			SET @p_sql := CONCAT("INSERT INTO showself_statistics_non_added_pure_visitor_sum_tmp (channel_id, device_type, non_added_pure_visitor_sum) 
					SELECT channel_id, device_type, non_added_pure_visitor FROM showself_non_added_pure_visitor_result
					WHERE dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
					;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			
			#####非新增的已注册用户 non_added_register_sum    = 当天联网的注册用户，且注册时间不在当天
			SET @p_sql := CONCAT("INSERT INTO showself_statistics_non_added_register_sum_tmp (channel_id, device_type, non_added_register_sum) 
			select o.channel_id, o.device_type, COUNT(o.uid) from showself_statistics_wild_online_register_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o 
			LEFT JOIN showself_statistics_wild_register_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," r ON r.uid = o.uid 
			WHERE r.uid IS NULL 
			GROUP BY o.channel_id, o.device_type;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			
			#####非新增纯游客注册用户 non_added_pure_visitor_register_sum  = 当天联网的非新增纯游客，且注册时间在当天
			## 修改说明: 采用新算法, 不清洗历史数据
			## 修改时间: 2015.12.05
			## 修改说明: 改为UID去重 v.idfa -> r.uid
			## 修改时间: 2016.03.24
			SET @p_sql := CONCAT("INSERT INTO showself_statistics_non_added_pure_visitor_register_sum_tmp (channel_id, device_type, non_added_pure_visitor_register_sum)
					SELECT v.channel_id, v.device_type, COUNT(DISTINCT r.uid) FROM showself_non_added_pure_visitor AS v
					INNER JOIN showself_statistics_wild_register_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," r ON r.idfa = v.idfa
					WHERE v.dateline BETWEEN ",p_start_time," AND ",p_end_time,"
					GROUP BY v.channel_id, v.device_type
					;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			
			#####非新增纯游客用QQ注册 non_added_pure_visitor_qq_sum  = 当天联网的非新增纯游客，且注册时间在当天，且是用QQ注册
			## 修改说明: 改为UID去重 v.idfa -> r.uid
			## 修改时间: 2016.03.24		
			SET @p_sql := CONCAT("INSERT INTO showself_statistics_non_added_pure_visitor_qq_sum_tmp (channel_id, device_type, non_added_pure_visitor_qq_sum) 
					SELECT v.channel_id, v.device_type, COUNT(DISTINCT r.uid) FROM showself_non_added_pure_visitor AS v
					INNER JOIN showself_statistics_wild_register_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," r ON r.idfa = v.idfa
					WHERE v.dateline BETWEEN ",p_start_time," AND ",p_end_time,"
						AND r.qqstatus = 1
					GROUP BY v.channel_id, v.device_type
					;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			
			#####非新增的联网游客 non_added_visitor_sum  = 当天联网的游客用户，且是非新增的
			SET @p_sql := CONCAT("INSERT INTO showself_statistics_non_added_visitor_sum_tmp (channel_id, device_type, non_added_visitor_sum) 
			select o.channel_id, o.device_type, COUNT(o.idfa) from showself_statistics_wild_online_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o 
			LEFT JOIN showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," fv ON fv.idfa = o.idfa 
			WHERE fv.idfa IS NULL 
			GROUP BY o.channel_id, o.device_type;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			
			#####非新增的联网游客注册 non_added_visitor_register_sum   = 当天联网的游客用户，且当天注册，且是非新增的
			## 修改说明: 改为UID去重 o.idfa -> r.uid
			## 修改时间: 2016.03.24		
			SET @p_sql := CONCAT("INSERT INTO showself_statistics_non_added_visitor_register_sum_tmp (channel_id, device_type, non_added_visitor_register_sum) 
			select o.channel_id, o.device_type, COUNT(DISTINCT r.uid) from showself_statistics_wild_online_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o 
			INNER JOIN showself_statistics_wild_register_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," r ON r.idfa = o.idfa 
			LEFT JOIN showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," fv ON fv.idfa = o.idfa 
			WHERE fv.idfa IS NULL 
			GROUP BY o.channel_id, o.device_type;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			
			#####非新增的联网游客QQ注册 non_added_visitor_qq_sum   = 当天联网的游客用户，且当天注册，且是用QQ注册，且是非新增的
			## 修改说明: 改为UID去重 o.idfa -> r.uid
			## 修改时间: 2016.03.24		
			SET @p_sql := CONCAT("INSERT INTO showself_statistics_non_added_visitor_qq_sum_tmp (channel_id, device_type, non_added_visitor_qq_sum) 
			select o.channel_id, o.device_type, COUNT(DISTINCT r.uid) from showself_statistics_wild_online_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o 
			INNER JOIN showself_statistics_wild_register_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," r ON r.idfa = o.idfa 
			LEFT JOIN showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," fv ON fv.idfa = o.idfa 
			WHERE fv.idfa IS NULL AND r.qqstatus = 1 
			GROUP BY o.channel_id, o.device_type;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			
			#####非新增且付费用户  non_added_recharge_user_sum   = 当天联网的注册用户，且有充值记录，且不是新增的
			## 修改说明: 增加字段: 非新增付费金额 old_pay_money
			## 修改时间: 2016.02.29
			SET @p_sql := CONCAT("INSERT INTO showself_statistics_non_added_recharge_user_sum_tmp (channel_id, device_type, non_added_recharge_user_sum, old_pay_money) 
			SELECT o.channel_id, o.device_type, COUNT(o.uid), SUM(rc.money) from showself_statistics_wild_online_register_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o
				INNER JOIN showself_statistics_wild_recharge_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," rc ON rc.uid = o.uid 
				LEFT JOIN showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," fv ON fv.idfa = o.idfa 
				WHERE fv.idfa IS NULL 
				GROUP BY o.channel_id, o.device_type;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			
			#####非新增发言用户  non_added_chat_user_sum   =当天联网的注册用户，且有发言记录，且不是新增的
			SET @p_sql := CONCAT("INSERT INTO showself_statistics_non_added_chat_user_sum_tmp (channel_id, device_type, non_added_chat_user_sum) 
			SELECT o.channel_id, o.device_type, COUNT(o.uid) from showself_statistics_wild_online_register_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o
				INNER JOIN showself_chat_",FROM_UNIXTIME(p_start_time,'%Y')," ct ON ct.uid = o.uid AND ct.dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
				LEFT JOIN showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," fv ON fv.idfa = o.idfa 
				WHERE fv.idfa IS NULL 
				GROUP BY o.channel_id, o.device_type;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
	
			#####非新增发言条数 non_added_user_chat_sum   =当天联网的注册用户，且有发言记录，且不是新增的
			SET @p_sql := CONCAT("INSERT INTO showself_statistics_non_added_user_chat_sum_tmp (channel_id, device_type, non_added_user_chat_sum) 
			SELECT o.channel_id, o.device_type, SUM(ct.chat_num) from showself_statistics_wild_online_register_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o
				INNER JOIN showself_chat_",FROM_UNIXTIME(p_start_time,'%Y')," ct ON ct.uid = o.uid AND ct.dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
				LEFT JOIN showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," fv ON fv.idfa = o.idfa 
				WHERE fv.idfa IS NULL 
				GROUP BY o.channel_id, o.device_type;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
	
			#####非新增送免费花用户  non_added_flower_user_sum   =当天联网的注册用户，且有送花记录，且不是新增的
			SET @p_sql := CONCAT("INSERT INTO showself_statistics_non_added_flower_user_sum_tmp (channel_id, device_type, non_added_flower_user_sum) 
			SELECT o.channel_id, o.device_type, COUNT(o.uid) from showself_statistics_wild_online_register_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o
				INNER JOIN showself_flower_",FROM_UNIXTIME(p_start_time,'%Y')," f ON f.uid = o.uid AND f.dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
				LEFT JOIN showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," fv ON fv.idfa = o.idfa 
				WHERE fv.idfa IS NULL 
				GROUP BY o.channel_id, o.device_type;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			
			#####非新增送免费花次数  non_added_user_flower_sum  =当天联网的注册用户，且有送花记录，且不是新增的
			SET @p_sql := CONCAT("INSERT INTO showself_statistics_non_added_user_flower_sum_tmp (channel_id, device_type, non_added_user_flower_sum) 
			SELECT o.channel_id, o.device_type, SUM(f.flower_num) from showself_statistics_wild_online_register_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," o
				INNER JOIN showself_flower_",FROM_UNIXTIME(p_start_time,'%Y')," f ON f.uid = o.uid AND f.dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
				LEFT JOIN showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d')," fv ON fv.idfa = o.idfa 
				WHERE fv.idfa IS NULL 
				GROUP BY o.channel_id, o.device_type;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			
			###############################删除临时表###############################
			#####删除在线游客临时表  showself_statistics_wild_online_visitor_
			SET @p_sql := CONCAT("DROP TABLE IF EXISTS `showself_statistics_wild_online_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"`;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			
			#####删除在线注册临时表  showself_statistics_wild_online_register_
			SET @p_sql := CONCAT("DROP TABLE IF EXISTS `showself_statistics_wild_online_register_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"`;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			
			#####删除首次联网临时表  showself_statistics_wild_first_visitor_
			SET @p_sql := CONCAT("DROP TABLE IF EXISTS `showself_statistics_wild_first_visitor_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"`;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			
			
			#####删除注册临时表  showself_statistics_wild_register_detail_
			SET @p_sql := CONCAT("DROP TABLE IF EXISTS `showself_statistics_wild_register_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"`;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

			#####删除心跳临时表  showself_statistics_wild_heartbeat_by_day_
			SET @p_sql := CONCAT("DROP TABLE IF EXISTS `showself_statistics_wild_heartbeat_by_day_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"`;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

			#####删除消费临时表  showself_statistics_wild_user_consume_
			SET @p_sql := CONCAT("DROP TABLE IF EXISTS `showself_statistics_wild_user_consume_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"`;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			

			#####删除充值临时表  showself_statistics_wild_recharge_detail_
			SET @p_sql := CONCAT("DROP TABLE IF EXISTS `showself_statistics_wild_recharge_detail_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"`;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
				
			SET @p_sql := CONCAT("INSERT INTO  ",p_target_table, "
			(id,
			dt,
			year,
			month,
			day,
			dateline,
			create_time,
			channel_id,
			device_type,
			non_added_user_sum,
			md5_pass_num,
			md5_fail_num,
			non_added_pure_visitor_sum,
			non_added_general_sum,
			non_added_register_sum,
			non_added_pure_visitor_register_sum,
			non_added_pure_visitor_qq_sum,
			non_added_visitor_sum,
			non_added_visitor_register_sum,
			non_added_visitor_qq_sum,
			non_added_recharge_user_sum,
			old_pay_money,
			non_added_chat_user_sum,
			non_added_user_chat_sum,
			non_added_flower_user_sum,
			non_added_user_flower_sum)
			
			SELECT 
			null,
			FROM_UNIXTIME(",p_start_time,", '%Y%m%d'),
			YEAR(FROM_UNIXTIME(",p_start_time,")),
			MONTH(FROM_UNIXTIME(",p_start_time,")),
			DAY(FROM_UNIXTIME(",p_start_time,")),
			",p_start_time+((p_end_time-p_start_time)/2),",
			UNIX_TIMESTAMP(),
			ch.channel_id,
			ch.device_type,
			t1.non_added_user_sum,
			t15.md5_pass_num,
			t16.md5_fail_num,
			t2.non_added_pure_visitor_sum,
			0,
			t4.non_added_register_sum,
			t5.non_added_pure_visitor_register_sum,
			t6.non_added_pure_visitor_qq_sum,
			t7.non_added_visitor_sum,
			t8.non_added_visitor_register_sum,
			t9.non_added_visitor_qq_sum, 
			t10.non_added_recharge_user_sum,
			t10.old_pay_money,
			t11.non_added_chat_user_sum, 
			t12.non_added_user_chat_sum, 
			t13.non_added_flower_user_sum, 
			t14.non_added_user_flower_sum 
			FROM  ( (select channel_id, device_type from showself_statistics_non_added_user_sum_tmp) 					UNION  
					(select channel_id, device_type from showself_statistics_non_added_pure_visitor_sum_tmp) 			UNION 
					(select channel_id, device_type from showself_statistics_non_added_register_sum_tmp) 				UNION 
					(select channel_id, device_type from showself_statistics_non_added_pure_visitor_register_sum_tmp) 	UNION
					(select channel_id, device_type from showself_statistics_non_added_pure_visitor_qq_sum_tmp) 		UNION 
					(select channel_id, device_type from showself_statistics_non_added_visitor_sum_tmp) 				UNION 
					(select channel_id, device_type from showself_statistics_non_added_visitor_register_sum_tmp) 		UNION 
					(select channel_id, device_type from showself_statistics_non_added_visitor_qq_sum_tmp) 				UNION 
					(select channel_id, device_type from showself_statistics_non_added_recharge_user_sum_tmp) 			UNION 
					(select channel_id, device_type from showself_statistics_non_added_chat_user_sum_tmp) 				UNION 
					(select channel_id, device_type from showself_statistics_non_added_user_chat_sum_tmp) 				UNION 
					(select channel_id, device_type from showself_statistics_non_added_flower_user_sum_tmp) 			UNION 
					(select channel_id, device_type from showself_statistics_non_added_user_flower_sum_tmp)				UNION
					(select channel_id, device_type from showself_statistics_non_added_md5_pass_num_tmp)				UNION
					(select channel_id, device_type from showself_statistics_non_added_md5_fail_num_tmp)
					) AS ch
			LEFT JOIN   showself_statistics_non_added_user_sum_tmp           		t1  ON ch.channel_id=t1.channel_id  AND ch.device_type=t1.device_type
			LEFT JOIN 	showself_statistics_non_added_pure_visitor_sum_tmp   		t2  ON ch.channel_id=t2.channel_id  AND ch.device_type=t2.device_type
			LEFT JOIN 	showself_statistics_non_added_register_sum_tmp       		t4  ON ch.channel_id=t4.channel_id  AND ch.device_type=t4.device_type
			LEFT JOIN 	showself_statistics_non_added_pure_visitor_register_sum_tmp t5  ON ch.channel_id=t5.channel_id  AND ch.device_type=t5.device_type
			LEFT JOIN 	showself_statistics_non_added_pure_visitor_qq_sum_tmp       t6  ON ch.channel_id=t6.channel_id  AND ch.device_type=t6.device_type
			LEFT JOIN 	showself_statistics_non_added_visitor_sum_tmp       		t7  ON ch.channel_id=t7.channel_id  AND ch.device_type=t7.device_type
			LEFT JOIN 	showself_statistics_non_added_visitor_register_sum_tmp      t8  ON ch.channel_id=t8.channel_id  AND ch.device_type=t8.device_type
			LEFT JOIN 	showself_statistics_non_added_visitor_qq_sum_tmp 		    t9  ON ch.channel_id=t9.channel_id  AND ch.device_type=t9.device_type 
			LEFT JOIN 	showself_statistics_non_added_recharge_user_sum_tmp         t10 ON ch.channel_id=t10.channel_id AND ch.device_type=t10.device_type 
			LEFT JOIN 	showself_statistics_non_added_chat_user_sum_tmp             t11 ON ch.channel_id=t11.channel_id AND ch.device_type=t11.device_type 
			LEFT JOIN 	showself_statistics_non_added_user_chat_sum_tmp             t12 ON ch.channel_id=t12.channel_id AND ch.device_type=t12.device_type 
			LEFT JOIN 	showself_statistics_non_added_flower_user_sum_tmp           t13 ON ch.channel_id=t13.channel_id AND ch.device_type=t13.device_type 
			LEFT JOIN 	showself_statistics_non_added_user_flower_sum_tmp           t14 ON ch.channel_id=t14.channel_id AND ch.device_type=t14.device_type 
			LEFT JOIN 	showself_statistics_non_added_md5_pass_num_tmp           	t15 ON ch.channel_id=t15.channel_id AND ch.device_type=t15.device_type 
			LEFT JOIN 	showself_statistics_non_added_md5_fail_num_tmp           	t16 ON ch.channel_id=t16.channel_id AND ch.device_type=t16.device_type 
			GROUP BY ch.channel_id, ch.device_type 
			HAVING   t1.non_added_user_sum <> 0 OR
					 t2.non_added_pure_visitor_sum <> 0 OR
					 t4.non_added_register_sum <> 0 OR
					 t5.non_added_pure_visitor_register_sum <> 0 OR
					 t6.non_added_pure_visitor_qq_sum <> 0 OR
					 t7.non_added_visitor_sum <> 0 OR
					 t8.non_added_visitor_register_sum <> 0 OR
					 t9.non_added_visitor_qq_sum <> 0 OR
					 t10.non_added_recharge_user_sum <> 0 OR 
					 t11.non_added_chat_user_sum <> 0 OR 
					 t12.non_added_user_chat_sum <> 0 OR 
					 t13.non_added_flower_user_sum <> 0 OR 
					 t14.non_added_user_flower_sum <> 0 OR
					 t15.md5_pass_num <> 0 OR
					 t16.md5_fail_num <> 0
					 ;");

			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT; 
			
			####处理垃圾数据
			SET @p_sql := CONCAT("DELETE FROM ",p_target_table," 
					WHERE channel_id IS NULL 
					OR channel_id = '0' OR channel_id ='' 
					OR channel_id ='yibao' OR channel_id ='EN_APP_KEY_PLACEHOLDER'
					OR RIGHT(channel_id,6) LIKE '2138%'
					AND dateline BETWEEN ",p_start_time," AND ",p_end_time,"
					;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			DEALLOCATE PREPARE STMT;     ###释放掉预处理段

			UPDATE showself_statistics_task_def SET start_time=p_end_time+1,end_time=p_end_time+(p_period*60*60), update_time=UNIX_TIMESTAMP() WHERE id=p_id;
     
    END IF;
  UNTIL v_done 
  END REPEAT;
  CLOSE v_new_statistics_cursor;
END