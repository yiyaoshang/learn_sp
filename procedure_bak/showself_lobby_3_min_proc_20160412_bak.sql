CREATE PROCEDURE `showself_lobby_3_min_proc`()
    COMMENT '大厅连续停留3分钟存储过程'
BEGIN

/**
  * 实现说明: 每个设备的每天第一次联网的MD5校验结果.
  * 注意事项: 只限制iPhone 和 iPad
	* 数据来源: 请求表: imeeta_ext_db.sys_req_data_total_
	* 目标表格: showself_lobby_3_min
	* 调用说明: CALL showself_lobby_3_min_proc();
	* 查询说明: SELECT * FROM showself_lobby_3_min t LIMIT 1000;
	* 状态查询: SELECT FROM_UNIXTIME(start_time),FROM_UNIXTIME(end_time) FROM showself_statistics_task_def WHERE proc_name='showself_lobby_3_min_proc';
  * 创建说明: 数据从 2016.01.01 开始
	* 创建时间: 2016.02.18
  */

	DECLARE v_done 	  			  			INT DEFAULT 0;	#变量
	DECLARE p_id 										INT DEFAULT 0; 	#主键
  DECLARE p_start_time 						INT DEFAULT 0; 	#开始时间
	DECLARE p_end_time 							INT DEFAULT 0;	#结束时间
  DECLARE p_period 	  						INT DEFAULT 0;	#执行周期
  DECLARE p_target_table    			VARCHAR(250) DEFAULT '';  #生成数据的表
	DECLARE p_sql    								VARCHAR(1024) DEFAULT '';  #sql

	#声明游标
	DECLARE v_md5_checksum_cursor CURSOR FOR
	SELECT id,start_time,end_time,period,target_table FROM showself_statistics_task_def 
	WHERE proc_name='showself_lobby_3_min_proc' AND UNIX_TIMESTAMP() >=end_time AND `status`=1;


  #############################################################################
  #                           生成大厅连续停留3分钟数据
  #############################################################################
  -- 声明游标的异常处理，设置一个终止标记 
  DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET v_done=1; 
	
  SET v_done = 0; ## 
	
  OPEN v_md5_checksum_cursor;
	
  REPEAT
    FETCH v_md5_checksum_cursor INTO p_id, p_start_time, p_end_time, p_period, p_target_table ;

    IF NOT v_done THEN
		
		## 标识存储过程正在执行
		UPDATE showself_statistics_task_def 
		SET `status` = 2, update_time = UNIX_TIMESTAMP() 
		WHERE id = p_id;

		##清空注册临时表
		TRUNCATE showself_lobby_3_min_daily;

		/**
		* 实现说明: 只取terminal IN (3, 4)
		* 注意事项: 
		* 创建时间: 2016.02.18
		*/
		SET @p_sql := CONCAT("INSERT INTO showself_lobby_3_min_daily (idfa, uid, channelid, terminal, dateline)
				SELECT idfa, uid, channelid, terminal, dateline FROM imeeta_ext_db.sys_req_data_total_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"
				WHERE action = 'serv_getnotif'
					AND terminal IN (3, 4)
				GROUP BY macaddr, dateline
				ORDER BY macaddr, dateline
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		#### 计算时间差
		SET @p_sql := CONCAT("UPDATE showself_lobby_3_min_daily AS t1
				INNER JOIN showself_lobby_3_min_daily AS t2 ON t1.idfa = t2.idfa AND t1.id = t2.id - 1
				SET t1.dt_diff = t2.dateline - t1.dateline
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		#### 计算时间差2
		SET @p_sql := CONCAT("UPDATE showself_lobby_3_min_daily AS t1
				INNER JOIN showself_lobby_3_min_daily AS t2 ON t1.idfa = t2.idfa AND t1.id = t2.id - 1
				SET t1.dt_diff2 = IF(t2.dt_diff BETWEEN 90 AND 100, 1, NULL) - IF(t1.dt_diff BETWEEN 90 AND 100, 1, NULL)
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		##清空注册临时表
		TRUNCATE showself_lobby_3_min_daily_id;

		#### 去最大ID
		SET @p_sql := CONCAT("INSERT INTO showself_lobby_3_min_daily_id (id)
				SELECT MAX(id) FROM showself_lobby_3_min_daily
				GROUP BY idfa
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		#### 设置标志位
		SET @p_sql := CONCAT("UPDATE showself_lobby_3_min_daily AS d
				INNER JOIN showself_lobby_3_min_daily_id As i ON i.id = d.id
				SET d.mark = 0
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;
		
		## 把主播标出来 special = 2
		SET @p_sql := CONCAT("UPDATE showself_lobby_3_min_daily AS d 
				INNER JOIN imeeta_utf8.shall_cust_user AS a on a.uid = d.uid
				SET d.special = 2
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;
	
		#### 数据入库
		SET @p_sql := CONCAT("INSERT INTO showself_lobby_3_min (dt, idfa, uid, channelid, terminal, special, create_time)
				SELECT ",FROM_UNIXTIME(p_start_time,'%Y%m%d'),", idfa, uid, channelid, terminal, special, UNIX_TIMESTAMP()
				FROM showself_lobby_3_min_daily
				WHERE dt_diff2 = 0 AND mark = 1
				GROUP BY idfa
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		DEALLOCATE PREPARE STMT;     ###释放掉预处理段 

		/**
		* 实现说明: 更新任务列表的起止时间
		* 执行周期: 24小时
		* 创建时间: 2015-11-23
		* 创建说明: 2015-11-23
		*/
		UPDATE showself_statistics_task_def 
		SET start_time=p_end_time+1,end_time=p_end_time+(p_period*60*60), `status` = 1, update_time=UNIX_TIMESTAMP() 
		WHERE id=p_id;
     
    END IF;
  UNTIL v_done 
  END REPEAT;

  CLOSE v_md5_checksum_cursor;
END