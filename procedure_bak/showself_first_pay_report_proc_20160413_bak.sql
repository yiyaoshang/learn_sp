CREATE PROCEDURE `showself_first_pay_report_proc`()
    COMMENT '首充存储过程'
BEGIN

/**
  * 实现说明: 把平台充值用户的idfa和渠道号补上, 用充值是实时联网的idfa.
  * 注意事项: ios用户用激活时候的渠道号, 查询的时候只查询special = 0的用户, 主播的 special = 2.
	* 数据来源: 1. 充值表: showself_statistics_wild_first_recharge
	* 					2. 联网表: showself_statistics_wild_online_
	* 					3. 主播表: imeeta_utf8.shall_cust_user
	* 中间表格: 1. showself_statistics_wild_first_recharge
	*           2. showself_statistics_wild_online_
	* 调用说明: CALL showself_first_pay_report_proc();
	* 查询说明: SELECT * FROM showself_first_pay_report WHERE special = 0 ORDER BY dateline DESC LIMIT 1000;
	* 状态查询: SELECT FROM_UNIXTIME(start_time),FROM_UNIXTIME(end_time) FROM showself_statistics_task_def WHERE proc_name='showself_first_pay_report_proc';
  * 创建说明: 数据从 2016.02.01 开始
	* 创建时间: 2016.02.05
  */

	DECLARE v_done 	  			  			INT DEFAULT 0;	#变量
	DECLARE p_id 										INT DEFAULT 0; 	#主键
  DECLARE p_start_time 						INT DEFAULT 0; 	#开始时间
	DECLARE p_end_time 							INT DEFAULT 0;	#结束时间
  DECLARE p_period 	  						INT DEFAULT 0;	#执行周期
  DECLARE p_target_table    			VARCHAR(250) DEFAULT '';  #生成数据的表
	DECLARE p_sql    								VARCHAR(1024) DEFAULT '';  #sql

	#声明游标
	DECLARE v_first_recharge_cursor CURSOR FOR
	SELECT id,start_time,end_time,period,target_table FROM showself_statistics_task_def 
	WHERE proc_name='showself_first_pay_report_proc' AND UNIX_TIMESTAMP() >=end_time AND `status`=1;


  #############################################################################
  #                           生成数据
  #############################################################################
  -- 声明游标的异常处理，设置一个终止标记 
  DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET v_done=1; 
	
  SET v_done = 0; ## 
	
  OPEN v_first_recharge_cursor;
	
  REPEAT
    FETCH v_first_recharge_cursor INTO p_id,p_start_time,p_end_time,p_period,p_target_table ;

    IF NOT v_done THEN

		UPDATE showself_statistics_task_def 
		SET `status` = 2, update_time=UNIX_TIMESTAMP() 
		WHERE id=p_id;

		#### 清空临时表
		TRUNCATE showself_first_pay_online_non_pay;

		SET @p_sql := CONCAT("INSERT INTO showself_first_pay_online_non_pay (channel_id, device_type, online_non_pay)
				SELECT CASE 
						WHEN LEFT(channel_id, 1) = 's' AND SUBSTRING(channel_id, 11, 4) = '2130' THEN LEFT(channel_id, 17)
						WHEN LEFT(channel_id, 1) = 's'                                           THEN LEFT(channel_id, 15)
						WHEN LEFT(channel_id, 1) IN ('2', '6')                                   THEN LEFT(channel_id, 8)
						WHEN LEFT(channel_id, 5) = '10102'                                       THEN LEFT(channel_id, 14)
						WHEN LEFT(channel_id, 5) = '10135'                                       THEN LEFT(channel_id, 14)
						WHEN LEFT(channel_id, 5) = '10301'                                       THEN LEFT(channel_id, 14)
						ELSE 0
					END, o.device_type, COUNT(DISTINCT o.uid) FROM showself_statistics_wild_online_",FROM_UNIXTIME(p_start_time,'%Y%m')," AS o 
				LEFT JOIN showself_statistics_wild_first_recharge AS f ON f.uid = o.uid AND f.dateline < ",p_start_time,"
				WHERE f.uid IS NULL
					AND o.dateline BETWEEN ",p_start_time," AND ",p_end_time,"
					AND SUBSTRING(o.channel_id, -6, 4) <> '2138'
					AND LENGTH(o.channel_id) >= 8
				GROUP BY CASE 
						WHEN LEFT(channel_id, 1) = 's' AND SUBSTRING(channel_id, 11, 4) = '2130' THEN LEFT(channel_id, 17)
						WHEN LEFT(channel_id, 1) = 's'                                           THEN LEFT(channel_id, 15)
						WHEN LEFT(channel_id, 1) IN ('2', '6')                                   THEN LEFT(channel_id, 8)
						WHEN LEFT(channel_id, 5) = '10102'                                       THEN LEFT(channel_id, 14)
						WHEN LEFT(channel_id, 5) = '10135'                                       THEN LEFT(channel_id, 14)
						WHEN LEFT(channel_id, 5) = '10301'                                       THEN LEFT(channel_id, 14)
						ELSE 0
					END
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		SET @p_sql := CONCAT("INSERT INTO ",p_target_table," (dt, channel_id, device_type, online_non_pay, create_time)
				SELECT ",FROM_UNIXTIME(p_start_time,'%Y%m%d'),", channel_id, device_type, online_non_pay, UNIX_TIMESTAMP()
				FROM showself_first_pay_online_non_pay
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;
		
		#### 清空临时表
		TRUNCATE showself_first_pay_daily;

		SET @p_sql := CONCAT("INSERT INTO showself_first_pay_daily (uid, money, type, first_second)
				SELECT uid, money, type, first_second FROM showself_statistics_wild_first_recharge
				WHERE dateline BETWEEN ",p_start_time," AND ",p_end_time,"
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		SET @p_sql := CONCAT("UPDATE showself_first_pay_daily AS d
				INNER JOIN showself_statistics_wild_online_",FROM_UNIXTIME(p_start_time,'%Y%m')," AS o ON o.uid = d.uid
				SET d.channel_id = o.channel_id, d.device_type = o.device_type
				WHERE o.dateline BETWEEN ",p_start_time," AND ",p_end_time,"
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		SET @p_sql := CONCAT("UPDATE showself_first_pay_daily AS d
				INNER JOIN showself_statistics_wild_register_detail AS r ON r.uid = d.uid
				SET d.channel_id = r.channel_id, d.device_type = r.device_type
				WHERE d.channel_id = '0'
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		SET @p_sql := CONCAT("UPDATE showself_first_pay_daily AS d
				INNER JOIN income_aggr AS s ON s.uid = d.uid
				SET d.money_total = s.money
				WHERE s.dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
					AND s.special = 0
					AND s.money > 0
					AND s.success = 1
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		#### 清空临时表
		TRUNCATE showself_first_pay_num;

		SET @p_sql := CONCAT("INSERT INTO showself_first_pay_num (channel_id, first_pay, first_official, first_official_11, first_pay_money, 
														first_pay_official_money, first_official_11_money, first_pay_all_money, first_official_all_money, first_official_11_all_money)
				SELECT channel_id, 
					COUNT(DISTINCT uid) AS first_pay,
					COUNT(DISTINCT IF(type = 1, uid, NULL)) AS first_official,
					COUNT(DISTINCT IF(money >= 11 AND type = 1, uid, NULL)) AS first_official_11,
					SUM(IF(first_second = 1, money, 0)) AS first_pay_money,
					SUM(IF(type = 1, money, 0)) AS first_pay_official_money,
					SUM(IF(money >= 11 AND type = 1, money, 0)) AS first_official_11_money,
					SUM(money_total) AS first_pay_all_money,
					SUM(IF(type = 1, money_total, 0)) AS first_official_all_money,
					SUM(IF(money >= 11 AND type = 1, money_total, 0)) AS first_official_11_all_money
				FROM showself_first_pay_daily
				GROUP BY channel_id
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		SET @p_sql := CONCAT("INSERT INTO ",p_target_table," (dt, channel_id, first_pay, first_official, first_official_11, first_pay_money, first_pay_official_money, first_official_11_money, 
					first_pay_all_money, first_official_all_money, first_official_11_all_money, create_time)
				SELECT ",FROM_UNIXTIME(p_start_time,'%Y%m%d'),", channel_id, first_pay, first_official, first_official_11, first_pay_money, first_pay_official_money, first_official_11_money, 
					first_pay_all_money, first_official_all_money, first_official_11_all_money, UNIX_TIMESTAMP()
				FROM showself_first_pay_num
				ON DUPLICATE KEY UPDATE first_pay = VALUES(first_pay),
													 first_official = VALUES(first_official),
												first_official_11 = VALUES(first_official_11),
												  first_pay_money = VALUES(first_pay_money),
												  first_pay_official_money = VALUES(first_pay_official_money),
								  first_official_11_money = VALUES(first_official_11_money),
										  first_pay_all_money = VALUES(first_pay_all_money),
								 first_official_all_money = VALUES(first_official_all_money),
							first_official_11_all_money = VALUES(first_official_11_all_money),
							                create_time = UNIX_TIMESTAMP()
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		SET @p_sql := CONCAT("UPDATE ",p_target_table,"
				SET channel_type = 
						CASE
							WHEN LEFT(channel_id, 1) IN ('2', '6') AND LENGTH(channel_id) = 8        																  THEN 1
							WHEN LEFT(channel_id, 5) = '10301' AND LENGTH(channel_id) = 14                                            THEN 1
							WHEN LEFT(channel_id, 6) = 's10202' AND LENGTH(channel_id) = 17 AND SUBSTRING(channel_id, 11, 4) = '2130' THEN 2
							WHEN LEFT(channel_id, 6) = 's10202' AND LENGTH(channel_id) = 15                													  THEN 2
							WHEN LEFT(channel_id, 5) = '10102' AND LENGTH(channel_id) = 14                                        		THEN 3
							WHEN LEFT(channel_id, 5) = '10102' AND LENGTH(channel_id) = 16 AND SUBSTRING(channel_id, -8, 4) = '2131'  THEN 3
							WHEN LEFT(channel_id, 5) = '10135' AND LENGTH(channel_id) = 14                                            THEN 4
							ELSE 0
						END,
						channel_version = 
						CASE
							WHEN LEFT(channel_id, 1) IN ('2', '6') AND LENGTH(channel_id) = 8        																  THEN 0
							WHEN LEFT(channel_id, 5) = '10301' AND LENGTH(channel_id) = 14                                            THEN SUBSTRING(channel_id, 6, 3)
							WHEN LEFT(channel_id, 6) = 's10202' AND LENGTH(channel_id) = 17 AND SUBSTRING(channel_id, 11, 4) = '2130' THEN SUBSTRING(channel_id, 7, 3)
							WHEN LEFT(channel_id, 6) = 's10202' AND LENGTH(channel_id) = 15                													  THEN SUBSTRING(channel_id, 7, 3)
							WHEN LEFT(channel_id, 5) = '10102' AND LENGTH(channel_id) = 14                                        		THEN SUBSTRING(channel_id, 6, 3)
							WHEN LEFT(channel_id, 5) = '10102' AND LENGTH(channel_id) = 16 AND SUBSTRING(channel_id, -8, 4) = '2131'  THEN SUBSTRING(channel_id, 6, 3)
							WHEN LEFT(channel_id, 5) = '10135' AND LENGTH(channel_id) = 14                                            THEN SUBSTRING(channel_id, 6, 3)
							ELSE 0
						END,
						channel_code = 
						CASE
							WHEN LEFT(channel_id, 1) IN ('2', '6') AND LENGTH(channel_id) = 8        																  THEN LEFT(channel_id, 4)
							WHEN LEFT(channel_id, 5) = '10301' AND LENGTH(channel_id) = 14                                            THEN SUBSTRING(channel_id, -6, 4)
							WHEN LEFT(channel_id, 6) = 's10202' AND LENGTH(channel_id) = 17 AND SUBSTRING(channel_id, 11, 4) = '2130' THEN 2130
							WHEN LEFT(channel_id, 6) = 's10202' AND LENGTH(channel_id) = 15                													  THEN SUBSTRING(channel_id, -6, 4)
							WHEN LEFT(channel_id, 5) = '10102' AND LENGTH(channel_id) = 14                                        		THEN SUBSTRING(channel_id, -6, 4)
							WHEN LEFT(channel_id, 5) = '10102' AND LENGTH(channel_id) = 16 AND SUBSTRING(channel_id, -8, 4) = '2131'  THEN 2131
							WHEN LEFT(channel_id, 5) = '10135' AND LENGTH(channel_id) = 14                                            THEN SUBSTRING(channel_id, -6, 4)
							ELSE 0
						END,
						channel_subcode = 
						CASE
							WHEN LEFT(channel_id, 1) IN ('2', '6') AND LENGTH(channel_id) = 8        																  THEN RIGHT(channel_id, 4)
							WHEN LEFT(channel_id, 5) = '10301' AND LENGTH(channel_id) = 14                                            THEN RIGHT(channel_id, 2)
							WHEN LEFT(channel_id, 6) = 's10202' AND LENGTH(channel_id) = 17 AND SUBSTRING(channel_id, 11, 4) = '2130' THEN RIGHT(channel_id, 4)
							WHEN LEFT(channel_id, 6) = 's10202' AND LENGTH(channel_id) = 15                													  THEN RIGHT(channel_id, 2)
							WHEN LEFT(channel_id, 5) = '10102' AND LENGTH(channel_id) = 14                                        		THEN RIGHT(channel_id, 2)
							WHEN LEFT(channel_id, 5) = '10102' AND LENGTH(channel_id) = 16 AND SUBSTRING(channel_id, -8, 4) = '2131'  THEN RIGHT(channel_id, 4)
							WHEN LEFT(channel_id, 5) = '10135' AND LENGTH(channel_id) = 14                                            THEN RIGHT(channel_id, 2)
							ELSE 0
						END
				WHERE dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		SET @p_sql := CONCAT("UPDATE ",p_target_table," AS r
				INNER JOIN showself_channel_config AS c ON c.chancode = r.channel_code
				SET r.statis_type = c.statis_type, r.channel_name = c.channame
				WHERE dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		SET @p_sql := CONCAT("UPDATE ",p_target_table,"
				SET first_pay_ratio = IFNULL(ROUND(first_pay/online_non_pay*100,2), 0),
						first_official_ratio = IFNULL(ROUND(first_official/online_non_pay*100,2), 0),
						first_official_11_ratio = IFNULL(ROUND(first_official_11/online_non_pay*100,2), 0),
						first_pay_arpu = IFNULL(ROUND(first_pay_money/first_pay,2), 0),
						first_official_arpu = IFNULL(ROUND(first_pay_official_money/first_official,2), 0),
						first_official_11_arpu = IFNULL(ROUND(first_official_11_money/first_official_11,2), 0)
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;


		###################################### 计算总计 ############################################

		#### 1=每天总计
		 
		SET @p_sql := CONCAT("INSERT INTO ",p_target_table," (dt, channel_id, channel_name, online_non_pay, first_pay, first_official, first_official_11, first_pay_money, first_pay_official_money,
					first_official_11_money, first_pay_all_money, first_official_all_money, first_official_11_all_money, first_pay_ratio, first_official_ratio, first_official_11_ratio,
					first_pay_arpu, first_official_arpu, first_official_11_arpu, display, create_time)
				SELECT dt, CONCAT(id,'渠道总数: ',COUNT(DISTINCT channel_id)), '每天总计' AS channel_name, SUM(online_non_pay), SUM(first_pay), SUM(first_official), SUM(first_official_11), 
					SUM(first_pay_money), SUM(first_pay_official_money),SUM(first_official_11_money), SUM(first_pay_all_money), SUM(first_official_all_money), 
					SUM(first_official_11_all_money), 
					IFNULL(ROUND(SUM(first_pay)/SUM(online_non_pay)*100,2), 0) AS first_pay_ratio, 
					IFNULL(ROUND(SUM(first_official)/SUM(online_non_pay)*100,2), 0) AS first_official_ratio, 
					IFNULL(ROUND(SUM(first_official_11)/SUM(online_non_pay)*100,2), 0) AS first_official_11_ratio,
					IFNULL(ROUND(SUM(first_pay_money)/SUM(first_pay),2), 0) AS first_pay_arpu, 
					IFNULL(ROUND(SUM(first_pay_official_money)/SUM(first_official),2), 0) AS first_official_arpu, 
					IFNULL(ROUND(SUM(first_official_11_money)/SUM(first_official_11),2), 0) AS first_official_11_arpu, 1 AS display, UNIX_TIMESTAMP()
				FROM ",p_target_table,"
				WHERE dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		 
		#### 2=父渠道总计
		SET @p_sql := CONCAT("INSERT INTO ",p_target_table," (dt, channel_id, channel_name,device_type,statis_type,channel_type,channel_code,online_non_pay, first_pay, first_official, first_official_11, first_pay_money, first_pay_official_money,
					first_official_11_money, first_pay_all_money, first_official_all_money, first_official_11_all_money, first_pay_ratio, first_official_ratio, first_official_11_ratio,
					first_pay_arpu, first_official_arpu, first_official_11_arpu, display, create_time)
				SELECT dt, CONCAT(id,'子渠道总数: ',COUNT(DISTINCT channel_id)), CONCAT(channel_name,' ',IF(channel_type=1,'Web',IF(channel_type=2,'安卓',IF(channel_type=3,'iPhone',IF(channel_type=4,'iPad',''))))), device_type,statis_type,channel_type,channel_code,
					SUM(online_non_pay), SUM(first_pay), SUM(first_official), SUM(first_official_11), SUM(first_pay_money), SUM(first_pay_official_money),SUM(first_official_11_money), SUM(first_pay_all_money), SUM(first_official_all_money), 
					SUM(first_official_11_all_money), 
					IFNULL(ROUND(SUM(first_pay)/SUM(online_non_pay)*100,2), 0) AS first_pay_ratio, 
					IFNULL(ROUND(SUM(first_official)/SUM(online_non_pay)*100,2), 0) AS first_official_ratio, 
					IFNULL(ROUND(SUM(first_official_11)/SUM(online_non_pay)*100,2), 0) AS first_official_11_ratio,
					IFNULL(ROUND(SUM(first_pay_money)/SUM(first_pay),2), 0) AS first_pay_arpu, 
					IFNULL(ROUND(SUM(first_pay_official_money)/SUM(first_official),2), 0) AS first_official_arpu, 
					IFNULL(ROUND(SUM(first_official_11_money)/SUM(first_official_11),2), 0) AS first_official_11_arpu, 2 AS display, UNIX_TIMESTAMP()
				FROM ",p_target_table,"
				WHERE dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
				GROUP BY channel_code, channel_type
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		#### 3=Web100000
		 
		SET @p_sql := CONCAT("INSERT INTO ",p_target_table," (dt, channel_id, channel_name,device_type,statis_type,channel_type,channel_code,online_non_pay, first_pay, first_official, first_official_11, first_pay_money, first_pay_official_money,
					first_official_11_money, first_pay_all_money, first_official_all_money, first_official_11_all_money, first_pay_ratio, first_official_ratio, first_official_11_ratio,
					first_pay_arpu, first_official_arpu, first_official_11_arpu, display, create_time)
				SELECT dt, CONCAT(id,'子渠道总数: ',COUNT(DISTINCT channel_id)), channel_name,device_type,statis_type,channel_type,channel_code, SUM(online_non_pay), SUM(first_pay), SUM(first_official), SUM(first_official_11), 
					SUM(first_pay_money), SUM(first_pay_official_money),SUM(first_official_11_money), SUM(first_pay_all_money), SUM(first_official_all_money), 
					SUM(first_official_11_all_money), 
					IFNULL(ROUND(SUM(first_pay)/SUM(online_non_pay)*100,2), 0) AS first_pay_ratio, 
					IFNULL(ROUND(SUM(first_official)/SUM(online_non_pay)*100,2), 0) AS first_official_ratio, 
					IFNULL(ROUND(SUM(first_official_11)/SUM(online_non_pay)*100,2), 0) AS first_official_11_ratio,
					IFNULL(ROUND(SUM(first_pay_money)/SUM(first_pay),2), 0) AS first_pay_arpu, 
					IFNULL(ROUND(SUM(first_pay_official_money)/SUM(first_official),2), 0) AS first_official_arpu, 
					IFNULL(ROUND(SUM(first_official_11_money)/SUM(first_official_11),2), 0) AS first_official_11_arpu, 3 AS display, UNIX_TIMESTAMP()
				FROM ",p_target_table,"
				WHERE dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
					AND channel_type = 1 AND channel_code = 1000 AND channel_subcode = 0
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		#### 4=安卓CPA
		 
		SET @p_sql := CONCAT("INSERT INTO ",p_target_table," (dt, channel_id, channel_name,device_type,statis_type,channel_type,channel_code,online_non_pay, first_pay, first_official, first_official_11, first_pay_money, first_pay_official_money,
					first_official_11_money, first_pay_all_money, first_official_all_money, first_official_11_all_money, first_pay_ratio, first_official_ratio, first_official_11_ratio,
					first_pay_arpu, first_official_arpu, first_official_11_arpu, display, create_time)
				SELECT dt, CONCAT(id,'子渠道总数: ',COUNT(DISTINCT channel_id)), '安卓CPA' AS channel_name,device_type,statis_type,channel_type,0 AS channel_code, SUM(online_non_pay), SUM(first_pay), SUM(first_official), SUM(first_official_11), 
					SUM(first_pay_money), SUM(first_pay_official_money),SUM(first_official_11_money), SUM(first_pay_all_money), SUM(first_official_all_money), 
					SUM(first_official_11_all_money), 
					IFNULL(ROUND(SUM(first_pay)/SUM(online_non_pay)*100,2), 0) AS first_pay_ratio, 
					IFNULL(ROUND(SUM(first_official)/SUM(online_non_pay)*100,2), 0) AS first_official_ratio, 
					IFNULL(ROUND(SUM(first_official_11)/SUM(online_non_pay)*100,2), 0) AS first_official_11_ratio,
					IFNULL(ROUND(SUM(first_pay_money)/SUM(first_pay),2), 0) AS first_pay_arpu, 
					IFNULL(ROUND(SUM(first_pay_official_money)/SUM(first_official),2), 0) AS first_official_arpu, 
					IFNULL(ROUND(SUM(first_official_11_money)/SUM(first_official_11),2), 0) AS first_official_11_arpu, 4 AS display, UNIX_TIMESTAMP()
				FROM ",p_target_table,"
				WHERE dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
					AND channel_type = 2 AND statis_type = 1
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		#### 5=安卓CPC
		 
		SET @p_sql := CONCAT("INSERT INTO ",p_target_table," (dt, channel_id, channel_name,device_type,statis_type,channel_type,channel_code,online_non_pay, first_pay, first_official, first_official_11, first_pay_money, first_pay_official_money,
					first_official_11_money, first_pay_all_money, first_official_all_money, first_official_11_all_money, first_pay_ratio, first_official_ratio, first_official_11_ratio,
					first_pay_arpu, first_official_arpu, first_official_11_arpu, display, create_time)
				SELECT dt, CONCAT(id,'子渠道总数: ',COUNT(DISTINCT channel_id)), '安卓CPC' AS channel_name,device_type,statis_type,channel_type,0 AS channel_code,SUM(online_non_pay), SUM(first_pay), SUM(first_official), SUM(first_official_11), 
					SUM(first_pay_money), SUM(first_pay_official_money),SUM(first_official_11_money), SUM(first_pay_all_money), SUM(first_official_all_money), 
					SUM(first_official_11_all_money), 
					IFNULL(ROUND(SUM(first_pay)/SUM(online_non_pay)*100,2), 0) AS first_pay_ratio, 
					IFNULL(ROUND(SUM(first_official)/SUM(online_non_pay)*100,2), 0) AS first_official_ratio, 
					IFNULL(ROUND(SUM(first_official_11)/SUM(online_non_pay)*100,2), 0) AS first_official_11_ratio,
					IFNULL(ROUND(SUM(first_pay_money)/SUM(first_pay),2), 0) AS first_pay_arpu, 
					IFNULL(ROUND(SUM(first_pay_official_money)/SUM(first_official),2), 0) AS first_official_arpu, 
					IFNULL(ROUND(SUM(first_official_11_money)/SUM(first_official_11),2), 0) AS first_official_11_arpu, 5 AS display, UNIX_TIMESTAMP()
				FROM ",p_target_table,"
				WHERE dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
					AND channel_type = 2 AND statis_type = 4
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		#### 6=安卓电子市场
		 
		SET @p_sql := CONCAT("INSERT INTO ",p_target_table," (dt, channel_id, channel_name,device_type,statis_type,channel_type,channel_code, online_non_pay, first_pay, first_official, first_official_11, first_pay_money, first_pay_official_money,
					first_official_11_money, first_pay_all_money, first_official_all_money, first_official_11_all_money, first_pay_ratio, first_official_ratio, first_official_11_ratio,
					first_pay_arpu, first_official_arpu, first_official_11_arpu, display, create_time)
				SELECT dt, CONCAT(id,'子渠道总数: ',COUNT(DISTINCT channel_id)), '安卓电子市场' AS channel_name,device_type,statis_type,channel_type,0 AS channel_code, SUM(online_non_pay), SUM(first_pay), SUM(first_official), SUM(first_official_11), 
					SUM(first_pay_money), SUM(first_pay_official_money),SUM(first_official_11_money), SUM(first_pay_all_money), SUM(first_official_all_money), 
					SUM(first_official_11_all_money), 
					IFNULL(ROUND(SUM(first_pay)/SUM(online_non_pay)*100,2), 0) AS first_pay_ratio, 
					IFNULL(ROUND(SUM(first_official)/SUM(online_non_pay)*100,2), 0) AS first_official_ratio, 
					IFNULL(ROUND(SUM(first_official_11)/SUM(online_non_pay)*100,2), 0) AS first_official_11_ratio,
					IFNULL(ROUND(SUM(first_pay_money)/SUM(first_pay),2), 0) AS first_pay_arpu, 
					IFNULL(ROUND(SUM(first_pay_official_money)/SUM(first_official),2), 0) AS first_official_arpu, 
					IFNULL(ROUND(SUM(first_official_11_money)/SUM(first_official_11),2), 0) AS first_official_11_arpu, 6 AS display, UNIX_TIMESTAMP()
				FROM ",p_target_table,"
				WHERE dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
					AND channel_type = 2 AND statis_type = 3
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		#### 7=iOS官网
		 
		SET @p_sql := CONCAT("INSERT INTO ",p_target_table," (dt, channel_id, channel_name,device_type,statis_type,channel_type,channel_code, online_non_pay, first_pay, first_official, first_official_11, first_pay_money, first_pay_official_money,
					first_official_11_money, first_pay_all_money, first_official_all_money, first_official_11_all_money, first_pay_ratio, first_official_ratio, first_official_11_ratio,
					first_pay_arpu, first_official_arpu, first_official_11_arpu, display, create_time)
				SELECT dt, CONCAT(id,'子渠道总数: ',COUNT(DISTINCT channel_id)), 'iOS官网' AS channel_name,device_type,statis_type,channel_type,channel_code, SUM(online_non_pay), SUM(first_pay), SUM(first_official), SUM(first_official_11), 
					SUM(first_pay_money), SUM(first_pay_official_money),SUM(first_official_11_money), SUM(first_pay_all_money), SUM(first_official_all_money), 
					SUM(first_official_11_all_money), 
					IFNULL(ROUND(SUM(first_pay)/SUM(online_non_pay)*100,2), 0) AS first_pay_ratio, 
					IFNULL(ROUND(SUM(first_official)/SUM(online_non_pay)*100,2), 0) AS first_official_ratio, 
					IFNULL(ROUND(SUM(first_official_11)/SUM(online_non_pay)*100,2), 0) AS first_official_11_ratio,
					IFNULL(ROUND(SUM(first_pay_money)/SUM(first_pay),2), 0) AS first_pay_arpu, 
					IFNULL(ROUND(SUM(first_pay_official_money)/SUM(first_official),2), 0) AS first_official_arpu, 
					IFNULL(ROUND(SUM(first_official_11_money)/SUM(first_official_11),2), 0) AS first_official_11_arpu, 7 AS display, UNIX_TIMESTAMP()
				FROM ",p_target_table,"
				WHERE dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
					AND channel_type = 3 AND channel_code = 1001 AND channel_subcode = 0
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		#### 8=iPad
		 
		SET @p_sql := CONCAT("INSERT INTO ",p_target_table," (dt, channel_id, channel_name,device_type,statis_type,channel_type,channel_code, online_non_pay, first_pay, first_official, first_official_11, first_pay_money, first_pay_official_money,
					first_official_11_money, first_pay_all_money, first_official_all_money, first_official_11_all_money, first_pay_ratio, first_official_ratio, first_official_11_ratio,
					first_pay_arpu, first_official_arpu, first_official_11_arpu, display, create_time)
				SELECT dt, CONCAT(id,'子渠道总数: ',COUNT(DISTINCT channel_id)), 'iPad' AS channel_name,device_type,statis_type,channel_type,channel_code, SUM(online_non_pay), SUM(first_pay), SUM(first_official), SUM(first_official_11), 
					SUM(first_pay_money), SUM(first_pay_official_money),SUM(first_official_11_money), SUM(first_pay_all_money), SUM(first_official_all_money), 
					SUM(first_official_11_all_money), 
					IFNULL(ROUND(SUM(first_pay)/SUM(online_non_pay)*100,2), 0) AS first_pay_ratio, 
					IFNULL(ROUND(SUM(first_official)/SUM(online_non_pay)*100,2), 0) AS first_official_ratio, 
					IFNULL(ROUND(SUM(first_official_11)/SUM(online_non_pay)*100,2), 0) AS first_official_11_ratio,
					IFNULL(ROUND(SUM(first_pay_money)/SUM(first_pay),2), 0) AS first_pay_arpu, 
					IFNULL(ROUND(SUM(first_pay_official_money)/SUM(first_official),2), 0) AS first_official_arpu, 
					IFNULL(ROUND(SUM(first_official_11_money)/SUM(first_official_11),2), 0) AS first_official_11_arpu, 8 AS display, UNIX_TIMESTAMP()
				FROM ",p_target_table,"
				WHERE dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
					AND channel_type = 4
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		#### 9=iOS CPA
		 
		SET @p_sql := CONCAT("INSERT INTO ",p_target_table," (dt, channel_id, channel_name,device_type,statis_type,channel_type,channel_code, online_non_pay, first_pay, first_official, first_official_11, first_pay_money, first_pay_official_money,
					first_official_11_money, first_pay_all_money, first_official_all_money, first_official_11_all_money, first_pay_ratio, first_official_ratio, first_official_11_ratio,
					first_pay_arpu, first_official_arpu, first_official_11_arpu, display, create_time)
				SELECT dt, CONCAT(id,'子渠道总数: ',COUNT(DISTINCT channel_id)), 'iOS CPA' AS channel_name,device_type,statis_type,channel_type,0 AS channel_code, SUM(online_non_pay), SUM(first_pay), SUM(first_official), SUM(first_official_11), 
					SUM(first_pay_money), SUM(first_pay_official_money),SUM(first_official_11_money), SUM(first_pay_all_money), SUM(first_official_all_money), 
					SUM(first_official_11_all_money), 
					IFNULL(ROUND(SUM(first_pay)/SUM(online_non_pay)*100,2), 0) AS first_pay_ratio, 
					IFNULL(ROUND(SUM(first_official)/SUM(online_non_pay)*100,2), 0) AS first_official_ratio, 
					IFNULL(ROUND(SUM(first_official_11)/SUM(online_non_pay)*100,2), 0) AS first_official_11_ratio,
					IFNULL(ROUND(SUM(first_pay_money)/SUM(first_pay),2), 0) AS first_pay_arpu, 
					IFNULL(ROUND(SUM(first_pay_official_money)/SUM(first_official),2), 0) AS first_official_arpu, 
					IFNULL(ROUND(SUM(first_official_11_money)/SUM(first_official_11),2), 0) AS first_official_11_arpu, 9 AS display, UNIX_TIMESTAMP()
				FROM ",p_target_table,"
				WHERE dt = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
					AND channel_type IN (3, 4) AND statis_type = 1
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

  CLOSE v_first_recharge_cursor;
END