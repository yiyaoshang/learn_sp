CREATE PROCEDURE `showself_statistics_sql_data_by_anchor_rec_proc_20160330_bak`(IN p_start_time INT, IN p_end_time INT)
BEGIN
	DECLARE p_target_table    	VARCHAR(250) DEFAULT '';  #生成数据的表 
	DECLARE p_sql    						VARCHAR(1024) DEFAULT '';  #sql

	DECLARE p_start_date    			INT DEFAULT 0;	
	DECLARE p_end_date    				INT DEFAULT 0;	
  DECLARE i        						INT(10) DEFAULT 0;
  DECLARE p_part_sql        		VARCHAR(250) DEFAULT ''; 

  DECLARE p_times        			INT(10) DEFAULT 1;
  DECLARE p_part_table     		VARCHAR(250) DEFAULT ''; 


	SET p_start_date=FROM_UNIXTIME(p_start_time,'%Y%m%d');  ###时间
	SET p_end_date=FROM_UNIXTIME(p_end_time,'%Y%m%d');  ###时间


  #############################################################################
  #                          主播留推荐数据分析																#
  #############################################################################
		###开始执行存储过程
		INSERT INTO showself_statistics_task_status(proc_name,last_time,userid,proc_status,descr) 
		VALUES( 'showself_statistics_sql_data_by_anchor_rec_proc',UNIX_TIMESTAMP(),0,1,'主播推荐数据分析' )
		ON DUPLICATE KEY UPDATE last_time=UNIX_TIMESTAMP(),proc_status=1;

		
			###########时间处理
			select datediff(p_end_date,p_start_date) INTO p_times; 
			###拼接时长表
			TRUNCATE  showself_sta_valid_long_room_rec_tmp;	 ##清空数据表
			TRUNCATE  showself_sta_valid_long_room_rec_tmp2;	 ##清空数据表
			TRUNCATE  showself_sta_recommend_anchor_rec_tmp;
			
			SET i = 0;
			WHILE i <= p_times
			DO
				SELECT REPLACE(ADDDATE(p_start_date,i),'-','') INTO p_part_table;

				SET @p_sql := CONCAT("CREATE TABLE IF NOT EXISTS showself_statistics_wild_user_valid_long_room_",p_part_table,"
														LIKE showself_statistics_wild_user_valid_long_room");			
				PREPARE STMT FROM @p_sql;   
				EXECUTE STMT;	
				DEALLOCATE PREPARE STMT;     ###释放掉预处理段


				SET @p_sql := CONCAT("INSERT INTO showself_sta_valid_long_room_rec_tmp
						SELECT t.uid,t.roomid,MIN(t.dateline) start_dt,MAX(t.dateline) end_dt,t.idfa,t.data_time,t.dateline,t.nickname,
						SUM(t.longs) longs,t.channel_id,t.terminal,t.useragent,t.ipaddr 
						FROM showself_statistics_wild_user_valid_long_room_",p_part_table," t
						INNER JOIN showself_sta_anchor_online_all_tmp p
						ON t.uid=p.uid
						WHERE 1=1 
						GROUP BY t.uid,t.roomid;");
				PREPARE STMT FROM @p_sql;   
				EXECUTE STMT;	
				DEALLOCATE PREPARE STMT; ###释放掉预处理段

				SET @p_sql := CONCAT("INSERT INTO showself_sta_recommend_anchor_rec_tmp
						SELECT roomid,cal_rank 
						FROM imeeta_ext_db.shall_admin_recommend_anchor_place_log_",p_part_table," ;");
				PREPARE STMT FROM @p_sql;   
				EXECUTE STMT;	
				DEALLOCATE PREPARE STMT; ###释放掉预处理段

				IF i = p_times THEN
						SET @p_sql := CONCAT("INSERT INTO showself_sta_valid_long_room_rec_tmp2
						SELECT uid,roomid,MIN(start_dt) start_dt,MAX(end_dt) end_dt,idfa,data_time,dateline,nickname,
						SUM(longs) longs,channel_id,terminal,useragent,ipaddr 
						FROM showself_sta_valid_long_room_rec_tmp 
						WHERE 1=1 
						GROUP BY uid,roomid;");
						PREPARE STMT FROM @p_sql;   
						EXECUTE STMT;	
						DEALLOCATE PREPARE STMT; ###释放掉预处理段

				END IF;
				SET i = i + 1;
			END WHILE;

			####日均一小时主播
			DROP table  IF  EXISTS t_anchor_times_rec_tmp;
			CREATE TABLE t_anchor_times_rec_tmp AS
			SELECT roomid,ROUND(SUM(total)/3600,2) total 
			FROM imeeta_utf8.shall_rpt_anchor_workload
			WHERE 1=1
			AND cal_year=YEAR(p_start_date)
			AND cal_month BETWEEN MONTH(p_start_date) AND MONTH(p_end_date)
			AND cal_day BETWEEN DAY(p_start_date) AND DAY(p_end_date)
			GROUP BY roomid 
			HAVING SUM(total) > 3600*(p_times+1) ;

			DROP table  IF  EXISTS t_anchor_rec_tmp;
			CREATE TABLE  t_anchor_rec_tmp AS
			SELECT t.roomid,COUNT(1) user_num FROM showself_sta_valid_long_room_rec_tmp2 t
			INNER JOIN t_anchor_times_rec_tmp p ON t.roomid=p.roomid
			GROUP BY t.roomid;

			#####删除黑名单主播
			DELETE t.* FROM t_anchor_rec_tmp t
			INNER JOIN showself_sta_blacklist_room_info p ON t.roomid=p.roomid;

			INSERT INTO showself_statistics_data_by_anchor_rec(start_time,end_time,roomid,user_num,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15)
			SELECT p_start_date,p_end_date,t.roomid,t.user_num,
			SUM(IF(m.cal_rank=1,1,0)),
			SUM(IF(m.cal_rank=2,1,0)),
			SUM(IF(m.cal_rank=3,1,0)),
			SUM(IF(m.cal_rank=4,1,0)),
			SUM(IF(m.cal_rank=5,1,0)),
			SUM(IF(m.cal_rank=6,1,0)),
			SUM(IF(m.cal_rank=7,1,0)),
			SUM(IF(m.cal_rank=8,1,0)),
			SUM(IF(m.cal_rank=9,1,0)),
			SUM(IF(m.cal_rank=10,1,0)),
			SUM(IF(m.cal_rank=11,1,0)),
			SUM(IF(m.cal_rank=12,1,0)),
			SUM(IF(m.cal_rank=13,1,0)),
			SUM(IF(m.cal_rank=14,1,0)),
			SUM(IF(m.cal_rank=15,1,0))
			FROM t_anchor_rec_tmp t 
			INNER JOIN showself_sta_recommend_anchor_rec_tmp m ON t.roomid=m.roomid
			GROUP BY t.roomid
			ON DUPLICATE KEY UPDATE  user_num=VALUES(user_num);

			# 普通推荐
			UPDATE showself_statistics_data_by_anchor_rec AS r
			INNER JOIN imeeta_utf8.shall_cust_anchor_recommend AS p ON p.roomid = r.roomid AND r.start_time = FROM_UNIXTIME(p.dateline,'%Y%m%d')
			SET r.recommend_type = 1
			WHERE r.start_time = p_start_date;

			# 优质推荐
			UPDATE showself_statistics_data_by_anchor_rec AS r
			INNER JOIN imeeta_utf8.shall_testfine_cust_anchor_recommend_fine AS p ON p.roomid = r.roomid AND r.start_time = FROM_UNIXTIME(p.dateline,'%Y%m%d')
			SET r.recommend_type = 2
			WHERE r.start_time = p_start_date;

			###执行存储过程结束
	UPDATE showself_statistics_task_status set last_time=UNIX_TIMESTAMP(),proc_status=0 WHERE proc_name='showself_statistics_sql_data_by_anchor_rec_proc' ;		

END