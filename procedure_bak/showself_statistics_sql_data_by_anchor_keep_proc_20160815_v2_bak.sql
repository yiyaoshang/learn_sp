CREATE PROCEDURE `showself_statistics_sql_data_by_anchor_keep_proc`(IN p_start_time INT, IN p_end_time INT, IN p_statis_type INT, IN p_deviceType INT,  IN f_cids varchar(255), IN p_cids varchar(255), IN f_type INT, IN p_type INT, IN p_user_name varchar(255))
BEGIN
	DECLARE p_target_table    	VARCHAR(250) DEFAULT '';  #生成数据的表 
	DECLARE p_sql    						VARCHAR(1024) DEFAULT '';  #sql

	DECLARE p_start_date    			INT DEFAULT 0;	
	DECLARE p_end_date    				INT DEFAULT 0;	
  DECLARE i        						INT(10) DEFAULT 0;
  DECLARE p_part_sql        		VARCHAR(250) DEFAULT ''; 

  DECLARE p_times        			INT(10) DEFAULT 1;
  DECLARE p_part_table     		VARCHAR(250) DEFAULT ''; 
  DECLARE p_counts        		INT(10) DEFAULT 1;




	#SET num_days=7 * 24 * 3600;

	SET p_start_date=FROM_UNIXTIME(p_start_time,'%Y%m%d');  ###时间
	SET p_end_date=FROM_UNIXTIME(p_end_time,'%Y%m%d');  ###时间


  #############################################################################
  #                          主播留存用户能力数据分析															#
  #############################################################################
		###开始执行存储过程
		INSERT INTO showself_statistics_task_status(proc_name,last_time,userid,proc_status,descr) 
		VALUES( 'showself_statistics_sql_data_by_anchor_keep_proc',UNIX_TIMESTAMP(),0,1,'主播留存用户能力数据分析' )
		ON DUPLICATE KEY UPDATE last_time=UNIX_TIMESTAMP(),proc_status=1;

			####过滤联网用户
			###拼装动态sql
			IF p_deviceType=1 THEN
					SET p_part_sql= " AND LOCATE('s',o.channel_id)!=1 and LEFT(o.channel_id,5)!='10102' AND LEFT(o.channel_id,5)!='10135' ";
			ELSEIF p_deviceType=2 THEN
					SET p_part_sql=" AND LOCATE('s',o.channel_id)=1  ";
			ELSEIF p_deviceType=3 THEN
					SET p_part_sql=" AND LEFT(o.channel_id,5)='10102'  ";
			ELSEIF p_deviceType=4 THEN
					SET p_part_sql=" AND LEFT(o.channel_id,5)='10135'  ";
			END IF;

			#####去掉2138
			SET p_part_sql=CONCAT(p_part_sql," AND RIGHT(o.channel_id,6) NOT LIKE '2138%' ");

			####父渠道
			IF f_cids  IS NOT NULL and f_cids !='' THEN
				IF f_type=1  THEN	
					##包含
					SET p_part_sql=CONCAT(p_part_sql," AND IF(LENGTH(o.channel_id)=8,LEFT(o.channel_id,4),LEFT(RIGHT(o.channel_id,6),4))  IN ( '",f_cids,"')");
				ELSEIF f_type=2  THEN	
					##不包含
					SET p_part_sql=CONCAT(p_part_sql," AND IF(LENGTH(o.channel_id)=8,LEFT(o.channel_id,4),LEFT(RIGHT(o.channel_id,6),4)) NOT IN ( '",f_cids,"')");
				END if;
			END IF;

			####子渠道
			IF p_cids  IS NOT NULL and p_cids !='' THEN
				IF p_type=1  THEN	
					##包含
					SET p_part_sql=CONCAT(p_part_sql," AND IF(LENGTH(o.channel_id)=8,o.channel_id,RIGHT(o.channel_id,6)) IN ( '",p_cids,"')");
				ELSEIF p_type=2  THEN	
					##不包含
					SET p_part_sql=CONCAT(p_part_sql," AND IF(LENGTH(o.channel_id)=8,o.channel_id,RIGHT(o.channel_id,6)) NOT IN ( '",p_cids,"')");
				END if;
			END IF;

			####拼接表
			IF FROM_UNIXTIME(p_start_time,'%Y%m')!=FROM_UNIXTIME(p_end_time ,'%Y%m') THEN
				SET p_times=2;
				SET @p_sql := CONCAT("CREATE TABLE IF NOT EXISTS showself_statistics_wild_online_",FROM_UNIXTIME(p_end_time ,'%Y%m'),"
														LIKE showself_statistics_wild_online");					
				PREPARE STMT FROM @p_sql;   
				EXECUTE STMT;	
				DEALLOCATE PREPARE STMT;     ###释放掉预处理段
			END IF;	

			TRUNCATE  showself_sta_anchor_online_tmp;	 ##清空数据表
			TRUNCATE  showself_sta_anchor_online_all_tmp;	 ##清空数据表

			SET i = 1;
			WHILE i <= p_times
			DO
				SET @p_sql := CONCAT("INSERT INTO showself_sta_anchor_online_tmp
						SELECT uid,dateline,device_type,idfa,channel_id  
						FROM showself_statistics_wild_online_",FROM_UNIXTIME(p_start_time,'%Y%m')," o  
						WHERE 1=1 AND  uid <> 3000006
						",p_part_sql,"
						AND dateline BETWEEN ",p_start_time," AND ",p_end_time,"  
						GROUP BY uid;");
				PREPARE STMT FROM @p_sql;   
				EXECUTE STMT;	
				DEALLOCATE PREPARE STMT; ###释放掉预处理段

				IF i = p_times THEN
						SET @p_sql := CONCAT("INSERT INTO showself_sta_anchor_online_all_tmp
						SELECT uid,dateline,device_type,idfa,channel_id FROM showself_sta_anchor_online_tmp  
						GROUP BY uid;");
						PREPARE STMT FROM @p_sql;   
						EXECUTE STMT;	
						DEALLOCATE PREPARE STMT; ###释放掉预处理段
				END IF;
				SET i = i + 1;
			END WHILE;

			#####合作方式处理
			IF p_statis_type != 0 THEN
				SET @p_sql := CONCAT("DELETE o.* FROM showself_sta_anchor_online_all_tmp o 
				INNER JOIN gamedata_channel_info t1 
				ON IF(LENGTH(o.channel_id)=8,LEFT(o.channel_id,4),LEFT(RIGHT(o.channel_id,6),4))=left(t1.`code`,4)
				WHERE t1.statis_type != ",p_statis_type ,";");
				PREPARE STMT FROM @p_sql;   
				EXECUTE STMT;
				DEALLOCATE PREPARE STMT;     ###释放掉预处理段
			END IF;
			####
			SELECT COUNT(1) into p_counts FROM showself_sta_anchor_online_all_tmp;
	IF p_counts > 0 THEN

			###########时间处理
			select datediff(p_end_date,p_start_date) INTO p_times; 
			###拼接时长表
			TRUNCATE  showself_sta_user_valid_long_room_tmp;	 ##清空数据表
			TRUNCATE  showself_sta_user_valid_long_room_tmp2;	 ##清空数据表
			TRUNCATE  showself_sta_user_valid_long_room_all_tmp;	 ##清空数据表
			TRUNCATE  showself_sta_recommend_anchor_tmp;
			TRUNCATE  showself_sta_recommend_anchor_by_room_tmp;

			SET i = 0;
			WHILE i <= p_times
			DO
				SELECT REPLACE(ADDDATE(p_start_date,i),'-','') INTO p_part_table;

				SET @p_sql := CONCAT("CREATE TABLE IF NOT EXISTS showself_statistics_wild_user_valid_long_room_",p_part_table,"
														LIKE showself_statistics_wild_user_valid_long_room");			
				PREPARE STMT FROM @p_sql;   
				EXECUTE STMT;	
				DEALLOCATE PREPARE STMT;     ###释放掉预处理段

				SET @p_sql := CONCAT("INSERT INTO showself_sta_user_valid_long_room_tmp
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

				SET @p_sql := CONCAT("INSERT INTO showself_sta_recommend_anchor_tmp
						SELECT roomid,COUNT(1) recommend_times
						FROM imeeta_ext_db.shall_admin_recommend_anchor_place_log WHERE dt = ",p_part_table," GROUP BY roomid;");
				PREPARE STMT FROM @p_sql;   
				EXECUTE STMT;	
				DEALLOCATE PREPARE STMT; ###释放掉预处理段

				IF i = p_times THEN
						SET @p_sql := CONCAT("INSERT INTO showself_sta_user_valid_long_room_tmp2
						SELECT uid,roomid,MIN(start_dt) start_dt,MAX(end_dt) end_dt,idfa,data_time,dateline,nickname,
						SUM(longs) longs,channel_id,terminal,useragent,ipaddr 
						FROM showself_sta_user_valid_long_room_tmp 
						WHERE 1=1 
						GROUP BY uid,roomid
						HAVING SUM(longs) > 300;");
						PREPARE STMT FROM @p_sql;   
						EXECUTE STMT;	
						DEALLOCATE PREPARE STMT; ###释放掉预处理段

						SET @p_sql := CONCAT("INSERT INTO showself_sta_recommend_anchor_by_room_tmp
								SELECT roomid,SUM(recommend_times) recommend_times
								FROM showself_sta_recommend_anchor_tmp GROUP BY roomid;");
						PREPARE STMT FROM @p_sql;   
						EXECUTE STMT;	
						DEALLOCATE PREPARE STMT; ###释放掉预处理段

				END IF;
				SET i = i + 1;
			END WHILE;

			####日均一小时主播
			TRUNCATE t_anchor_times_tmp;
			INSERT INTO t_anchor_times_tmp
			SELECT roomid,ROUND(SUM(total)/3600,2) total 
			FROM imeeta_utf8.shall_rpt_anchor_workload
			WHERE 1=1
			AND cal_year=YEAR(p_start_date)
			AND cal_month BETWEEN MONTH(p_start_date) AND MONTH(p_end_date)
			AND cal_day BETWEEN DAY(p_start_date) AND DAY(p_end_date)
			GROUP BY roomid 
			HAVING SUM(total) > 3600*(p_times+1) ;

			INSERT INTO showself_sta_user_valid_long_room_all_tmp
			SELECT t.* FROM showself_sta_user_valid_long_room_tmp2 t
			INNER JOIN t_anchor_times_tmp p ON t.roomid=p.roomid;
			#####删除黑名单主播
			DELETE t.* FROM showself_sta_user_valid_long_room_all_tmp t
			INNER JOIN showself_sta_blacklist_room_info p ON t.roomid=p.roomid;

			#####次周计算
			select datediff(ADDDATE(p_end_date,13),ADDDATE(p_start_date,7)) INTO p_times; 
			###拼接时长表
			TRUNCATE  showself_sta_user_valid_long_room_week_all_tmp;	 ##清空数据表
			SET i = 0;
			WHILE i <= p_times
			DO
				SELECT REPLACE(ADDDATE(p_start_date,i+7),'-','') INTO p_part_table;

				SET @p_sql := CONCAT("CREATE TABLE IF NOT EXISTS showself_statistics_wild_user_valid_long_room_",p_part_table,"
														LIKE showself_statistics_wild_user_valid_long_room");			
				PREPARE STMT FROM @p_sql;   
				EXECUTE STMT;	
				DEALLOCATE PREPARE STMT;     ###释放掉预处理段

				SET @p_sql := CONCAT("INSERT INTO showself_sta_user_valid_long_room_week_all_tmp
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

				SET i = i + 1;
			END WHILE;
			
			#####次月计算
			select datediff(ADDDATE(p_end_date,59),ADDDATE(p_start_date,30)) INTO p_times; 
			###拼接时长表
			TRUNCATE  showself_sta_user_valid_long_room_month_all_tmp;	 ##清空数据表
			SET i = 0;
			WHILE i <= p_times
			DO
				SELECT REPLACE(ADDDATE(p_start_date,i+30),'-','') INTO p_part_table;

				SET @p_sql := CONCAT("CREATE TABLE IF NOT EXISTS showself_statistics_wild_user_valid_long_room_",p_part_table,"
														LIKE showself_statistics_wild_user_valid_long_room");			
				PREPARE STMT FROM @p_sql;   
				EXECUTE STMT;	
				DEALLOCATE PREPARE STMT;     ###释放掉预处理段

				SET @p_sql := CONCAT("INSERT INTO showself_sta_user_valid_long_room_month_all_tmp
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

				SET i = i + 1;
			END WHILE;
			
			INSERT INTO showself_statistics_data_by_anchor_keep(start_time,end_time,statis_type,device_type,f_type,p_type,f_cids,p_cids,
			user_name,roomid,nickname,`level`,total,user_num)
			SELECT p_start_date,p_end_date,p_statis_type,p_deviceType,f_type,p_type,f_cids,p_cids,p_user_name,
			t.roomid,t.nickname,IFNULL(p.`level`,0),IFNULL(m.total,0),COUNT(DISTINCT t.uid) user_num 
			FROM showself_sta_user_valid_long_room_all_tmp t 
			INNER JOIN t_anchor_times_tmp m ON t.roomid=m.roomid
			LEFT JOIN showself_statistics_wild_anchor_info p
			ON t.roomid=p.roomid AND p.date_time=p_end_date
			GROUP BY t.roomid
			ON DUPLICATE KEY UPDATE  user_num=VALUES(user_num);

			######推荐
			INSERT INTO showself_statistics_data_by_anchor_keep(start_time,end_time,statis_type,device_type,f_type,p_type,f_cids,p_cids,
			roomid,recommend_times) 
			SELECT p_start_date,p_end_date,p_statis_type,p_deviceType,f_type,p_type,f_cids,p_cids,
			t.roomid,IFNULL(p.`recommend_times`,0) recommend_times
			FROM showself_sta_user_valid_long_room_all_tmp t 
			LEFT JOIN showself_sta_recommend_anchor_by_room_tmp p
			ON t.roomid=p.roomid 
			GROUP BY t.roomid
			ON DUPLICATE KEY UPDATE  recommend_times=VALUES(recommend_times);

			###周
			DROP table  IF  EXISTS t_anchor_weeK_tmp;
			CREATE TABLE t_anchor_weeK_tmp AS
			SELECT t.roomid,t.uid,SUM(p.longs) longs FROM showself_sta_user_valid_long_room_all_tmp t
			INNER JOIN showself_sta_user_valid_long_room_week_all_tmp p
			ON t.uid=p.uid AND t.roomid=p.roomid
			AND p.start_dt BETWEEN t.start_dt+(7*24*3600) AND t.start_dt+(13*24*3600)
			GROUP BY t.roomid,t.uid;
			alter table t_anchor_weeK_tmp ADD index roomid(roomid);
			alter table t_anchor_weeK_tmp ADD index uid(uid);
			DELETE FROM t_anchor_weeK_tmp WHERE longs<300;
			###月
			DROP table  IF  EXISTS t_anchor_month_tmp;
			CREATE TABLE t_anchor_month_tmp AS
			SELECT t.roomid,t.uid,SUM(p.longs) longs FROM showself_sta_user_valid_long_room_all_tmp t
			INNER JOIN showself_sta_user_valid_long_room_month_all_tmp p
			ON t.uid=p.uid AND t.roomid=p.roomid
			AND p.start_dt BETWEEN t.start_dt+(30*24*3600) AND t.start_dt+(59*24*3600)
			GROUP BY t.roomid,t.uid;
			alter table t_anchor_month_tmp ADD index roomid(roomid);
			alter table t_anchor_month_tmp ADD index uid(uid);
			DELETE FROM t_anchor_month_tmp WHERE longs<300;

			#分
			INSERT INTO showself_statistics_data_by_anchor_keep(start_time,end_time,statis_type,device_type,f_type,p_type,f_cids,p_cids,roomid,week_num)
			SELECT p_start_date,p_end_date,p_statis_type,p_deviceType,f_type,p_type,f_cids,p_cids,
			t.roomid,COUNT(DISTINCT t.uid) week_num FROM t_anchor_weeK_tmp t GROUP BY t.roomid
			ON DUPLICATE KEY UPDATE  week_num=VALUES(week_num);

			INSERT INTO showself_statistics_data_by_anchor_keep(start_time,end_time,statis_type,device_type,f_type,p_type,f_cids,p_cids,roomid,month_num)
			SELECT p_start_date,p_end_date,p_statis_type,p_deviceType,f_type,p_type,f_cids,p_cids,
			t.roomid,COUNT(DISTINCT t.uid) month_num FROM t_anchor_month_tmp t GROUP BY t.roomid 
			ON DUPLICATE KEY UPDATE  month_num=VALUES(month_num);
			#总
			INSERT INTO showself_statistics_data_by_anchor_keep(start_time,end_time,statis_type,device_type,f_type,p_type,f_cids,p_cids,
			user_name,roomid,nickname,`level`,total,user_num)
			SELECT p_start_date,p_end_date,p_statis_type,p_deviceType,f_type,p_type,f_cids,p_cids,p_user_name,
			100 roomid,'' nickname,0 `level`,IFNULL(SUM(m.total),0) total,COUNT(DISTINCT t.uid) user_num 
			FROM showself_sta_user_valid_long_room_all_tmp t 
			INNER JOIN t_anchor_times_tmp m ON t.roomid=m.roomid
			ON DUPLICATE KEY UPDATE  user_num=VALUES(user_num);


			INSERT INTO showself_statistics_data_by_anchor_keep(start_time,end_time,statis_type,device_type,f_type,p_type,f_cids,p_cids,roomid,week_num)
			SELECT p_start_date,p_end_date,p_statis_type,p_deviceType,f_type,p_type,f_cids,p_cids,
			100 roomid,COUNT(DISTINCT t.uid) week_num FROM t_anchor_weeK_tmp t 
			ON DUPLICATE KEY UPDATE  week_num=VALUES(week_num);

			INSERT INTO showself_statistics_data_by_anchor_keep(start_time,end_time,statis_type,device_type,f_type,p_type,f_cids,p_cids,roomid,month_num)
			SELECT p_start_date,p_end_date,p_statis_type,p_deviceType,f_type,p_type,f_cids,p_cids,
			100 roomid,COUNT(DISTINCT t.uid) month_num FROM t_anchor_month_tmp t 
			ON DUPLICATE KEY UPDATE  month_num=VALUES(month_num);


			UPDATE showself_statistics_data_by_anchor_keep
			SET week_pro=IF(user_num=0,0,week_num/user_num),
			month_pro=IF(user_num=0,0,month_num/user_num)
			WHERE user_num<>0 AND (week_pro=0 OR month_pro=0);

			###普通推荐
			UPDATE showself_statistics_data_by_anchor_keep AS r
			INNER JOIN imeeta_utf8.shall_cust_anchor_recommend AS p ON p.roomid = r.roomid AND r.start_time = FROM_UNIXTIME(p.dateline,'%Y%m%d')
			SET r.recommend_type = 1
			WHERE r.start_time = p_start_date;

			##优质推荐
			UPDATE showself_statistics_data_by_anchor_keep AS r
			INNER JOIN imeeta_utf8.shall_testfine_cust_anchor_recommend_fine AS p ON p.roomid = r.roomid AND r.start_time = FROM_UNIXTIME(p.dateline,'%Y%m%d')
			SET r.recommend_type = 2
			WHERE r.start_time = p_start_date;

			##性别
			UPDATE showself_statistics_data_by_anchor_keep AS k
			INNER JOIN imeeta_utf8.shall_cust_user AS r ON r.roomid = k.roomid
			SET k.gender = r.gender
			WHERE k.start_time = p_start_date;

		END IF;
			###执行存储过程结束
	UPDATE showself_statistics_task_status set last_time=UNIX_TIMESTAMP(),proc_status=0 WHERE proc_name='showself_statistics_sql_data_by_anchor_keep_proc' ;		

END