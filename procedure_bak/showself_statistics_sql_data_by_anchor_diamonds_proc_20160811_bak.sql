CREATE PROCEDURE `showself_statistics_sql_data_by_anchor_diamonds_proc`(IN p_start INT, IN p_end INT, IN q_type INT)
BEGIN

  DECLARE p_target_table    	VARCHAR(250) DEFAULT '';  #生成数据的表 
	DECLARE p_sql    						VARCHAR(1024) DEFAULT '';  #sql

	DECLARE num_days    				INT DEFAULT 0;	#n天
	DECLARE sta_date    				INT DEFAULT 0;	
	DECLARE end_date    				INT DEFAULT 0;	
  DECLARE i        						INT(10) DEFAULT 0;
	DECLARE p_id 								INT DEFAULT 0; 	#
  DECLARE p_start_time 				INT DEFAULT 0; 	#
	DECLARE p_end_time 					INT DEFAULT 0;	#
  DECLARE p_period 	  				INT DEFAULT 0;	#


	SET num_days=7 * 24 * 3600;

		IF q_type =1 THEN
				SELECT id,start_time,end_time,period INTO p_id,p_start_time,p_end_time,p_period FROM showself_statistics_task_def 
				WHERE proc_name='showself_statistics_sql_data_by_anchor_diamonds_proc' AND UNIX_TIMESTAMP() >=end_time AND `status`<>0;
				SET p_target_table ='showself_statistics_data_by_anchor_diamonds_detail';
		ELSE
				SET p_start_time=p_start ;
				SET p_end_time =p_end ;
				SET p_target_table ='showself_statistics_data_by_anchor_diamonds';
		END IF;

	SET sta_date=FROM_UNIXTIME(p_start_time,'%Y%m%d');  ###时间
	SET end_date=FROM_UNIXTIME(p_end_time,'%Y%m%d');  	###时间

	

  #############################################################################
  #                          主播秀钻监控统计分析															#
  #############################################################################
IF p_start_time IS NOT NULL AND p_start_time != 0 THEN
			###开始执行存储过程
			INSERT INTO showself_statistics_task_status(proc_name,last_time,userid,proc_status,descr) 
			VALUES( 'showself_statistics_sql_data_by_anchor_diamonds_proc',UNIX_TIMESTAMP(),0,1,'主播秀钻监控统计分析' )
			ON DUPLICATE KEY UPDATE last_time=UNIX_TIMESTAMP(),proc_status=1;
				


			####主播收礼记录 AND category !=5  ABS(diamond)
			TRUNCATE TABLE showself_statistics_wild_diamond_detail_tmp ;
			IF FROM_UNIXTIME(p_start_time,'%Y%m')=FROM_UNIXTIME(p_end_time ,'%Y%m') THEN
				SET @p_sql := CONCAT("INSERT INTO showself_statistics_wild_diamond_detail_tmp(id,roomid,fuid,diamond)
						SELECT NULL,roomid,fuid,ABS(diamond) FROM op_stat_db.showself_statistics_wild_diamond_detail_",FROM_UNIXTIME(p_start_time,'%Y%m'),"   WHERE 1=1
						  AND  fuid !=0 AND is_anchor=1 AND category IN (1, 2)
							AND dateline BETWEEN ",p_start_time," AND ",p_end_time,"  
						;");
			ELSE
					SET @p_sql := CONCAT("CREATE TABLE IF NOT EXISTS op_stat_db.showself_statistics_wild_diamond_detail_",FROM_UNIXTIME(p_end_time ,'%Y%m'),"
															LIKE op_stat_db.showself_statistics_wild_diamond_detail");					
					PREPARE STMT FROM @p_sql;   
					EXECUTE STMT;	
					DEALLOCATE PREPARE STMT;     ###释放掉预处理段
					SET @p_sql := CONCAT("INSERT INTO showself_statistics_wild_diamond_detail_tmp(id,roomid,fuid,diamond)
						SELECT  NULL,t.roomid,t.fuid,t.diamond FROM (
								SELECT roomid,fuid,ABS(diamond) diamond FROM op_stat_db.showself_statistics_wild_diamond_detail_",FROM_UNIXTIME(p_start_time,'%Y%m'),"   WHERE  1=1
										AND  fuid !=0 AND is_anchor=1 AND category IN (1, 2)
										AND dateline BETWEEN ",p_start_time," AND ",p_end_time,"  
								UNION ALL
								SELECT roomid,fuid,ABS(diamond) diamond FROM op_stat_db.showself_statistics_wild_diamond_detail_",FROM_UNIXTIME(p_end_time,'%Y%m'),"   WHERE  1=1
										AND  fuid !=0 AND is_anchor=1 AND category IN (1, 2)
										AND dateline BETWEEN ",p_start_time," AND ",p_end_time,"  
								) t ;");

			END IF;	
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			DEALLOCATE PREPARE STMT;     ###释放掉预处理段


			###计算秀钻
			SET @p_sql := CONCAT("INSERT INTO ",p_target_table,"
			(start_time,end_time,roomid,diamonds)
			SELECT ",sta_date,",",end_date," as endtime,
			roomid,SUM(diamond) diamonds 
			FROM showself_statistics_wild_diamond_detail_tmp 
			GROUP BY roomid 
			ON DUPLICATE KEY UPDATE diamonds=VALUES(diamonds);");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;
			DEALLOCATE PREPARE STMT;     ###释放掉预处理段

				###总时长
				SET @p_sql := CONCAT("INSERT INTO ",p_target_table,"(start_time,end_time,roomid,`online`,days)
				SELECT ",sta_date,",",end_date," as endtime,
				roomid,ROUND(SUM(`online`)/3600,2) `online`,SUM(days) days FROM (
						SELECT FROM_UNIXTIME(end_dt,'%Y%m%d'),roomid,
							SUM(u.total) `online`,IF(SUM(u.total)>=7200,1,0) days
							FROM yujia.shall_rpt_anchor_workload_band u
							WHERE u.start_dt>=",p_start_time,"  AND u.end_dt<=",p_end_time,"
							GROUP BY FROM_UNIXTIME(u.end_dt,'%Y%m%d'),u.roomid 
				) p
				GROUP BY p.roomid
				ON DUPLICATE KEY UPDATE `online`=VALUES(`online`),days=VALUES(days);");
				PREPARE STMT FROM @p_sql;   
				EXECUTE STMT;
				DEALLOCATE PREPARE STMT;     ###释放掉预处理段


				#####等级插入
				## 红人暂时没有主播等级 -- 20160811 `anchor_level`=VALUES(`anchor_level`),
				#WHERE 1=1 AND date_time=",sta_date,"
				SET @p_sql := CONCAT("INSERT INTO ",p_target_table,"(start_time,end_time,roomid,anchor_level,nickname)
				SELECT ",sta_date,",",end_date," as endtime,
				roomid,`level` as anchor_level,nickname FROM showself_statistics_wild_anchor_info
				GROUP BY roomid 
				ON DUPLICATE KEY UPDATE nickname=VALUES(`nickname`);");
				PREPARE STMT FROM @p_sql;   
				EXECUTE STMT;
				DEALLOCATE PREPARE STMT;     ###释放掉预处理段
/*
				#####删除黑名单主播
				SET @p_sql := CONCAT("
				DELETE t.* FROM  ",p_target_table," t
				INNER JOIN showself_sta_blacklist_room_info p ON t.roomid=p.roomid;;");
				PREPARE STMT FROM @p_sql;   
				EXECUTE STMT;
				DEALLOCATE PREPARE STMT;     ###释放掉预处理段
			*/
				####总计计算
				SET @p_sql := CONCAT("INSERT INTO ",p_target_table,"(start_time,end_time,roomid,anchor_level,nickname,`online`,days,diamonds)
				SELECT start_time,end_time,
				100 AS roomid,0 AS anchor_level,'总计' nickname,SUM(`online`) `online`,SUM(days) days,SUM(diamonds) diamonds
				FROM ",p_target_table,"
				WHERE 1=1 AND start_time=",sta_date,"   AND  end_time=",end_date," 
				ON DUPLICATE KEY UPDATE `online`=VALUES(`online`),days=VALUES(`days`),diamonds=VALUES(`diamonds`);");
				PREPARE STMT FROM @p_sql;   
				EXECUTE STMT;
				DEALLOCATE PREPARE STMT;     ###释放掉预处理段

		#####清除垃圾数据

			DELETE  FROM showself_statistics_data_by_anchor_diamonds WHERE
			online=0 AND days=0 AND diamonds=0;

			DELETE FROM showself_statistics_data_by_anchor_diamonds_detail WHERE
			online=0 AND days=0 AND diamonds=0;

				####比例计算
				SET @p_sql := CONCAT("UPDATE ",p_target_table," SET diamonds_pro=IF(`online` >0,ROUND(diamonds/`online`,2),0)
				WHERE start_time=",sta_date,"   AND  end_time=",end_date," ;");
				PREPARE STMT FROM @p_sql;   
				EXECUTE STMT;
				DEALLOCATE PREPARE STMT;     ###释放掉预处理段



				##q_type=1为计算每天数据
				IF q_type =1 THEN
						
						####跟新自动生成时间
						UPDATE showself_statistics_task_def SET start_time=p_end_time+1,end_time=p_end_time+(p_period*60*60), update_time=UNIX_TIMESTAMP() WHERE id=p_id;
			
				END IF;
				

	
			###执行存储过程结束
			UPDATE showself_statistics_task_status set last_time=UNIX_TIMESTAMP(),proc_status=0 WHERE proc_name='showself_statistics_sql_data_by_anchor_diamonds_proc' ;		
	end IF;		

END