CREATE PROCEDURE `showself_statistics_wild_first_consume_proc`()
BEGIN
	DECLARE v_done 	  			  	INT DEFAULT 0;	#变量
	DECLARE p_id 					INT DEFAULT 0; 	#主键
    DECLARE p_start_time 			INT DEFAULT 0; 	#开始时间
	DECLARE p_end_time 				INT DEFAULT 0;	#结束时间
    DECLARE p_period 	  			INT DEFAULT 0;	#执行周期
    DECLARE p_target_table    	    VARCHAR(250) DEFAULT '';  #生成数据的表
	DECLARE p_sql    				VARCHAR(1024) DEFAULT '';  #sql

	#声明游标
	DECLARE v_new_statistics_cursor CURSOR FOR
	SELECT id,start_time,end_time,period,target_table FROM showself_statistics_task_def 
	WHERE proc_name='showself_statistics_wild_first_consume_proc' AND UNIX_TIMESTAMP() >=end_time AND `status`<>0;


  #############################################################################
  #                           首次消费用户表
  #############################################################################
  -- 声明游标的异常处理，设置一个终止标记 
  DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET v_done=1; 
	
  SET v_done = 0; ## 
	
  OPEN v_new_statistics_cursor;
	
  REPEAT
    FETCH v_new_statistics_cursor INTO p_id,p_start_time,p_end_time,p_period,p_target_table ;

    IF NOT v_done THEN

			SET @p_sql := CONCAT("INSERT INTO ",p_target_table,
			" SELECT NULL,uid,MIN(dateline),consume_coin,consume_type,consume_type_son,COUNT(1) times 
				FROM showself_statistics_wild_user_consume_",FROM_UNIXTIME(p_start_time,'%Y%m'),"
			  WHERE 1=1
				AND consume_coin<>0
				AND uid<>3000096
				AND dateline BETWEEN ",p_start_time," AND  ",p_end_time,"  
				GROUP BY uid
				ON DUPLICATE KEY UPDATE times=times+VALUES(times) ;");		
					 
				PREPARE STMT FROM @p_sql;   
				EXECUTE STMT; 
			#####第二次消费
			SET @p_sql := CONCAT("INSERT INTO showself_statistics_wild_second_consume
				SELECT NULL,uid,dateline,consume_coin,consume_type,consume_type_son,1
				FROM showself_statistics_wild_user_consume_",FROM_UNIXTIME(p_start_time,'%Y%m'),"
			  WHERE 1=1
				AND consume_coin<>0
				AND uid<>3000096
				AND dateline BETWEEN ",p_start_time," AND  ",p_end_time,"  
				GROUP BY id
				ON DUPLICATE KEY UPDATE times=times+1,dateline=IF(times=2,VALUES(dateline),dateline)
				,consume_coin=IF(times=2,VALUES(consume_coin),consume_coin);");		

				PREPARE STMT FROM @p_sql;   
				EXECUTE STMT; 
			UPDATE showself_statistics_task_def SET start_time=p_end_time+1,end_time=p_end_time+(p_period*60*60), update_time=UNIX_TIMESTAMP() WHERE id=p_id;
     
    END IF;
  UNTIL v_done 
  END REPEAT;

  CLOSE v_new_statistics_cursor;


END