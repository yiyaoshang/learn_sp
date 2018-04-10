CREATE PROCEDURE `showself_statistics_create_datas_proc`()
BEGIN
	DECLARE v_done 	  			  	INT DEFAULT 0;	#变量
	DECLARE p_id 								INT DEFAULT 0; 	#主键
  DECLARE p_start_time 				INT DEFAULT 0; 	#开始时间
	DECLARE p_end_time 					INT DEFAULT 0;	#结束时间
  DECLARE p_period 	  				INT DEFAULT 0;	#执行周期
  DECLARE p_target_table    	VARCHAR(250) DEFAULT '';  #生成数据的表
	DECLARE p_sql    						VARCHAR(1024) DEFAULT '';  #sql
	DECLARE p_proc_name    			VARCHAR(1024) DEFAULT '';  #sql
	DECLARE p_parameter    			VARCHAR(1024) DEFAULT '';  #sql
  DECLARE p_days	  				  INT DEFAULT 0;	#相差天数
  DECLARE i	  				 				INT DEFAULT 0;	#相差天数




	#声明游标
	DECLARE v_new_statistics_cursor CURSOR FOR
	SELECT id,start_time,end_time,period,target_table,proc_name,parameter FROM showself_statistics_task_def 
	WHERE 1=1 AND UNIX_TIMESTAMP() >=end_time AND `status`=3 ORDER BY oid;


  #############################################################################
  #                           调用每日脚本
  #############################################################################
  -- 声明游标的异常处理，设置一个终止标记 
  DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET v_done=1; 
	
  SET v_done = 0; ## 
	
  OPEN v_new_statistics_cursor;
	
  REPEAT
    FETCH v_new_statistics_cursor INTO p_id,p_start_time,p_end_time,p_period,p_target_table,p_proc_name,p_parameter ;

    IF NOT v_done THEN

			######
			SELECT TIMESTAMPDIFF(day,FROM_UNIXTIME(p_start_time),CURDATE()) INTO p_days;
 
			IF p_days <= 365 THEN
					
				SET i = 1;
				
			
				WHILE i <= p_days  
				DO
						/**
						SET @p_sql := CONCAT("CALL ",p_proc_name,"(",p_parameter,");");
						PREPARE STMT FROM @p_sql;   
						#EXECUTE STMT; */

						IF p_proc_name='showself_statistics_sql_data_by_anchor_diamonds_proc' THEN
							CALL showself_statistics_sql_data_by_anchor_diamonds_proc(1,1,1);
						ELSEIF p_proc_name='showself_statistics_sql_data_by_anchor_proc' THEN
							CALL showself_statistics_sql_data_by_anchor_proc(1,1,1);
						ELSEIF p_proc_name='showself_statistics_wild_first_consume_proc' THEN
							CALL showself_statistics_wild_first_consume_proc();
						ELSEIF p_proc_name='showself_statistics_wild_first_recharge_proc' THEN
							CALL showself_statistics_wild_first_recharge_proc();
						ELSEIF p_proc_name='showself_statistics_create_first_following_proc' THEN
							CALL showself_statistics_create_first_following_proc();
					#	ELSEIF p_proc_name='showself_statistics_create_stay_long_proc' THEN
						#	CALL showself_statistics_create_stay_long_proc();
						####代理
						ELSEIF p_proc_name='showself_statistics_create_proxy_detail_proc' THEN
							CALL showself_statistics_create_proxy_detail_proc();
						END IF ;

						SET i = i + 1;

				END WHILE; 

			END IF;

     
    END IF;
  UNTIL v_done 
  END REPEAT;

  CLOSE v_new_statistics_cursor;


END