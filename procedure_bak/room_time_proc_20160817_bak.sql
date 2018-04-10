CREATE PROCEDURE `room_time_proc`(IN p_cal_dt INT, IN p_type INT)
    COMMENT '156_房间停留时长'
BEGIN

	/**
  * 存储过程:	房间停留时长
  * 任务	ID:	tid 				= 156
  * 任务名称: proc_name		= room_time_proc
  * 表格名称: table_name	= room_time
  * 依赖任务: depend			= 0 (无)
  * 可否并行: parallel		= 3 (可以)
  * 任务状态: t_status		= 1 (有效)
  * 运行类型: type 				= 1 (全新运行)
	* 调用说明: CALL room_time_proc(20160520, 1);
	* 查询说明: SELECT * FROM room_time t LIMIT 100;
	* 数据来源: 房间停留时长日志表: sys_req_data_total_new_
  * 注意事项: 
  * 创建说明: 2016.05.31
	* 创建时间: 2016.05.31
  */

  -- Declare variables to hold diagnostics area information
  DECLARE sqlstate_code	CHAR(5)				DEFAULT '00000';
  DECLARE message_text	TEXT;
	DECLARE mysql_errno		INT 					DEFAULT 0;	 #
	DECLARE full_error		VARCHAR(250)	DEFAULT '';  #完整出错信息

	DECLARE p_cur_dt    	INT 					DEFAULT 0;	 #当前日期
	DECLARE p_cal_dt_prev INT 					DEFAULT 0;	 #结算日期前一天
	DECLARE p_start_time  INT 					DEFAULT 0;	 #时间
	DECLARE p_end_time  	INT 					DEFAULT 0;	 #时间

	DECLARE p_oper_db			VARCHAR(50)		DEFAULT '';  #业务数据库
  DECLARE p_stat_db			VARCHAR(50)		DEFAULT '';  #统计数据库
  DECLARE p_ext_db			VARCHAR(50)		DEFAULT '';  #存档数据库
  DECLARE p_cal_db			VARCHAR(50)		DEFAULT '';  #运营数据库

	DECLARE p_aggr_table		VARCHAR(150)   DEFAULT '';  #生成数据的表
	DECLARE p_target_table	VARCHAR(150)   DEFAULT '';  #生成数据的表

	DECLARE t_web					VARCHAR(10)		DEFAULT '';  #Web端产品编码
  DECLARE t_android			VARCHAR(10)		DEFAULT '';  #安卓端产品编码
  DECLARE t_iPhone			VARCHAR(10)		DEFAULT '';  #iPhone端产品编码
  DECLARE t_iPad				VARCHAR(10)		DEFAULT '';  #iPad端产品编码
  DECLARE t_h5					VARCHAR(10)		DEFAULT '';  #H5端产品编码
  
	## 错误处理
	####################################################################################
  -- Declare exception handler
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1
      sqlstate_code = RETURNED_SQLSTATE,
      mysql_errno		=	MYSQL_ERRNO,
      message_text 	= MESSAGE_TEXT;

	SET full_error = CONCAT("ERROR ",mysql_errno," (",sqlstate_code,"): ",message_text);

	## 更新任务列表状态
	UPDATE task_list
	SET checkpoint = 10, run_status = full_error
	WHERE tid = 156;

	## ROLLBACK
	SET p_target_table	= "room_time";
	SET p_aggr_table		= "room_time_aggr_idfa";

	SET @p_sql := CONCAT("DELETE FROM ",p_target_table," WHERE dt = ",p_cal_dt,";");					
	PREPARE STMT FROM @p_sql;   
	EXECUTE STMT;
	DEALLOCATE PREPARE STMT;     ###释放掉预处理段
	
	END;
	####################################################################################

	## 配置数据库
	SET p_oper_db = 'imeeta_utf8';
	SET p_stat_db = 'op_stat_db';
	SET p_ext_db  = 'imeeta_ext_db';
	SET p_cal_db  = 'op_cal_db';

	SET p_cur_dt			= DATE_FORMAT(CURDATE(), '%Y%m%d');
	SET p_cal_dt_prev	= DATE_FORMAT(DATE_SUB(p_cal_dt,INTERVAL 1 DAY), '%Y%m%d');
	SET p_start_time	= UNIX_TIMESTAMP(p_cal_dt);
	SET p_end_time		= p_start_time + 24*3600 - 1;

	## 配置表格
	SET p_target_table	= "room_time";
	SET p_aggr_table		= "room_time_aggr_idfa";

	## 配置产品编码
	SET t_web  			= '10301';
	SET t_android  	= '10202';
	SET t_iPhone 		= '10102';
	SET t_iPad  		= '10135';
	SET t_h5	  		= '10536';

	## 执行代码
	####################################################################################
	## 标识任务开始执行
	UPDATE task_list
	SET start_dt = UNIX_TIMESTAMP(), checkpoint = 1
	WHERE tid = 156;

	## 重跑数据, 先清空数据
	IF p_cur_dt > p_cal_dt AND p_type = 0 THEN

		SET @p_sql := CONCAT("DELETE FROM ",p_target_table," WHERE dt = ",p_cal_dt,";");					
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

	END IF;

	IF p_cur_dt > p_cal_dt THEN

		## 心跳日志
		TRUNCATE room_time_tmp_heartbeat_log;	 

		SET @p_sql := CONCAT("INSERT INTO room_time_tmp_heartbeat_log (uid,idfa,roomid,terminal,dateline,channelid,ipaddr)
				SELECT uid, idfa, roomid, terminal, dateline, channelid, ipaddr
				FROM ",p_ext_db,".shall_cust_user_heartbeat_log_new_",p_cal_dt,"
				;");
		PREPARE STMT FROM @p_sql;
		EXECUTE STMT;
		
		## 主播麦时
		TRUNCATE room_time_tmp_anchor_workload;
		
		SET @p_sql := CONCAT("INSERT INTO room_time_tmp_anchor_workload (roomid, start_dt, end_dt, total)
				SELECT roomid, start_dt, end_dt, total
				FROM ",p_cal_db,".shall_rpt_anchor_workload_band_4realcal
				WHERE cal_year 	= YEAR(",p_cal_dt,")
					AND cal_month = MONTH(",p_cal_dt,")
					AND cal_day 	= DAY(",p_cal_dt,")
				GROUP BY roomid, start_dt, end_dt
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		### 有效心跳
		TRUNCATE room_time_tmp_heartbeat_anchor;

		SET @p_sql := CONCAT("INSERT INTO room_time_tmp_heartbeat_anchor 
				(uid, idfa, roomid, terminal, dateline, channelid, ipaddr, start_dt, end_dt)
				SELECT h.uid, h.idfa, h.roomid, h.terminal, h.dateline, h.channelid, h.ipaddr, a.start_dt, a.end_dt
				FROM room_time_tmp_heartbeat_log AS h
				INNER JOIN room_time_tmp_anchor_workload AS a ON a.roomid = h.roomid AND h.dateline BETWEEN a.start_dt AND a.end_dt
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		### 注册用户停留时长
		TRUNCATE room_time_tmp_user;

		SET @p_sql := CONCAT("INSERT INTO room_time_tmp_user 
				(uid, idfa, roomid, terminal, dateline, channelid, ipaddr, start_dt, end_dt, room_time)
				SELECT uid, idfa, roomid, terminal, dateline, channelid, ipaddr, start_dt, end_dt, room_time
				FROM room_time_tmp_heartbeat_anchor
				WHERE uid > 3000006
				GROUP BY id
				ORDER BY dateline
				ON DUPLICATE KEY UPDATE room_time = IF(VALUES(dateline) - dateline > 70, room_time, room_time + (VALUES(dateline) - dateline)),
																dateline  = VALUES(dateline)
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		### 加上激活ID
		SET @p_sql := CONCAT("UPDATE room_time_tmp_user AS t
				INNER JOIN activation AS a ON a.idfa = t.idfa
				SET t.actid = a.id
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		### 游客停留时长
		TRUNCATE room_time_tmp_visitor;

		SET @p_sql := CONCAT("INSERT INTO room_time_tmp_visitor 
				(idfa, roomid, terminal, dateline, channelid, ipaddr, start_dt, end_dt, room_time)
				SELECT idfa, roomid, terminal, dateline, channelid, ipaddr, start_dt, end_dt, room_time
				FROM room_time_tmp_heartbeat_anchor
				WHERE uid = 3000006
				GROUP BY id
				ORDER BY dateline
				ON DUPLICATE KEY UPDATE room_time = IF(VALUES(dateline) - dateline > 70, room_time, room_time + (VALUES(dateline) - dateline)),
																dateline  = VALUES(dateline)
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		### 加上激活ID
		SET @p_sql := CONCAT("UPDATE room_time_tmp_visitor AS t
				INNER JOIN activation AS a ON a.idfa = t.idfa
				SET t.actid = a.id
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		############################################# 合表 #############################################################
		## 归档 注册用户房间停留时长表
		SET @p_sql := CONCAT("INSERT INTO room_time
				(dt, uid, idfa, actid, roomid, terminal, dateline, channelid, ipaddr, start_dt, end_dt, room_time)
				SELECT ",p_cal_dt,", uid, idfa, actid, roomid, terminal, dateline, channelid, ipaddr, start_dt, end_dt, room_time
				FROM room_time_tmp_user
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 归档 游客房间停留时长表
		SET @p_sql := CONCAT("INSERT INTO ",p_target_table,"
				(dt, uid, idfa, actid, roomid, terminal, dateline, channelid, ipaddr, start_dt, end_dt, room_time)
				SELECT ",p_cal_dt,", 3000006 AS uid, idfa, actid, roomid, terminal, dateline, channelid, ipaddr, start_dt, end_dt, room_time
				FROM room_time_tmp_visitor
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 按设备分的每日汇总表
		SET @p_sql := CONCAT("INSERT INTO ",p_aggr_table," 
				(dt, idfa, actid, room_num, room_time, terminal, channelid, ipaddr)
				SELECT dt, idfa, actid, COUNT(DISTINCT roomid) AS room_num, SUM(room_time), terminal, channelid, ipaddr
				FROM ",p_target_table,"
				WHERE dt = ",p_cal_dt,"
				GROUP BY actid
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;
		DEALLOCATE PREPARE STMT;     ###释放掉预处理段

	END IF;
	####################################################################################

	## 重跑数据, 结算日期不变
	IF p_cur_dt > p_cal_dt AND p_type = 0 THEN

		## 标识任务执行结束
		UPDATE task_list
		SET end_dt = UNIX_TIMESTAMP(), checkpoint = 0
		WHERE tid = 156;

	## 第一次跑, 结算日期+1, 继续执行下一个任务
	ELSEIF p_cur_dt > p_cal_dt AND p_type = 1 THEN

		## 标识任务执行结束
		UPDATE task_list
		SET cal_dt = DATE_FORMAT(DATE_ADD(cal_dt,INTERVAL 1 DAY), '%Y%m%d'), end_dt = UNIX_TIMESTAMP(), checkpoint = 0
		WHERE tid = 156;

		################################## 下一个任务 ######################################
		## 接着下一个任务 大厅连续停留3分钟 tid = 26
		#CALL lobby_3_min_proc(cal_dt, 1);
		####################################################################################

	END IF;

END