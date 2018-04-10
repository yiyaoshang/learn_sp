CREATE　PROCEDURE `income_detail_proc`(IN p_cal_dt INT, IN p_type INT)
    COMMENT '032_收入明细'
BEGIN

	/**
  * 存储过程:	收入明细
  * 任务	ID:	tid 				= 32
  * 任务名称: proc_name		= income_detail_proc
  * 表格名称: table_name	= income_detail
  * 依赖任务: depend			= 0 (无)
  * 可否并行: parallel		= 3 (可以)
  * 任务状态: t_status		= 1 (有效)
  * 运行类型: type 				= 1 (全新运行)
	* 调用说明: CALL income_detail_proc(20160520, 1);
	* 查询说明: SELECT * FROM income_detail t LIMIT 100;
	* 数据来源: 收入表: showself_statistics_wild_recharge_detail_
  * 注意事项: 
  * 创建说明: 2016.05.31
	* 创建时间: 2016.05.31
  */

  -- Declare variables to hold diagnostics area information
  DECLARE sqlstate_code	CHAR(5)				DEFAULT '00000';
  DECLARE message_text	TEXT;
	DECLARE mysql_errno		INT 					DEFAULT 0;	 #
	DECLARE full_error		VARCHAR(250)	DEFAULT '';  #完整出错信息

	DECLARE p_id    			INT 					DEFAULT 0;	 #任务	ID
	DECLARE p_cur_dt    	INT 					DEFAULT 0;	 #当前日期
	DECLARE p_cal_dt_prev INT 					DEFAULT 0;	 #结算日期前一天
	DECLARE p_month				INT 					DEFAULT 0;	 #结算日期年月
	DECLARE p_start_time  INT 					DEFAULT 0;	 #时间
	DECLARE p_end_time  	INT 					DEFAULT 0;	 #时间

	DECLARE p_oper_db			VARCHAR(50)		DEFAULT '';  #业务数据库
  DECLARE p_stat_db			VARCHAR(50)		DEFAULT '';  #统计数据库
  DECLARE p_ext_db			VARCHAR(50)		DEFAULT '';  #存档数据库
  DECLARE p_cal_db			VARCHAR(50)		DEFAULT '';  #运营数据库

	DECLARE p_base_table		VARCHAR(150)   DEFAULT '';  #生成数据的表
	DECLARE p_target_table	VARCHAR(150)   DEFAULT '';  #生成数据的表
	DECLARE p_aggr_table		VARCHAR(150)   DEFAULT '';  #生成数据的表
	DECLARE p_first_table		VARCHAR(150)   DEFAULT '';  #生成数据的表

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

	### 任务ID ###
	SET p_id = 32;

	## 更新任务列表状态
	UPDATE task_list SET checkpoint = 10, run_status = full_error WHERE tid = p_id;

	## ROLLBACK
	SET p_month					= LEFT(p_cal_dt, 6);
	SET p_target_table	= "income_detail";
	SET p_aggr_table		= "income_aggr";
	SET p_first_table		= "income_first";

	SET @p_sql := CONCAT("DELETE FROM ",p_target_table," WHERE dt = ",p_cal_dt,";");					
	PREPARE STMT FROM @p_sql;   
	EXECUTE STMT;

	SET @p_sql := CONCAT("DELETE FROM ",p_aggr_table," WHERE dt = ",p_cal_dt,";");					
	PREPARE STMT FROM @p_sql;   
	EXECUTE STMT;

	SET @p_sql := CONCAT("DELETE FROM ",p_first_table," WHERE dt = ",p_cal_dt,";");					
	PREPARE STMT FROM @p_sql;
	EXECUTE STMT;

	SET @p_sql := CONCAT("UPDATE ",p_first_table,"
			SET dt2 = 0, type2 = 0, dateline2 = 0
			WHERE dt2 = ",p_cal_dt,"
			;");
	PREPARE STMT FROM @p_sql;
	EXECUTE STMT;

	DEALLOCATE PREPARE STMT;     ###释放掉预处理段
	
	END;
	####################################################################################

	### 任务ID ###
	SET p_id = 32;

	### 配置数据库 ###
	SELECT oper, stat, ext, cal INTO p_oper_db, p_stat_db, p_ext_db, p_cal_db FROM admin_db;
	
	### 配置产品编码 ###
	SELECT web, android, iphone, ipad, h5 INTO t_web, t_android, t_iPhone, t_iPad, t_h5 FROM admin_terminal;

	## 配置表格
	SET p_target_table	= "income_detail";
	SET p_aggr_table		= "income_aggr";
	SET p_first_table		= "income_first";

	SET p_cur_dt			= DATE_FORMAT(CURDATE(), '%Y%m%d');
	SET p_cal_dt_prev	= DATE_FORMAT(DATE_SUB(p_cal_dt,INTERVAL 1 DAY), '%Y%m%d');
	SET p_start_time	= UNIX_TIMESTAMP(p_cal_dt);
	SET p_end_time		= p_start_time + 24*3600 - 1;
	SET p_month				= LEFT(p_cal_dt, 6);

	## 执行代码
	####################################################################################
	## 标识任务开始执行
	UPDATE task_list
	SET start_dt = UNIX_TIMESTAMP(), checkpoint = 1
	WHERE tid = p_id;

	## 重跑数据, 先清空数据
	IF p_cur_dt > p_cal_dt AND p_type <> 1 THEN

		SET @p_sql := CONCAT("DELETE FROM ",p_target_table," WHERE dt = ",p_cal_dt,";");					
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		SET @p_sql := CONCAT("DELETE FROM ",p_aggr_table," WHERE dt = ",p_cal_dt,";");					
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		SET @p_sql := CONCAT("DELETE FROM ",p_first_table," WHERE dt = ",p_cal_dt,";");					
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		SET @p_sql := CONCAT("UPDATE ",p_first_table,"
				SET dt2 = 0, type2 = 0, dateline2 = 0
				WHERE dt2 = ",p_cal_dt,"
				;");
		PREPARE STMT FROM @p_sql;
		EXECUTE STMT;

	END IF;

	IF p_cur_dt > p_cal_dt THEN

		##清空充值临时表
		TRUNCATE income_detail_daily;
		
		## 从充值表取出前一天的充值用户
		SET @p_sql := CONCAT("INSERT INTO income_detail_daily (uid,dateline,type,money,refund,coin,success,productid,orderid,create_time)
				SELECT uid, dateline, rechange_channel AS type, 
					IF(rechange_channel < 20, money, 0) AS money, 
					0 AS refund,
					IF(rechange_channel >= 20, money, 0) AS coin, 
					IF(rechange_status=30,1,0) AS success, 
					product_id, orderid, UNIX_TIMESTAMP()
				FROM showself_statistics_wild_recharge_detail_",p_month,"
				WHERE dateline BETWEEN ",p_start_time," AND ",p_end_time,"
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		### 过滤重复: 同一个订单号只算一条
		TRUNCATE income_detail_orderid;

		SET @p_sql := CONCAT("INSERT INTO income_detail_orderid (income_id, orderid)
				SELECT MIN(id) AS income_id, orderid FROM income_detail_daily 
				WHERE type < 10 AND success = 1 AND money > 0
				GROUP BY orderid 
				HAVING COUNT(0) > 1
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		SET @p_sql := CONCAT("DELETE d.* FROM income_detail_daily AS d
				INNER JOIN income_detail_orderid AS t ON t.orderid = d.orderid
				WHERE d.id > t.income_id
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 特殊渠道
		SET @p_sql := CONCAT("UPDATE income_detail_daily
				SET special = 1
				WHERE chancode = 2138
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 特殊渠道
		SET @p_sql := CONCAT("UPDATE income_detail_daily AS d
				INNER JOIN register AS r ON r.uid = d.uid AND r.special = 1
				SET d.special = 1
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 内部人员充值
		SET @p_sql := CONCAT("UPDATE income_detail_daily AS d
				INNER JOIN admin_income_internal_user AS i ON i.uid = d.uid
				SET d.special = 3
				WHERE d.type = 10
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 内部人员充值
		SET @p_sql := CONCAT("UPDATE admin_income_internal_user AS i 
				INNER JOIN (SELECT uid, SUM(money) money FROM income_detail_daily WHERE special = 3 AND type = 10 GROUP BY uid) AS d ON d.uid = i.uid
				SET i.money = i.money + d.money, create_time = UNIX_TIMESTAMP()
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 刷量用户
		SET @p_sql := CONCAT("UPDATE income_detail_daily AS d
				INNER JOIN admin_income_special_user AS i ON i.uid = d.uid
				SET d.special = 4
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 加入联网信息(调用充值接口的) AND ABS(TIMESTAMPDIFF(SECOND,FROM_UNIXTIME(i.dateline),FROM_UNIXTIME(d.dateline))) <= 5
		SET @p_sql := CONCAT("UPDATE income_detail_daily AS i
				INNER JOIN pay_req_detail AS d ON d.uid = i.uid 
																						AND d.dt = ",p_cal_dt,"
																						AND i.dateline = d.dateline
				SET i.idfa = d.idfa,
						i.actid = d.actid,
						i.ipaddr = d.ipaddr,
						i.channelid = d.channelid,
						i.chantype = d.chantype,
						i.chanver = d.chanver,
						i.chancode = d.chancode,
						i.terminal = d.terminal,
						i.appver = d.appver
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 加入联网信息(调用充值接口的) 
		SET @p_sql := CONCAT("UPDATE income_detail_daily AS i
				INNER JOIN pay_req_detail AS d ON d.uid = i.uid 
																						AND d.dt = ",p_cal_dt,"
																						AND ABS(TIMESTAMPDIFF(SECOND,FROM_UNIXTIME(i.dateline),FROM_UNIXTIME(d.dateline))) <= 5
				SET i.idfa = d.idfa,
						i.actid = d.actid,
						i.ipaddr = d.ipaddr,
						i.channelid = d.channelid,
						i.chantype = d.chantype,
						i.chanver = d.chanver,
						i.chancode = d.chancode,
						i.terminal = d.terminal,
						i.appver = d.appver
				WHERE i.channelid = 0
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;
/*
		############################在 2016.02.02 前, 用户秀币收入从以下4个临时表取得##################################################
		## 21=秀钻换秀币
		SET @p_sql := CONCAT("INSERT INTO income_detail_daily (uid, dateline, type, money, coin, success, productid, orderid, create_time)
				SELECT uid, dateline, 21 AS type, 0 AS money, coin, 1 AS success, product_id, '0' AS orderid, UNIX_TIMESTAMP()
				FROM showself_diamond_exchange_gold
				WHERE dt = ",p_cal_dt,"
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 22=幸运礼物中奖的秀币
		SET @p_sql := CONCAT("INSERT INTO income_detail_daily (uid, dateline, type, money, coin, success, productid, orderid, create_time)
				SELECT uid, dateline, 22 AS type, 0 AS money, coin, 1 AS success, 0 AS productid, roomid, UNIX_TIMESTAMP()
				FROM showself_luck_gift_coin
				WHERE dt = ",p_cal_dt,"
				;");
		PREPARE STMT FROM @p_sql;
		EXECUTE STMT;

		## 23=抢红包
		SET @p_sql := CONCAT("INSERT INTO income_detail_daily (uid, dateline, type, money, coin, success, productid, orderid, create_time)
				SELECT uid, dateline, 23 AS type, 0 AS money, coin, 1 AS success, 0 AS productid, roomid, UNIX_TIMESTAMP()
				FROM showself_red_packet
				WHERE dt = ",p_cal_dt,"
				;");
		PREPARE STMT FROM @p_sql;
		EXECUTE STMT;

		## 24=做任务得秀币
		SET @p_sql := CONCAT("INSERT INTO income_detail_daily (uid, dateline, type, money, coin, success, productid, orderid, create_time)
				SELECT uid, dateline, 24 AS type, 0 AS money, coin, 1 AS success, product_id, '0' AS orderid, UNIX_TIMESTAMP()
				FROM showself_task_earn_coin
				WHERE dt = ",p_cal_dt,"
				;");
		PREPARE STMT FROM @p_sql;
		EXECUTE STMT;
		##############################################################################
*/

		################################## 计算退款 ############################################

		## 清空充值临时表
		TRUNCATE income_detail_agent_recharge;

		## 代理商充值
		SET @p_sql := CONCAT("INSERT INTO income_detail_agent_recharge (uid, dateline, money, type, productid, special)
				SELECT uid, dateline, FLOOR(money/100), IF(productid = 147, 10, 11) AS type, productid,
					IF(salesperson IN ('dl_dzlf','jzljs','dl_tianli_7328','ywdl','czdl_g2_xs'), 3, 0) AS special
				FROM ",p_oper_db,".cust_user_trans_addmoney
				WHERE productid IN (147, 149) 
					AND money > 0
				AND dateline BETWEEN ",p_start_time," AND ",p_end_time,"
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 清空充值临时表
		TRUNCATE income_detail_agent_refund;

		## 代理商充值退款
		SET @p_sql := CONCAT("INSERT INTO income_detail_agent_refund (uid, dateline, money, type, productid)
				SELECT uid, dateline, FLOOR(ABS(money)/100), IF(productid = 147, 10, 11) AS type, productid
				FROM ",p_oper_db,".cust_user_trans_addmoney
				WHERE productid IN (147, 149) and money < 0
					AND dateline BETWEEN ",p_start_time," AND ",p_end_time + 3600,"
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 标识退款
		SET @p_sql := CONCAT("UPDATE income_detail_agent_recharge AS c
				INNER JOIN income_detail_agent_refund AS f ON f.uid = c.uid 
																									AND f.productid = c.productid 
																									AND f.money = c.money 
																									AND f.dateline > c.dateline 
																									AND f.dateline <= c.dateline + 3600
				SET c.is_refund = 1
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 只把退款部分入库
		SET @p_sql := CONCAT("INSERT INTO income_detail_daily 
				(uid, dateline, type, money, refund, coin, success, productid, orderid, special, create_time)
				SELECT uid, dateline, type, 0 AS money, money AS refund, 0 AS coin, 1 AS success, productid, '0' AS orderid, special, UNIX_TIMESTAMP()
				FROM income_detail_agent_recharge
				WHERE is_refund = 1
				;");
		PREPARE STMT FROM @p_sql;
		EXECUTE STMT;

		IF p_type = 1 THEN
	
			## 标识内部充值
			SET @p_sql := CONCAT("UPDATE income_detail_daily AS d
					INNER JOIN income_detail_agent_recharge AS a ON a.uid = d.uid
					SET d.special = 3
					WHERE d.type = 10 AND a.special = 3
					;");
			PREPARE STMT FROM @p_sql;
			EXECUTE STMT;

		END IF;
		
		##############################################################################

		## 把主播标出来 special = 2
		SET @p_sql := CONCAT("UPDATE income_detail_daily AS d 
				INNER JOIN ",p_oper_db,".shall_cust_user AS a on a.uid = d.uid
				SET d.special = 2
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		##清空临时表
		TRUNCATE income_detail_daily_uid;

		## 取出前一天有充值记录的用户UID(没有联网调支付接口的)
		SET @p_sql := CONCAT("INSERT INTO income_detail_daily_uid (uid)
				SELECT uid FROM income_detail_daily
				WHERE chantype = 0
				GROUP BY uid
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		##清空联网临时表
		TRUNCATE income_detail_online_detail;
		
		## 从联网表取出前一天有充值记录的联网用户
		SET @p_sql := CONCAT("INSERT INTO income_detail_online_detail 
				(uid,dateline,idfa,actid,ipaddr,channelid,chantype,chanver,chancode,terminal,appver)
				SELECT o.uid,o.dateline,o.idfa,o.actid,o.ipaddr,o.channelid,o.chantype,o.chanver,o.chancode,o.terminal,o.appver
				FROM online_detail_",p_month," AS o
				INNER JOIN income_detail_daily_uid AS r ON r.uid = o.uid
				WHERE o.dt = ",p_cal_dt,"
				GROUP BY o.uid, o.actid, o.dateline
				ORDER BY o.uid, o.dateline
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 清空联网和充值交叉临时表
		TRUNCATE income_detail_online_detail_diff;
		
		## 通过uid把联网表和充值表关联
		SET @p_sql := CONCAT("INSERT INTO income_detail_online_detail_diff (recharge_id,uid,idfa,actid,ipaddr,channelid,chantype,chanver,
					chancode,terminal,appver,type,money,refund,coin,success,productid,orderid,special,dt_recharge,dt_online,dt_diff,mark)
				SELECT r.id, r.uid, 
					IFNULL(IF(r.chantype > 0, r.idfa,      o.idfa),      0),
					IFNULL(IF(r.chantype > 0, r.actid,     o.actid),     0),
					IFNULL(IF(r.chantype > 0, r.ipaddr,    o.ipaddr),    0),
					IFNULL(IF(r.chantype > 0, r.channelid, o.channelid), 0),
					IFNULL(IF(r.chantype > 0, r.chantype,  o.chantype),  0),
					IFNULL(IF(r.chantype > 0, r.chanver,   o.chanver),   0),
					IFNULL(IF(r.chantype > 0, r.chancode,  o.chancode),  0),
					IFNULL(IF(r.chantype > 0, r.terminal,  o.terminal),  0),
					IFNULL(IF(r.chantype > 0, r.appver,    o.appver),    0),
					r.type, r.money, r.refund, r.coin, r.success, r.productid, r.orderid, r.special, r.dateline, 
					IFNULL(o.dateline, 0), 
					IFNULL(r.dateline - o.dateline, 0) AS n, 0 AS mark
				FROM income_detail_daily AS r
				LEFT JOIN income_detail_online_detail AS o ON r.uid = o.uid AND r.dateline >= o.dateline
				ORDER BY o.uid, n
			;");
		PREPARE STMT FROM @p_sql;
		EXECUTE STMT;

		## 清空临时表
		TRUNCATE income_detail_online_detail_diff_id;
		
		## 取出时间最接近的id
		SET @p_sql := CONCAT("INSERT INTO income_detail_online_detail_diff_id (id)
				SELECT MIN(id) FROM income_detail_online_detail_diff
				GROUP BY recharge_id
			;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 通过id标记选定uid
		SET @p_sql := CONCAT("UPDATE income_detail_online_detail_diff AS d
				INNER JOIN income_detail_online_detail_diff_id AS i ON i.id = d.id
				SET d.mark = 1
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 
		SET @p_sql := CONCAT("UPDATE income_detail_online_detail_diff AS d
				INNER JOIN activation AS a ON a.idfa = d.idfa
				SET d.actid = a.id
				WHERE d.actid = 0
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 数据入收入明细库
		SET @p_sql := CONCAT("INSERT INTO ",p_target_table," (dt,uid,dateline,idfa,actid,ipaddr,channelid,chantype,chanver,
					chancode,terminal,appver,type,money,refund,coin,success,productid,orderid,special,create_time)
				SELECT ",p_cal_dt,",uid,dt_recharge,idfa,actid,ipaddr,channelid,chantype,chanver,chancode,
					terminal,appver,type,money,refund,coin,success,productid,orderid,special,UNIX_TIMESTAMP()
				FROM income_detail_online_detail_diff
				WHERE mark = 1
				;");
		PREPARE STMT FROM @p_sql;
		EXECUTE STMT;
		
		## 首次充值表
		TRUNCATE income_first_tmp;

		SET @p_sql := CONCAT("INSERT INTO income_first_tmp (uid, type, dateline)
				SELECT uid, IF(type < 10, 1, 2) AS type, MIN(dt_recharge)
				FROM income_detail_online_detail_diff
				WHERE mark = 1
					AND success = 1
					AND type < 20
					AND money > 0
					AND uid > 3000006
				GROUP BY uid, IF(type < 10, 1, 2)
				ORDER BY uid, MIN(dt_recharge)
				;");
		PREPARE STMT FROM @p_sql;
		EXECUTE STMT;
	
		SET @p_sql := CONCAT("INSERT IGNORE ",p_first_table,"(dt, uid, type, dateline, money, special, create_time)
				SELECT ",p_cal_dt,", i.uid, f.type, i.dt_recharge, i.money, i.special, UNIX_TIMESTAMP()
				FROM income_detail_online_detail_diff AS i
				INNER JOIN income_first_tmp AS f ON f.uid = i.uid AND f.dateline = i.dt_recharge
				WHERE i.mark = 1
					AND i.success = 1 
					AND i.money > 0
					AND i.type < 20
					AND i.uid > 3000006
					AND f.id = 1
				;");
		PREPARE STMT FROM @p_sql;
		EXECUTE STMT;

		SET @p_sql := CONCAT("UPDATE ",p_first_table," AS f
				INNER JOIN (
					SELECT ",p_cal_dt," AS dt2, i.uid AS uid, f.type AS type2, i.dt_recharge AS dateline2, SUM(i.money) AS money2
					FROM income_detail_online_detail_diff AS i
					INNER JOIN income_first_tmp AS f ON f.uid = i.uid AND f.dateline = i.dt_recharge
					WHERE i.mark = 1
						AND i.success = 1
						AND i.money > 0
						AND i.type < 20
						AND i.uid > 3000006
						AND f.id = 2
					GROUP BY i.uid ) AS t ON t.uid = f.uid
				SET f.dt2 = t.dt2,
						f.type2 = t.type2,
						f.dateline2 = t.dateline2,
						f.money2 = t.money2
				WHERE f.dt2 = 0
				;");
		PREPARE STMT FROM @p_sql;
		EXECUTE STMT;

		## 充值简表 成功
		SET @p_sql := CONCAT("INSERT INTO ",p_aggr_table," (dt,uid,idfa,actid,ipaddr,channelid,chantype,chanver,chancode,terminal,appver,
					money,refund,coin,money_3,money_4,money_5,money_6,money_8,money_9,money_10,money_21,money_22,money_23,money_24,
				 times,times_refund,times_coin,times_3,times_4,times_5,times_6,times_8,times_9,times_10,times_21,times_22,times_23,times_24,
				 success,special,create_time)
				SELECT ",p_cal_dt,",uid,idfa,actid,ipaddr,channelid,chantype,chanver,chancode,terminal,appver,
					SUM(money) 										  AS money,
					SUM(refund) 									  AS refund,
					SUM(coin) 										  AS coin,
					SUM(IF(type=3, money, 0))  		  AS money_3,
					SUM(IF(type=4, money, 0))  		  AS money_4,
					SUM(IF(type=5, money, 0))  		  AS money_5,
					SUM(IF(type=6, money, 0))  		  AS money_6,
					SUM(IF(type=8, money, 0))  		  AS money_8,
					SUM(IF(type=9, money, 0))  		  AS money_9,
					SUM(IF(type=10, money, 0)) 		  AS money_10,
					SUM(IF(type=21, money, 0)) 		  AS money_21,
					SUM(IF(type=22, money, 0)) 		  AS money_22,
					SUM(IF(type=23, money, 0)) 		  AS money_23,
					SUM(IF(type=24, money, 0)) 		  AS money_24,
					COUNT(IF(money>0, money, NULL)) 		            AS times,
					COUNT(IF(refund>0, refund, NULL)) 	            AS times_refund,
					COUNT(IF(coin>0, coin, NULL)) 			            AS times_coin,
					COUNT(IF(type=3  AND money>0,   money, NULL))  AS times_3,
					COUNT(IF(type=4  AND money>0,   money, NULL))  AS times_4,
					COUNT(IF(type=5  AND money>0,   money, NULL))  AS times_5,
					COUNT(IF(type=6  AND money>0,   money, NULL))  AS times_6,
					COUNT(IF(type=8  AND money>0,   money, NULL))  AS times_8,
					COUNT(IF(type=9  AND money>0,   money, NULL))  AS times_9,
					COUNT(IF(type=10 AND money>0, money, NULL))  AS times_10,
					COUNT(IF(type=21 AND money>0, money, NULL))  AS times_21,
					COUNT(IF(type=22 AND money>0, money, NULL))  AS times_22,
					COUNT(IF(type=23 AND money>0, money, NULL))  AS times_23,
					COUNT(IF(type=24 AND money>0, money, NULL))  AS times_24,
					success, special, UNIX_TIMESTAMP()
				FROM income_detail_online_detail_diff
				WHERE mark = 1
					AND success = 1
				GROUP BY uid
				;");
		PREPARE STMT FROM @p_sql;   
		EXECUTE STMT;

		## 充值简表 失败
		SET @p_sql := CONCAT("INSERT INTO ",p_aggr_table," (dt,uid,idfa,actid,ipaddr,channelid,chantype,chanver,chancode,terminal,appver,
					money,money_3,money_4,money_5,money_6,money_8,money_9,money_10,money_21,money_22,money_23,money_24,
				 times,times_3,times_4,times_5,times_6,times_8,times_9,times_10,times_21,times_22,times_23,times_24,
				 success,special,create_time)
				SELECT ",p_cal_dt,",uid,idfa,actid,ipaddr,channelid,chantype,chanver,chancode,terminal,appver,
					SUM(money) 										  AS money,
					SUM(IF(type=3, money, 0))  		  AS money_3,
					SUM(IF(type=4, money, 0))  		  AS money_4,
					SUM(IF(type=5, money, 0))  		  AS money_5,
					SUM(IF(type=6, money, 0))  		  AS money_6,
					SUM(IF(type=8, money, 0))  		  AS money_8,
					SUM(IF(type=9, money, 0))  		  AS money_9,
					SUM(IF(type=10, money, 0)) 		  AS money_10,
					SUM(IF(type=21, money, 0)) 		  AS money_21,
					SUM(IF(type=22, money, 0)) 		  AS money_22,
					SUM(IF(type=23, money, 0)) 		  AS money_23,
					SUM(IF(type=24, money, 0)) 		  AS money_24,
					COUNT(IF(money>0, money, NULL)) 								AS times,
					COUNT(IF(type=3  AND money>0,   money, NULL))  AS times_3,
					COUNT(IF(type=4  AND money>0,   money, NULL))  AS times_4,
					COUNT(IF(type=5  AND money>0,   money, NULL))  AS times_5,
					COUNT(IF(type=6  AND money>0,   money, NULL))  AS times_6,
					COUNT(IF(type=8  AND money>0,   money, NULL))  AS times_8,
					COUNT(IF(type=9  AND money>0,   money, NULL))  AS times_9,
					COUNT(IF(type=10 AND money>0, money, NULL))  AS times_10,
					COUNT(IF(type=21 AND money>0, money, NULL))  AS times_21,
					COUNT(IF(type=22 AND money>0, money, NULL))  AS times_22,
					COUNT(IF(type=23 AND money>0, money, NULL))  AS times_23,
					COUNT(IF(type=24 AND money>0, money, NULL))  AS times_24,
					success, special, UNIX_TIMESTAMP()
				FROM income_detail_online_detail_diff
				WHERE mark = 1
					AND success = 0
				GROUP BY uid
				ON DUPLICATE KEY UPDATE income_aggr.money = income_aggr.money + VALUES(money),
																income_aggr.money_3 = income_aggr.money_3 + VALUES(money_3),
																income_aggr.money_4 = income_aggr.money_4 + VALUES(money_4),
																income_aggr.money_5 = income_aggr.money_5 + VALUES(money_5),
																income_aggr.money_6 = income_aggr.money_6 + VALUES(money_6),
																income_aggr.money_8 = income_aggr.money_8 + VALUES(money_8),
																income_aggr.money_9 = income_aggr.money_9 + VALUES(money_9),
																income_aggr.money_10 = income_aggr.money_10 + VALUES(money_10),
																income_aggr.money_21 = income_aggr.money_21 + VALUES(money_21),
																income_aggr.money_22 = income_aggr.money_22 + VALUES(money_22),
																income_aggr.money_23 = income_aggr.money_23 + VALUES(money_23),
																income_aggr.money_23 = income_aggr.money_24 + VALUES(money_24),
																income_aggr.times = income_aggr.times + VALUES(times),
																income_aggr.times_3 = income_aggr.times_3 + VALUES(times_3),
																income_aggr.times_4 = income_aggr.times_4 + VALUES(times_4),
																income_aggr.times_5 = income_aggr.times_5 + VALUES(times_5),
																income_aggr.times_6 = income_aggr.times_6 + VALUES(times_6),
																income_aggr.times_8 = income_aggr.times_8 + VALUES(times_8),
																income_aggr.times_9 = income_aggr.times_9 + VALUES(times_9),
																income_aggr.times_10 = income_aggr.times_10 + VALUES(times_10),
																income_aggr.times_21 = income_aggr.times_21 + VALUES(times_21),
																income_aggr.times_22 = income_aggr.times_22 + VALUES(times_22),
																income_aggr.times_23 = income_aggr.times_23 + VALUES(times_23),
																income_aggr.times_24 = income_aggr.times_24 + VALUES(times_24)
				;");
		PREPARE STMT FROM @p_sql;
		EXECUTE STMT;

		IF p_type = 1 THEN
	
			## 充值汇总表
			SET @p_sql := CONCAT("INSERT INTO income_total(uid,money,money_fail,refund,coin,money_3,money_4,money_5,money_6,money_8,
					 money_9,money_10,money_21,money_22,money_23,money_24,times,times_fail,times_refund,times_coin,times_3,times_4,
					 times_5,times_6,times_8,times_9,times_10,times_21,times_22,times_23,times_24,special,create_time)
					SELECT uid,
						IF(success=1,money,0)        AS money,
						IF(success=0,money,0)        AS money_fail,
						IF(success=1,refund,0)       AS refund,
						IF(success=1,coin,0)         AS coin,
						IF(success=1,money_3,0)      AS money_3,
						IF(success=1,money_4,0)      AS money_4,
						IF(success=1,money_5,0)      AS money_5,
						IF(success=1,money_6,0)      AS money_6,
						IF(success=1,money_8,0)      AS money_8,
						IF(success=1,money_9,0)      AS money_9,
						IF(success=1,money_10,0)     AS money_10,
						IF(success=1,money_21,0)     AS money_21,
						IF(success=1,money_22,0)     AS money_22,
						IF(success=1,money_23,0)     AS money_23,
						IF(success=1,money_24,0)     AS money_24,
						IF(success=1,times,0)        AS times,
						IF(success=0,times,0)        AS times_fail,
						IF(success=1,times_refund,0) AS times_refund,
						IF(success=1,times_coin,0)   AS times_coin,
						IF(success=1,times_3,0)      AS times_3,
						IF(success=1,times_4,0)      AS times_4,
						IF(success=1,times_5,0)      AS times_5,
						IF(success=1,times_6,0)      AS times_6,
						IF(success=1,times_8,0)    	 AS times_8,
						IF(success=1,times_9,0)      AS times_9,
						IF(success=1,times_10,0)     AS times_10,
						IF(success=1,times_21,0)     AS times_21,
						IF(success=1,times_22,0)     AS times_22,
						IF(success=1,times_23,0)     AS times_23,
						IF(success=1,times_24,0)     AS times_24, special, UNIX_TIMESTAMP()
					FROM ",p_aggr_table,"
					WHERE dt = ",p_cal_dt,"
					ON DUPLICATE KEY UPDATE income_total.money      = income_total.money      + VALUES(money),
																	income_total.money_fail = income_total.money_fail + VALUES(money_fail),
																	income_total.refund   = income_total.refund   + VALUES(refund),
																	income_total.coin     = income_total.coin     + VALUES(coin),
																	income_total.money_3  = income_total.money_3  + VALUES(money_3),
																	income_total.money_4  = income_total.money_4  + VALUES(money_4),
																	income_total.money_5  = income_total.money_5  + VALUES(money_5),
																	income_total.money_6  = income_total.money_6  + VALUES(money_6),
																	income_total.money_8  = income_total.money_8  + VALUES(money_8),
																	income_total.money_9  = income_total.money_9  + VALUES(money_9),
																	income_total.money_10 = income_total.money_10 + VALUES(money_10),
																	income_total.money_21 = income_total.money_21 + VALUES(money_21),
																	income_total.money_22 = income_total.money_22 + VALUES(money_22),
																	income_total.money_23 = income_total.money_23 + VALUES(money_23),
																	income_total.money_23 = income_total.money_24 + VALUES(money_24),
																	income_total.times    = income_total.times    + VALUES(times),
																	income_total.times_fail   = income_total.times_fail   + VALUES(times_fail),
																	income_total.times_refund = income_total.times_refund + VALUES(times_refund),
																	income_total.times_coin   = income_total.times_coin   + VALUES(times_coin),
																	income_total.times_3  = income_total.times_3  + VALUES(times_3),
																	income_total.times_4  = income_total.times_4  + VALUES(times_4),
																	income_total.times_5  = income_total.times_5  + VALUES(times_5),
																	income_total.times_6  = income_total.times_6  + VALUES(times_6),
																	income_total.times_8  = income_total.times_8  + VALUES(times_8),
																	income_total.times_9  = income_total.times_9  + VALUES(times_9),
																	income_total.times_10 = income_total.times_10 + VALUES(times_10),
																	income_total.times_21 = income_total.times_21 + VALUES(times_21),
																	income_total.times_22 = income_total.times_22 + VALUES(times_22),
																	income_total.times_23 = income_total.times_23 + VALUES(times_23),
																	income_total.times_24 = income_total.times_24 + VALUES(times_24)
					;");
			PREPARE STMT FROM @p_sql;   
			EXECUTE STMT;

		END IF;

		DEALLOCATE PREPARE STMT;     ###释放掉预处理段 

	END IF;
	####################################################################################

	## 重跑数据, 结算日期不变
	IF p_cur_dt > p_cal_dt AND p_type = 0 THEN

		## 标识任务执行结束
		UPDATE task_list
		SET end_dt = UNIX_TIMESTAMP(), checkpoint = 0
		WHERE tid = 32;

		## 加入执行日志
		INSERT INTO task_log (dt, tid, cal_dt, start_dt, end_dt, duration, parallel, type, proc_name, task_name, run_status)
		SELECT DATE_FORMAT(CURDATE(),'%Y%m%d') AS dt, tid, DATE_FORMAT(DATE_SUB(cal_dt,INTERVAL 1 DAY), '%Y%m%d') AS cal_dt, 
			start_dt, end_dt, end_dt - start_dt AS duration, parallel, type, proc_name, task_name, run_status
		FROM task_list
		WHERE tid = 32;

	## 第一次跑, 结算日期+1, 继续执行下一个任务
	ELSEIF p_cur_dt > p_cal_dt AND p_type >= 1 THEN

		##########################################################################################
		##																模式 1 重跑后续所有数据																##
		##########################################################################################
		IF p_type = 1 THEN 

			## 标识任务执行结束 结算日期+1
			UPDATE task_list
			SET cal_dt = DATE_FORMAT(DATE_ADD(cal_dt,INTERVAL 1 DAY), '%Y%m%d'), end_dt = UNIX_TIMESTAMP(), checkpoint = 0
			WHERE tid = 32;

		ELSEIF p_type > 1 THEN

			## 标识任务执行结束 结算日期不变
			UPDATE task_list
			SET end_dt = UNIX_TIMESTAMP(), checkpoint = 0
			WHERE tid = 32;

		END IF;

		## 加入执行日志
		INSERT INTO task_log (dt, tid, cal_dt, start_dt, end_dt, duration, parallel, type, proc_name, task_name, run_status)
		SELECT DATE_FORMAT(CURDATE(),'%Y%m%d') AS dt, tid, DATE_FORMAT(DATE_SUB(cal_dt,INTERVAL 1 DAY), '%Y%m%d') AS cal_dt, 
			start_dt, end_dt, end_dt - start_dt AS duration, parallel, type, proc_name, task_name, run_status
		FROM task_list
		WHERE tid = 32;

		################################## 下一个任务 ######################################
		## 接着下一个任务 心跳简表 tid = 34
		CALL heartbeat_brief_proc(p_cal_dt, p_type);
		####################################################################################

	END IF;

END