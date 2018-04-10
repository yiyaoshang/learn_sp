CREATE PROCEDURE `showself_channel_manage_proc`(IN start_time INT)
    COMMENT '秀色新技术规则'
BEGIN

/**
* 存储过程名称: 秀色新技术规则
* 存储过程说明: 统计出以设备为单位的激活用户, 并且该用户在主播开麦的房间内停留时间超过5分钟并且满足一系列条件的用户。
* 数据项定义: 新增用户按设备首次激活（idfa去重）,
* 实现说明: 取出查询时段的新增用户（idfa去重）, 通过idfa取得IP地址, 通过IP地址得到该IP地址在过去31天总的激活设备数,
* 输入参数: 1个, 日期
* 数据来源: 7个, 1. 首次联网表: showself_statistics_wild_first_visitor
*                2. 游客访问信息表: sys_req_data_total
*                3. 充值表: showself_statistics_wild_recharge_detail
*                4. 消费表: showself_statistics_wild_user_consume
*                5. 注册表: showself_statistics_wild_register_detail
*                6. 原始心跳表: imeeta_ext_db.shall_cust_user_heartbeat_log
*                7. 主播开麦时间表: imeeta_utf8.shall_rpt_anchor_workload
* 目标表格: 1个, showself_channel_manage_result.
* 调用说明: CALL showself_channel_manage_proc(20150825);
* 查询说明: SELECT * FROM showself_channel_manage_result;
* 创建时间: 2015-05-26
* 修改时间: 2015-05-27
* 修改说明: 修改访问房间次数计数规则, 以前没有汇总, 修改为新增用户查询时段访问所有房间累计总数
* 修改时间: 2015-06-04
* 修改说明: 增加登录次数 login_num
* 修改时间: 2015-06-05
* 修改说明: 从乐嗨移植到秀色
* 修改时间: 2015-08-25
* 修改说明: 加上乐嗨的所有补丁
* 修改时间: 2015-11-03
* 修改说明: 改用运管后台的起步价 gamedata_channel_info
* 修改时间: 2015.11.12
* 修改说明: 增加实际结算量 final_num_publish
* 修改时间: 2015.11.19
* 修改说明: 理论结算量环比 final_num_ratio
* 修改时间: 2015.11.23
* 修改说明: 导入数据后把发布按钮置为可用: 修改状态表: showself_statistics_task_status 状态位
* 修改时间: 2015-11-24
* 修改说明: 增加一个 p_proc_status 状态位, 支持一下子发布多天数据(在DM界面点击发布按钮会把没有发布的数据一并发布)
* 修改时间: 2015-11-27
* 修改说明: 版本7.6.3以下（不含）不结算, 从2016.02.18的数据开始生效(含18号)
* 修改时间: 2016-02-18
* 修改说明: 版本7.6.3以下（不含）不结算, 但显示出来, 从2016.02.19的数据开始生效(含19号)
* 修改时间: 2016-02-19
*/

  DECLARE p_target_table        VARCHAR(250)    DEFAULT '';  #生成数据的表
  DECLARE p_sql                 VARCHAR(1024)   DEFAULT ''; #sql
  DECLARE p_start_time          INT DEFAULT 0;  #开始时间
  DECLARE p_start_time_12       INT DEFAULT 0;  #开始时间 前12小时
  DECLARE p_end_time            INT DEFAULT 0;  #截至时间 当天最后一秒
	DECLARE p_proc_status         INT DEFAULT 0;  #天数

  SET p_target_table='showself_channel_manage';
  SET p_start_time=UNIX_TIMESTAMP(start_time);
  SET p_start_time_12=UNIX_TIMESTAMP(start_time) - 12*60*60;     #开始时间 前12小时
  SET p_end_time=UNIX_TIMESTAMP(start_time) + 24*60*60-1;        #截至时间 当天最后一秒

  /**
  * 实现说明: 取出查询时段及前一天中午12点后的新增用户
  * 去重标准: IDFA, 首次联网表是按IDFA去重的
  * 限制条件: 新增用户
  * 数据来源: 1个, 首次联网表: showself_statistics_wild_first_visitor
  * 查询时段: 查询当天及前一天中午12点后
  * 创建时间: 2015-05-26
  * 修改时间: 2015-05-27
  */
  ##先清空初次联网临时表
  TRUNCATE showself_channel_manage_first_visit_tmp;

  ##插入查询时段及前一天中午12点后初次联网数据
  ##标准渠道（安卓102000、ios为100100，ipad为100100）
  ##修改说明: 仅对 安卓7.5.3及以上版本
  ##修改时间: 2015-08-25
  ##修改说明: 版本7.6.3以下（不含）不结算, 从2016.02.18的数据开始生效(含18号)
	##修改时间: 2016-02-18
	##修改说明: 版本7.6.3以下（不含）不结算, 但显示出来, 从2016.02.19的数据开始生效(含19号)
	##修改时间: 2016-02-19
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_first_visit_tmp (idfa, ipaddr, channel_id, channel_type, device_type, dateline)
      SELECT idfa, ipaddr, channel_id,
        CASE
          WHEN LOCATE('s',channel_id)=1     AND RIGHT(channel_id,6) = '102000'  THEN 1
          WHEN LEFT(channel_id,5) = '10102' AND RIGHT(channel_id,6) = '100100'  THEN 2
          WHEN LEFT(channel_id,5) = '10135' AND RIGHT(channel_id,6) = '100100'  THEN 3
          ELSE 4
        END AS channel_type, device_type, dateline
      FROM showself_statistics_wild_first_visitor
      WHERE dateline BETWEEN ",p_start_time_12," AND ",p_end_time,"
      	AND IF(LOCATE('s', channel_id) = 1, SUBSTRING(channel_id, -9, 3) >= 753, 1 = 1)
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入目标表格
  SET @p_sql := CONCAT("INSERT INTO ",p_target_table," (dateline, channel_id, channel_type, idfa, device_type)
      SELECT dateline, channel_id, channel_type, idfa, device_type
      FROM showself_channel_manage_first_visit_tmp
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  /**
  * 实现说明: 删除并插入过去31天新增临时表
  * 去重标准: IDFA, 通过IDFA关联
  * 限制条件: 新增用户
  * 数据来源: 1个, 游客访问信息临时表: showself_channel_manage_sys_req_tmp
  * 查询时段: 查询当天及前一天中午12点后
  * 创建时间: 2015-05-26
  * 修改时间: 2015-05-27
  */
  ##删除31天前的数据
	##修复bug,31*3600 -> 31*24*3600
  SET @p_sql := CONCAT("DELETE FROM showself_channel_manage_first_visit_last_31_days
      WHERE dateline < ",p_start_time," - 31*24*3600
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入当天数据
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_first_visit_last_31_days (idfa, ipaddr, dateline)
      SELECT idfa, ipaddr, dateline
      FROM showself_channel_manage_first_visit_tmp
      WHERE dateline >= ",p_start_time,"
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  /**
  * 实现说明: 删除并插入过去31天新增临时表
  * 去重标准: IDFA, 通过IDFA关联
  * 限制条件: 新增用户
  * 数据来源: 1个, 游客访问信息临时表: showself_channel_manage_sys_req_tmp
  * 查询时段: 查询当天及前一天中午12点后
  * 创建时间: 2015-05-26
  * 修改时间: 2015-05-27
  */
  ##先清空游客访问信息临时表
  TRUNCATE showself_channel_manage_sys_req_tmp;

  ##插入查询时段IP地址和QQ登录以及微信登陆 serv_loginweixin
  ##修改说明: 仅对 安卓7.5.3及以上版本, QQ登录率变更为（QQ+微信）登录率
  ##修改时间: 2015-08-25
	##修改说明: 版本7.6.3以下（不含）不结算, 从2016.02.18的数据开始生效(含18号)
	##修改时间: 2016-02-18
	##修改说明: 版本7.6.3以下（不含）不结算, 但显示出来, 从2016.02.19的数据开始生效(含19号)
	##修改时间: 2016-02-19
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_sys_req_tmp (idfa, ipaddr, channel_id, login_qq, login_weixin, terminal, idfv)
      SELECT macaddr, INET_ATON(IF(LOCATE(',',ipaddr) > 0, SUBSTRING(ipaddr, LOCATE(',',ipaddr) + 2), ipaddr)), channelid, 
      COUNT(IF(action = 'serv_loginqq', 0, NULL)), 
      COUNT(IF(action = 'serv_loginweixin', 0, NULL)), 
      terminal, idfv
      FROM imeeta_ext_db.sys_req_data_total_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"
      WHERE macaddr IS NOT NULL AND macaddr <> '' AND macaddr <> '0'
      	AND IF(LOCATE('s', channelid) = 1, SUBSTRING(channelid, -9, 3) >= 753, 1 = 1)
      GROUP BY macaddr
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##先清空登录次数临时表
  TRUNCATE showself_channel_manage_login_num_tmp;

  ##插入登录次数(间隔大于90秒)
  ##修改说明: 仅对 安卓7.5.3及以上版本
  ##修改时间: 2015-08-25
	##修改说明: 版本7.6.3以下（不含）不结算, 从2016.02.18的数据开始生效(含18号)
	##修改时间: 2016-02-18
	##修改说明: 版本7.6.3以下（不含）不结算, 但显示出来, 从2016.02.19的数据开始生效(含19号)
	##修改时间: 2016-02-19
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_login_num_tmp (idfa, login_num)
      SELECT macaddr, COUNT(action) FROM imeeta_ext_db.sys_req_data_total_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"
      WHERE macaddr IS NOT NULL AND macaddr <> '' AND macaddr <> '0'
      	AND IF(LOCATE('s', channelid) = 1, SUBSTRING(channelid, -9, 3) >= 753, 1 = 1)
      GROUP BY macaddr
      HAVING MAX(dateline) - MIN(dateline) >= 90
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入并更新查询时段新增用户登录次数
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_sys_req_tmp AS s
      INNER JOIN showself_channel_manage_login_num_tmp AS g ON g.idfa = s.idfa
      SET s.login_num = g.login_num
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##qq
  SET @p_sql := CONCAT("UPDATE ",p_target_table," AS a
      INNER JOIN showself_channel_manage_sys_req_tmp AS s ON s.idfa = a.idfa
      SET a.login_qq = IF(s.login_qq > 0, 1, 0),
          a.login_weixin = IF(s.login_weixin > 0, 1, 0),
          a.login_num = s.login_num
      WHERE a.dateline BETWEEN ",p_start_time," AND ",p_end_time,"
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入并更新查询时段的新增用户的IP地址、每IP地址激活设备数及设备类型到目标表格
  #SET @p_sql := CONCAT("UPDATE showself_channel_manage_first_visit_last_31_days AS t
  #    INNER JOIN showself_channel_manage_sys_req_tmp AS s ON s.idfa = t.idfa
  #    SET t.ipaddr = s.ipaddr
  #    WHERE t.dateline >= ",p_start_time,"
  #    ;");
  #PREPARE STMT FROM @p_sql;
  #EXECUTE STMT;


  ##先清空对应临时表
  TRUNCATE showself_channel_manage_login_qq_baseline_tmp;

  ##计算标准渠道（安卓100900、ios为100100, ipad为100100）的QQ登录率基数
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_login_qq_baseline_tmp (channel_type, login_qq_baseline)
      SELECT channel_type, (SUM(login_qq) + SUM(login_weixin))/COUNT(login_qq)
      FROM showself_channel_manage
      WHERE dateline BETWEEN ",p_start_time," AND ",p_end_time,"
        AND channel_type IN (1, 2, 3)
      GROUP BY channel_type
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##计算标准渠道（安卓100900、ios为100100, ipad为100100）的QQ登录率基数
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_login_qq_baseline_tmp
      SET login_qq_baseline = IF(channel_type = 1, 0.5, IF(channel_type = 2, 0.48, 0.6))
      WHERE login_qq_baseline = 0
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##计算每IP地址激活设备数
  ##先清空对应临时表
  TRUNCATE showself_channel_manage_device_per_ip_tmp;

  ##统计出每IP地址激活设备数
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_device_per_ip_tmp (idfa, device_per_ip)
      SELECT idfa, COUNT(idfa)
      FROM showself_channel_manage_first_visit_last_31_days
      GROUP BY ipaddr
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入并更新查询时段及前一天中午12点后的新增用户的每IP地址激活设备数
  SET @p_sql := CONCAT("UPDATE ",p_target_table," AS t
      INNER JOIN showself_channel_manage_device_per_ip_tmp AS d ON d.idfa = t.idfa
      SET t.device_per_ip = d.device_per_ip
      WHERE t.dateline BETWEEN ",p_start_time," AND ",p_end_time,"
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  /**
  * 实现说明: 插入并更新查询时段新增用户的充值状态
  * 去重标准: IDFA, 通过IDFA关联
  * 限制条件: 新增用户
  * 数据来源: 1个, 充值临时表: showself_channel_manage_recharge_tmp
  * 查询时段: 查询当天
  * 创建时间: 2015-05-26
  * 修改时间: 2015-05-27
  */
  ##先清空注册临时表
  TRUNCATE showself_channel_manage_register_daily_tmp;

  ##插入查询时段数据到注册临时表
  ##修改说明: 仅对 安卓7.5.3及以上版本
  ##修改时间: 2015-08-25
	##修改说明: 版本7.6.3以下（不含）不结算, 从2016.02.18的数据开始生效(含18号)
	##修改时间: 2016-02-18
	##修改说明: 版本7.6.3以下（不含）不结算, 但显示出来, 从2016.02.19的数据开始生效(含19号)
	##修改时间: 2016-02-19
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_register_daily_tmp (uid, idfa, channel_type)
      SELECT uid, idfa,
        CASE
          WHEN LOCATE('s',channel_id)=1     AND RIGHT(channel_id,6) = '102000'  THEN 1
          WHEN LEFT(channel_id,5) = '10102' AND RIGHT(channel_id,6) = '100100'  THEN 2
          WHEN LEFT(channel_id,5) = '10135' AND RIGHT(channel_id,6) = '100100'  THEN 3
          ELSE 4
        END AS channel_type
      FROM showself_statistics_wild_register_detail
      WHERE dateline BETWEEN ",p_start_time," AND ",p_end_time,"
      	AND IF(LOCATE('s', channel_id) = 1, SUBSTRING(channel_id, -9, 3) >= 753, 1 = 1)
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##先清空注册临时表
  UPDATE showself_channel_manage_added_register_daily_tmp
  SET added_num = 0;

  ##插入查询时段数据到注册临时表
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_added_register_daily_tmp (channel_type, added_num)
      SELECT r.channel_type, @added_num := COUNT(fv.idfa)
      FROM showself_channel_manage_register_daily_tmp AS r
      INNER JOIN showself_channel_manage_first_visit_tmp AS fv ON fv.idfa = r.idfa
      WHERE fv.dateline > ",p_start_time,"
      GROUP BY r.channel_type
      ON DUPLICATE KEY UPDATE added_num = @added_num
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ## 新增注册=0 计算标准渠道（安卓100900、ios为100100, ipad为100100）的QQ登录率基数
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_login_qq_baseline_tmp AS q
      INNER JOIN showself_channel_manage_added_register_daily_tmp AS a ON a.channel_type = q.channel_type
      SET login_qq_baseline = IF(q.channel_type = 1, 0.5, IF(q.channel_type = 2, 0.48, 0.6))
      WHERE a.added_num = 0
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##先清空充值临时表
  TRUNCATE showself_channel_manage_recharge_tmp;

  ##插入查询时段数据到充值临时表
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_recharge_tmp (uid, idfa)
      SELECT rc.uid, r.idfa
      FROM showself_statistics_wild_recharge_detail_",FROM_UNIXTIME(p_start_time,'%Y%m')," AS rc
      INNER JOIN showself_channel_manage_register_daily_tmp AS r ON r.uid = rc.uid
      WHERE rc.dateline BETWEEN ",p_start_time," AND ",p_end_time,"
        AND rc.uid <> 3000006
        AND rc.rechange_status = 30
        AND rc.rechange_channel < 20
      GROUP BY rc.uid
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入并更新查询时段新增用户的充值状态到目标表格
  SET @p_sql := CONCAT("UPDATE ",p_target_table," AS t
      INNER JOIN showself_channel_manage_recharge_tmp AS rc ON rc.idfa = t.idfa
      SET t.recharge = 1
      WHERE t.dateline BETWEEN ",p_start_time," AND ",p_end_time,"
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  /**
  * 实现说明: 插入并更新查询时段新增用户的消费状态
  * 去重标准: IDFA, 通过IDFA关联
  * 限制条件: 新增用户
  * 数据来源: 1个, 消费临时表: showself_channel_manage_consume_tmp
  * 查询时段: 查询当天
  * 创建时间: 2015-05-26
  */
  ##先清空消费临时表
  TRUNCATE showself_channel_manage_consume_tmp;

  ##插入查询时段数据到消费临时表
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_consume_tmp (uid, idfa)
      SELECT c.uid, r.idfa
      FROM showself_statistics_wild_user_consume_",FROM_UNIXTIME(p_start_time,'%Y%m')," AS c
      INNER JOIN showself_channel_manage_register_daily_tmp AS r ON r.uid = c.uid
      WHERE c.dateline BETWEEN ",p_start_time," AND ",p_end_time,"
        AND c.uid <> 3000006
        AND c.consume_coin > 0
      GROUP BY c.uid
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入并更新查询时段新增用户的消费状态到目标表格
  SET @p_sql := CONCAT("UPDATE ",p_target_table," AS t
      INNER JOIN showself_channel_manage_consume_tmp AS c ON c.idfa = t.idfa
      SET t.consume = 1
      WHERE t.dateline BETWEEN ",p_start_time," AND ",p_end_time,"
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  /**
  * 实现说明: 插入并更新查询时段及前一天中午12点后的新增用户访问的有效时长
  * 去重标准: IDFA, 通过IDFA关联
  * 限制条件: 新增用户
  * 数据来源: 1个, 主播有效时长临时表: showself_channel_manage_heartbeat_tmp
  * 查询时段: 查询当天及前一天中午12点后
  * 创建时间: 2015-05-26
  */

  ##先清空主播开麦时间临时表
  TRUNCATE showself_channel_manage_anchor_workload_tmp;

  ##插入查询时段及前一天中午12点后主播工作时间数据
  ##dateline字段不能用,这个字段是系统计算的时间 - 2015.08.26
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_anchor_workload_tmp (roomid, dateline, start_dt, end_dt)
      SELECT roomid, dateline, start_dt, end_dt
      FROM imeeta_utf8.shall_rpt_anchor_workload
      WHERE start_dt < ",p_end_time,"
        AND end_dt   > ",p_start_time_12,"
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##先清空心跳临时表
  TRUNCATE showself_channel_manage_heartbeat_log_tmp;

  ##插入查询时段及前一天中午12点后心跳原始数据
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_heartbeat_log_tmp (roomid, idfa, dateline)
      SELECT roomid, idfa, dateline
      FROM imeeta_ext_db.shall_cust_user_heartbeat_log_",FROM_UNIXTIME(p_start_time_12,'%Y%m%d'),"
      WHERE dateline > ",p_start_time_12,"
      UNION ALL
      SELECT roomid, idfa, dateline FROM imeeta_ext_db.shall_cust_user_heartbeat_log_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##先清空有效时长临时表
  TRUNCATE showself_channel_manage_heartbeat_tmp;

  ##计算出查询时段心跳数据并插入临时表
  ##修改说明: 去掉间隔时间太短的心跳
  ##修改时间: 2015.11.03
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_heartbeat_tmp (idfa, heartbeat)
      SELECT h.idfa, COUNT(DISTINCT LEFT(dateline, 9)) AS heartbeat FROM showself_channel_manage_heartbeat_log_tmp AS h
      WHERE h.dateline > ",p_start_time,"
        AND LENGTH(h.idfa) > 1
      GROUP BY h.idfa;
      ");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入并更新查询时段新增用户访问的有效时长到目标表格
  SET @p_sql := CONCAT("UPDATE ",p_target_table," AS t
      INNER JOIN showself_channel_manage_heartbeat_tmp AS h ON h.idfa = t.idfa
      SET t.heartbeat = h.heartbeat
      WHERE t.dateline BETWEEN ",p_start_time," AND ",p_end_time,"
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  /**
  * 实现说明: 插入并更新查询时段及前一天中午12点后、自激活后12小时内、的新增用户访问的有效时长
  * 去重标准: IDFA, 通过IDFA关联
  * 限制条件: 新增用户
  * 数据来源: 1个, 有效时长临时表: showself_channel_manage_heartbeat_active_tmp
  * 查询时段: 查询当天及前一天中午12点后
  * 创建时间: 2015-05-26
  */
  ##先清空心跳(12小时前)临时表
  TRUNCATE showself_channel_manage_heartbeat_active_tmp;

  ##计算出查询时段及前一天中午12点后、自激活后12小时内、有效时长数据并插入临时表
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_heartbeat_active_tmp (idfa, heartbeat_active)
      SELECT h.idfa, COUNT(h.idfa)
      FROM showself_channel_manage_heartbeat_log_tmp AS h
      INNER JOIN showself_channel_manage_anchor_workload_tmp AS a ON a.roomid = h.roomid
      INNER JOIN showself_channel_manage_first_visit_tmp AS fv ON fv.idfa = h.idfa
      WHERE h.dateline <= fv.dateline + 12*60*60
      AND h.dateline BETWEEN a.start_dt AND a.end_dt
      GROUP BY h.idfa
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入并更新查询时段累计房间内停留时间不足2分钟的用户、从激活起12小时内的有效时间
  SET @p_sql := CONCAT("UPDATE ",p_target_table," AS t
      INNER JOIN showself_channel_manage_heartbeat_active_tmp AS h ON h.idfa = t.idfa
      SET t.heartbeat_active = h.heartbeat_active
      WHERE t.heartbeat < 2
        AND t.dateline BETWEEN ",p_start_time_12," AND ",p_end_time,"
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  /**
  * 实现说明: 插入并更新查询时段及前一天中午12点后的新增用户访问的主播在麦房间数
  * 去重标准: IDFA, 通过IDFA关联
  * 限制条件: 新增用户
  * 数据来源: 1个, 访问的房间个数临时表: showself_channel_manage_room_num_tmp
  * 查询时段: 查询当天及前一天中午12点后
  * 注意事项: 一定要是主播在麦的、房间数
  * 创建时间: 2015-05-26
  */
  ##先清空访问房间数临时表
  TRUNCATE showself_channel_manage_room_num_tmp;

  ##插入查询时段主播在麦房间数
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_room_num_tmp (idfa, room_num)
      SELECT h.idfa, COUNT(DISTINCT h.roomid)
      FROM showself_channel_manage_heartbeat_log_tmp AS h
      INNER JOIN showself_channel_manage_anchor_workload_tmp AS a ON a.roomid = h.roomid
      INNER JOIN showself_channel_manage_first_visit_tmp AS fv ON fv.idfa = h.idfa
      WHERE h.dateline > ",p_start_time,"
        AND a.dateline > ",p_start_time,"
        AND fv.dateline > ",p_start_time,"
        AND h.dateline BETWEEN a.start_dt AND a.end_dt
      GROUP BY h.idfa
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入并更新查询时段新增用户访问的房间个数
  SET @p_sql := CONCAT("UPDATE ",p_target_table," AS t
      INNER JOIN showself_channel_manage_room_num_tmp AS r ON r.idfa = t.idfa
      SET t.room_num = r.room_num
      WHERE t.dateline BETWEEN ",p_start_time," AND ",p_end_time,"
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  /**
  * 实现说明: 插入ios用户在软件内停留时间
  * 去重标准: IDFA, 通过IDFA关联
  * 限制条件: 新增用户
  * 数据来源: 1个, 表: imeeta_ext_db.sys_req_data_total_
  * 查询时段: 查询当天
  * 注意事项: 只对ios用户
  * 创建时间: 2015-10-29
  * 修改说明: 秀色
  * 修改时间: 2015-10-29
  */
  ##先清空ios停留时长临时表
  TRUNCATE showself_channel_manage_duration_tmp;

  ##插入查询时段ios停留时长
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_duration_tmp (idfa, dateline)
      SELECT macaddr, MAX(dateline) - MIN(dateline) FROM imeeta_ext_db.sys_req_data_total_",FROM_UNIXTIME(p_start_time,'%Y%m%d'),"
      WHERE LEFT(channelid, 5) = '10102' OR LEFT(channelid, 5) = '10135'
      GROUP BY macaddr
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  #########################################################把结果插入结果表######################################################################

  ##插入查询时段新增用户的全部激活数和统计数
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_result (dateline, channel_id, active_num, stat_num)
      SELECT FROM_UNIXTIME(dateline, '%Y%m%d'), channel_id, COUNT(DISTINCT idfa) AS active_num, COUNT(DISTINCT IF(device_per_ip < 5, idfa, NULL)) AS stat_num
      FROM showself_channel_manage
      WHERE dateline BETWEEN ",p_start_time," AND ",p_end_time,"
        AND channel_id <> '0'
        AND channel_id <> 'EN_APP_KEY_PLACEHOLDER'
        AND LENGTH(channel_id) BETWEEN 8 AND 17
        AND LOCATE('.', channel_id) < 1 AND LOCATE('_', channel_id) < 1 AND LOCATE('%', channel_id) < 1
      GROUP BY FROM_UNIXTIME(dateline, '%Y%m%d'), channel_id
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入查询时段 terminal与渠道号规则不符 剔除条件 1 数  
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_result (dateline, channel_id, del_1_num)
      SELECT FROM_UNIXTIME(dateline, '%Y%m%d'), channel_id, @del_1 := COUNT(DISTINCT idfa)
      FROM showself_channel_manage
      WHERE dateline BETWEEN ",p_start_time," AND ",p_end_time,"
        AND channel_id <> '0'
        AND channel_id <> 'EN_APP_KEY_PLACEHOLDER'
        AND LENGTH(channel_id) BETWEEN 8 AND 17
        AND LOCATE('.', channel_id) < 1 AND LOCATE('_', channel_id) < 1 AND LOCATE('%', channel_id) < 1
        AND device_per_ip <= 5
        AND ((LOCATE('s',channel_id) = 1 AND device_type <> 2) 
        	OR (LOCATE('s',channel_id) <> 1 AND device_type = 2) 
        	OR (LEFT(channel_id,5)='10102' AND device_type <> 3) 
        	OR (LEFT(channel_id,5)<>'10102' AND device_type = 3))
      GROUP BY FROM_UNIXTIME(dateline, '%Y%m%d'), channel_id
      ON DUPLICATE KEY UPDATE del_1_num = @del_1
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入查询时段 剔除条件 2 数  累计房间内心跳<3分钟
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_result (dateline, channel_id, del_2_num)
      SELECT FROM_UNIXTIME(dateline, '%Y%m%d'), channel_id, @del_2 := COUNT(DISTINCT idfa)
      FROM showself_channel_manage
      WHERE dateline BETWEEN ",p_start_time," AND ",p_end_time,"
        AND channel_id <> '0'
        AND channel_id <> 'EN_APP_KEY_PLACEHOLDER'
        AND LENGTH(channel_id) BETWEEN 8 AND 17
        AND LOCATE('.', channel_id) < 1 AND LOCATE('_', channel_id) < 1 AND LOCATE('%', channel_id) < 1
        AND device_per_ip <= 5
        AND ((LOCATE('s',channel_id) = 1 AND device_type = 2) OR (LEFT(channel_id,5)='10102' AND device_type = 3))
        AND heartbeat < 3
      GROUP BY FROM_UNIXTIME(dateline, '%Y%m%d'), channel_id
      ON DUPLICATE KEY UPDATE del_2_num = @del_2
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入查询时段 剔除条件 2 数    (仅对ios)
  ##创建说明: 在软件内停留时间不足3分钟；即max行为时间-激活时间，if这个值<0 then 记为0
  ##创建时间: 2015-10-29
  ##修改说明: 
  ##修改时间: 2015-10-29
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_result (dateline, channel_id, del_2_num)
      SELECT FROM_UNIXTIME(m.dateline, '%Y%m%d'), channel_id, @del_2 := COUNT(DISTINCT m.idfa)
      FROM showself_channel_manage AS m
      INNER JOIN showself_channel_manage_duration_tmp AS d ON d.idfa = m.idfa
      WHERE (LEFT(channel_id, 5) = '10102' OR LEFT(channel_id, 5) = '10135')
        AND m.dateline BETWEEN ",p_start_time," AND ",p_end_time,"
        AND device_per_ip <= 5
        AND ((LEFT(channel_id,5)='10102' AND (device_type = 3 OR device_type = 4))
          OR (LEFT(channel_id,5)='10135' AND (device_type = 3 OR device_type = 4)))
        AND d.dateline < 180
      GROUP BY FROM_UNIXTIME(m.dateline, '%Y%m%d'), channel_id
      ON DUPLICATE KEY UPDATE del_2_num = @del_2
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入查询时段 增加条件 1 数  (仅对非ios)
  ##修改说明: 更新乐嗨产品编码（乐嗨的产品编码不同于秀色）
  ##修改时间: 2015-08-06
  ##修改说明: 把停留时间由2分钟变成3分钟
  ##修改时间: 2015.10.12
  ##修改说明: 乐嗨的ios-cpa推广上线, 安卓和ios单独计算
  ##修改时间: 2015-10-29
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_result (dateline, channel_id, add_1_num)
    SELECT FROM_UNIXTIME(dateline, '%Y%m%d'), channel_id, @add_1 := COUNT(DISTINCT idfa)
    FROM showself_channel_manage
    WHERE (LEFT(channel_id, 5) <> '10102' AND LEFT(channel_id, 5) <> '10135')
      AND dateline BETWEEN ",p_start_time," AND ",p_end_time,"
      AND channel_id <> '0'
      AND channel_id <> 'EN_APP_KEY_PLACEHOLDER'
      AND LENGTH(channel_id) BETWEEN 8 AND 17
      AND LOCATE('.', channel_id) < 1 AND LOCATE('_', channel_id) < 1 AND LOCATE('%', channel_id) < 1
      AND device_per_ip <= 5
      AND (LOCATE('s',channel_id) = 1 AND device_type = 2)
      AND heartbeat < 3 AND heartbeat_active > 3
    GROUP BY FROM_UNIXTIME(dateline, '%Y%m%d'), channel_id
    ON DUPLICATE KEY UPDATE add_1_num = @add_1
    ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入查询时段 增加条件 1 数  (仅对ios) 暂时不用, 置为0, 用默认值
  ##创建说明: 增加条件1、条件4全部设置为0；
  ##创建时间: 2015-10-29
  ##修改说明: 乐嗨的ios-cpa推广上线, 安卓和ios单独计算
  ##修改时间: 2015-10-29
  #SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_result (dateline, channel_id, add_1_num)
  #    SELECT FROM_UNIXTIME(dateline, '%Y%m%d'), channel_id, @add_1 := COUNT(DISTINCT idfa)
  #    FROM showself_channel_manage
  #    WHERE (LEFT(channel_id, 5) = '10102' OR LEFT(channel_id, 5) = '20101')
  #        AND dateline BETWEEN ",p_start_time," AND ",p_end_time,"
  #      AND device_per_ip <= 5
  #      AND ((LEFT(channel_id,5)='10102' AND (device_type = 3 OR device_type = 4))
  #          OR (LEFT(channel_id,5)='20101' AND (device_type = 3 OR device_type = 4)))
  #    GROUP BY FROM_UNIXTIME(dateline, '%Y%m%d'), channel_id
  #    ON DUPLICATE KEY UPDATE add_1_num = @add_1
  #    ;");
  #PREPARE STMT FROM @p_sql;
  #EXECUTE STMT;

  ##插入查询时段 剔除条件 3 数  (仅对非ios)
  ##修改说明: 更新乐嗨产品编码（乐嗨的产品编码不同于秀色）
  ##修改时间: 2015-08-06
  ##修改说明: 把停留时间由2分钟变成3分钟
  ##修改时间: 2015.10.12
  ##修改说明: 乐嗨的ios-cpa推广上线, 安卓和ios单独计算
  ##修改时间: 2015-10-29
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_result (dateline, channel_id, del_3_num)
      SELECT FROM_UNIXTIME(dateline, '%Y%m%d'), channel_id, @del_3 := COUNT(DISTINCT idfa)
      FROM showself_channel_manage
      WHERE (LEFT(channel_id, 5) <> '10102' AND LEFT(channel_id, 5) <> '10135')
        AND dateline BETWEEN ",p_start_time," AND ",p_end_time,"
        AND channel_id <> '0'
        AND channel_id <> 'EN_APP_KEY_PLACEHOLDER'
        AND LENGTH(channel_id) BETWEEN 8 AND 17
        AND LOCATE('.', channel_id) < 1 AND LOCATE('_', channel_id) < 1 AND LOCATE('%', channel_id) < 1
        AND device_per_ip <= 5
        AND ((LOCATE('s',channel_id) = 1 AND device_type = 2) 
          OR (LEFT(channel_id,5)='10102' AND device_type = 3)
          OR (LEFT(channel_id,5)='10135' AND device_type = 4))
        AND heartbeat < 3 AND heartbeat_active > 3
        AND room_num < 1 AND recharge = 0 AND consume = 0
        AND login_num < 2
      GROUP BY FROM_UNIXTIME(dateline, '%Y%m%d'), channel_id
      ON DUPLICATE KEY UPDATE del_3_num = @del_3
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入查询时段 剔除条件 3 数  (仅对ios) 暂时不用, 置为0, 用默认值
  ##创建说明: 删除条件3 = （统计数-删除条件1）*((1-X)*(首次注册idfa/去重新增idfa))  暂时默认X=1 btw. 删除条件3 为预留条件，如以后确认appstore榜单需要注册率时，可直接调整X达到目的
  ##创建时间: 2015-10-29
  ##修改说明: 乐嗨的ios-cpa推广上线, 安卓和ios单独计算
  ##修改时间: 2015-10-29
  #SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_result (dateline, channel_id, del_3_num)
  #    SELECT FROM_UNIXTIME(dateline, '%Y%m%d'), channel_id, @del_3 := COUNT(DISTINCT idfa)
  #    FROM showself_channel_manage
  #    WHERE (LEFT(channel_id, 5) = '10102' OR LEFT(channel_id, 5) = '20101')
  #      AND dateline BETWEEN ",p_start_time," AND ",p_end_time,"
  #      AND device_per_ip <= 5
  #      AND ((LEFT(channel_id,5)='10102' AND (device_type = 3 OR device_type = 4))
  #        OR (LEFT(channel_id,5)='20101' AND (device_type = 3 OR device_type = 4)))
  #      AND heartbeat < 3 AND heartbeat_active > 3
  #      AND room_num < 1 AND recharge = 0 AND consume = 0
  #      AND login_num < 2
  #    GROUP BY FROM_UNIXTIME(dateline, '%Y%m%d'), channel_id
  #    ON DUPLICATE KEY UPDATE del_3_num = @del_3
  #    ;");
  #PREPARE STMT FROM @p_sql;
  #EXECUTE STMT;

  ##插入查询时段 剔除条件 4 数

  ##先清空访问房间数临时表
  TRUNCATE showself_channel_manage_login_qq_ratio_tmp;

  ##插入QQ登录率数
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_login_qq_ratio_tmp (channel_id, channel_type, login_qq_ratio)
      SELECT channel_id,
        CASE
          WHEN LOCATE('s',channel_id)=1     THEN 1
          WHEN LEFT(channel_id,5) = '10102' THEN 2
          WHEN LEFT(channel_id,5) = '10135' THEN 3
          ELSE 4
        END AS channel_type, (SUM(login_qq) + SUM(login_weixin))/COUNT(login_qq)
      FROM showself_channel_manage
      WHERE dateline BETWEEN ",p_start_time," AND ",p_end_time,"
        AND channel_id <> '0'
        AND channel_id <> 'EN_APP_KEY_PLACEHOLDER'
        AND LENGTH(channel_id) BETWEEN 8 AND 17
        AND LOCATE('.', channel_id) < 1 AND LOCATE('_', channel_id) < 1 AND LOCATE('%', channel_id) < 1
      GROUP BY channel_id
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入QQ登录率数
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_login_qq_ratio_tmp AS q
      INNER JOIN showself_channel_manage_login_qq_baseline_tmp AS b ON b.channel_type = q.channel_type
      SET q.login_qq_baseline = b.login_qq_baseline
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##更新 条件4 扣量系数  扣量系数=1-（1-实际渠道的QQ登录率/基数）/2
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_result AS r
      INNER JOIN showself_channel_manage_login_qq_ratio_tmp AS q ON q.channel_id = r.channel_id
      SET r.dis_4_num = 1 - (1 - login_qq_ratio/login_qq_baseline) / 2
      WHERE r.dateline = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##更新 条件4 扣量系数 如果大于 1 ，则=1
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_result
      SET dis_4_num = IF(dis_4_num > 1, 1, IFNULL(dis_4_num, 0))
      WHERE dateline = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##更新 条件4 扣量系数 ios=1
  ##修改说明: 安卓和ios单独计算
  ##修改时间: 2015-10-30
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_result
      SET dis_4_num = 1
      WHERE (LEFT(channel_id, 5) = '10102' OR LEFT(channel_id, 5) = '10135')
        AND dateline = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  /**
  * 实现说明: 按渠道计算MD5校验未通过数
  * 去重标准: IDFA, 通过IDFA关联
  * 限制条件: 安卓渠道, terminal = 2
  * 数据来源: 1个: imeeta_ext_db.sys_req_data_total_
  * 查询时段: 查询当天
  * 创建时间: 2015-06-05
  * 修改说明: 2015-06-05
  * 修改时间: 2015-06-05
  */
  ##先清空访问房间数临时表
  TRUNCATE showself_channel_manage_sys_req_md5_tmp;

  ##插入查询时段MD5校验数
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_sys_req_md5_tmp (idfa, channel_id, idfv)
      SELECT idfa, channel_id, IF(idfv = 1, 1, 0) FROM showself_channel_manage_sys_req_tmp
      WHERE terminal = 2
				AND channel_id <> 'EN_APP_KEY_PLACEHOLDER'
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##先清空访问房间数临时表
  TRUNCATE showself_channel_manage_md5_fail_tmp;

  ##插入并更新查询时段MD5校验未通过数
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_md5_fail_tmp (channel_id, md5_fail_num)
      SELECT channel_id, COUNT(idfv) FROM showself_channel_manage_sys_req_md5_tmp
      WHERE idfv = 0
      GROUP BY channel_id
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ## 更新 MD5校验未通过数
	## 修改说明: 修复MD5校验未通过数为0的bug. (LEFT(r.channel_id, 5) = '10102' OR LEFT(r.channel_id, 5) = '10135') -> LOCATE('s', r.channel_id) = 1
	## 修改时间: 2015.12.07
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_result AS r
      INNER JOIN showself_channel_manage_md5_fail_tmp AS m ON m.channel_id = r.channel_id
      SET r.md5_fail_num = m.md5_fail_num
      WHERE LOCATE('s', r.channel_id) = 1
        AND r.dateline = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##更新 激活量=（统计数-剔除条件1-剔除条件2+增加条件1-剔除条件3）*条件4
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_result
      SET cal_num = IF(ROUND(IFNULL((stat_num - del_1_num - del_2_num + add_1_num - del_3_num) * dis_4_num + 0.1, 0), 0) < 0, 0, ROUND(IFNULL((stat_num - del_1_num - del_2_num + add_1_num - del_3_num) * dis_4_num + 0.1, 0), 0))
      WHERE dateline = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##更新单价
	## 修改说明: 改用运管后台的起步价
	## 修改时间: 2015.11.12
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_result AS r
      INNER JOIN op_cal_db.gamedata_channel_info AS i ON 
				IF(LENGTH(r.channel_id)=8,LEFT(r.channel_id,4),RIGHT(r.channel_id,6)) LIKE CONCAT(LEFT(i.code,4),'%')
					AND IF(LEFT(r.channel_id,5)='10301',1,IF(LOCATE('s',r.channel_id)=1,2,IF(LEFT(r.channel_id,5)='10102',3,4))) = i.channel_platform
      SET r.price = i.start_price
      WHERE r.dateline = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##更新扣量比例
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_result AS r
      INNER JOIN imeeta_utf8.shall_partner_channel_discount AS p ON r.channel_id LIKE p.channelid
      SET r.discount_ratio = p.discount
      WHERE r.dateline = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
        AND p.id > 1
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

	##更新扣量比例(按等级先后, grade = 1, 然后2, 最后3) - 2016.02.03
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_result AS r
      INNER JOIN imeeta_utf8.shall_partner_channel_discount AS p ON r.channel_id LIKE p.channelid
      SET r.discount_ratio = p.discount
      WHERE grade = 2
        AND r.dateline = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
        AND p.id > 1
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##更新扣量比例(修复某些渠道扣量系数为0但还有数的问题) - 2015.09.15
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_result AS r
      INNER JOIN imeeta_utf8.shall_partner_channel_discount AS p ON r.channel_id LIKE p.channelid
      SET r.discount_ratio = p.discount
      WHERE grade = 3
        AND r.dateline = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
        AND p.id > 1
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入查询时段起扣量
  SET @p_sql := CONCAT("INSERT INTO showself_channel_manage_channel_total (channel_id, dateline, total)
      SELECT channel_id, @dateline := dateline, @total := cal_num
      FROM showself_channel_manage_result
      WHERE dateline = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
      ON DUPLICATE KEY UPDATE total = total + @total, dateline = @dateline
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入结算量=Σ（结算量1（起扣量以内）+结算量2（起扣量以外的激活量*扣量比例））
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_result AS r
      INNER JOIN showself_channel_manage_channel_total AS t ON t.channel_id = r.channel_id
      SET r.final_num = IF(t.remain = 0, r.cal_num * r.discount_ratio, IF(t.remain > r.cal_num, r.cal_num, t.remain + (r.cal_num - t.remain) * r.discount_ratio)),
					r.final_num_publish = IF(t.remain = 0, r.cal_num * r.discount_ratio, IF(t.remain > r.cal_num, r.cal_num, t.remain + (r.cal_num - t.remain) * r.discount_ratio))
      WHERE r.dateline = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

	##更新理论结算量环比
	##添加时间: 2015.11.23
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_result AS r1
			INNER JOIN showself_channel_manage_result AS r2 ON r2.dateline = DATE_FORMAT(DATE_ADD(r1.dateline, INTERVAL -1 DAY),'%Y%m%d') 
																											AND r2.channel_id = r1.channel_id
			SET r1.final_num_ratio = ROUND(IFNULL(r1.final_num/r2.final_num, 0) * 100, 2)
			WHERE r1.dateline = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##更新剩余扣量
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_channel_total AS t
      INNER JOIN showself_channel_manage_result AS r ON r.channel_id = t.channel_id
      SET t.remain = IF(t.remain - r.cal_num > 0, t.remain - r.cal_num, 0)
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入结算金额=结算量*单价
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_result
      SET total_price = IF(final_num IS NULL OR price IS NULL, 0, final_num * price)
      WHERE dateline = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入合作伙伴编号、名称
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_result AS r
      INNER JOIN imeeta_utf8.shall_partner_def AS p ON r.channel_id LIKE p.channelid
      SET r.partner_id = p.partner_id, r.partner_name = p.partner_name
      WHERE r.dateline = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

	##修改说明: 版本7.6.3以下（不含）不结算, 从2016.02.18的数据开始生效(含18号)
	##修改时间: 2016-02-18
	##修改说明: 版本7.6.3以下（不含）不结算, 但显示出来, 从2016.02.19的数据开始生效(含19号)
	##修改时间: 2016-02-19
  SET @p_sql := CONCAT("UPDATE showself_channel_manage_result 
			SET final_num = 0, final_num_publish = 0, total_price = 0
			WHERE dateline = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
				AND LEFT(channel_id, 1) = 's' AND SUBSTRING(channel_id, 7, 3) < 763
      ;");
  PREPARE STMT FROM @p_sql;
  EXECUTE STMT;

  ##插入网盟后台wm
  #SET @p_sql := CONCAT("INSERT INTO imeeta_utf8.shall_subchannel_daily_summary_internal (partner_id, channelid, dt, cal_dt, unit, unit_price, total)
  #    SELECT partner_id, channel_id, dateline, UNIX_TIMESTAMP(), final_num, price, total_price
  #    FROM showself_channel_manage_result
  #    WHERE dateline = FROM_UNIXTIME(",p_start_time,", '%Y%m%d')
  #    ;");
  #PREPARE STMT FROM @p_sql;
  #EXECUTE STMT;

  DEALLOCATE PREPARE STMT;     ###释放掉预处理段

	/**
  * 实现说明: 更新状态列表的标志位
  * 创建时间: proc_status = 0 表示可以发布
  * 创建说明: 2015-11-24
	* 修改说明: 增加一个 p_proc_status 状态位, 支持一下子发布多天数据(在DM界面点击发布按钮会把没有发布的数据一并发布)
	* 修改时间: 2015-11-27
  */
	##判断昨天的数据是否已经发布
	SELECT proc_status INTO p_proc_status FROM op_stat_db.showself_statistics_task_status WHERE proc_name = 'showself_sync_dm_to_wm_android_proc';

	##如果昨天的数据没有发布, 开始时间不变, 截至时间完后延长1天 end_time=p_end_time+(24*60*60) -> end_time=p_end_time
	IF(p_proc_status = 0) THEN
		UPDATE op_stat_db.showself_statistics_task_def 
			SET end_time=p_end_time, update_time=UNIX_TIMESTAMP() 
			WHERE proc_name = 'showself_sync_dm_to_wm_android_proc';

	##如果昨天的数据已经发布, 把状态列表的标志位置为0, 表示可以发布数据
	ELSE 
			UPDATE op_stat_db.showself_statistics_task_status
			SET proc_status = 0, last_time = UNIX_TIMESTAMP()
			WHERE proc_name = 'showself_sync_dm_to_wm_android_proc';
	END IF;

END