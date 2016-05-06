StartDay='2016-03-01'
EndDay='2016-04-01'
StartDayFT="`date -d "$StartDay" +"%s"`"
EndDayFT="`date -d "$EndDay" +"%s"`"

while (( "${StartDayFT}" < "${EndDayFT}" ))
do
	#echo -e "/n startDay is "$StartDay"/n"
	#在此处理时间
	mbaction_part=$StartDay
	fin_all_Act="use dw_htlbizdb;
	insert overwrite table dw_htlbizdb.uid_serverSDK_combine
	partition (d = '${mbaction_part}')
	select tmp.*
	, flr.star2_ratio
	, flr.uid_num
	from (select d d_date
		,uid
		,count(vid) pv
		,count(distinct vid) uv
		,count(distinct clientcode) s_pv
		,count(clientcode) client_num
		,count(uid) uid_num
		from dw_htlbizdb.htllist_sdk_imp
		where 1 = 1
		and d = '${mbaction_part}'
        and uid not in ( 'ctriptestat', 'djyd', '13845612110',
 	'test111111','13845611999','wwwwww', 'test1111',
	'test111111', 'jjyd', 'wwwwwww', 'leonwang71', 'jerrygao')
		group by d,uid) tmp
	left outer join dw_htlbizdb.tmp_userorder_flagR flr
		on tmp.uid = flr.uid;"
	# 展示SQL语句
	#echo "fin all act is "$fin_all_Act
	# 运行SQL语句,去掉#即可
	hive -e "$fin_all_Act"
	StartDay="`date -d "$StartDay +1 days" "+%Y-%m-%d"`"
	StartDayFT="`date -d "$StartDay" +"%s"`"
done


hive -e "USE dw_htlbizdb;
DROP TABLE IF EXISTS dw_htlbizdb.tmp_uid_pvuv_order_2c;
CREATE TABLE dw_htlbizdb.tmp_uid_pvuv_order_2c as
select ssc.*
,tmp.order_num
,(case when ssc.client_num > 0 then (case when tmp.order_num>0 then tmp.order_num else 0 end)*1.0/ssc.client_num else 0.0 end ) uid_cr
from (select *
	from dw_htlbizdb.uid_serverSDK_combine
	where 1 = 1
	and d >= '2016-03-01'
	and d <= '2016-03-22') ssc
left outer join (select *
	from dw_htlbizdb.tmp_uid_orderdate_num
	where 1 = 1
	and serverfrom = 'App') tmp
on lower(ssc.uid) = lower(tmp.uid)
and ssc.d_date = tmp.orderdate_d;"





insert overwrite table dw_htlbizdb.hotel_tracelog_index partition (d = '${selectDate}')
select
clientcode
,sid
,pvid
,starttime--前端触发服务时间
,pagecode
,actioncode
,get_json_object(exdata,'$.traceID') traceID --前端和服务端关联使用
,env['sourceID'] sourceID --来源ID
,env['os'] os --手机类型
,env['networkType'] networkType --网络设备
,env['osVersion'] osVersion --手机版本
,env['systemCode'] systemCode --系统id
,env['appVersion'] appVersion --携程版本
,env['UID'] UID
,env['deviceType'] deviceType --设备名称
from dw_mobdb.factmbtracelog_sdk
where d = '${selectDate}' and actioncode = 'o_trace_id'
and get_json_object(exdata,'$.traceID') != ''
and (clientcode != '00000000000000000000' and clientcode != '')
and lower(pagecode) in ('hotel_inland_list','hotel_inland_detail','hotel_inland_order','hotel_nearby_hotels','hotel_samebrand_hotels')
and env['UID'] not in ('ctriptestat', '13845612110', 'test111111','13845611999','wwwwww', 'test1111', 'test111111', 'wwwwwww', 'leonwang71', 'jerrygao','jjyd')
group by clientcode,sid,pvid,starttime,pagecode,actioncode,get_json_object(exdata,'$.traceID'),
env['sourceID'],env['os'],env['networkType'],env['osVersion'],env['systemCode'],env['appVersion'],env['UID'],env['deviceType']
;

USE dw_htlbizdb;
DROP TABLE IF EXISTS dw_htlbizdb.tmp_uid_index;
CREATE TABLE dw_htlbizdb.tmp_uid_index as
select d d_date
,uid
,count(pvid) pv
,count(distinct vid) uv
,count(concat(pvid, sid)) s_pv
,count(clientcode) client_num
,count(uid) uid_num
from dw_htlbizdb.hotel_tracelog_index
where 1 = 1
and d = '2016-03-01'
and pagecode = 'hotel_inland_list'
group by d,uid;


USE dw_htlbizdb;
DROP TABLE IF EXISTS dw_htlbizdb.tmp_uid_index2;
CREATE TABLE dw_htlbizdb.tmp_uid_index2 as
select d d_date
,uid
,count(pvid) pv
,count(distinct vid) uv
,count(concat(pvid, sid)) s_pv
,count(clientcode) client_num
,count(uid) uid_num
from dw_htlbizdb.htllist_sdk_imp
where 1 = 1
and d = '2016-03-01'
group by d,uid;


USE dw_htlbizdb;
DROP TABLE IF EXISTS dw_htlbizdb.tmp_uid_indexcheck;
CREATE TABLE dw_htlbizdb.tmp_uid_indexcheck as
select tmp.*
,flr.star2_ratio
,flr.uid_num
from dw_htlbizdb.tmp_uid_index2 tmp
left outer join dw_htlbizdb.tmp_userorder_flagR flr
	on tmp.uid = flr.uid;

select dw_htlbizdb.uid_serverSDK_combine



