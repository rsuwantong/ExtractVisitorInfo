/*
####################################################################################
# Name: 11ptsnkd_mobile_overlap
# Description: Compute number of tapad id overlap on Pantip, Sanook, Kaidee on MOBILE by day
# Input: default.id_syncs
# Version:
#   2016/11/08 RS: Initial version
#   
####################################################################################
*/

/*Check the available dates of Sanook data*/

select distinct a.partner_code, a.month, a.day from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id in (2248,2177,2243) and a.header.platform in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE', 'IPHONE') and c.key='TAPAD_COOKIE' order by partner_code asc, a.month asc, a.day asc;

/*

*/

/* Kaidee */
drop table if exists rata_sght.kd_txn;
create table rata_sght.kd_txn row format delimited fields terminated by '\t' as ( 

select sighted_date, hl_platform, case when carrier = 'DTAC' then 'DTAC' when carrier ='Wi-Fi' then 'Wi-Fi' else 'OMO' end as hl_carrier, channel, tapad_id from 
(select sighted_date, hl_platform, case when ip_number between 18087936 and 18153471 then 'TOT' when ip_number between 19791872 and 19922943 then 'DTAC' when ip_number between 456589312 and 456654847 then  'TMH' when ip_number between 837156864 and 837222399 then  'AIS'when ip_number between 837615616 and 837681151 then  'TMH' when ip_number between 1848705024 and 1848770559 then  'AIS' when ip_number between 1867776000 and 1867825151 then  'DTAC' when ip_number between 1867826176 and 1867841535 then  'DTAC' when ip_number between 1933770752 and 1933836287 then  'DTAC' when ip_number between 1998520320 and 1998553087 then  'AIS' when ip_number between 2523597824 and 2523598847 then  'OTH' when ip_number between 3033972736 and 3033980927 then  'TMH' when ip_number between 3068657664 and 3068723199 then  'AIS' when ip_number between 3398768640 and 3398769663 then  'AIS' when ip_number between 3415276128 and 3415276159 then  'TMH' when ip_number between 3742892032 and 3742957567 then  'TMH' else 'Wi-Fi' end as carrier, channel, tapad_id 

	from ( select regexp_replace(cast(cast(a.header.created_at/1000 as timestamp) as string),' .*','') as sighted_date, partner_code as channel,case when (lower(a.header.platform)='iphone' and (lower(a.header.user_agent) like ('%windows phone%') or lower(a.header.user_agent) like ('%lumia%')) or a.header.platform in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE','IPHONE')) then 'ANDROID' else 'IPHONE' end as hl_platform, cast(split_part(a.header.ip_address,'.',1) as INT)*16777216 + cast(split_part(a.header.ip_address,'.',2) as INT)*65536 + cast(split_part(a.header.ip_address,'.',3) as INT)*256+ cast(split_part(a.header.ip_address,'.',4) as INT) ip_number, b.value as tapad_id, a.header.user_agent as user_agent from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id in (2177) and YEAR=2016 and MONTH=12  and c.key='TAPAD_COOKIE' and a.header.platform in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE','IPHONE')) A) B );
	
select hl_carrier, count(distinct tapad_id) from rata_sght.kd_txn group by hl_carrier;
	
drop table if exists rata_sght.kd_bytpid;
create table rata_sght.kd_bytpid row format delimited fields terminated by '\t' as ( 	
 select sighted_date, tapad_id, hl_platform,  0 pt_flag, 0 sn_flag, 1 kd_flag,
	case when sum(omo_flag)>0 then 1 else 0 end as omo_flag,  
	case when sum(dtac_flag)>0 then 1 else 0 end as dtac_flag, 
	case when sum(wifi_flag)>0 then 1 else 0 end as wifi_flag from

		(select sighted_date, tapad_id, hl_platform,  
		 case when hl_carrier = 'OMO' then 1 else 0 end as omo_flag, 
		 case when hl_carrier ='DTAC' then 1 else 0 end as dtac_flag, 
		 case when hl_carrier ='Wi-Fi' then 1 else 0 end as wifi_flag from rata_sght.kd_txn) A group by sighted_date, tapad_id, hl_platform);
		
/*		
select * from rata_sght.kd_bytpid limit 5;

*/

/* Pantip */


drop table if exists rata_sght.pt_txn;
create table rata_sght.pt_txn row format delimited fields terminated by '\t' as (

select sighted_date, hl_platform, case when carrier = 'DTAC' then 'DTAC' when carrier ='Wi-Fi' then 'Wi-Fi' else 'OMO' end as hl_carrier, channel, tapad_id from 
(select sighted_date, hl_platform, case when ip_number between 18087936 and 18153471 then 'TOT' when ip_number between 19791872 and 19922943 then 'DTAC' when ip_number between 456589312 and 456654847 then  'TMH' when ip_number between 837156864 and 837222399 then  'AIS'when ip_number between 837615616 and 837681151 then  'TMH' when ip_number between 1848705024 and 1848770559 then  'AIS' when ip_number between 1867776000 and 1867825151 then  'DTAC' when ip_number between 1867826176 and 1867841535 then  'DTAC' when ip_number between 1933770752 and 1933836287 then  'DTAC' when ip_number between 1998520320 and 1998553087 then  'AIS' when ip_number between 2523597824 and 2523598847 then  'OTH' when ip_number between 3033972736 and 3033980927 then  'TMH' when ip_number between 3068657664 and 3068723199 then  'AIS' when ip_number between 3398768640 and 3398769663 then  'AIS' when ip_number between 3415276128 and 3415276159 then  'TMH' when ip_number between 3742892032 and 3742957567 then  'TMH' else 'Wi-Fi' end as carrier, channel, tapad_id 

	from ( select regexp_replace(cast(cast(a.header.created_at/1000 as timestamp) as string),' .*','') as sighted_date, partner_code as channel,case when (lower(a.header.platform)='iphone' and (lower(a.header.user_agent) like ('%windows phone%') or lower(a.header.user_agent) like ('%lumia%')) or a.header.platform in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE','IPHONE')) then 'ANDROID' else 'IPHONE' end as hl_platform, cast(split_part(a.header.ip_address,'.',1) as INT)*16777216 + cast(split_part(a.header.ip_address,'.',2) as INT)*65536 + cast(split_part(a.header.ip_address,'.',3) as INT)*256+ cast(split_part(a.header.ip_address,'.',4) as INT) ip_number, b.value as tapad_id, a.header.user_agent as user_agent from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id in (2243) and YEAR=2016 and MONTH=12  and c.key='TAPAD_COOKIE' and a.header.platform in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE','IPHONE')) A) B );
	
drop table if exists rata_sght.pt_bytpid;
create table rata_sght.pt_bytpid row format delimited fields terminated by '\t' as ( 	
 select sighted_date, tapad_id, hl_platform,  1 pt_flag, 0 sn_flag, 0 kd_flag,
	case when sum(omo_flag)>0 then 1 else 0 end as omo_flag,  
	case when sum(dtac_flag)>0 then 1 else 0 end as dtac_flag, 
	case when sum(wifi_flag)>0 then 1 else 0 end as wifi_flag from

		(select sighted_date, tapad_id, hl_platform,  
		 case when hl_carrier = 'OMO' then 1 else 0 end as omo_flag, 
		 case when hl_carrier ='DTAC' then 1 else 0 end as dtac_flag, 
		 case when hl_carrier ='Wi-Fi' then 1 else 0 end as wifi_flag from rata_sght.kd_txn) A group by sighted_date, tapad_id, hl_platform);
		
		 	 
select * from rata_sght.pt_bytpid limit 5;


/* Sanook */


drop table if exists rata_sght.sn_txn;
create table rata_sght.sn_txn row format delimited fields terminated by '\t' as ( 

select sighted_date, hl_platform, case when carrier = 'DTAC' then 'DTAC' when carrier ='Wi-Fi' then 'Wi-Fi' else 'OMO' end as hl_carrier, channel, tapad_id from 
(select sighted_date, hl_platform, case when ip_number between 18087936 and 18153471 then 'TOT' when ip_number between 19791872 and 19922943 then 'DTAC' when ip_number between 456589312 and 456654847 then  'TMH' when ip_number between 837156864 and 837222399 then  'AIS'when ip_number between 837615616 and 837681151 then  'TMH' when ip_number between 1848705024 and 1848770559 then  'AIS' when ip_number between 1867776000 and 1867825151 then  'DTAC' when ip_number between 1867826176 and 1867841535 then  'DTAC' when ip_number between 1933770752 and 1933836287 then  'DTAC' when ip_number between 1998520320 and 1998553087 then  'AIS' when ip_number between 2523597824 and 2523598847 then  'OTH' when ip_number between 3033972736 and 3033980927 then  'TMH' when ip_number between 3068657664 and 3068723199 then  'AIS' when ip_number between 3398768640 and 3398769663 then  'AIS' when ip_number between 3415276128 and 3415276159 then  'TMH' when ip_number between 3742892032 and 3742957567 then  'TMH' else 'Wi-Fi' end as carrier, channel, tapad_id 

	from ( select regexp_replace(cast(cast(a.header.created_at/1000 as timestamp) as string),' .*','') as sighted_date, partner_code as channel,case when (lower(a.header.platform)='iphone' and (lower(a.header.user_agent) like ('%windows phone%') or lower(a.header.user_agent) like ('%lumia%')) or a.header.platform in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE','IPHONE')) then 'ANDROID' else 'IPHONE' end as hl_platform, cast(split_part(a.header.ip_address,'.',1) as INT)*16777216 + cast(split_part(a.header.ip_address,'.',2) as INT)*65536 + cast(split_part(a.header.ip_address,'.',3) as INT)*256+ cast(split_part(a.header.ip_address,'.',4) as INT) ip_number, b.value as tapad_id, a.header.user_agent as user_agent from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id in (2248) and YEAR=2016 and MONTH=12  and c.key='TAPAD_COOKIE' and a.header.platform in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE','IPHONE')) A) B );
	
select * from rata_sght.sn_txn limit 5;
	
drop table if exists rata_sght.sn_bytpid;
create table rata_sght.sn_bytpid row format delimited fields terminated by '\t' as ( 	
 select sighted_date, tapad_id, hl_platform,  0 pt_flag, 1 sn_flag, 0 kd_flag,
	case when sum(omo_flag)>0 then 1 else 0 end as omo_flag,  
	case when sum(dtac_flag)>0 then 1 else 0 end as dtac_flag, 
	case when sum(wifi_flag)>0 then 1 else 0 end as wifi_flag from

		(select sighted_date, tapad_id, hl_platform,  
		 case when hl_carrier = 'OMO' then 1 else 0 end as omo_flag, 
		 case when hl_carrier ='DTAC' then 1 else 0 end as dtac_flag, 
		 case when hl_carrier ='Wi-Fi' then 1 else 0 end as wifi_flag from rata_sght.kd_txn) A group by sighted_date, tapad_id, hl_platform);
		
/*	 

+-------------------------+
| summary                 |
+-------------------------+
| Inserted 3928282 row(s) |
+-------------------------+

*/
		 
select * from rata_sght.sn_bytpid limit 5;




/*Union Kd, Pt, Sn and group by tapad_id*/

drop table if exists rata_sght.kdptsn_bytpid;
create table rata_sght.kdptsn_bytpid row format delimited fields terminated by '\t' as ( 
select sighted_date, tapad_id, hl_platform, case when hl_platform='IPHONE' then 1 else 0 end as iphone_flag, 
	case when sum(pt_flag)>0 then 1 else 0 end as pt_flag, 
	case when sum(sn_flag)>0 then 1 else 0 end as sn_flag, 
	case when sum(kd_flag)>0 then 1 else 0 end as kd_flag, 
	case when sum(omo_flag)>0 then 1 else 0 end as omo_flag,  
	case when sum(dtac_flag)>0 then 1 else 0 end as dtac_flag, 
	case when sum(wifi_flag)>0 then 1 else 0 end as wifi_flag from
(
select * from (select * from (select * from (select * from rata_sght.kd_bytpid union all select * from rata_sght.pt_bytpid) A) B union all select * from rata_sght.sn_bytpid) C) D group by sighted_date, tapad_id , hl_platform
);

select *  from rata_sght.kdptsn_bytpid where sn_flag = 0 limit 5;

select pt_flag, sn_flag, kd_flag, count(distinct tapad_id) from rata_sght.kdptsn_bytpid  group by pt_flag, sn_flag, kd_flag;

/* Create table of operator (Dtac/ OMO/ .../ Spinner/ Wifi flag by tapad id */

drop table if exists rata_sght.kdptsn_networkusg_bytpid;
create table rata_sght.kdptsn_networkusg_bytpid row format delimited fields terminated by '\t' as ( 
select tapad_id, hl_platform, 
	case 
		when omoonly_flag > 0 then 'omo_only' 
		when dtaconly_flag > 0 then 'dtac_only' 
		when dtacspinner_flag > 0 then 'dtac_spinner'	
		when wifionly_flag > 0 then 'wifi_only' else null
	end as network_usg,
	wifi_flag
from 
	(select tapad_id, hl_platform, 
		case when sum(omo_flag)>0 and  sum(dtac_flag)= 0 then 1 else 0 end as omoonly_flag,  
		case when sum(dtac_flag)>0 and sum(omo_flag)= 0  then 1 else 0 end as dtaconly_flag,
		case when sum(dtac_flag)>0 and sum(omo_flag)>0  then 1 else 0 end as dtacspinner_flag,
		case when sum(dtac_flag)=0 and sum(omo_flag)=0  then 1 else 0 end as wifionly_flag,
		case when sum(wifi_flag)>0 then 1 else 0 end as wifi_flag from
	rata_sght.kdptsn_bytpid group by tapad_id, hl_platform, iphone_flag ) A
);



select network_usg, count(distinct tapad_id) from rata_sght.kdptsn_networkusg_bytpid group by network_usg ;
select * from (select tapad_id, hl_platform, 
		case when sum(omo_flag)>0 and  sum(dtac_flag)= 0 then 1 else 0 end as omoonly_flag,  
		case when sum(dtac_flag)>0 and sum(omo_flag)= 0  then 1 else 0 end as dtaconly_flag,
		case when sum(dtac_flag)>0 and sum(omo_flag)>0  then 1 else 0 end as dtacspinner_flag,
		case when sum(dtac_flag)=0 and sum(omo_flag)=0  then 1 else 0 end as wifionly_flag,
		case when sum(wifi_flag)>0 then 1 else 0 end as wifi_flag from
	rata_sght.kdptsn_bytpid group by tapad_id, hl_platform, iphone_flag) A where wifi_flag = 1 and omoonly_flag+dtaconly_flag>0 limit 20; 

drop table if exists rata_sght.pt_test;
create table rata_sght.pt_test row format delimited fields terminated by '\t' as ( 
SELECT hl_carrier, day,
   COUNT(*) AS distinct_tpids
  ,SUM(flag) AS new_tpids
  ,SUM(SUM(flag)) 
   OVER (partition by hl_carrier ORDER BY hl_carrier, day 
         ROWS UNBOUNDED PRECEDING) AS cumulative_new_tpids 
FROM
 (
   SELECT
      hl_carrier
	 ,tapad_id
     ,day
     ,CASE 
         WHEN day
            = MIN(day) 
              OVER (PARTITION BY hl_carrier, tapad_id) 
         THEN 1 
         ELSE 0 
      END AS flag
   FROM (select * from rata_sght.pt_txn; where carrier ='OMO') C
   GROUP BY hl_carrier, day, tapad_id
 ) AS dt
GROUP BY hl_carrier, day
order by hl_carrier asc, day asc);

select hl_carrier, avg(distinct_tpids) from rata.pt_tpidnum_accum_byhlcarrier_nonDtac group by hl_carrier;




impala-shell -i impala.prd.sg1.tapad.com:21000 -B -o /local/home/rata.suwantong/kdptsn_bytpid_pre.csv --output_delimiter=',' -q "use rata;  select * from rata_sght.kdptsn_bytpid"

echo $'SIGHTED_DATE, TAPAD_ID, IPHONE_FLAG, PT_FLAG, SN_FLAG, KD_FLAG, PT_OMO_FLAG, PT_DTAC_FLAG, PT_WIFI_FLAG, SN_OMO_FLAG, SN_DTAC_FLAG, SN_WIFI_FLAG, KD_OMO_FLAG, KD_DTAC_FLAG, KD_WIFI_FLAG, OMO_FLAG, DTAC_FLAG, WIFI_FLAG' | cat - kdptsn_bytpid_pre.csv > kdptsn_bytpid.csv

zip -r kdptsn_bytpid.zip kdptsn_bytpid.csv
  


select kd_flag, pt_flag, sn_flag, count(distinct tapad_id) from rata_sght.kdptsn_bytpid group by kd_flag, pt_flag, sn_flag order by kd_flag asc, pt_flag asc, sn_flag asc;

/*
+---------+---------+---------+--------------------------+
| kd_flag | pt_flag | sn_flag | count(distinct tapad_id) |
+---------+---------+---------+--------------------------+
| 0       | 0       | 1       | 3689578                  |
| 0       | 1       | 0       | 2732500                  |
| 0       | 1       | 1       | 196340                   |
| 1       | 0       | 0       | 385926                   |
| 1       | 0       | 1       | 31175                    |
| 1       | 1       | 0       | 48726                    |
| 1       | 1       | 1       | 11168                    |
+---------+---------+---------+--------------------------+
*/

select kd_flag, pt_flag, sn_flag, count(distinct tapad_id) from rata_sght.kdptsn_bytpid where dtac_flag=0 group by kd_flag, pt_flag, sn_flag order by kd_flag asc, pt_flag asc, sn_flag asc;


+---------+---------+---------+--------------------------+
| kd_flag | pt_flag | sn_flag | count(distinct tapad_id) |
+---------+---------+---------+--------------------------+
| 0       | 0       | 1       | 3113491                  |
| 0       | 1       | 0       | 2262694                  |
| 0       | 1       | 1       | 157438                   |
| 1       | 0       | 0       | 328286                   |
| 1       | 0       | 1       | 25657                    |
| 1       | 1       | 0       | 39912                    |
| 1       | 1       | 1       | 8904                     |
+---------+---------+---------+--------------------------+

select kd_flag, pt_flag, sn_flag, count(distinct tapad_id) from rata_sght.kdptsn_bytpid where dtac_flag=0 and omo_flag=1 group by kd_flag, pt_flag, sn_flag order by kd_flag asc, pt_flag asc, sn_flag asc;


+---------+---------+---------+--------------------------+
| kd_flag | pt_flag | sn_flag | count(distinct tapad_id) |
+---------+---------+---------+--------------------------+
| 0       | 0       | 1       | 1328590                  |
| 0       | 1       | 0       | 929517                   |
| 0       | 1       | 1       | 84829                    |
| 1       | 0       | 0       | 136761                   |
| 1       | 0       | 1       | 13648                    |
| 1       | 1       | 0       | 19574                    |
| 1       | 1       | 1       | 4887                     |
+---------+---------+---------+--------------------------+



select omo_flag, wifi_flag, kd_flag, pt_flag, sn_flag, count(distinct tapad_id) from rata_sght.kdptsn_bytpid where dtac_flag=0 group by omo_flag, wifi_flag, kd_flag, pt_flag, sn_flag order by omo_flag asc, wifi_flag asc, kd_flag asc, pt_flag asc, sn_flag asc;


+----------+-----------+---------+---------+---------+--------------------------+
| omo_flag | wifi_flag | kd_flag | pt_flag | sn_flag | count(distinct tapad_id) |
+----------+-----------+---------+---------+---------+--------------------------+
| 0        | 1         | 0       | 0       | 1       | 1784902                  |
| 0        | 1         | 0       | 1       | 0       | 1333180                  |
| 0        | 1         | 0       | 1       | 1       | 72609                    |
| 0        | 1         | 1       | 0       | 0       | 191526                   |
| 0        | 1         | 1       | 0       | 1       | 12009                    |
| 0        | 1         | 1       | 1       | 0       | 20338                    |
| 0        | 1         | 1       | 1       | 1       | 4017                     |
| 1        | 0         | 0       | 0       | 1       | 1263317                  |
| 1        | 0         | 0       | 1       | 0       | 815785                   |
| 1        | 0         | 0       | 1       | 1       | 41592                    |
| 1        | 0         | 1       | 0       | 0       | 122951                   |
| 1        | 0         | 1       | 0       | 1       | 8366                     |
| 1        | 0         | 1       | 1       | 0       | 9944                     |
| 1        | 0         | 1       | 1       | 1       | 1989                     |
| 1        | 1         | 0       | 0       | 1       | 65274                    |
| 1        | 1         | 0       | 1       | 0       | 113733                   |
| 1        | 1         | 0       | 1       | 1       | 43237                    |
| 1        | 1         | 1       | 0       | 0       | 13810                    |
| 1        | 1         | 1       | 0       | 1       | 5282                     |
| 1        | 1         | 1       | 1       | 0       | 9630                     |
| 1        | 1         | 1       | 1       | 1       | 2898                     |
+----------+-----------+---------+---------+---------+--------------------------+

select case when platform ='IPHONE' then 1 else 0 end as iphone_flag,kd_flag, pt_flag, sn_flag, count(distinct tapad_id) from rata_sght.kdptsn_bytpid where dtac_flag=0 group by iphone_flag, kd_flag, pt_flag, sn_flag order by iphone_flag asc, kd_flag asc, pt_flag asc, sn_flag asc;
