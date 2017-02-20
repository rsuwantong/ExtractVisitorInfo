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


/* Kaidee */

drop table if exists rata_sght.kd_txn_pre;
create table rata_sght.kd_txn_pre row format delimited fields terminated by '\t' as ( 
select tapad_id,  cast(split_part(ip_address,'.',1) as INT)*16777216 + cast(split_part(ip_address,'.',2) as INT)*65536 + cast(split_part(ip_address,'.',3) as INT)*256+ cast(split_part(ip_address,'.',4) as INT) ip_number, platform  
										from ( select e.* from (select b.value as tapad_id,  a.header.platform as platform, a.header.ip_address as ip_address from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id =2177 and YEAR=2016 and MONTH=12  and c.key='TAPAD_COOKIE') E join (select ip_string as ip_address from dga.semcasting_data where country ='THA' ) F where E.ip_address = F.ip_address ) I group by tapad_id, ip_address, platform  
);

select count(distinct tapad_id) from 
( select e.* from (select b.value as tapad_id,  a.header.platform as platform, a.header.ip_address as ip_address from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id =2177 and YEAR=2016 and MONTH=12  and c.key='TAPAD_COOKIE') E join (select ip_string as ip_address from dga.semcasting_data where country ='THA' ) F where E.ip_address = F.ip_address ) I;

drop table if exists rata_sght.kd_txn;
create table rata_sght.kd_txn row format delimited fields terminated by '\t' as ( 
select tapad_id, case when carrier = 'DTAC' then 'DTAC' when carrier ='Wi-Fi' then 'Wi-Fi' else 'OMO' end as hl_carrier, 'kd' as channel, hl_platform 
	from ( select tapad_id,  
				case when platform in ("ANDROID_TABLET", "ANDROID", 'WINDOWS_PHONE', 'WINDOWS_TABLET') then 'Android-WP'
					 when platform IN ("IPAD", "IPHONE") then 'iOS'
					 when platform IN ("COMPUTER") then 'Computer'
					 else 'Other' end as hl_platform, 
				case when ip_number between 18087936 and 18153471 then 'TOT' when ip_number between 19791872 and 19922943 then 'DTAC' when ip_number between 456589312 and 456654847 then  'TMH' when ip_number between 837156864 and 837222399 then  'AIS'when ip_number between 837615616 and 837681151 then  'TMH' when ip_number between 1848705024 and 1848770559 then  'AIS' when ip_number between 1867776000 and 1867825151 then  'DTAC' when ip_number between 1867826176 and 1867841535 then  'DTAC' when ip_number between 1933770752 and 1933836287 then  'DTAC' when ip_number between 1998520320 and 1998553087 then  'AIS' when ip_number between 2523597824 and 2523598847 then  'OTH' when ip_number between 3033972736 and 3033980927 then  'TMH' when ip_number between 3068657664 and 3068723199 then  'AIS' when ip_number between 3398768640 and 3398769663 then  'AIS' when ip_number between 3415276128 and 3415276159 then  'TMH' when ip_number between 3742892032 and 3742957567 then  'TMH' else 'Wi-Fi' end as carrier
					 from rata_sght.kd_txn_pre
		   ) A  );
	
select hl_carrier, hl_platform, count(distinct tapad_id) from rata_sght.kd_txn group by hl_carrier, hl_platform order by 1, 2;
	
drop table if exists rata_sght.kd_bytpid;
create table rata_sght.kd_bytpid row format delimited fields terminated by '\t' as ( 	
 select tapad_id, hl_platform,  0 pt_flag, 0 sn_flag, 1 kd_flag,
	case when sum(omo_flag)>0 then 1 else 0 end as omo_flag,  
	case when sum(dtac_flag)>0 then 1 else 0 end as dtac_flag, 
	case when sum(wifi_flag)>0 then 1 else 0 end as wifi_flag from

		(select tapad_id, hl_platform,  
		 case when hl_carrier = 'OMO' then 1 else 0 end as omo_flag, 
		 case when hl_carrier ='DTAC' then 1 else 0 end as dtac_flag, 
		 case when hl_carrier ='Wi-Fi' then 1 else 0 end as wifi_flag from rata_sght.kd_txn) A group by tapad_id, hl_platform);
		
/*		
select * from rata_sght.kd_bytpid limit 20;
select count(*) from rata_sght.kd_bytpid ;
*/

/* Pantip */


drop table if exists rata_sght.pt_txn_pre;
create table rata_sght.pt_txn_pre row format delimited fields terminated by '\t' as ( 
select tapad_id,  cast(split_part(ip_address,'.',1) as INT)*16777216 + cast(split_part(ip_address,'.',2) as INT)*65536 + cast(split_part(ip_address,'.',3) as INT)*256+ cast(split_part(ip_address,'.',4) as INT) ip_number, platform  
										from ( select e.* from (select b.value as tapad_id,  a.header.platform as platform, a.header.ip_address as ip_address from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id =2243 and YEAR=2016 and MONTH=12  and c.key='TAPAD_COOKIE') E join (select ip_string as ip_address from dga.semcasting_data where country ='THA' ) F where E.ip_address = F.ip_address ) I group by tapad_id, ip_address, platform  
);

drop table if exists rata_sght.pt_txn;
create table rata_sght.pt_txn row format delimited fields terminated by '\t' as ( 
select tapad_id, case when carrier = 'DTAC' then 'DTAC' when carrier ='Wi-Fi' then 'Wi-Fi' else 'OMO' end as hl_carrier, 'kd' as channel, hl_platform 
	from ( select tapad_id,  
				case when platform in ("ANDROID_TABLET", "ANDROID", 'WINDOWS_PHONE', 'WINDOWS_TABLET') then 'Android-WP'
					 when platform IN ("IPAD", "IPHONE") then 'iOS'
					 when platform IN ("COMPUTER") then 'Computer'
					 else 'Other' end as hl_platform, 
				case when ip_number between 18087936 and 18153471 then 'TOT' when ip_number between 19791872 and 19922943 then 'DTAC' when ip_number between 456589312 and 456654847 then  'TMH' when ip_number between 837156864 and 837222399 then  'AIS'when ip_number between 837615616 and 837681151 then  'TMH' when ip_number between 1848705024 and 1848770559 then  'AIS' when ip_number between 1867776000 and 1867825151 then  'DTAC' when ip_number between 1867826176 and 1867841535 then  'DTAC' when ip_number between 1933770752 and 1933836287 then  'DTAC' when ip_number between 1998520320 and 1998553087 then  'AIS' when ip_number between 2523597824 and 2523598847 then  'OTH' when ip_number between 3033972736 and 3033980927 then  'TMH' when ip_number between 3068657664 and 3068723199 then  'AIS' when ip_number between 3398768640 and 3398769663 then  'AIS' when ip_number between 3415276128 and 3415276159 then  'TMH' when ip_number between 3742892032 and 3742957567 then  'TMH' else 'Wi-Fi' end as carrier
					 from rata_sght.pt_txn_pre
		   ) A  );
	
drop table if exists rata_sght.pt_bytpid;
create table rata_sght.pt_bytpid row format delimited fields terminated by '\t' as ( 	
 select tapad_id, hl_platform,  1 pt_flag, 0 sn_flag, 0 kd_flag,
	case when sum(omo_flag)>0 then 1 else 0 end as omo_flag,  
	case when sum(dtac_flag)>0 then 1 else 0 end as dtac_flag, 
	case when sum(wifi_flag)>0 then 1 else 0 end as wifi_flag from

		(select tapad_id, hl_platform,  
		 case when hl_carrier = 'OMO' then 1 else 0 end as omo_flag, 
		 case when hl_carrier ='DTAC' then 1 else 0 end as dtac_flag, 
		 case when hl_carrier ='Wi-Fi' then 1 else 0 end as wifi_flag from rata_sght.pt_txn) A group by tapad_id, hl_platform);
		
		 	 
select * from rata_sght.pt_bytpid limit 5;

select count(*) from rata_sght.pt_bytpid ;
/* Sanook */

drop table if exists rata_sght.sn_txn_pre;
create table rata_sght.sn_txn_pre row format delimited fields terminated by '\t' as ( 
select tapad_id,  cast(split_part(ip_address,'.',1) as INT)*16777216 + cast(split_part(ip_address,'.',2) as INT)*65536 + cast(split_part(ip_address,'.',3) as INT)*256+ cast(split_part(ip_address,'.',4) as INT) ip_number, platform  
										from ( select e.* from (select b.value as tapad_id,  a.header.platform as platform, a.header.ip_address as ip_address from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id =2248 and YEAR=2016 and MONTH=12  and c.key='TAPAD_COOKIE') E join (select ip_string as ip_address from dga.semcasting_data where country ='THA' ) F where E.ip_address = F.ip_address ) I group by tapad_id, ip_address, platform  
);

drop table if exists rata_sght.sn_txn;
create table rata_sght.sn_txn row format delimited fields terminated by '\t' as ( 
select tapad_id, case when carrier = 'DTAC' then 'DTAC' when carrier ='Wi-Fi' then 'Wi-Fi' else 'OMO' end as hl_carrier, 'kd' as channel, hl_platform 
	from ( select tapad_id,  
				case when platform in ("ANDROID_TABLET", "ANDROID", 'WINDOWS_PHONE', 'WINDOWS_TABLET') then 'Android-WP'
					 when platform IN ("IPAD", "IPHONE") then 'iOS'
					 when platform IN ("COMPUTER") then 'Computer'
					 else 'Other' end as hl_platform, 
				case when ip_number between 18087936 and 18153471 then 'TOT' when ip_number between 19791872 and 19922943 then 'DTAC' when ip_number between 456589312 and 456654847 then  'TMH' when ip_number between 837156864 and 837222399 then  'AIS'when ip_number between 837615616 and 837681151 then  'TMH' when ip_number between 1848705024 and 1848770559 then  'AIS' when ip_number between 1867776000 and 1867825151 then  'DTAC' when ip_number between 1867826176 and 1867841535 then  'DTAC' when ip_number between 1933770752 and 1933836287 then  'DTAC' when ip_number between 1998520320 and 1998553087 then  'AIS' when ip_number between 2523597824 and 2523598847 then  'OTH' when ip_number between 3033972736 and 3033980927 then  'TMH' when ip_number between 3068657664 and 3068723199 then  'AIS' when ip_number between 3398768640 and 3398769663 then  'AIS' when ip_number between 3415276128 and 3415276159 then  'TMH' when ip_number between 3742892032 and 3742957567 then  'TMH' else 'Wi-Fi' end as carrier
					 from rata_sght.sn_txn_pre
		   ) A  );

		   select * from rata_sght.sn_txn limit 5;
		   
select count(distinct tapad_id) from rata_sght.sn_txn where hl_carrier = 'DTAC' and hl_platform !='Computer';
select count(distinct tapad_id) from rata_sght.pt_txn where hl_carrier = 'DTAC' and hl_platform !='Computer';
	
drop table if exists rata_sght.sn_bytpid;
create table rata_sght.sn_bytpid row format delimited fields terminated by '\t' as ( 	
 select tapad_id, hl_platform,  0 pt_flag, 1 sn_flag, 0 kd_flag,
	case when sum(omo_flag)>0 then 1 else 0 end as omo_flag,  
	case when sum(dtac_flag)>0 then 1 else 0 end as dtac_flag, 
	case when sum(wifi_flag)>0 then 1 else 0 end as wifi_flag from

		(select tapad_id, hl_platform,  
		 case when hl_carrier = 'OMO' then 1 else 0 end as omo_flag, 
		 case when hl_carrier ='DTAC' then 1 else 0 end as dtac_flag, 
		 case when hl_carrier ='Wi-Fi' then 1 else 0 end as wifi_flag from rata_sght.sn_txn) A group by tapad_id, hl_platform);
	
select * from rata_sght.sn_bytpid limit 5;

	
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
select tapad_id, hl_platform, 
	case when sum(pt_flag)>0 then 1 else 0 end as pt_flag, 
	case when sum(sn_flag)>0 then 1 else 0 end as sn_flag, 
	case when sum(kd_flag)>0 then 1 else 0 end as kd_flag, 
	case when sum(omo_flag)>0 then 1 else 0 end as omo_flag,  
	case when sum(dtac_flag)>0 then 1 else 0 end as dtac_flag, 
	case when sum(wifi_flag)>0 then 1 else 0 end as wifi_flag from
(
select * from (select * from (select * from (select * from rata_sght.kd_bytpid union all select * from rata_sght.pt_bytpid) A) B union all select * from rata_sght.sn_bytpid) C) D group by tapad_id , hl_platform
);

select * from rata_sght.kdptsn_bytpid limit 5;

/* Create table of operator (Dtac/ OMO/ .../ Spinner/ Wifi flag by tapad id */

drop table if exists rata_sght.kdptsn_networkusg_bytpid;
create table rata_sght.kdptsn_networkusg_bytpid row format delimited fields terminated by '\t' as ( 
select tapad_id, hl_platform, 
	case
		when pt_flag = 1 and sn_flag = 0 and kd_flag = 0 then 'pt only'
		when pt_flag = 1 and sn_flag = 1 and kd_flag = 0 then 'pt-sn'
		when pt_flag = 1 and sn_flag = 0 and kd_flag = 1 then 'pt-kd'
		when pt_flag = 1 and sn_flag = 1 and kd_flag = 1 then 'pt-sn-kd'
		when pt_flag = 0 and sn_flag = 1 and kd_flag = 0 then 'sn only'
		when pt_flag = 0 and sn_flag = 0 and kd_flag = 1 then 'kd only'
		when pt_flag = 0 and sn_flag = 1 and kd_flag = 1 then 'sn-kd'
	end as web_usg,
	case 
		when omoonly_flag > 0 then 'omo_only' 
		when dtaconly_flag > 0 then 'dtac_only' 
		when dtacspinner_flag > 0 then 'dtac_spinner'	
		when wifionly_flag > 0 then 'wifi_only' else null
	end as network_usg,
	wifi_flag
from 
	(select tapad_id, hl_platform, pt_flag, sn_flag, kd_flag, 
		case when sum(omo_flag)>0 and  sum(dtac_flag)= 0 then 1 else 0 end as omoonly_flag,  
		case when sum(dtac_flag)>0 and sum(omo_flag)= 0  then 1 else 0 end as dtaconly_flag,
		case when sum(dtac_flag)>0 and sum(omo_flag)>0  then 1 else 0 end as dtacspinner_flag,
		case when sum(dtac_flag)=0 and sum(omo_flag)=0  then 1 else 0 end as wifionly_flag,
		case when sum(wifi_flag)>0 then 1 else 0 end as wifi_flag from
	rata_sght.kdptsn_bytpid group by 1,2,3,4,5 ) A
);

/* drop table if exists rata_sght.kdptsn_cnt;
create table rata_sght.kdptsn_cnt row format delimited fields terminated by '\t' as ( 
select network_usg, hl_platform, web_usg,
		case when hl_platform = 'iOS' then round(uniq_cnt/6.8697608)
			 when hl_platform = 'Android-WP' then round(uniq_cnt/2.7923819)
			 else round(uniq_cnt/2.0553605) end as uniq_cnt
from (select network_usg, hl_platform, web_usg, count(distinct tapad_id) as uniq_cnt from rata_sght.kdptsn_networkusg_bytpid group by 1, 2, 3 order by 1, 2, 3) A order by 1, 2, 3);
 */

drop table if exists rata_sght.kdptsn_cnt;
create table rata_sght.kdptsn_cnt row format delimited fields terminated by '\t' as ( 
select network_usg, hl_platform, web_usg, count(distinct tapad_id) as uniq_cnt from rata_sght.kdptsn_networkusg_bytpid group by 1, 2, 3 order by 1, 2, 3);

 
select network_usg, web_usg, count(distinct tapad_id) from rata_sght.kdptsn_networkusg_bytpid group by 1, 2 order by 1, 2;


impala-shell -i impala.prd.sg1.tapad.com:21000 -B -o /local/home/rata.suwantong/kdptsn_overlap_pre.csv --output_delimiter=',' -q "select * from rata_sght.kdptsn_networkusg_bytpid"
impala-shell -i impala.prd.sg1.tapad.com:21000 -B -o /local/home/rata.suwantong/kdptsn_overlap_cnt_pre.csv --output_delimiter=',' -q "select * from rata_sght.kdptsn_cnt order by 1, 2, 3"

echo $'TAPAD_ID, HL_PLATFORM, WEB_USG, NETWORK_USG, WIFI_FLAG ' | cat - kdptsn_overlap_pre.csv > kdptsn_overlap.csv
echo $'NETWORK_USG, HL_PLATFORM, WEB_USG, UNIQ_TPID' | cat - kdptsn_overlap_cnt_pre.csv > kdptsn_overlap_cnt.csv

  

select count* from rata_sght.kdptsn_cnt order by 1, 2, 3
  

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


/*

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

*/