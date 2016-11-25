/*
####################################################################################
# Name: meas_table
# Description: Create an Impala table of measurement data
# Input: taps & tracked_events
# Version:
#   2016/11/25 RS: Initial version
#   
####################################################################################
*/


drop table if exists meas_ana.meas_table;

create table meas_ana.meas_table 
(sight_date STRING, tapad_id STRING, hl_platform STRING, dvc_techname STRING, carrier STRING, offer STRING, source STRING, room_id INT, ful_channel STRING, 
imps BIGINT, clicks BIGINT, selects BIGINT, landings BIGINT, submits BIGINT );

insert into meas_ana.meas_table 

select case when c.sight_date is not null then c.sight_date else f.sight_date end as sight_date, 
		case when c.tapad_id is not null then c.tapad_id else f.tapad_id end as tapad_id, 
		case when c.hl_platform is not null then c.hl_platform else f.hl_platform end as hl_platform, 
		case when c.dvc_techname is not null then c.dvc_techname else f.dvc_techname end as dvc_techname, 
		case when c.carrier is not null then c.carrier else f.carrier end as carrier, 
		case when c.offer is not null then c.offer else f.offer end as offer, 
		case when c.source is not null then c.source else f.source end as source, c.room_id, f.ful_channel, c.imps, c.clicks, f.selects, f.landings, f.submits from 
 (select sight_date, tapad_id, hl_platform, dvc_techname, carrier, offer,  'kd' as source, room_id,  
sum(imp_flg) as imps, sum(click_flg) as clicks  from 
(select sight_date, tapad_id, 
	case when action_id ='impression' then 1 else 0 end as imp_flg, 
    case when action_id ='click' then 1 else 0 end as click_flg,
	case when platform in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE') then 'ANDROID' when platform='IPHONE' then 'IPHONE' else 'PC_OTHERS' end as hl_platform, 
	case 
	    when platform not in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE','IPHONE') then platform  
		when lcase(user_agent) like '%cpu iphone os%' and lcase(user_agent) like '%ipod%' and lcase(platform)='iphone' then 'ipod' 
		when lcase(user_agent) like '%cpu iphone os%' or lcase(user_agent) like '%iphone; u; cpu iphone%' or lcase(user_agent) like '%iphone; cpu os%' and lcase(platform)='iphone' then regexp_replace(regexp_replace(regexp_replace(lcase(user_agent),'.*iphone;( u;)? cpu ',''),'like mac os.*',''),'_.*','') 
		when lcase(user_agent) like '%(null) [fban%' and lcase(user_agent) like '%fbdv/iphone%' and lcase(platform)='iphone' then regexp_extract(regexp_replace(lcase(user_agent),'.*fbdv/',''),'iphone[0-9]',0) 
		when lcase(user_agent) like '%android; mobile; rv%' or lcase(user_agent) like '%mobile rv[0-9][0-9].[0-9] gecko%' then 'unidentified android' 
		when lcase(user_agent) like '%android; tablet; rv%' or lcase(user_agent) like '%tablet rv[0-9][0-9].[0-9] gecko%' then 'unidentified tablet' 
		else  regexp_replace(regexp_replace(regexp_replace(trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lcase(user_agent),'.*android [0-9](.[0-9](.[0-9])?)?; ',''),' build.*|; android/.*|\\) 
		applewebkit.*|/v[0-9] linux.*|v_td.*|_td/v[0-9].*|i_style.*',''),'.*(th|en|zh|zz)(-|_)(gb|au|ph|th|us|cn|nz|gb|tw|fi|jp|za|sg|ie|zz);? |.*nokia; ',''),'/.*|linux.*','')),'[^0-9a-z\- \.]',''),'.*samsung(-| )|.*lenovo |.*microsoft |.*th- ',''),'like.*|lollipop.*','') end as dvc_techname, 		
	case when ip_number between 18087936 and 18153471 then 'TOT' when ip_number between 19791872 and 19922943 then 'DTAC' when ip_number between 456589312 and 456654847 then  'TMH' when ip_number between 837156864 and 837222399 then  'AIS'when ip_number between 837615616 and 837681151 then  'TMH' when ip_number between 1848705024 and 1848770559 then  'AIS' when ip_number between 1867776000 and 1867825151 then  'DTAC' when ip_number between 1867826176 and 1867841535 then  'DTAC' when ip_number between 1933770752 and 1933836287 then  'DTAC' when ip_number between 1998520320 and 1998553087 then  'AIS' when ip_number between 2523597824 and 2523598847 then  'OTH' when ip_number between 3033972736 and 3033980927 then  'TMH' when ip_number between 3068657664 and 3068723199 then  'AIS' when ip_number between 3398768640 and 3398769663 then  'AIS' when ip_number between 3415276128 and 3415276159 then  'TMH' when ip_number between 3742892032 and 3742957567 then  'TMH' else 'Wi-Fi' end as carrier, 
	offer, room_id from 
(select regexp_replace(cast(cast(a.header.created_at/1000 as timestamp) as string),' .*','') as sight_date, b.value as tapad_id, a.action_id as action_id, case when lower(a.header.platform)='iphone' and (lower(a.header.user_agent) like ('%windows phone%') or lower(a.header.user_agent) like ('%lumia%')) then 'WINDOWS_PHONE' else a.header.platform end as platform,  a.header.user_agent as user_agent, cast(split_part(a.header.ip_address,'.',1) as INT)*16777216 + cast(split_part(a.header.ip_address,'.',2) as INT)*65536 + cast(split_part(a.header.ip_address,'.',3) as INT)*256+ cast(split_part(a.header.ip_address,'.',4) as INT) ip_number, 
	case
		when a.tactic_id = 186858 then 'mnp-device-discount-samsung'
		when a.tactic_id = 191242 then 'mnp-device-discount-samsung'
		when a.tactic_id = 191243 then 'mnp-free-device'
		when a.tactic_id = 191244  then 'tariff'
		when a.tactic_id = 199183 then 'booster'
		when a.tactic_id in (197236, 213768) then 'mnp-samsung-galaxy-j2'
		when a.tactic_id = 200320 then 'mnp-samsung-galaxy-j5'
		when a.tactic_id = 201014 then 'mnp-samsung-galaxy-a5'
		when a.tactic_id in (203164,217118) then 'mnp-asus-zenfone-45' 
		when a.tactic_id = 214301 then 'mnp-oppo-mirror5' 
		when a.tactic_id = 217958 then 'mnp-free-dtac-pocket-wifi' end as offer, 
	cast(q.value as int) as room_id 
 from default.taps a, a.header.incoming_ids b, a.header.query_params q where q.key='ext_cat' and a.campaign_id=5138 and a.action_id IN ('impression','click') and a.tactic_id in (186858,191242,191243,191244,199183,197236, 213768,200320,201014 ,203164,217118,214301,217958) ) A) B group by sight_date, tapad_id, hl_platform, dvc_techname, carrier, offer, source, room_id) C 
 
 full outer join 
 
 (select  sight_date, tapad_id, hl_platform, dvc_techname, carrier, offer,  source, 0 as room_id, ful_channel,  
		sum(select_flg) as selects, sum(landing_flg) as landings, sum(submit_flg) as submits  from 
 
(select sight_date, 
	   tapad_id, 
	   case when action_id ='undefined' then 1 else 0 end as landing_flg, 
	   case when action_id like '%select%' then 1 else 0 end as select_flg, 
       case when action_id like '%submit%' then 1 else 0 end as submit_flg, 
	   case when platform in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE') then 'ANDROID' when platform='IPHONE' then 'IPHONE' else 'PC_OTHERS' end as hl_platform,
	   case 
	    when platform not in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE','IPHONE') then platform  
		when lcase(user_agent) like '%cpu iphone os%' and lcase(user_agent) like '%ipod%' and lcase(platform)='iphone' then 'ipod' 
		when lcase(user_agent) like '%cpu iphone os%' or lcase(user_agent) like '%iphone; u; cpu iphone%' or lcase(user_agent) like '%iphone; cpu os%' and lcase(platform)='iphone' then regexp_replace(regexp_replace(regexp_replace(lcase(user_agent),'.*iphone;( u;)? cpu ',''),'like mac os.*',''),'_.*','') 
		when lcase(user_agent) like '%(null) [fban%' and lcase(user_agent) like '%fbdv/iphone%' and lcase(platform)='iphone' then regexp_extract(regexp_replace(lcase(user_agent),'.*fbdv/',''),'iphone[0-9]',0) 
		when lcase(user_agent) like '%android; mobile; rv%' or lcase(user_agent) like '%mobile rv[0-9][0-9].[0-9] gecko%' then 'unidentified android' 
		when lcase(user_agent) like '%android; tablet; rv%' or lcase(user_agent) like '%tablet rv[0-9][0-9].[0-9] gecko%' then 'unidentified tablet' 
		else  regexp_replace(regexp_replace(regexp_replace(trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lcase(user_agent),'.*android [0-9](.[0-9](.[0-9])?)?; ',''),' build.*|; android/.*|\\) 
		applewebkit.*|/v[0-9] linux.*|v_td.*|_td/v[0-9].*|i_style.*',''),'.*(th|en|zh|zz)(-|_)(gb|au|ph|th|us|cn|nz|gb|tw|fi|jp|za|sg|ie|zz);? |.*nokia; ',''),'/.*|linux.*','')),'[^0-9a-z\- \.]',''),'.*samsung(-| )|.*lenovo |.*microsoft |.*th- ',''),'like.*|lollipop.*','') end as dvc_techname, 		
	case 
		when ip_number between 18087936 and 18153471 then 'TOT' when ip_number between 19791872 and 19922943 then 'DTAC' when ip_number between 456589312 and 456654847 then  'TMH' when ip_number between 837156864 and 837222399 then  'AIS'when ip_number between 837615616 and 837681151 then  'TMH' when ip_number between 1848705024 and 1848770559 then  'AIS' when ip_number between 1867776000 and 1867825151 then  'DTAC' when ip_number between 1867826176 and 1867841535 then  'DTAC' when ip_number between 1933770752 and 1933836287 then  'DTAC' when ip_number between 1998520320 and 1998553087 then  'AIS' when ip_number between 2523597824 and 2523598847 then  'OTH' when ip_number between 3033972736 and 3033980927 then  'TMH' when ip_number between 3068657664 and 3068723199 then  'AIS' when ip_number between 3398768640 and 3398769663 then  'AIS' when ip_number between 3415276128 and 3415276159 then  'TMH' when ip_number between 3742892032 and 3742957567 then  'TMH' else 'Wi-Fi' end as carrier,
	case 
		when referrer_url like '%special-package%' then 'tariff' 
		when referrer_url like '%asus-zenfone-45%' then 'mnp-asus-zenfone-45' 
		else regexp_replace(regexp_replace(regexp_replace(referrer_url,'.*specialoffer/',''),'\\.html.*',''),'-lite.*','') end as offer, 
	case
		when referrer_url like '%kaidee%' then 'kd'
		when referrer_url like '%facebook%' then 'fb' else 'oth' end as source,
	case 
		when action_id like '%line%' then 'line' 
		when action_id like '%callcenter%' then 'callcenter' 
		when action_id like '%onlinechannel%' then 'onlinechannel'  
		end as ful_channel 
from 
(
select regexp_replace(cast(cast(a.header.created_at/1000 as timestamp) as string),' .*','') as sight_date, b.value as tapad_id, a.action_id as action_id, case when lower(a.header.platform)='iphone' and (lower(a.header.user_agent) like ('%windows phone%') or lower(a.header.user_agent) like ('%lumia%')) then 'WINDOWS_PHONE' else a.header.platform end as platform,  a.header.user_agent as user_agent, cast(split_part(a.header.ip_address,'.',1) as INT)*16777216 + cast(split_part(a.header.ip_address,'.',2) as INT)*65536 + cast(split_part(a.header.ip_address,'.',3) as INT)*256+ cast(split_part(a.header.ip_address,'.',4) as INT) ip_number, a.header.referrer_url as referrer_url 

 from default.tracked_events a, a.header.incoming_ids b where a.property_id = '2868' and (a.action_id like '%submit%' or a.action_id like '%select%' or a.action_id ='undefined') and a.header.referrer_url like '%specialoffer%') D ) E group by sight_date, tapad_id, hl_platform, dvc_techname, carrier, offer,  source, room_id, ful_channel) F 
 on c.sight_date=f.sight_date and c.tapad_id=f.tapad_id and c.hl_platform=f.hl_platform and c.dvc_techname = f.dvc_techname and c.carrier=f.carrier and c.offer=f.offer and c.source = f.source  ;


 /*
 select offer, sum(imps), count(distinct tapad_id), sum(submits) from meas_ana.meas_table where sight_date < '2016-11-14' and sight_date >= '2016-11-01' and source ='kd' group by offer ;
 */
 
 /*
 +------------+--------------------------------------+-------------+-----------------+---------+---------------------------+--------+---------+-------------+------+--------+---------+----------+---------+
| sight_date | tapad_id                             | hl_platform | dvc_techname    | carrier | offer                     | source | room_id | ful_channel | imps | clicks | selects | landings | submits |
+------------+--------------------------------------+-------------+-----------------+---------+---------------------------+--------+---------+-------------+------+--------+---------+----------+---------+
| 2016-09-11 | c1521350-7805-11e6-b0d1-005056a23433 | ANDROID     | smart 3.5 touch | TMH     | mnp-samsung-galaxy-j2     | fb     | NULL    | NULL        | NULL | NULL   | 0       | 2        | 0       |
| 2016-08-26 | 1cc1f3a0-6b99-11e6-a71b-005056a210c3 | ANDROID     | pulp            | TMH     | mnp-samsung-galaxy-j2     | fb     | NULL    | NULL        | NULL | NULL   | 0       | 1        | 0       |
| 2016-09-09 | 5971f441-f884-11e5-ad98-06fe5a06de83 | ANDROID     | n5111           | TMH     | mnp-samsung-galaxy-j5     | kd     | 1       | NULL        | 6    | 1      | 0       | 1        | 0       |
| 2016-09-14 | 8f7caef1-51ae-11e4-b627-005056a21455 | ANDROID     | iris700         | Wi-Fi   | mnp-samsung-galaxy-j2     | fb     | NULL    | NULL        | NULL | NULL   | 0       | 1        | 0       |
| 2016-10-03 | 1ecb04a0-8996-11e6-abe6-005056a23433 | IPHONE      | iphone os 10    | Wi-Fi   | mnp-device-clearance      | fb     | NULL    | NULL        | NULL | NULL   | 0       | 1        | 0       |
| 2016-11-23 | a0238e81-7a61-11e6-953e-005056a2566f | ANDROID     | sm-n920c        | TMH     | mnp-iphone-se             | fb     | NULL    | NULL        | NULL | NULL   | 0       | 1        | 0       |
| 2016-11-19 | fb461eb1-24a1-11e6-ad16-0275b17dc6d7 | ANDROID     | k012            | AIS     | mnp-free-dtac-pocket-wifi | kd     | 12      | NULL        | 1    | 1      | 0       | 1        | 0       |
| 2016-08-31 | b43faa91-654c-11e6-8ca7-005056a27a0a | ANDROID     | gt-i9200        | AIS     | mnp-samsung-galaxy-j2     | kd     | 1       | line        | 2    | 0      | 2       | 0        | 1       |
| 2016-08-31 | b43faa91-654c-11e6-8ca7-005056a27a0a | ANDROID     | gt-i9200        | AIS     | mnp-samsung-galaxy-j2     | kd     | 13      | line        | 13   | 1      | 2       | 0        | 1       |
| 2016-09-11 | f9cf0a12-77da-11e6-b008-005056a27d22 | ANDROID     | s930            | Wi-Fi   | mnp-samsung-galaxy-j5     | fb     | NULL    | NULL        | NULL | NULL   | 0       | 1        | 0       |
+------------+--------------------------------------+-------------+-----------------+---------+---------------------------+--------+---------+-------------+------+--------+---------+----------+---------+

 */
 
