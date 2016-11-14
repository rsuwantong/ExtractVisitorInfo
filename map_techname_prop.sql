/*
####################################################################################
# Name: map_techname_prop
# Description: Map device techname from Pt, Kd, Sn to device properties
# Input: default.id_syncs, mobile_atlas_wcluster table
# Version:
#   2016/11/10 RS: Initial version
#   
####################################################################################
*/

drop table if exists rata_sght.techname_list;
create table rata_sght.techname_list row format delimited fields terminated by '\t' as (
select case
	    when hl_platform = 'PC_OTHERS' then platform 
		when lcase(user_agent) like '%cpu iphone os%' and lcase(user_agent) like '%ipod%' and lcase(platform)='iphone' then 'ipod' 
		when lcase(user_agent) like '%cpu iphone os%' or lcase(user_agent) like '%iphone; u; cpu iphone%' or lcase(user_agent) like '%iphone; cpu os%' and lcase(platform)='iphone' then regexp_replace(regexp_replace(regexp_replace(lcase(user_agent),'.*iphone;( u;)? cpu ',''),'like mac os.*',''),'_.*','') 
		when lcase(user_agent) like '%(null) [fban%' and lcase(user_agent) like '%fbdv/iphone%' and lcase(platform)='iphone' then regexp_extract(regexp_replace(lcase(user_agent),'.*fbdv/',''),'iphone[0-9]',0) 
		when lcase(user_agent) like '%android; mobile; rv%' or lcase(user_agent) like '%mobile rv[0-9][0-9].[0-9] gecko%' then 'unidentified android' 
		when lcase(user_agent) like '%android; tablet; rv%' or lcase(user_agent) like '%tablet rv[0-9][0-9].[0-9] gecko%' then 'unidentified tablet' 
		when lcase(user_agent) like 'iris405' then 'iris405'  when lcase(user_agent) like 'iris700' then 'iris700'   
		else  regexp_replace(regexp_replace(regexp_replace(trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lcase(user_agent),'.*android [0-9](.[0-9](.[0-9])?)?; ',''),' build.*|; android/.*|\\) applewebkit.*|/v[0-9] linux.*|v_td.*|_td/v[0-9].*|i_style.*',''),'.*([a-z][a-z])(-|_)([a-z][a-z]);? |.*nokia; ',''),'/.*|linux.*','')),'[^0-9a-z\- \.]',''),'.*samsung(-| )|.*lenovo |.*microsoft |.*th- ',''),'like.*|lollipop.*','') end as device_techname, platform 
	from (
select case when lower(a.header.platform)='iphone' and (lower(a.header.user_agent) like ('%windows phone%') or lower(a.header.user_agent) like ('%lumia%')) then 'WINDOWS_PHONE' else a.header.platform end as platform, case when a.header.platform in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE') then 'ANDROID' when a.header.platform='IPHONE' then 'IPHONE' else 'PC_OTHERS' end as hl_platform, a.header.user_agent as user_agent from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id in (2177,2243,2248) and YEAR=2016 and MONTH>=10 and c.key='TAPAD_COOKIE' group by platform, hl_platform, user_agent) A group by device_techname, platform);

/*Join the device_techname from the rata_sght.techname_list table to the mobile atlas table and choose the minimum popularity rank (maximum popularity) when there are multiple matches*/
  
drop table if exists rata_sght.techname_matched2atlas;
create table rata_sght.techname_matched2atlas row format delimited fields terminated by '\t' as (
  select * from (SELECT a.device_techname as device_techname_raw, min(b.popularity) as popularity
  FROM rata_sght.techname_list a 
left JOIN (select device_techname, popularity from rata_sght.mobile_atlas_wcluster order by popularity asc)  b 
  ON a.device_techname LIKE CONCAT(b.device_techname, '%') group by device_techname_raw order by device_techname_raw desc) C where popularity is not null) ;

  
/*Add device properties from mobile atlas table to techname_list table*/

drop table if exists apollo_util.techname_prop_map;
create table apollo_util.techname_prop_map row format delimited fields terminated by '\t' as (
  select a.device_techname_raw, cast (a.popularity as double) as popularity, case when a.device_techname_raw='lenny' then  'lenny' when a.device_techname_raw='lenny2' then  'lenny2' else b.device_techname end as device_techname, b.brand, case when a.device_techname_raw='lenny' then  'lenny' when a.device_techname_raw='lenny2' then  'lenny2'  else b.device_commercname end as device_commercname, case when a.device_techname_raw='lenny' then  '2014' when a.device_techname_raw='lenny2' then  '2015' else b.release_year end as release_year, case when a.device_techname_raw='lenny' then  'August' when a.device_techname_raw='lenny2' then  'September' else b.release_month end as release_month, b.release_price, b.screensize, b.cluster from  rata_sght.techname_matched2atlas a left join rata_sght.mobile_atlas_wcluster b on a.popularity=b.popularity order by a.popularity asc);
  
  select * from apollo_util.techname_prop_map order by popularity asc limit 20;
  
  /*
  +------------------------------------------+------------+-----------------+---------+--------------------+--------------+---------------+---------------+------------+---------+
| device_techname_raw                      | popularity | device_techname | brand   | device_commercname | release_year | release_month | release_price | screensize | cluster |
+------------------------------------------+------------+-----------------+---------+--------------------+--------------+---------------+---------------+------------+---------+
| sm-j700k                                 | 1          | sm-j700         | samsung | galaxy j7          | 2015         | June          | 250           | 5.5        | 4       |
| sm-j700t                                 | 1          | sm-j700         | samsung | galaxy j7          | 2015         | June          | 250           | 5.5        | 4       |
| sm-j700p                                 | 1          | sm-j700         | samsung | galaxy j7          | 2015         | June          | 250           | 5.5        | 4       |
| sm-j700t1                                | 1          | sm-j700         | samsung | galaxy j7          | 2015         | June          | 250           | 5.5        | 4       |
| sm-j700h                                 | 1          | sm-j700         | samsung | galaxy j7          | 2015         | June          | 250           | 5.5        | 4       |
| sm-j700f u2                              | 1          | sm-j700         | samsung | galaxy j7          | 2015         | June          | 250           | 5.5        | 4       |
| sm-j7008                                 | 1          | sm-j700         | samsung | galaxy j7          | 2015         | June          | 250           | 5.5        | 4       |
| sm-j700m                                 | 1          | sm-j700         | samsung | galaxy j7          | 2015         | June          | 250           | 5.5        | 4       |
| sm-j700                                  | 1          | sm-j700         | samsung | galaxy j7          | 2015         | June          | 250           | 5.5        | 4       |
| sm-j700f                                 | 1          | sm-j700         | samsung | galaxy j7          | 2015         | June          | 250           | 5.5        | 4       |
| sm-j200m                                 | 2          | sm-j200         | samsung | galaxy j2          | 2015         | September     | 150           | 4.7        | 10      |
| sm-j200gu                                | 2          | sm-j200         | samsung | galaxy j2          | 2015         | September     | 150           | 4.7        | 10      |
| sm-j200g                                 | 2          | sm-j200         | samsung | galaxy j2          | 2015         | September     | 150           | 4.7        | 10      |
| sm-j200gu u2                             | 2          | sm-j200         | samsung | galaxy j2          | 2015         | September     | 150           | 4.7        | 10      |

  
  */
