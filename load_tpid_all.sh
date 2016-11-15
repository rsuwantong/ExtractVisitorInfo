#!/bin/bash
###############################################################
# Description: The bash script to loop through impala table 
#              and load to the new one
# Version: 0.1 (Beta): 
#   11-15-2016: Initial version - need hardcode on start and end
###############################################################
# Variable declaration
year=2016
month=11
day=4

## Loop till day...
while [ $day -lt 14 ]; do
 query=$"insert into table net.tpid_sighting_all partition(year=$year, month=$month, day=$day)
select 
a.sight_time
,a.partner
,case 
when partner = 'Pantip' and a.referrer_url like '%topic%' then 'Topic' 
when partner = 'Pantip' and a.referrer_url like '%forum/' then 'Forum Main Page'
when partner = 'Pantip' and a.referrer_url like '%forum%' then 'Forum'
when partner = 'Pantip' and a.referrer_url like '%tag%' then 'Tag' 
when partner = 'Pantip' and a.referrer_url like '%club%' then 'Club'
when partner = 'Pantip' and a.referrer_url like '%m.pantip.com%' then 'Main Page Mobile version'
when partner = 'Pantip' and a.referrer_url like '%pantip.com%' then 'Main Page Desktop version'
when partner = 'Kaidee' and regexp_like(url, 'ta_cat=') then 'Forum'
when partner = 'Kaidee' and not(regexp_like(url, 'ta_cat=')) then 'Main Page'
when partner = 'Sanook' and a.referrer_url like '%www.sanook.com/' then 'Main Page'
when partner = 'Sanook' then 'Forum'
end as page_type
,coalesce(
case when partner = 'Kaidee' and regexp_like(url, 'ta_cat=') then trim(split_part(split_part(regexp_replace(url,'&ts=[0-9]*', ''), '&ta_cat=', 2), 'l1=', 2)) else NULL end,
case when partner = 'Pantip' and regexp_like(url, 'ta_cat=') then regexp_replace(split_part(url, 'ta_cat=group%3D', 2), '%.*', '')  else NULL end,
case when partner = 'Pantip' and regexp_like(referrer_url, 'forum') then split_part(referrer_url, 'forum/', 2)  else NULL end,
case when partner = 'Sanook' and regexp_like(url, 'ta_cat=') then split_part(url, 'ta_cat=', 2)  else NULL end)
as forum
, coalesce(
case when partner = 'Kaidee' and regexp_like(url, 'ta_cat=') then regexp_replace(split_part(split_part(regexp_replace(url, '&ts=[0-9]*', ''), 'ta_cat=', 2), 'l2=', 2), 'l1=.*', '') else NULL end
,case when partner = 'Pantip' then regexp_replace(split_part(split_part(url, 'ta_cat=group%3D', 2), '%26C1%3D', 2), '%26.*', '') else NULL end
) as level_1
,coalesce(
case when partner = 'Kaidee' and regexp_like(url, 'ta_cat=') then regexp_replace(split_part(split_part(regexp_replace(url, '&ts=[0-9]*', ''), 'ta_cat=', 2), 'l3=', 2), 'l2=.*', '') else NULL end
,case when partner = 'Pantip' then regexp_replace(split_part(split_part(url, 'ta_cat=group%3D', 2), '%26C2%3D', 2), '%26.*', '') else NULL end
) as level_2
,coalesce(
case when partner = 'Kaidee' and regexp_like(url, 'ta_cat=') then regexp_replace(split_part(split_part(regexp_replace(url, '&ts=[0-9]*', ''), 'ta_cat=', 2), 'l4=', 2), 'l3=.*', '') else NULL end
,case when partner = 'Pantip' then regexp_replace(split_part(split_part(url, 'ta_cat=group%3D', 2), '%26C3%3D', 2), '%26.*', '') else NULL end
) as level_3
,coalesce(
case when partner = 'Kaidee' and regexp_like(url, 'ta_cat=') then regexp_replace(split_part(split_part(regexp_replace(url, '&ts=[0-9]*', ''), 'ta_cat=', 2), 'l5=', 2), 'l4=.*', '') else NULL end
,case when partner = 'Pantip' then regexp_replace(split_part(split_part(url, 'ta_cat=group%3D', 2), '%26C4%3D', 2), '%26.*', '') else NULL end
) as level_4
,coalesce(
case when partner = 'Kaidee' and regexp_like(url, 'ta_cat=') then regexp_replace(split_part(split_part(regexp_replace(url, '&ts=[0-9]*', ''), 'ta_cat=', 2), 'l6=', 2), 'l5=.*', '') else NULL end
,case when partner = 'Pantip' then regexp_replace(split_part(split_part(url, 'ta_cat=group%3D', 2), '%26C5%3D', 2), '%26.*', '') else NULL end
) as level_5
,a.url
,a.referrer_url
,a.platform
,case when upper(a.platform) in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE', 'IPHONE') then 'Mobile' else 'Non Mobile' end as dvc_type
,a.dvc_techname 
, case when a.ip_number between 18087936 and 18153471 then 'OTH' 
when a.ip_number between 19791872 and 19922943 then 'DTAC' 
when a.ip_number between 456589312 and 456654847 then  'TMH' 
when a.ip_number between 837156864 and 837222399 then  'AIS'
when a.ip_number between 837615616 and 837681151 then  'TMH' 
when a.ip_number between 1848705024 and 1848770559 then  'AIS' 
when a.ip_number between 1867776000 and 1867825151 then  'DTAC' 
when a.ip_number between 1867826176 and 1867841535 then  'DTAC' 
when a.ip_number between 1933770752 and 1933836287 then  'DTAC' 
when a.ip_number between 1998520320 and 1998553087 then  'AIS' 
when a.ip_number between 2523597824 and 2523598847 then  'OTH' 
when a.ip_number between 3033972736 and 3033980927 then  'TMH' 
when a.ip_number between 3068657664 and 3068723199 then  'AIS' 
when a.ip_number between 3398768640 and 3398769663 then  'AIS' 
when a.ip_number between 3415276128 and 3415276159 then  'TMH' 
when a.ip_number between 3742892032 and 3742957567 then  'TMH' 
else 'Wi-Fi' end as carrier
,b.country as country
,a.tapad_id
from (
select 
from_unixtime(cast(a.header.created_at/1000 as int), 'dd') as sight_day
,from_unixtime(cast(a.header.created_at/1000 as int), 'MM') as sight_month
,from_unixtime(cast(a.header.created_at/1000 as int), 'yyyy') as sight_year
,from_unixtime(cast(a.header.created_at/1000 as int), 'HH:mm:ss') as sight_time
,partner_code as partner
,case when lower(a.header.platform)='iphone' and (lower(a.header.user_agent) like ('%windows phone%') or lower(a.header.user_agent) like ('%lumia%')) then 'WINDOWS_PHONE' else a.header.platform end as platform
,cast(split_part(a.header.ip_address,'.',1) as INT)*16777216 +
 cast(split_part(a.header.ip_address,'.',2) as INT)*65536 +
 cast(split_part(a.header.ip_address,'.',3) as INT)*256+
 cast(split_part(a.header.ip_address,'.',4) as INT) ip_number
,b.value as tapad_id
,a.header.url as url
,a.header.referrer_url as referrer_url
,case
when lcase(a.header.user_agent) like '%cpu iphone os%' and lcase(a.header.user_agent) like '%ipod%' and lcase(a.header.platform)='iphone' then 'ipod' 
when lcase(a.header.user_agent) like '%cpu iphone os%' or lcase(a.header.user_agent) like '%iphone; u; cpu iphone%' or lcase(a.header.user_agent) like '%iphone; cpu os%' and lcase(a.header.platform)='iphone' then regexp_replace(regexp_replace(regexp_replace(lcase(a.header.user_agent),'.*iphone;( u;)? cpu ',''),'like mac os.*',''),'_.*','') 
when lcase(a.header.user_agent) like '%(null) [fban%' and lcase(a.header.user_agent) like '%fbdv/iphone%' and lcase(a.header.platform)='iphone' then regexp_extract(regexp_replace(lcase(a.header.user_agent),'.*fbdv/',''),'iphone[0-9]',0) 
when lcase(a.header.user_agent) like '%android; mobile; rv%' or lcase(a.header.user_agent) like '%mobile rv[0-9][0-9].[0-9] gecko%' then 'unidentified android' 
when lcase(a.header.user_agent) like '%android; tablet; rv%' or lcase(a.header.user_agent) like '%tablet rv[0-9][0-9].[0-9] gecko%' then 'unidentified tablet' 
else regexp_replace(regexp_replace(regexp_replace(trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lcase(a.header.user_agent),'.*android [0-9](.[0-9](.[0-9])?)?; ',''),' build.*|; android/.*|\\\\) applewebkit.*|/v[0-9] linux.*|v_td.*|_td/v[0-9].*|i_style.*',''),'.*(th|en|zh|zz)(-|_)(gb|au|ph|th|us|cn|nz|gb|tw|fi|jp|za|sg|ie|zz);? |.*nokia; ',''),'/.*|linux.*','')),'[^0-9a-z\\- \\.]',''),'.*samsung(-| )|.*lenovo |.*microsoft |.*th- ',''),'like.*|lollipop.*','') end as dvc_techname 
from default.id_syncs a,
a.header.incoming_ids b,
b.sightings_by_id_type c
where partner_id in (2177, 2243, 2248)  and year=$year and month = $month and day = $day and c.key='TAPAD_COOKIE' ) a
left join (
select ip
,country
from default.semcasting
) b on a.ip_number = b.ip 
;"
 echo "$day is runnin"
 impala-shell -i impala.prd.sg1.tapad.com:21000 -q "$query"
 let day=day+1 
done
