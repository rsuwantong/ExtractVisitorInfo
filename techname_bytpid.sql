/*
####################################################################################
# Name: techname_bytpid
# Description: Create tpid - device_techname map
# Input: default.id_syncs
# Version:
#   2016/11/21 RS: Initial version
####################################################################################
*/


drop table if exists rata_sght.techname_bytpid;
create table rata_sght.techname_bytpid row format delimited fields terminated by '\t' as (
select b.value as tapad_id, 
	case
	    when a.header.platform not in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE','IPHONE') then a.header.platform  
		when lcase(a.header.user_agent) like '%cpu iphone os%' and lcase(a.header.user_agent) like '%ipod%' and lcase(a.header.platform)='iphone' then 'ipod' 
		when lcase(a.header.user_agent) like '%cpu iphone os%' or lcase(a.header.user_agent) like '%iphone; u; cpu iphone%' or lcase(a.header.user_agent) like '%iphone; cpu os%' and lcase(a.header.platform)='iphone' then regexp_replace(regexp_replace(regexp_replace(lcase(a.header.user_agent),'.*iphone;( u;)? cpu ',''),'like mac os.*',''),'_.*','') 
		when lcase(a.header.user_agent) like '%(null) [fban%' and lcase(a.header.user_agent) like '%fbdv/iphone%' and lcase(a.header.platform)='iphone' then regexp_extract(regexp_replace(lcase(a.header.user_agent),'.*fbdv/',''),'iphone[0-9]',0) 
		when lcase(a.header.user_agent) like '%android; mobile; rv%' or lcase(a.header.user_agent) like '%mobile rv[0-9][0-9].[0-9] gecko%' then 'unidentified android' 
		when lcase(a.header.user_agent) like '%android; tablet; rv%' or lcase(a.header.user_agent) like '%tablet rv[0-9][0-9].[0-9] gecko%' then 'unidentified tablet' 
		else  regexp_replace(regexp_replace(regexp_replace(trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lcase(a.header.user_agent),'.*android [0-9](.[0-9](.[0-9])?)?; ',''),' build.*|; android/.*|\\) applewebkit.*|/v[0-9] linux.*|v_td.*|_td/v[0-9].*|i_style.*',''),'.*(th|en|zh|zz)(-|_)(gb|au|ph|th|us|cn|nz|gb|tw|fi|jp|za|sg|ie|zz);? |.*nokia; ',''),'/.*|linux.*','')),'[^0-9a-z\- \.]',''),'.*samsung(-| )|.*lenovo |.*microsoft |.*th- ',''),'like.*|lollipop.*','') end as device_techname 
	
	from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id in (2243) and YEAR=2016 and MONTH=11 and day >=2 and c.key='TAPAD_COOKIE' group by tapad_id, device_techname 
	);
