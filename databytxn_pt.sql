/*
####################################################################################
# Name: databytxn_pt
# Description: Create a based data table from Pantip's id_syncs
# Input: default.id_syncs
# Version:
#   2016/11/11 RS: Initial version
#   
####################################################################################
*/


/* Pantip */


drop table if exists rata_sght.data_txn_pt_pre;
create table rata_sght.data_txn_pt_pre row format delimited fields terminated by '\t' as ( 

select sight_date, platform, hl_platform, case when ip_number between 18087936 and 18153471 then 'TOT' when ip_number between 19791872 and 19922943 then 'DTAC' when ip_number between 456589312 and 456654847 then  'TMH' when ip_number between 837156864 and 837222399 then  'AIS'when ip_number between 837615616 and 837681151 then  'TMH' when ip_number between 1848705024 and 1848770559 then  'AIS' when ip_number between 1867776000 and 1867825151 then  'DTAC' when ip_number between 1867826176 and 1867841535 then  'DTAC' when ip_number between 1933770752 and 1933836287 then  'DTAC' when ip_number between 1998520320 and 1998553087 then  'AIS' when ip_number between 2523597824 and 2523598847 then  'OTH' when ip_number between 3033972736 and 3033980927 then  'TMH' when ip_number between 3068657664 and 3068723199 then  'AIS' when ip_number between 3398768640 and 3398769663 then  'AIS' when ip_number between 3415276128 and 3415276159 then  'TMH' when ip_number between 3742892032 and 3742957567 then  'TMH' else 'Wi-Fi' end as carrier, channel, case
	    when hl_platform = 'PC_OTHERS' then platform 
		when lcase(user_agent) like '%cpu iphone os%' and lcase(user_agent) like '%ipod%' and lcase(platform)='iphone' then 'ipod' 
		when lcase(user_agent) like '%cpu iphone os%' or lcase(user_agent) like '%iphone; u; cpu iphone%' or lcase(user_agent) like '%iphone; cpu os%' and lcase(platform)='iphone' then regexp_replace(regexp_replace(regexp_replace(lcase(user_agent),'.*iphone;( u;)? cpu ',''),'like mac os.*',''),'_.*','') 
		when lcase(user_agent) like '%(null) [fban%' and lcase(user_agent) like '%fbdv/iphone%' and lcase(platform)='iphone' then regexp_extract(regexp_replace(lcase(user_agent),'.*fbdv/',''),'iphone[0-9]',0) 
		when lcase(user_agent) like '%android; mobile; rv%' or lcase(user_agent) like '%mobile rv[0-9][0-9].[0-9] gecko%' then 'unidentified android' 
		when lcase(user_agent) like '%android; tablet; rv%' or lcase(user_agent) like '%tablet rv[0-9][0-9].[0-9] gecko%' then 'unidentified tablet' 
		else  regexp_replace(regexp_replace(regexp_replace(trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lcase(user_agent),'.*android [0-9](.[0-9](.[0-9])?)?; ',''),' build.*|; android/.*|\\) applewebkit.*|/v[0-9] linux.*|v_td.*|_td/v[0-9].*|i_style.*',''),'.*(th|en|zh|zz)(-|_)(gb|au|ph|th|us|cn|nz|gb|tw|fi|jp|za|sg|ie|zz);? |.*nokia; ',''),'/.*|linux.*','')),'[^0-9a-z\- \.]',''),'.*samsung(-| )|.*lenovo |.*microsoft |.*th- ',''),'like.*|lollipop.*','') end as device_techname, 
		
		page_type, case when referrer_url like '%/forum/%' then 		
				regexp_replace(split_part(regexp_replace(regexp_replace(regexp_replace(referrer_url,'.*forum/',''),'\\?.*',''),'\\&.*',''),'',1),'/.*','')
			when url like '%ta_cat=group%' then regexp_replace(regexp_replace(url,'.*ta_cat=group%3D',''),'%{1}.*','') end as forum, tag_num, 
		case  when referrer_url like '%/tag/%' then regexp_replace(regexp_replace(regexp_replace(referrer_url,'.*tag/',''),'[\\?\\(].*',''),'_%?$','') 
			  when tag_num between 1 and 5 then regexp_replace(regexp_replace(url,'.*%26C1%3D',''),'%26C2.*','') 
		end as tag1, 
	    case when tag_num between 2 and 5 then regexp_replace(regexp_replace(url,'.*%26C2%3D',''),'%26C3.*','') end as tag2, 
	    case when tag_num between 3 and 5 then regexp_replace(regexp_replace(url,'.*%26C3%3D',''),'%26C4.*','') end as tag3,  
	    case when tag_num between 4 and 5 then regexp_replace(regexp_replace(url,'.*%26C4%3D',''),'%26C5.*','') end as tag4, 
	    case when tag_num =5 then regexp_replace(url,'.*%26C5%3D','') end as tag5, tapad_id  

	from ( select regexp_replace(cast(cast(a.header.created_at/1000 as timestamp) as string),' .*','') as sight_date, partner_code as channel,case when lower(a.header.platform)='iphone' and (lower(a.header.user_agent) like ('%windows phone%') or lower(a.header.user_agent) like ('%lumia%')) then 'WINDOWS_PHONE' else a.header.platform end as platform, case when a.header.platform in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE') then 'ANDROID' when a.header.platform='IPHONE' then 'IPHONE' else 'PC_OTHERS' end as hl_platform, cast(split_part(a.header.ip_address,'.',1) as INT)*16777216 + cast(split_part(a.header.ip_address,'.',2) as INT)*65536 + cast(split_part(a.header.ip_address,'.',3) as INT)*256+ cast(split_part(a.header.ip_address,'.',4) as INT) ip_number, b.value as tapad_id, a.header.user_agent as user_agent, a.header.url as url, case when a.header.referrer_url like '%tag%' then regexp_replace(a.header.referrer_url,'(\\(?%E0|à).*','') when a.header.referrer_url like '%topic%' then NULL else a.header.referrer_url end as referrer_url, case when a.header.referrer_url like '%tag%' then 'tag' when a.header.referrer_url like '%forum%' then 'forum' when a.header.referrer_url like '%topic%' then 'topic' when a.header.referrer_url ='http://pantip.com/' or a.header.referrer_url ='http://m.pantip.com/' then 'main' end as page_type, case when (a.header.url like '%ta_cat%' and a.header.url like '%26C%') then cast(regexp_replace(regexp_replace(a.header.url,'.*(%26C)+',''),'%3D.*','') as double) else 0 end as tag_num 
	
	from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id in (2243) and YEAR=2016 and MONTH=11 and c.key='TAPAD_COOKIE') A );
	
/*Replace tag_id to tag_name*/	

drop table if exists rata_sght.data_txn_pt;
create table rata_sght.data_txn_pt row format delimited fields terminated by '\t' as ( 
	
select i.sight_date, i.platform, i.hl_platform, i.carrier, i.channel, i.device_techname, i.page_type, i.forum, i.tag_num, i.tag1,  i.tag2,  i.tag3, i.tag4, j.tag_name as tag5 from 
(	
select g.sight_date, g.platform, g.hl_platform, g.carrier, g.channel, g.device_techname, g.page_type, g.forum, g.tag_num, g.tag1,  g.tag2,  g.tag3, h.tag_name as tag4, g.tag5 from 
(	
select e.sight_date, e.platform, e.hl_platform, e.carrier, e.channel, e.device_techname, e.page_type, e.forum, e.tag_num, e.tag1,  e.tag2, f.tag_name as tag3, e.tag4, e.tag5 from 
(	
select c.sight_date, c.platform, c.hl_platform, c.carrier, c.channel, c.device_techname, c.page_type, c.forum, c.tag_num, c.tag1, d.tag_name as tag2, c.tag3, c.tag4, c.tag5 from 
(select a.sight_date, a.platform, a.hl_platform, a.carrier, a.channel, a.device_techname, a.page_type, a.forum, a.tag_num, b.tag_name as tag1, a.tag2, a.tag3, a.tag4, a.tag5 from rata_sght.data_txn_pt_pre a left join apollo_util.tagid2tagname_pt b on a.tag1=b.tag_id) c left join apollo_util.tagid2tagname_pt d on c.tag2=d.tag_id) e left join apollo_util.tagid2tagname_pt f on e.tag3=f.tag_id) g left join apollo_util.tagid2tagname_pt h on g.tag4=h.tag_id) i left join apollo_util.tagid2tagname_pt j on i.tag5=j.tag_id)  ;


/*
[impala.prd.sg1.tapad.com:21000] > select * from rata_sght.data_txn_pt limit 20;
Query: select * from rata_sght.data_txn_pt limit 20
+------------+----------------+-------------+---------+---------+-----------------+-----------+---------------+---------+------------------------------------------+--------------------+-----------------+-------------------+--------------------+
| sight_date | platform       | hl_platform | carrier | channel | device_techname | page_type | forum         | tag_num | tag1                                     | tag2               | tag3            | tag4              | tag5               |
+------------+----------------+-------------+---------+---------+-----------------+-----------+---------------+---------+------------------------------------------+--------------------+-----------------+-------------------+--------------------+
| 2016-11-07 | COMPUTER       | PC_OTHERS   | Wi-Fi   | Pantip  | COMPUTER        | topic     | tvshow        | 0       | NULL                                     | NULL               | NULL            | NULL              | NULL               |
| 2016-11-07 | ANDROID        | ANDROID     | DTAC    | Pantip  | r7plusf         | topic     | supachalasai  | 2       | กีฬา                                     | วอลเลย์บอล         | NULL            | NULL              | NULL               |
| 2016-11-07 | COMPUTER       | PC_OTHERS   | DTAC    | Pantip  | COMPUTER        | topic     | mbk           | 5       | 4G                                       | Mobile Operator    | truemove H      | dtac              | AIS                |
| 2016-11-07 | COMPUTER       | PC_OTHERS   | Wi-Fi   | Pantip  | COMPUTER        | topic     | tvshow        | 4       | อัครณัฐ อริยฤทธิ์วิกุล (น๊อต)              | นักแสดง            | สถานีโทรทัศน์   | รายการข่าว         | NULL               |
| 2016-11-07 | COMPUTER       | PC_OTHERS   | Wi-Fi   | Pantip  | COMPUTER        | tag       | NULL          | 0       | NULL                                     | NULL               | NULL            | NULL              | NULL               |
| 2016-11-07 | COMPUTER       | PC_OTHERS   | Wi-Fi   | Pantip  | COMPUTER        | forum     | cartoon       | 0       | NULL                                     | NULL               | NULL            | NULL              | NULL               |
| 2016-11-07 | COMPUTER       | PC_OTHERS   | Wi-Fi   | Pantip  | COMPUTER        | topic     | siliconvalley | 0       | NULL                                     | NULL               | NULL            | NULL              | NULL               |
| 2016-11-07 | COMPUTER       | PC_OTHERS   | Wi-Fi   | Pantip  | COMPUTER        | topic     | social        | 2       | ที่ดิน                                    | กฎหมายชาวบ้าน       | NULL            | NULL              | NULL               |
| 2016-11-07 | COMPUTER       | PC_OTHERS   | Wi-Fi   | Pantip  | COMPUTER        | topic     | siam          | 5       | The Shock                                | เรื่องเล่าสยองขวัญ   | รายการวิทยุ      | กพล ทองพลับ (ป๋อง) | สิ่งลี้ลับ (mystery) |
| 2016-11-07 | COMPUTER       | PC_OTHERS   | Wi-Fi   | Pantip  | COMPUTER        | topic     | food          | 0       | NULL                                     | NULL               | NULL            | NULL              | NULL               |
| 2016-11-07 | ANDROID        | ANDROID     | Wi-Fi   | Pantip  | htc onem8       | topic     | mbk           | 4       | iPhone                                   | iOS                | โทรศัพท์มือถือ  | สมาร์ทโฟน         | NULL               |
| 2016-11-07 | WINDOWS_TABLET | ANDROID     | Wi-Fi   | Pantip  | mozilla         | topic     | chalermthai   | 5       | พิธีกรรายการโทรทัศน์                     | นักแสดงไทย         | นักแสดงภาพยนตร์ | นักแสดง           | สังคมชาวพันทิป     |
| 2016-11-07 | COMPUTER       | PC_OTHERS   | Wi-Fi   | Pantip  | COMPUTER        | topic     | tvshow        | 4       | Moon Lovers: Scarlet Heart Ryeo (ซีรีส์) | ซีรีส์ฮ่องกง-ไต้หวัน | ซีรีส์จีน       | ซีรีส์เกาหลี      | NULL               |
| 2016-11-07 | COMPUTER       | PC_OTHERS   | Wi-Fi   | Pantip  | COMPUTER        | main      | NULL          | 0       | NULL                                     | NULL               | NULL            | NULL              | NULL               |
| 2016-11-07 | ANDROID        | ANDROID     | AIS     | Pantip  | vivo v3         | NULL      | NULL          | 0       | NULL                                     | NULL               | NULL            | NULL              | NULL               |
| 2016-11-07 | COMPUTER       | PC_OTHERS   | Wi-Fi   | Pantip  | COMPUTER        | topic     | silom         | 2       | มนุษย์เงินเดือน                           | งานไอที            | NULL            | NULL              | NULL               |
| 2016-11-07 | IPAD           | PC_OTHERS   | Wi-Fi   | Pantip  | IPAD            | forum     | cartoon       | 0       | NULL                                     | NULL               | NULL            | NULL              | NULL               |
| 2016-11-07 | COMPUTER       | PC_OTHERS   | Wi-Fi   | Pantip  | COMPUTER        | topic     | chalermkrung  | 0       | NULL                                     | NULL               | NULL            | NULL              | NULL               |
| 2016-11-07 | ANDROID        | ANDROID     | TOT     | Pantip  | sm-n900         | topic     | wahkor        | 5       | สุขภาพกาย                                 | คลับสุขภาพ          | ยา              | เภสัชกร           | ชีววิทยา           |
| 2016-11-07 | ANDROID        | ANDROID     | Wi-Fi   | Pantip  | asusz008d       | topic     | beauty        | 0       | NULL                                     | NULL               | NULL            | NULL              | NULL               |
+------------+----------------+-------------+---------+---------+-----------------+-----------+---------------+---------+------------------------------------------+--------------------+-----------------+-------------------+--------------------+


*/
