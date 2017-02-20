/*
####################################################################################
# Name: databytxn_pt_reduced
# Description: Create a based data table from Pantip's id_syncs
# Input: default.id_syncs
# Version:
#   2016/11/11 RS: Initial version
#   2016/11/14 RS: Add tapad_id and erase platform to/ from data_txn_pt, correct page_type and num_tag 
####################################################################################
*/


/* Pantip */


drop table if exists rata_sght.data_txn_pt_pre;
create table rata_sght.data_txn_pt_pre row format delimited fields terminated by '\t' as ( 

select sight_date,
		case when platform in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE') then 'ANDROID' when platform='IPHONE' then 'IPHONE' else 'PC_OTHERS' end as hl_platform, 
		case when ip_number between 18087936 and 18153471 then 'TOT' when ip_number between 19791872 and 19922943 then 'DTAC' when ip_number between 456589312 and 456654847 then  'TMH' when ip_number between 837156864 and 837222399 then  'AIS'when ip_number between 837615616 and 837681151 then  'TMH' when ip_number between 1848705024 and 1848770559 then  'AIS' when ip_number between 1867776000 and 1867825151 then  'DTAC' when ip_number between 1867826176 and 1867841535 then  'DTAC' when ip_number between 1933770752 and 1933836287 then  'DTAC' when ip_number between 1998520320 and 1998553087 then  'AIS' when ip_number between 2523597824 and 2523598847 then  'OTH' when ip_number between 3033972736 and 3033980927 then  'TMH' when ip_number between 3068657664 and 3068723199 then  'AIS' when ip_number between 3398768640 and 3398769663 then  'AIS' when ip_number between 3415276128 and 3415276159 then  'TMH' when ip_number between 3742892032 and 3742957567 then  'TMH' else 'Wi-Fi' end as carrier,  
		page_type, 
		case when referrer_url like '%/forum/%' then 		
				regexp_replace(split_part(regexp_replace(regexp_replace(regexp_replace(referrer_url,'.*forum/',''),'\\?.*',''),'\\&.*',''),'',1),'/.*','')
			when url like '%ta_cat=group%' then regexp_replace(regexp_replace(url,'.*ta_cat=group%3D',''),'%{1}.*','') end as forum, 
		tag_num, 
		case  when referrer_url like '%/tag/%' then regexp_replace(regexp_replace(regexp_replace(referrer_url,'.*tag/',''),'[\\?\\(].*',''),'_%?$','') 
			  when tag_num between 1 and 5 then regexp_replace(regexp_replace(url,'.*%26C1%3D',''),'%26C2.*','') 
		end as tag1, 
	    case when tag_num between 2 and 5 then regexp_replace(regexp_replace(url,'.*%26C2%3D',''),'%26C3.*','') end as tag2, 
	    case when tag_num between 3 and 5 then regexp_replace(regexp_replace(url,'.*%26C3%3D',''),'%26C4.*','') end as tag3,  
	    case when tag_num between 4 and 5 then regexp_replace(regexp_replace(url,'.*%26C4%3D',''),'%26C5.*','') end as tag4, 
	    case when tag_num =5 then regexp_replace(url,'.*%26C5%3D','') end as tag5, tapad_id  

	from ( select regexp_replace(cast(cast(a.header.created_at/1000 as timestamp) as string),' .*','') as sight_date,case when lcase(a.header.platform)='iphone' and (lcase(a.header.user_agent) like ('%windows phone%') or lcase(a.header.user_agent) like ('%lumia%')) then 'WINDOWS_PHONE' else a.header.platform end as platform, cast(split_part(a.header.ip_address,'.',1) as INT)*16777216 + cast(split_part(a.header.ip_address,'.',2) as INT)*65536 + cast(split_part(a.header.ip_address,'.',3) as INT)*256+ cast(split_part(a.header.ip_address,'.',4) as INT) ip_number, b.value as tapad_id, a.header.url as url, case when a.header.referrer_url like '%tag%' then regexp_replace(a.header.referrer_url,'(\\(?%E0|à).*','') when a.header.referrer_url like '%topic%' then NULL else a.header.referrer_url end as referrer_url, case when a.header.referrer_url like '%pantip.com/tag%' then 'tag' 
		when a.header.referrer_url like '%pantip.com/forum%' then 'forum' 
		when a.header.referrer_url like '%pantip.com/topic%' then 'topic' 
		when (a.header.referrer_url ='http://pantip.com/' or a.header.referrer_url ='http://m.pantip.com/' or a.header.referrer_url like '%pantip.com/home%'
		or a.header.referrer_url like '%pantip.com/pick%' or a.header.referrer_url like '%pantip.com/trend%' or a.header.referrer_url like '%pantip.com/ourlove%'
		) then 'home' 
		when a.header.referrer_url like '%pantip.com/profile/%' then 'profile' 
		when a.header.referrer_url like '%pantip.com/club%' then 'club' 
		when a.header.referrer_url like '%pantip.com/register%' then 'register' 
		when a.header.referrer_url like '%account%' or a.header.referrer_url like '%setting%' or a.header.referrer_url like '%login%' then 'account' 
		when a.header.referrer_url like '%pantip.com/about%' or a.header.referrer_url like '%pantip.com/activities%' or a.header.referrer_url like '%pantip.com/advertising%' then 'act-abt-ads' else 'others' end as page_type, case when regexp_replace(a.header.url,'.*(ta_cat=group)','') like '%\\%26C%' then cast(regexp_replace(regexp_replace(a.header.url,'.*(%26C)+',''),'%3D.*','') as double) else 0 end as tag_num
	
	from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id in (2243) and YEAR=2016 and MONTH=11 and day >=2 and c.key='TAPAD_COOKIE') A );
	
/*Replace tag_id to tag_name*/	

drop table if exists rata_sght.data_txn_pt;
create table rata_sght.data_txn_pt row format delimited fields terminated by '\t' as ( 
	
select i.sight_date, i.tapad_id, i.hl_platform, i.carrier, i.page_type, i.forum, i.tag_num, i.tag1,  i.tag2,  i.tag3, i.tag4, j.tag_name as tag5 from 
(	
select g.sight_date, g.tapad_id, g.hl_platform, g.carrier, g.page_type, g.forum, g.tag_num, g.tag1,  g.tag2,  g.tag3, h.tag_name as tag4, g.tag5 from 
(	
select e.sight_date, e.tapad_id, e.hl_platform, e.carrier, e.page_type, e.forum, e.tag_num, e.tag1,  e.tag2, f.tag_name as tag3, e.tag4, e.tag5 from 
(	
select c.sight_date, c.tapad_id, c.hl_platform, c.carrier, c.page_type, c.forum, c.tag_num, c.tag1, d.tag_name as tag2, c.tag3, c.tag4, c.tag5 from 
(select a.sight_date, a.tapad_id, a.hl_platform, a.carrier, a.page_type, a.forum, a.tag_num, b.tag_name as tag1, a.tag2, a.tag3, a.tag4, a.tag5 from rata_sght.data_txn_pt_pre a left join apollo_util.tagid2tagname_pt b on a.tag1=b.tag_id) c left join apollo_util.tagid2tagname_pt d on c.tag2=d.tag_id) e left join apollo_util.tagid2tagname_pt f on e.tag3=f.tag_id) g left join apollo_util.tagid2tagname_pt h on g.tag4=h.tag_id) i left join apollo_util.tagid2tagname_pt j on i.tag5=j.tag_id)  ;

drop table if exists rata_sght.data_txn_pt_activd;
create table rata_sght.data_txn_pt_activd row format delimited fields terminated by '\t' as ( 
select  tapad_id, count(distinct sight_date) as active_days from rata_sght.data_txn_pt group by tapad_id  
);


/*
[impala.prd.sg1.tapad.com:21000] > select * from rata_sght.data_txn_pt limit 20;
Query: select * from rata_sght.data_txn_pt limit 20
+------------+--------------------------------------+-------------+---------+---------+---------------------+-----------+--------------+---------+------------------+-----------------+-----------------+-----------------+----------------+
| sight_date | tapad_id                             | hl_platform | carrier | channel | device_techname     | page_type | forum        | tag_num | tag1             | tag2            | tag3            | tag4            | tag5           |
+------------+--------------------------------------+-------------+---------+---------+---------------------+-----------+--------------+---------+------------------+-----------------+-----------------+-----------------+----------------+
| 2016-11-05 | 89e37fc1-9291-11e6-8b4b-005056a27c72 | ANDROID     | AIS     | Pantip  | lumia 620           | forum     | food         | 0       | NULL             | NULL            | NULL            | NULL            | NULL           |
| 2016-11-05 | f007d601-48d8-11e6-a6c9-06fe5a06de83 | PC_OTHERS   | Wi-Fi   | Pantip  | IPAD                | topic     | chalermthai  | 0       | NULL             | NULL            | NULL            | NULL            | NULL           |
| 2016-11-05 | 34d6b781-091c-11e6-af81-0680bb2a2415 | PC_OTHERS   | Wi-Fi   | Pantip  | COMPUTER            | topic     | beauty       | 0       | NULL             | NULL            | NULL            | NULL            | NULL           |
| 2016-11-05 | a8cd1e11-a343-11e6-ba34-005056a272df | IPHONE      | Wi-Fi   | Pantip  | iphone os 10        | topic     | bangrak      | 5       | ประสบการณ์ชีวิตคู่ | ปัญหาความรัก    | ความรักวัยรุ่น    | ความรักวัยทำงาน | ปัญหาชีวิต     |
| 2016-11-05 | db112fe1-456c-11e6-9483-0275b17dc6d7 | ANDROID     | DTAC    | Pantip  | gt-n7100            | topic     | home         | 0       | NULL             | NULL            | NULL            | NULL            | NULL           |
| 2016-11-05 | 7edb12c1-993d-11e6-8b4b-005056a27c72 | PC_OTHERS   | Wi-Fi   | Pantip  | IPAD                | topic     | supachalasai | 2       | ฟุตบอลต่างประเทศ   | พรีเมียร์ลีก    | NULL            | NULL            | NULL           |
| 2016-11-05 | c3afec41-a301-11e6-ba34-005056a272df | IPHONE      | DTAC    | Pantip  | iphone os 10        | topic     | bangrak      | 5       | ปัญหาความรัก     | ปัญหาชีวิต      | ความรักวัยทำงาน | ความรักวัยรุ่น    | ปัญหาครอบครัว  |
| 2016-11-05 | 07d4c1c1-a325-11e6-be06-005056a262f4 | PC_OTHERS   | Wi-Fi   | Pantip  | IPAD                | topic     | blueplanet   | 5       | เที่ยวต่างประเทศ   | ประเทศเกาหลีใต้  | ประเทศญี่ปุ่น      | สนามบิน         | วีซ่า           |
| 2016-11-05 | bbe1e061-8eeb-11e6-a20f-005056a27a0a | PC_OTHERS   | Wi-Fi   | Pantip  | COMPUTER            | topic     | bangrak      | 5       | แต่งงาน           | เที่ยวทะเล       | สถานที่จัดเลี้ยง  | ขอแต่งงาน        | เกาะช้าง        |
| 2016-11-05 | c86a4261-3558-11e6-bf9a-0294e573e689 | PC_OTHERS   | Wi-Fi   | Pantip  | COMPUTER            | topic     | blueplanet   | 0       | NULL             | NULL            | NULL            | NULL            | NULL           |
| 2016-11-05 | ea36e821-6ac4-11e6-88f9-005056a2566f | ANDROID     | DTAC    | Pantip  | sm-g360h            | topic     | siam         | 0       | NULL             | NULL            | NULL            | NULL            | NULL           |
| 2016-11-05 | 38811381-a33b-11e6-ba34-005056a272df | IPHONE      | Wi-Fi   | Pantip  | iphone os 10        | topic     | siam         | 0       | NULL             | NULL            | NULL            | NULL            | NULL           |
| 2016-11-05 | 5a1b1001-a165-11e5-a50e-005056a24e8f | ANDROID     | Wi-Fi   | Pantip  | sm-j700f            | topic     | social       | 5       | ธนาคาร           | คุ้มครองผู้บริโภค   | เตือนภัย        | การเงิน         | ธนาคารกสิกรไทย |
| 2016-11-05 | 67fd0ea1-560d-11e6-ac0d-02daa3147571 | ANDROID     | Wi-Fi   | Pantip  | sm-a800f            | topic     | bangrak      | 4       | ปัญหาชีวิต       | ความรักวัยทำงาน | ศาลาคนเศร้า      | ปัญหาความรัก    | NULL           |
| 2016-11-05 | c25c1611-350d-11e5-97da-005056a20bde | ANDROID     | AIS     | Pantip  | sm-a700fd           | topic     | ratchada     | 0       | NULL             | NULL            | NULL            | NULL            | NULL           |
| 2016-11-05 | 4a49fc21-dbbe-11e5-bc81-0275b17dc6d7 | ANDROID     | Wi-Fi   | Pantip  | gt-n7100            | topic     | food         | 4       | อาหารคาว         | ทำอาหาร         | อาหาร           | อาหารไทย        | NULL           |
| 2016-11-05 | 5f5e2701-a317-11e6-9983-005056a27c72 | PC_OTHERS   | DTAC    | Pantip  | IPAD                | topic     | klaibann     | 4       | ชีวิตในต่างแดน    | ทำงานต่างประเทศ  | แม่บ้านต่างแดน     | แต่งเรื่องสั้น     | NULL           |
| 2016-11-05 | 0d544421-94e7-11e4-929d-005056a276c4 | ANDROID     | Wi-Fi   | Pantip  | mobile rv49.0 gecko | topic     | home         | 0       | NULL             | NULL            | NULL            | NULL            | NULL           |
| 2016-11-05 | ab155291-9caf-11e6-abe6-005056a23433 | IPHONE      | Wi-Fi   | Pantip  | iphone os 10        | topic     | mbk          | 5       | iPhone 7         | สมาร์ทโฟน       | Android         | iOS             | โทรศัพท์มือถือ |
| 2016-11-05 | 297be841-a356-11e6-8297-005056a26b8c | IPHONE      | AIS     | Pantip  | iphone os 10        | topic     | sinthorn     | 5       | ธนาคาร           | คุ้มครองผู้บริโภค   | เตือนภัย        | การเงิน         | ธนาคารกสิกรไทย |
+------------+--------------------------------------+-------------+---------+---------+---------------------+-----------+--------------+---------+------------------+-----------------+-----------------+-----------------+----------------+



*/