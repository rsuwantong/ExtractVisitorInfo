/*
####################################################################################
# Name: url2forumtag_pt
# Description: Extract page type, forum and tags from Pantip's url and referrer_url
# Input: default.id_syncs
# Version:
#   2016/11/11 RS: Initial version
#   
####################################################################################
*/

select page_type, case when referrer_url like '%/forum/%' then 		
				regexp_replace(split_part(regexp_replace(regexp_replace(regexp_replace(referrer_url,'.*forum/',''),'\\?.*',''),'\\&.*',''),'',1),'/.*','')
			when url like '%ta_cat=group%' then regexp_replace(regexp_replace(url,'.*ta_cat=group%3D',''),'%{1}.*','') end as forum, tag_num, 
		case  when referrer_url like '%/tag/%' then regexp_replace(regexp_replace(regexp_replace(referrer_url,'.*tag/',''),'[\\?\\(].*',''),'_%?$','') 
			  when tag_num between 1 and 5 then regexp_replace(regexp_replace(url,'.*%26C1%3D',''),'%26C2.*','') 
		end as tag1, 
	    case when tag_num between 2 and 5 then regexp_replace(regexp_replace(url,'.*%26C2%3D',''),'%26C3.*','') end as tag2, 
	    case when tag_num between 3 and 5 then regexp_replace(regexp_replace(url,'.*%26C3%3D',''),'%26C4.*','') end as tag3,  
	    case when tag_num between 4 and 5 then regexp_replace(regexp_replace(url,'.*%26C4%3D',''),'%26C5.*','') end as tag4, 
	    case when tag_num =5 then regexp_replace(url,'.*%26C5%3D','') end as tag5 
		from 
(select a.header.url as url, case when a.header.referrer_url like '%tag%' then regexp_replace(a.header.referrer_url,'(\\(?%E0|à).*','') when a.header.referrer_url like '%topic%' then NULL else a.header.referrer_url end as referrer_url, case when a.header.referrer_url like '%tag%' then 'tag' when a.header.referrer_url like '%forum%' then 'forum' when a.header.referrer_url like '%topic%' then 'topic' when a.header.referrer_url ='http://pantip.com/' or a.header.referrer_url ='http://m.pantip.com/' then 'main' end as page_type, case when (a.header.url like '%ta_cat%' and a.header.url like '%26C%') then cast(regexp_replace(regexp_replace(a.header.url,'.*(%26C)+',''),'%3D.*','') as double) else 0 end as tag_num from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id in (2243) and YEAR=2016 and MONTH=11 and c.key='TAPAD_COOKIE' ) A where url is not null  limit 20;

/*
+-----------+--------------+---------+------+-------+------+------+------+
| page_type | forum        | tag_num | tag1 | tag2  | tag3 | tag4 | tag5 |
+-----------+--------------+---------+------+-------+------+------+------+
| topic     | supachalasai | 2       | 574  | 561   | NULL | NULL | NULL |
| topic     | blueplanet   | 5       | 360  | 346   | 359  | 350  | 8305 |
| topic     | family       | 0       | NULL | NULL  | NULL | NULL | NULL |
| topic     | sinthorn     | 2       | 7539 | 650   | NULL | NULL | NULL |
| topic     | rajdumnern   | 5       | 523  | 7440  | 530  | 521  | 526  |
| topic     | blueplanet   | 0       | NULL | NULL  | NULL | NULL | NULL |
| topic     | isolate      | 0       | NULL | NULL  | NULL | NULL | NULL |
| main      | NULL         | 0       | NULL | NULL  | NULL | NULL | NULL |
| forum     | bangrak      | 0       | NULL | NULL  | NULL | NULL | NULL |
| topic     | food         | 2       | 668  | 35    | NULL | NULL | NULL |
| topic     | isolate      | 3       | 463  | 10645 | 472  | NULL | NULL |
| topic     | home         | 0       | NULL | NULL  | NULL | NULL | NULL |
| topic     | klaibann     | 0       | NULL | NULL  | NULL | NULL | NULL |
| topic     | food         | 0       | NULL | NULL  | NULL | NULL | NULL |
| topic     | camera       | 0       | NULL | NULL  | NULL | NULL | NULL |
| topic     | tvshow       | 2       | 183  | 530   | NULL | NULL | NULL |
| topic     | ratchada     | 0       | NULL | NULL  | NULL | NULL | NULL |
| topic     | rajdumnern   | 3       | 521  | 7440  | 62   | NULL | NULL |
| topic     | beauty       | 0       | NULL | NULL  | NULL | NULL | NULL |
| topic     | siam         | 2       | 599  | 6674  | NULL | NULL | NULL |
+-----------+--------------+---------+------+-------+------+------+------+
*/
