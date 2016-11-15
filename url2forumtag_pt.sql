/*
####################################################################################
# Name: url2forumtag_pt
# Description: Extract page type, forum and tags from Pantip's url and referrer_url
# Input: default.id_syncs
# Version:
#   2016/11/11 RS: Initial version
#   2016/11/15 RS: Additional page types, new code for tag_num
####################################################################################
*/

select page_type, case when referrer_url like '%/forum/%' then 		
				regexp_replace(split_part(regexp_replace(regexp_replace(regexp_replace(referrer_url,'.*forum/',''),'\\?.*',''),'\\&.*',''),'',1),'/.*','')
			when url like '%ta_cat=group%' then regexp_replace(regexp_replace(url,'.*ta_cat=group%3D',''),'%{1}.*','') end as forum, 
		tag_num, 
		case  when referrer_url like '%/tag/%' then regexp_replace(regexp_replace(regexp_replace(referrer_url,'.*tag/',''),'[\\?\\(].*',''),'_%?$','') 
			  when tag_num between 1 and 5 then regexp_replace(regexp_replace(url,'.*%26C1%3D',''),'%26C2.*','') 
		end as tag1, 
	    case when tag_num between 2 and 5 then regexp_replace(regexp_replace(url,'.*%26C2%3D',''),'%26C3.*','') end as tag2, 
	    case when tag_num between 3 and 5 then regexp_replace(regexp_replace(url,'.*%26C3%3D',''),'%26C4.*','') end as tag3,  
	    case when tag_num between 4 and 5 then regexp_replace(regexp_replace(url,'.*%26C4%3D',''),'%26C5.*','') end as tag4, 
	    case when tag_num =5 then regexp_replace(url,'.*%26C5%3D','') end as tag5 
		from 
(select a.header.url as url, case when a.header.referrer_url like '%tag%' then regexp_replace(a.header.referrer_url,'(\\(?%E0|à).*','') when a.header.referrer_url like '%topic%' then NULL else a.header.referrer_url end as referrer_url, 
case when a.header.referrer_url like '%pantip.com/tag%' then 'tag' 
		when a.header.referrer_url like '%pantip.com/forum%' then 'forum' 
		when a.header.referrer_url like '%pantip.com/topic%' then 'topic' 
		when (a.header.referrer_url ='http://pantip.com/' or a.header.referrer_url ='http://m.pantip.com/' or a.header.referrer_url like '%pantip.com/home%'
		or a.header.referrer_url like '%pantip.com/pick%' or a.header.referrer_url like '%pantip.com/trend%' or a.header.referrer_url like '%pantip.com/ourlove%' \*Tribute to the King*\
		) then 'home' 
		when a.header.referrer_url like '%pantip.com/profile/%' then 'profile' 
		when a.header.referrer_url like '%pantip.com/club%' then 'club' 
		when a.header.referrer_url like '%pantip.com/register%' then 'register' 
		when a.header.referrer_url like '%account%' or a.header.referrer_url like '%setting%' or a.header.referrer_url like '%login%' then 'account' 
		when a.header.referrer_url like '%pantip.com/about%' or a.header.referrer_url like '%pantip.com/activities%' or a.header.referrer_url like '%pantip.com/advertising%' then 'act-abt-ads' else 'others' end as page_type, 
case when regexp_replace(a.header.url,'.*(ta_cat=group)','') like '%\\%26C%' then cast(regexp_replace(regexp_replace(a.header.url,'.*(%26C)+',''),'%3D.*','') as double) else 0 end as tag_num from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id in (2243) and YEAR=2016 and MONTH=11 and c.key='TAPAD_COOKIE' ) A where url is not null  limit 20;

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

/* with referrer_url and url
+-----------+---------------+---------+------+------+------+------+------+---------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| page_type | forum         | tag_num | tag1 | tag2 | tag3 | tag4 | tag5 | referrer_url_raw                                                                                                    | url                                                                                                                                                                |
+-----------+---------------+---------+------+------+------+------+------+---------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| topic     | supachalasai  | 3       | 574  | 581  | 578  | NULL | NULL | http://m.pantip.com/topic/35764701?                                                                                 | /tapestry/1?ta_partner_id=2243&ta_partner_did=o0mphmqrr2RgZn71qnL&ta_format=png&ta_cat=group%3Dsupachalasai%26C1%3D574%26C2%3D581%26C3%3D578                       |
| topic     | ratchada      | 0       | NULL | NULL | NULL | NULL | NULL | http://pantip.com/topic/35308655                                                                                    | /tapestry/1?ta_partner_id=2243&ta_partner_did=o7bc221poMnLoN48kgG&ta_format=png&ta_cat=group%3Dratchada                                                            |
| topic     | mbk           | 0       | NULL | NULL | NULL | NULL | NULL | http://pantip.com/topic/32334000                                                                                    | /tapestry/1?ta_partner_id=2243&ta_partner_did=offzuchqumKds53ctMJ&ta_format=png&ta_cat=group%3Dmbk                                                                 |
| topic     | cartoon       | 5       | 6955 | 334  | 168  | 142  | 6948 | http://pantip.com/topic/35773639                                                                                    | /tapestry/1?ta_partner_id=2243&ta_partner_did=ntgtskvjvLNoG9Fzsg2&ta_format=png&ta_cat=group%3Dcartoon%26C1%3D6955%26C2%3D334%26C3%3D168%26C4%3D142%26C5%3D6948    |
| topic     | library       | 0       | NULL | NULL | NULL | NULL | NULL | http://pantip.com/topic/35735113                                                                                    | /tapestry/1?ta_partner_id=2243&ta_partner_did=mrxcck31lzt22HeKp1D&ta_format=png&ta_cat=group%3Dlibrary                                                             |
| topic     | chalermthai   | 5       | 202  | 143  | 162  | 166  | 1586 | http://m.pantip.com/topic/35769162?                                                                                 | /tapestry/1?ta_partner_id=2243&ta_partner_did=ofovwrcy7K4mhD3Npby&ta_format=png&ta_cat=group%3Dchalermthai%26C1%3D202%26C2%3D143%26C3%3D162%26C4%3D166%26C5%3D1586 |
| topic     | bangrak       | 0       | NULL | NULL | NULL | NULL | NULL | http://m.pantip.com/topic/35228547?                                                                                 | /tapestry/1?ta_partner_id=2243&ta_partner_did=nvurhraro845T2gZYyA&ta_format=png&ta_cat=group%3Dbangrak                                                             |
| topic     | siliconvalley | 0       | NULL | NULL | NULL | NULL | NULL | http://pantip.com/topic/31653176                                                                                    | /tapestry/1?ta_partner_id=2243&ta_partner_did=og4q7u7b8NEOVvvii8G&ta_format=png&ta_cat=group%3Dsiliconvalley                                                       |
| topic     | lumpini       | 0       | NULL | NULL | NULL | NULL | NULL | http://m.pantip.com/topic/30466861                                                                                  | /tapestry/1?ta_partner_id=2243&ta_partner_did=ofv3niigpzniy3sDI1w&ta_format=png&ta_cat=group%3Dlumpini                                                             |
| topic     | food          | 0       | NULL | NULL | NULL | NULL | NULL | http://m.pantip.com/topic/30359068?                                                                                 | /tapestry/1?ta_partner_id=2243&ta_partner_did=n29w91k3vr58Hy321Az&ta_format=png&ta_cat=group%3Dfood                                                                |
| topic     | beauty        | 0       | NULL | NULL | NULL | NULL | NULL | http://pantip.com/topic/32574336                                                                                    | /tapestry/1?ta_partner_id=2243&ta_partner_did=ofw4za1xkSsyMZ3L2fM&ta_format=png&ta_cat=group%3Dbeauty                                                              |
| topic     | home          | 4       | 7535 | 533  | 7536 | 535  | NULL | http://pantip.com/topic/35769838                                                                                    | /tapestry/1?ta_partner_id=2243&ta_partner_did=odsnksk51y5M9RNHZfv&ta_format=png&ta_cat=group%3Dhome%3Ftid%3D35770375%26C1%3D7535%26C2%3D533%26C3%3D7536%26C4%3D535 |
| topic     | food          | 0       | NULL | NULL | NULL | NULL | NULL | http://pantip.com/topic/30663822?utm_source=facebook&utm_medium=pantip_food&utm_content=preaw&utm_campaign=30663822 | /tapestry/1?ta_partner_id=2243&ta_partner_did=o71igwbg3r642YVG83W&ta_format=png&ta_cat=group%3Dfood                                                                |
| topic     | home          | 2       | 236  | 239  | NULL | NULL | NULL | http://m.pantip.com/topic/35766159                                                                                  | /tapestry/1?ta_partner_id=2243&ta_partner_did=nnw2zqckLLT0UB0dWT&ta_format=png&ta_cat=group%3Dhome%26C1%3D236%26C2%3D239                                           |
| topic     | chalermthai   | 3       | 162  | 173  | 166  | NULL | NULL | http://m.pantip.com/topic/35764723?                                                                                 | /tapestry/1?ta_partner_id=2243&ta_partner_did=nq781oht3CTTHOJNPsi&ta_format=png&ta_cat=group%3Dchalermthai%26C1%3D162%26C2%3D173%26C3%3D166                        |
| topic     | beauty        | 0       | NULL | NULL | NULL | NULL | NULL | http://m.pantip.com/topic/30944234?                                                                                 | /tapestry/1?ta_partner_id=2243&ta_partner_did=o8g46mn1uefBPtG64Pb&ta_format=png&ta_cat=group%3Dbeauty                                                              |
| topic     | isolate       | 1       | 9382 | NULL | NULL | NULL | NULL | http://pantip.com/topic/35770143                                                                                    | /tapestry/1?ta_partner_id=2243&ta_partner_did=odu4e85re2ul13qk3Lx&ta_format=png&ta_cat=group%3Disolate%26C1%3D9382                                                 |
| topic     | beauty        | 0       | NULL | NULL | NULL | NULL | NULL | http://pantip.com/topic/31202334                                                                                    | /tapestry/1?ta_partner_id=2243&ta_partner_did=n93vx2g6nASuBOBU6Gy&ta_format=png&ta_cat=group%3Dbeauty                                                              |
| topic     | family        | 0       | NULL | NULL | NULL | NULL | NULL | http://pantip.com/topic/31104895                                                                                    | /tapestry/1?ta_partner_id=2243&ta_partner_did=nm4px0e1m20vJvQzW3d&ta_format=png&ta_cat=group%3Dfamily                                                              |
| topic     | siam          | 0       | NULL | NULL | NULL | NULL | NULL | http://pantip.com/topic/34230622                                                                                    | /tapestry/1?ta_partner_id=2243&ta_partner_did=ofpko7c8o6iqJ5gboiD&ta_format=png&ta_cat=group%3Dsiam                                                                |
+-----------+---------------+---------+------+------+------+------+------+---------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+

*/
