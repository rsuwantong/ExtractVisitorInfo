
drop table if exists rata_sght.pt_cnt_dtacvertical;
create table rata_sght.pt_cnt_dtacvertical row format delimited fields terminated by '\t' as (
select carrier
, hl_platform
, count(distinct tapad_id) as uniq_cnt
, count(*) as cnt
from (
select carrier
, tapad_id
, case
when platform in ("ANDROID_TABLET", "ANDROID") then 'Android'
when platform IN ("IPAD", "IPHONE") then 'iOS'
when platform IN ("COMPUTER") then 'Computer'
else 'Other' end as hl_platform
from apollo.dtac_vertical_dataset
where url REGEXP '.*ta_partner_id=2243.*' and year=2016 and month=12
)totals
group by 1,2
order by 1,2);

/*
+-----------+-------------+----------+----------+
| carrier   | hl_platform | uniq_cnt | cnt      |
+-----------+-------------+----------+----------+
| AIS       | Android     | 569722   | 12594967 |
| AIS       | Computer    | 82749    | 1345127  |
| AIS       | Other       | 1881     | 49970    |
| AIS       | iOS         | 1259703  | 5036488  |
| DTAC      | Android     | 411747   | 9933921  |
| DTAC      | Computer    | 58258    | 960623   |
| DTAC      | Other       | 1250     | 41485    |
| DTAC      | iOS         | 1166048  | 4604159  |
| TOT       | Android     | 27945    | 321862   |
| TOT       | Computer    | 62502    | 711899   |
| TOT       | Other       | 164      | 2028     |
| TOT       | iOS         | 38111    | 133042   |
| True Move | Android     | 413218   | 9309917  |
| True Move | Computer    | 67636    | 1082806  |
| True Move | Other       | 942      | 18376    |
| True Move | iOS         | 1254383  | 4809795  |
| WIFI      | Android     | 873491   | 19380892 |
| WIFI      | Computer    | 2418548  | 57966010 |
| WIFI      | Other       | 8185     | 137945   |
| WIFI      | iOS         | 2298753  | 9411125  |
+-----------+-------------+----------+----------+
*/

drop table if exists rata_sght.pt_cnt_idsync;
create table rata_sght.pt_cnt_idsync row format delimited fields terminated by '\t' as (
select carrier
, hl_platform
, count(distinct tapad_id) as uniq_cnt
, count(*) as cnt
from (select tapad_id, 
			case when platform in ("ANDROID_TABLET", "ANDROID") then 'Android'
				 when platform IN ("IPAD", "IPHONE") then 'iOS'
				when platform IN ("COMPUTER") then 'Computer'
			else 'Other' end as hl_platform , 
			case when ip_number between 18087936 and 18153471 then 'TOT' 
				when ip_number between 19791872 and 19922943 then 'DTAC' 
				when ip_number between 456589312 and 456654847 then  'TMH' 
				when ip_number between 837156864 and 837222399 then  'AIS' 
				when ip_number between 837615616 and 837681151 then  'True Move' 
				when ip_number between 1848705024 and 1848770559 then  'AIS' 
				when ip_number between 1867776000 and 1867825151 then  'DTAC' 
				when ip_number between 1867826176 and 1867841535 then  'DTAC' 
				when ip_number between 1933770752 and 1933836287 then  'DTAC' 
				when ip_number between 1998520320 and 1998553087 then  'AIS' 
				when ip_number between 2523597824 and 2523598847 then  'TOT' 
				when ip_number between 3033972736 and 3033980927 then  'True Move' 
				when ip_number between 3068657664 and 3068723199 then  'AIS' 
				when ip_number between 3398768640 and 3398769663 then  'AIS' 
				when ip_number between 3415276128 and 3415276159 then  'True Move' 
				when ip_number between 3742892032 and 3742957567 then  'True Move' 
			else 'WIFI' end as carrier 

		from ( select tapad_id,  platform, cast(split_part(ip_address,'.',1) as INT)*16777216 + cast(split_part(ip_address,'.',2) as INT)*65536 + cast(split_part(ip_address,'.',3) as INT)*256+ cast(split_part(ip_address,'.',4) as INT) ip_number from ( select e.* from (select b.value as tapad_id,  a.header.platform as platform, a.header.ip_address as ip_address from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id =2243 and YEAR=2016 and MONTH=12  and c.key='TAPAD_COOKIE') E join (select ip_string as ip_address from dga.semcasting_data where country ='THA' ) F where E.ip_address = F.ip_address ) I  ) G
	) H 
group by 1,2
order by 1,2);

select * from rata_sght.pt_cnt_idsync order by carrier, hl_platform;

/*

+-----------+-------------+----------+-----------+
| carrier   | hl_platform | uniq_cnt | cnt       |
+-----------+-------------+----------+-----------+
| AIS       | Android     | 1118800  | 23114822  |
| AIS       | Computer    | 141095   | 2268634   |
| AIS       | Other       | 3669     | 89080     |
| AIS       | iOS         | 2275572  | 9006990   |
| DTAC      | Android     | 879292   | 21130764  |
| DTAC      | Computer    | 122923   | 2052202   |
| DTAC      | Other       | 2640     | 91112     |
| DTAC      | iOS         | 2494087  | 9809758   |
| TMH       | Android     | 531973   | 7525014   |
| TMH       | Computer    | 71025    | 886164    |
| TMH       | Other       | 1171     | 18622     |
| TMH       | iOS         | 1093280  | 3932570   |
| TOT       | Android     | 59384    | 670862    |
| TOT       | Computer    | 132960   | 1527894   |
| TOT       | Other       | 364      | 4064      |
| TOT       | iOS         | 81806    | 285566    |
| True Move | Android     | 700887   | 12350522  |
| True Move | Computer    | 103898   | 1446238   |
| True Move | Other       | 1540     | 30604     |
| True Move | iOS         | 1704416  | 6317214   |
| WIFI      | Android     | 3146554  | 74651692  |
| WIFI      | Computer    | 7576890  | 192481808 |
| WIFI      | Other       | 30830    | 465886    |
| WIFI      | iOS         | 9660918  | 38072168  |
+-----------+-------------+----------+-----------+


*/

select a.carrier, a.hl_platform, a.uniq_cnt as uniq_cnt_vertical, b.uniq_cnt as uniq_cnt_idsync, a.cnt as cnt_vertical, b.cnt as cnt_idsync, b.uniq_cnt/a.uniq_cnt as uniq_cnt_ratio, b.cnt/a.cnt as cnt_ratio from rata_sght.pt_cnt_dtacvertical A join rata_sght.pt_cnt_idsync B on a.carrier=b.carrier and a.hl_platform = b.hl_platform order by 1, 2;
