
drop table if exists rata_sght.pt_cnt_dtacvertical;
create table rata_sght.pt_cnt_dtacvertical row format delimited fields terminated by '\t' as (
select hl_platform
, count(distinct tapad_id) as uniq_cnt
, count(*) as cnt
from (
select tapad_id
, case
when platform in ("ANDROID_TABLET", "ANDROID") then 'Android'
when platform IN ("IPAD", "IPHONE") then 'iOS'
when platform IN ("COMPUTER") then 'Computer'
else 'Other' end as hl_platform
from apollo.dtac_vertical_dataset
where url REGEXP '.*ta_partner_id=2243.*' and year=2016 and month=12
)totals
group by 1
order by 1);

/*
+-------------+----------+----------+
| hl_platform | uniq_cnt | cnt      |
+-------------+----------+----------+
| Android     | 1906788  | 51541559 |
| Computer    | 2596770  | 62066465 |
| Other       | 11631    | 249804   |
| iOS         | 5871869  | 23994609 |
+-------------+----------+----------+

*/

drop table if exists rata_sght.pt_cnt_idsync;
create table rata_sght.pt_cnt_idsync row format delimited fields terminated by '\t' as (
select hl_platform
, count(distinct tapad_id) as uniq_cnt
, count(*) as cnt
from (select tapad_id, 
			case when platform in ("ANDROID_TABLET", "ANDROID") then 'Android'
				 when platform IN ("IPAD", "IPHONE") then 'iOS'
				when platform IN ("COMPUTER") then 'Computer'
			else 'Other' end as hl_platform 

		from ( select b.value as tapad_id,  a.header.platform as platform from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id =2243 and YEAR=2016 and MONTH=12  and c.key='TAPAD_COOKIE' ) A
	) B 
group by 1
order by 1);

/*


*/

select a.hl_platform, a.uniq_cnt as uniq_cnt_vertical, b.uniq_cnt as uniq_cnt_idsync, a.cnt as cnt_vertical, b.cnt as cnt_idsync, b.uniq_cnt/a.uniq_cnt as uniq_cnt_ratio, b.cnt/a.cnt as cnt_ratio from 
	(select hl_platform
, count(distinct tapad_id) as uniq_cnt
, count(*) as cnt
from 
	(select tapad_id
	, case
	when platform in ("ANDROID_TABLET", "ANDROID") then 'Android'
	when platform IN ("IPAD", "IPHONE") then 'iOS'
	when platform IN ("COMPUTER") then 'Computer'
	else 'Other' end as hl_platform
	from apollo.dtac_vertical_dataset
	where url REGEXP '.*ta_partner_id=2243.*' and year=2016 and month=12
	)totals
	group by 1
	order by 1) A 
join 
	(select hl_platform
	, count(distinct tapad_id) as uniq_cnt
	, count(*) as cnt
	from (select tapad_id, 
				case when platform in ("ANDROID_TABLET", "ANDROID") then 'Android'
					 when platform IN ("IPAD", "IPHONE") then 'iOS'
					when platform IN ("COMPUTER") then 'Computer'
				else 'Other' end as hl_platform 

			from ( select b.value as tapad_id,  a.header.platform as platform from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id =2243 and YEAR=2016 and MONTH=12  and c.key='TAPAD_COOKIE' ) C
		) D
	group by 1
	order by 1) B 
on a.hl_platform = b.hl_platform order by 1;
