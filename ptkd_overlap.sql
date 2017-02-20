/*
####################################################################################
# Name: 08ptkd_overlap
# Description: Count the number of the overlapped distinct tapad id on kaidee and pantip (monthly)
# Input: Tables pt_tpidlist_byday_carrier_device_0925_2410, kd_tpidlist_byday_carrier_device_0925_2410
# Version:
#   2016/10/20 RS: Initial version
#   
####################################################################################
*/

select count(A.tapad_id) from (select distinct tapad_id from rata.pt_tpidlist_byday_carrier_device_0925_2410 where platform ='IPHONE') A join (select distinct tapad_id from rata.kd_tpidlist_byday_carrier_device_0925_2410 where platform ='IPHONE' ) B on A.tapad_id=B.tapad_id ;

select count(distinct tapad_id) from rata.pt_tpidlist_byday_carrier_device_0925_2410 where platform ='IPHONE';
select count(distinct tapad_id) from rata.kd_tpidlist_byday_carrier_device_0925_2410 where platform ='IPHONE';

select count(A.tapad_id) from (select distinct tapad_id from rata.pt_tpidlist_byday_carrier_device_0925_2410 where platform !='IPHONE') A join (select distinct tapad_id from rata.kd_tpidlist_byday_carrier_device_0925_2410 where platform !='IPHONE' ) B on A.tapad_id=B.tapad_id ;

select count(distinct tapad_id) from rata.pt_tpidlist_byday_carrier_device_0925_2410 where platform !='IPHONE';
select count(distinct tapad_id) from rata.kd_tpidlist_byday_carrier_device_0925_2410 where platform !='IPHONE';


