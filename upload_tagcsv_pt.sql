/*
####################################################################################
# Name: upload_tagcsv_pt
# Description: Create an Impala table of Pantip's tagid2tagname from the uploaded csv file
# Input: csv table in the folder /user/rata.suwantong/impala_rata2/utilities/tagid2tagname_pt
# Version:
#   2016/11/11 RS: Initial version
#   
####################################################################################
*/
drop table if exists rata_util.tagid2tagname_pt_pre;
CREATE EXTERNAL TABLE rata_util.tagid2tagname_pt_pre
 (
	tag_id DOUBLE,
    tag_name STRING
 )   
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ESCAPED BY ','
 LOCATION '/user/rata.suwantong/impala_rata2/utilities/tagid2tagname_pt';
 
drop table if exists rata_util.tagid2tagname_pt;
create table rata_util.tagid2tagname_pt row format delimited fields terminated by '\t' as ( select * 
from rata_util.tagid2tagname_pt_pre where tag_name!='name');

select * from rata_util.tagid2tagname_pt limit 20;

/*

+--------+-----------------+
| tag_id | tag_name        |
+--------+-----------------+
| 6822   | Hewlett-Packard |
| 7599   | สามี (ละคร)     |
| 7602   | อีสา รวีช่วงโชติ |
| 7603   | คมพยาบาท        |
| 7604   | ผู้ชนะสิบทิศ      |
| 7605   | Britney Spears  |
| 7607   | เกมปริศนา       |
| 7608   | เกมฝึกสมอง      |
| 7609   | เกมลับสมอง      |
| 7610   | เกมประลองปัญญา  |
| 7611   | เกมทดสอบ IQ     |
| 7612   | เกมชวนคิด       |
| 7613   | ปัญหาเชาวน์     |
| 7614   | บีช ซอคเกอร์    |
| 7615   | ขี่ม้า            |
| 7616   | ลีลาศ           |
| 7617   | เรือใบ          |
| 7620   | น้ำตกทีลอซู       |
| 7621   | ฮาวาย           |
| 7622   | กระทะ           |
+--------+-----------------+

Thai language can be used for making a query.

Query: select * from rata_util.tagid2tagname_pt where tag_name ='ลีลาศ'
+--------+----------+
| tag_id | tag_name |
+--------+----------+
| 7616   | ลีลาศ    |
+--------+----------+

*/
