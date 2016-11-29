/**************************************************************************/
/* Objective: Upload Room categories master file to Impala database
/* Version Control (MM/DD/YYYY):
/*   11/29/2016 SN - Initial release
/**************************************************************************/
/* Step 1:
/* Need to upload the master csv from local to hdfs
/**************************************************************************/

<SHELL MODE>
[satsawat.natakarnkit@XXXXX ~]$ hdfs dfs -ls /user/satsawat.natakarnkit
Found 1 items
drwx------   - satsawat.natakarnkit satsawat.natakarnkit          0 2016-11-09 13:43 /user/satsawat.natakarnkit/.staging

hdfs dfs -mkdir -p /user/satsawat.natakarnkit/master_kaidee_room
[satsawat.natakarnkit@XXXXX ~]$ ll
total 7850252
-rw-r--r-- 1 satsawat.natakarnkit satsawat.natakarnkit      13898 Nov 29 08:27 dim_kd_rm.csv
-rwxr-xr-x 1 satsawat.natakarnkit satsawat.natakarnkit       7578 Nov 23 09:22 load_tpid_sight.sh
-rwxr-xr-x 1 satsawat.natakarnkit satsawat.natakarnkit        357 Nov 10 08:18 script.sh

[satsawat.natakarnkit@XXXXX ~]$ hdfs dfs -put dim_kd_rm.csv /user/satsawat.natakarnkit/master_kaidee_room
[satsawat.natakarnkit@XXXXX ~]$ hdfs dfs -ls /user/satsawat.natakarnkit/master_kaidee_room
Found 1 items
-rw-r--r--   3 satsawat.natakarnkit satsawat.natakarnkit      13898 2016-11-29 08:40 /user/satsawat.natakarnkit/master_kaidee_room/dim_kd_rm.csv

/**************************************************************************/
/* Step 2:
/* Create Impala table refers to the master csv file in hdfs
/**************************************************************************/

<Impala Mode>
drop table if exists net.stg_dim_kaidee_room;
create external table net.stg_dim_kaidee_room
(
 room_id INT,
 room_nm_en STRING,
 room_nm_th STRING,
 room_nm_full STRING
) row format delimited fields terminated by ','
location '/user/satsawat.natakarnkit/master_kaidee_room';

drop table if exists net.dim_kaidee_room;
create table net.dim_kaidee_room row format delimited fields terminated by '\t' as ( select * 
from net.stg_dim_kaidee_room where room_id is not null);

/**** OUTPUT *****/
Query submitted at: 2016-11-29 10:05:32 (Coordinator: None)
Query progress can be monitored at: None/query_plan?query_id=5e4994fb1438d0a7:3a5d75f0c33dffaf
+---------------------+
| summary             |
+---------------------+
| Inserted 255 row(s) |
+---------------------+

/* TABLE Structure */
+---------+--------------------------------+----------------------------+-----------------------------------------------------------+
| room_id | room_nm_en                     | room_nm_th                 | room_nm_full                                              |
+---------+--------------------------------+----------------------------+-----------------------------------------------------------+
| 29      | phone_device                   | มือถือ แท็บเล็ต            | c29-phone_device                                          |
| 27      | computer                       | คอมพิวเตอร์                | c27-computer                                              |
| 60      | music                          | เครื่องดนตรี                | c60-music                                                 |
| 61      | sport                          | กีฬา                       | c61-sport                                                 |
| 123     | bicycles                       | จักรยาน                    | c123-bicycles                                             |
...
...
| 286     | furniture                      | เฟอร์นิเจอร์               | c286-donation-furniture                                   |
| 287     | other_donation                 | อื่น ๆ                      | c287-donation-other_donation                              |
+---------+--------------------------------+----------------------------+-----------------------------------------------------------+


