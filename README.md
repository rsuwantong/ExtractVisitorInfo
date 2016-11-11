# ExtractVisitorInfo
SQL (Impala) scripts for extracting information on a partner website's visitors such as carrier, device, room, forum, tags 

Use databytxn_pt.sql to create a table like: 

+------------+----------------+-------------+---------+---------+-----------------+-----------+---------------+---------+------------------------------------------+--------------------+-----------------+-------------------+--------------------+
| sight_date | platform       | hl_platform | carrier | channel | device_techname | page_type | forum         | tag_num | tag1                                     | tag2               | tag3            | tag4              | tag5               |
+------------+----------------+-------------+---------+---------+-----------------+-----------+---------------+---------+------------------------------------------+--------------------+-----------------+-------------------+--------------------+
| 2016-11-07 | COMPUTER       | PC_OTHERS   | Wi-Fi   | Pantip  | COMPUTER        | topic     | tvshow        | 0       | NULL                                     | NULL               | NULL            | NULL              | NULL               |
| 2016-11-07 | ANDROID        | ANDROID     | DTAC    | Pantip  | r7plusf         | topic     | supachalasai  | 2       | กีฬา                                     | วอลเลย์บอล         | NULL            | NULL              | NULL               |
| 2016-11-07 | COMPUTER       | PC_OTHERS   | DTAC    | Pantip  | COMPUTER        | topic     | mbk           | 5       | 4G                                       | Mobile Operator    | truemove H      | dtac              | AIS                |
| 2016-11-07 | COMPUTER       | PC_OTHERS   | Wi-Fi   | Pantip  | COMPUTER        | topic     | tvshow        | 4       | อัครณัฐ อริยฤทธิ์วิกุล (น๊อต)              | นักแสดง            | สถานีโทรทัศน์   | รายการข่าว         | NULL               |
------------+----------------+-------------+---------+---------+-----------------+-----------+---------------+---------+------------------------------------------+--------------------+-----------------+-------------------+--------------------+
