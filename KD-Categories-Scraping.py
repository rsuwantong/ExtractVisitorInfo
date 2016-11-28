# -*- coding: utf-8 -*-

#########################################################
# Kaidee (Main) Categories Scraping
#########################################################
# Version Control:
#  11/29/2016 SN - Initial Version
#########################################################

import time
import datetime
from lxml import html
import urllib.request
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import re

base_url = 'https://www.kaidee.com/'
cat_url = base_url + 'categories'

# Universal Function: Get status
def read_url(url):
    request = urllib.request.Request(url)
    req_ind = False
    attempt = 0
    while req_ind is False:
        attempt+=1
        try:
            print("{}: Try openning the URL: {}".format(datetime.datetime.now(),url))
            response = urlopen(request)
            if response.getcode() == 200:
                req_ind = True
                html_resp = response.read().decode('utf-8')
                print("{}: Successfully reading: {}".format(datetime.datetime.now(), url))            
        except Exception:
            print("{}: Error connect to {}".format(datetime.datetime.now(), url))
            if attempt == 10:
                print("{}: Error {} may not be valid URL. End attemp {}!".format(datetime.datetime.now(), url, attempt))
                break
            time.sleep(5) # Wait 5 seconds to reconnect
    return html_resp

page = read_url(cat_url)
soup = BeautifulSoup(page, "html.parser")

# Version 2.0 
# Objective is to get all category IDs with thier associated names
staging = soup.find("ul", class_="all-categories clear").contents
# Store as list, can refer by indexing... starting from 0 to len(staging) - 1

# Initialize list
main_cat = []
i = 0
for data in staging:
    i+=1
    try:
        tmp = BeautifulSoup(str(data), "lxml")
        url_room_name = tmp.find('a').get('href')
        url_room_name = url_room_name.strip("/")        
        room_id = url_room_name.split('-')[0]
        room_id = int(''.join(map(str,re.findall("\d+", room_id))))       
        eng_room_name = url_room_name.split('-')[1]
        thai_room_name = tmp.find('span').contents
        thai_room_name = str(thai_room_name).strip("['']")
        print("{}: {}".format(i, url_room_name))
        print("{}: {}, {}".format(i, room_id, eng_room_name))
        print("{}: {}".format(i, str(thai_room_name)))
        main_cat.append((room_id, eng_room_name, thai_room_name, url_room_name))
    except Exception:
        print("skip")

main_cat_df = pd.DataFrame(main_cat)
main_cat_df.columns = ['room_id', 'en_room_name', 'th_room_name', 'url_room_name']
main_cat_df.to_csv("<Your save path>/Kaidee_room.csv", sep = ',', header = True)

# Version 1.0 - initial version
#for link in soup.find_all('a'):
#    print(link.get('href'))


########################################
         PARTIAL CONSOLE OUTPUT
########################################
2016-11-29 01:50:43.184677: Try openning the URL: https://www.kaidee.com/categories
2016-11-29 01:50:43.464693: Successfully reading: https://www.kaidee.com/categories
1: c29-phone_device
1: 29, phone_device
1: มือถือ แท็บเล็ต
2: c27-computer
2: 27, computer
2: คอมพิวเตอร์
... etc ...

########################################
          Dataframe output
########################################
     room_id           en_room_name             th_room_name  \
0        29           phone_device          มือถือ แท็บเล็ต   
1        27               computer              คอมพิวเตอร์   
2        60                  music             เครื่องดนตรี   
3        61                  sport                     กีฬา   
4       123               bicycles                  จักรยาน   
5       130            mom_and_kid               แม่และเด็ก   
6        95                    bag                  กระเป๋า   
7        99                  watch                   นาฬิกา   
8        96                  shoes                  รองเท้า   
9         5                fashion  เสื้อผ้า เครื่องแต่งกาย   
10        6          beauty_health         สุขภาพและความงาม   
11        3  appliances_decoration               บ้านและสวน   
12       57                 amulet               พระเครื่อง   
13      103             collection                  ของสะสม   
14       28     camera_accessories                    กล้อง   
15       70               electric          เครื่องใช้ไฟฟ้า   
16       31                   game                    เกมส์   
17       62                    pet              สัตว์เลี้ยง   
18        2             realestate          อสังหาริมทรัพย์   
19       11                   auto                 รถมือสอง   
20      270              auto_part      อะไหล่รถ ประดับยนต์   
21      149             motorcycle              มอเตอร์ไซค์   
22       10              lifestyle                งานอดิเรก   
23        7   job_business_service                   ธุรกิจ   
24       45       business_service                   บริการ   
25        9                 travel               ท่องเที่ยว   
26        8              education                 การศึกษา   
27      283               donation                  แบ่งปัน   

               url_room_name  
0           c29-phone_device  
1               c27-computer  
2                  c60-music  
3                  c61-sport  
4              c123-bicycles  
5           c130-mom_and_kid  
6                    c95-bag  
7                  c99-watch  
8                  c96-shoes  
9                 c5-fashion  
10          c6-beauty_health  
11  c3-appliances_decoration  
12                c57-amulet  
13           c103-collection  
14    c28-camera_accessories  
15              c70-electric  
16                  c31-game  
17                   c62-pet  
18             c2-realestate  
19              c11-auto-car  
20            c270-auto_part  
21           c149-motorcycle  
22             c10-lifestyle  
23   c7-job_business_service  
24      c45-business_service  
25                 c9-travel  
26              c8-education  
27             c283-donation  
