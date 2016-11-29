# -*- coding: utf-8 -*-

#########################################################
# Kaidee (Main) Categories Scraping
#########################################################
# Version Control:
#  11/29/2016 SN - Initial Version
#  11/29/2016 SN - Update to get sub-cat within each main category (this covers 99%)
#########################################################

import time
import datetime
from lxml import html
import urllib.request
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import re

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


####################################################################################
# Part 1 - Read the main category from category page
####################################################################################
# Version 2.0 
# Objective is to get all category IDs with thier associated names
####################################################################################
base_url = 'https://www.kaidee.com/'
cat_url = base_url + 'categories'
page = read_url(cat_url)
soup = BeautifulSoup(page, "html.parser")
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
        print("{}: Reading {} {}".format(datetime.datetime.now(), url_room_name, thai_room_name))
        main_cat.append((room_id, eng_room_name, thai_room_name, url_room_name))
    except Exception:
        print("skip")

main_cat_df = pd.DataFrame(main_cat)
col_nm = ['room_id', 'en_room_name', 'th_room_name', 'url_room_name']
main_cat_df.columns = col_nm

####################################################################################
# Part 2 - Loop through the list to read each main room
####################################################################################
main_room_lst = main_cat_df.values.T.tolist()[3]
for room in main_room_lst:
    main_cat_url = base_url + room
    print("\n{}: Set URL: {}".format(datetime.datetime.now(), main_cat_url))
    page_data = read_url(main_cat_url)
    soup = BeautifulSoup(page_data, "html.parser") 
    sub_staging = soup.find("div", class_='categories-list facet-list').contents

    for sub_cat in sub_staging:
        try:
            sub_soup = BeautifulSoup(str(sub_staging), "lxml")
            for sub in sub_soup.find_all('a'):
                url_room_name = sub.get('href')
                url_room_name = url_room_name.strip("/")
                room_id = url_room_name.split('-')[0]
                room_id = int(''.join(map(str, re.findall("\d+", room_id))))
                eng_room_name = url_room_name.split('-')[2]
                thai_room_name = sub.contents
                thai_room_name = str(thai_room_name).strip("['']")
                print("{}: Reading {} {}".format(datetime.datetime.now(), url_room_name, thai_room_name))
                main_cat.append((room_id, eng_room_name, thai_room_name, url_room_name))
        except Exception:
            print("Error")

main_cat_df = pd.DataFrame(main_cat)
col_nm = ['room_id', 'en_room_name', 'th_room_name', 'url_room_name']
main_cat_df.columns = col_nm

print("########### SAMPLE OUTPUT ###########")
print(main_cat_df.head(2))
print(main_cat_df.tail(2))
############################################################################
main_cat_df.to_csv("<Your Directory>/Kaidee_room.csv", sep = ',', header = True, columns=col_nm)


########################################
         PARTIAL CONSOLE OUTPUT
########################################
2016-11-29 15:13:40.225636: Try openning the URL: https://www.kaidee.com/categories
2016-11-29 15:13:40.332636: Successfully reading: https://www.kaidee.com/categories
2016-11-29 15:13:40.371636: Reading c29-phone_device มือถือ แท็บเล็ต
2016-11-29 15:13:40.372636: Reading c27-computer คอมพิวเตอร์
2016-11-29 15:13:40.373636: Reading c60-music เครื่องดนตรี
2016-11-29 15:13:40.374636: Reading c61-sport กีฬา
2016-11-29 15:13:40.374636: Reading c123-bicycles จักรยาน
2016-11-29 15:13:40.375636: Reading c130-mom_and_kid แม่และเด็ก
2016-11-29 15:13:40.375636: Reading c95-bag กระเป๋า
2016-11-29 15:13:40.376636: Reading c99-watch นาฬิกา
2016-11-29 15:13:40.377636: Reading c96-shoes รองเท้า
2016-11-29 15:13:40.377636: Reading c5-fashion เสื้อผ้า เครื่องแต่งกาย
2016-11-29 15:13:40.378636: Reading c6-beauty_health สุขภาพและความงาม
2016-11-29 15:13:40.380636: Reading c3-appliances_decoration บ้านและสวน
2016-11-29 15:13:40.380636: Reading c57-amulet พระเครื่อง
...
2016-11-29 15:13:41.612636: Set URL: https://www.kaidee.com/c61-sport
2016-11-29 15:13:41.612636: Try openning the URL: https://www.kaidee.com/c61-sport
2016-11-29 15:13:42.088636: Successfully reading: https://www.kaidee.com/c61-sport
2016-11-29 15:13:42.140636: Reading c216-sport-exercise_ machine เครื่องออกกำลังกาย
2016-11-29 15:13:42.140636: Reading c217-sport-sport_equipment อุปกรณ์กีฬา
2016-11-29 15:13:42.140636: Reading c218-sport-sportwear ชุดกีฬา
2016-11-29 15:13:42.140636: Reading c219-sport-sportshoes รองเท้ากีฬา
            
########### SAMPLE OUTPUT ###########
   room_id  en_room_name     th_room_name     url_room_name
0       29  phone_device  มือถือ แท็บเล็ต  c29-phone_device
1       27      computer      คอมพิวเตอร์      c27-computer
     room_id    en_room_name  th_room_name                 url_room_name
253      286       furniture  เฟอร์นิเจอร์       c286-donation-furniture
254      287  other_donation        อื่น ๆ  c287-donation-other_donation
