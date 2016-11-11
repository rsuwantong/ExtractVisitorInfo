# coding: utf-8

# In[7]:

####################################################################################
# Name: get_tagapi_pt
# Description: Get pantip tags from API
# Version:
#   2016/11/11 RS: Initial version
####################################################################################

from urllib.request import urlopen
import json
from pprint import pprint
import pandas as pd
import numpy as np
import requests


response = requests.get("https://service.pantip.com/api/get_all_tags?access_token=4c042fa1ba597f5da5ace8e3ce1f09726fcfe83f")
txt = response.text
data = json.loads(txt)
print(data)
dt = pd.DataFrame(data)
dt.to_csv('tagid2tagname_pt'+'.csv', encoding='utf-8', index=False)
