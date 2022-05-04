#!/usr/bin/env python
# coding: utf-8

# In[53]:


import pandas as pd
import numpy as np
import pymysql
import datetime as dt
import os
import gspread

from googleapiclient.discovery  import build
from google.oauth2 import service_account


# In[54]:


# cwd = os.getcwd() # To know current working directory.


# In[55]:


conn = pymysql.connections.Connection(host="<IP>",user='<username>',password='<password>',db='<DB name>', port=<port number>)


# In[56]:


query = """
         SELECT id,reg_number,Ins_certification_status,cast(ce_start_date AS CHAR) 'ce_start_date',
CONCAT('GCD',client_reference_id+1000)'GCD_Code',Allocated_Store,ce_comments,Backend_comments,Last_Cert_Date,Certification_Type
 FROM (
SELECT ucc.reg_number,ucc.id,ucc.ref_id,Case when ucc.certification_status='0' then 'Not Certified' 
when ucc.certification_status='1'  then  'Certified'
when ucc.certification_status='2' then  'Pending'
when ucc.certification_status='3' then  'Dealer Request'
when ucc.certification_status='4' then  'Needs Refurbishment'
when ucc.certification_status='5' then  'In-Process'
when ucc.certification_status='6' then  'Rejected'
when ucc.certification_status='7' then  'Expired'
when ucc.certification_status='8' then  'UnAvailable' END 'Ins_certification_status',ucc.ce_start_date,ucc.ce_end_date,ucc.last_updated_date,ucc.date_certified,co.client_reference_id,co.name 'Allocated_Store',cec.ce_comments,bec.Backend_comments,
cast(  DATE(ucc.ce_start_date) as char) AS 'Last_Cert_Date',
case when cvv.certification_type='0' then 'TM' 
when cvv.certification_type='1' then 'Value+' END 'Certification_Type'
  FROM evaluation.cm_vehicle_certification ucc
LEFT JOIN (SELECT id,NAME,client_reference_id FROM cm_owner) co ON ucc.owner_id=co.id

left join cm_vehicle_certification_cust_details_ncd cvv ON cvv.vcc_id=ucc.id

LEFT JOIN (SELECT Q1.vcc_id,concat(ifnull(Q1.comment,''),' ',ifnull(Q2.comment,''))'ce_comments' from
(SELECT a.vcc_id, a.`comment` FROM cm_vehicle_certification_status_logs a WHERE 
 a.comment_for='ce_comments' AND a.`comment`!=''
and a.date_added>'2021-01-01'
  GROUP BY 1)Q1

LEFT JOIN (SELECT a.vcc_id, a.`comment` FROM cm_vehicle_certification_status_logs a WHERE 
 a.comment_for='ce_rejection_reasons' AND a.`comment`!='' and a.date_added>'2021-01-01'  GROUP BY 1)Q2
ON Q1.vcc_id=Q2.vcc_id
GROUP BY 1)cec ON cec.vcc_id=ucc.id

LEFT JOIN (SELECT Q1.vcc_id,concat(ifnull(Q1.comment,''),' ',ifnull(Q2.comment,''))'Backend_comments' from
(SELECT a.vcc_id, a.`comment` FROM cm_vehicle_certification_status_logs a WHERE 
 a.comment_for='admin_comments' AND a.`comment`!=''
and a.date_added>'2021-01-01'
  GROUP BY 1)Q1

LEFT JOIN (SELECT a.vcc_id, a.`comment` FROM cm_vehicle_certification_status_logs a WHERE 
 a.comment_for='admin_rej_reasons' AND a.`comment`!='' and a.date_added>'2021-01-01'  GROUP BY 1)Q2
ON Q1.vcc_id=Q2.vcc_id
GROUP BY 1)bec ON bec.vcc_id=ucc.id

 WHERE ucc.id IN (SELECT a.id FROM (SELECT a.reg_number,MAX(a.id)'id' FROM cm_vehicle_certification a WHERE a.client_id='1' AND a.product_id=1 AND a.ce_start_date>='2020-01-01' GROUP BY 1)a) 
 GROUP  BY ucc.id ORDER BY ucc.id desc
 )k


"""


# In[57]:


cursor = conn.cursor()
cursor.execute(query)


# In[58]:


record = cursor.fetchall()


# In[59]:


#record


# In[60]:


columns = [col[0] for col in cursor.description]


# In[61]:


#columns


# In[62]:


# record


# In[63]:


df = pd.DataFrame(record, columns = columns)


# In[64]:


# df


# In[65]:


SERVICE_ACCOUNT_FILE = r'/home/saloni/Python_Scripts/automationteam-340909-010ed1988360.json'

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds= None

creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE,scopes =SCOPES)

SAMPLE_SPREADSHEET_ID = '1U9Y8UGbG08GdxjYUdnjjcShVdcNMrymuwOhr4M6LYUo'

service = build('sheets','v4',credentials= creds)

sheet = service.spreadsheets()


# In[66]:


final_data = df.replace(np.nan,"")

finaldata = final_data.astype('str')

kar = df.to_records(index = False)

# data0 = [tuple(x) for x in kar]

# data = {'values' : data0}


# In[67]:


sheet_range = 'MIS_Working_Sheet!A2' 


# In[68]:


service.spreadsheets().values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID,
        range = 'MIS_Working_Sheet!A:J', body={}).execute()
print("clearing the spreadsheet")


# In[69]:


# Insert dataset
def schema(kar, dimension: str = 'ROWS') -> dict:
    try:
        req_body = {
            'majorDimension': dimension,
            'values': kar
        }
        return req_body
    except Exception as e:
        print(e)
        return {}


# In[70]:


columns = schema([columns])

service.spreadsheets().values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
        range = 'MIS_Working_Sheet!A1' , body = columns, valueInputOption='USER_ENTERED').execute()
print("updating the spreadsheet table 1")


# In[71]:


body_values = schema(record)

service.spreadsheets().values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
        range=sheet_range, body = body_values, valueInputOption='USER_ENTERED').execute()
print("updating the spreadsheet table 1")


# In[72]:


#########################################################


# In[73]:


conn2 = pymysql.connections.Connection(host="1.0.30.160",user='atik.rehman',password='KCeV8dhFvx5Wk',db='dealers', port=3306)

query2 = """
         SELECT reg_no,Listing_Status, cast( now() as char) as 'Data_update_time' FROM (SELECT cnt.id,cnt.reg_no,cnt.dealer_id 'Dealer_id',d.gcd_code 'GCD_Code',d.organization 'Store',cnt.version_id,mm.make 'Make',mm.model 'Model',mv.db_version 'Variant',c.city_name 'City',sl.state_list_name 'State',cnt.km_driven,cnt.car_price,cnt.make_year,cnt.colour,cnt.created_date,cnt.last_update_date,cnt.last_deactivation_date,cnt.is_trustmark_car,
case when cnt.car_status='0' then 'TM Inactive'
when cnt.car_status='1' AND cnt.is_trustmark_car='1' then 'TM Active'
when cnt.car_status='1' AND cnt.is_valueplus='1' then 'Value+ Active'
when cnt.car_status='1' AND cnt.is_trustmark_car='0' then 'TM Inactive'
when cnt.car_status='2' then 'TM Inactive'
when cnt.car_status='3' then 'TM Sold'
when cnt.car_status='4' then 'TM Booked' ELSE cnt.car_status END 'Listing_Status',cnt.certification_status FROM 
cnt_used_car cnt 
INNER JOIN
dc_dealers d ON cnt.dealer_id = d.id
INNER JOIN
dc_showrooms s ON s.dealer_id = d.id
AND s.is_primary = '1'
AND s.status = '1'
INNER JOIN
city_list c ON s.city_id = c.city_id
LEFT JOIN state_list sl ON sl.state_list_id=c.state_id
INNER JOIN
model_version mv ON cnt.version_id = mv.db_version_id
INNER JOIN
make_model mm ON mv.model_id = mm.id

LEFT JOIN dc_dealers_tm_store tm ON tm.dealer_id=d.id
WHERE cnt.car_status IN ('1','3','4') AND tm.tm_store_type IN ('1','2')

UNION ALL

SELECT cnt.id,cnt.reg_no,cnt.dealer_id 'Dealer_id',d.gcd_code 'GCD_Code',d.organization 'Store',cnt.version_id,mm.make 'Make',mm.model 'Model',mv.db_version 'Variant',c.city_name 'City',sl.state_list_name 'State',cnt.km_driven,cnt.car_price,cnt.make_year,cnt.colour,cnt.created_date,cnt.last_update_date,cnt.last_deactivation_date,cnt.is_trustmark_car,
case when cnt.car_status='0' then 'TM Inactive'
when cnt.car_status='1' AND cnt.is_trustmark_car='1' then 'TM Active'
when cnt.car_status='1' AND cnt.is_trustmark_car='0' then 'TM Inactive'
when cnt.car_status='2' then 'TM Inactive'
when cnt.car_status='3' then 'TM Sold'
when cnt.car_status='4' then 'TM Booked' ELSE cnt.car_status END 'Listing_Status',cnt.certification_status FROM 
cnt_used_car cnt 
INNER JOIN
dc_dealers d ON cnt.dealer_id = d.id
INNER JOIN
dc_showrooms s ON s.dealer_id = d.id
AND s.is_primary = '1'
AND s.status = '1'
INNER JOIN
city_list c ON s.city_id = c.city_id
LEFT JOIN state_list sl ON sl.state_list_id=c.state_id
INNER JOIN
model_version mv ON cnt.version_id = mv.db_version_id
INNER JOIN
make_model mm ON mv.model_id = mm.id

LEFT JOIN dc_dealers_tm_store tm ON tm.dealer_id=d.id
WHERE cnt.car_status IN ('0','2') AND tm.tm_store_type IN ('1','2')
AND cnt.reg_no NOT IN (SELECT a.reg_no FROM cnt_used_car a INNER JOIN dc_dealers_tm_store tm ON tm.dealer_id=a.dealer_id
WHERE tm.tm_store_type IN ('1','2') AND a.car_status IN ('1','3','4')) AND cnt.id IN (SELECT a.id FROM(SELECT a.reg_no,MAX(a.id)'id' FROM cnt_used_car a INNER JOIN dc_dealers_tm_store tm ON tm.dealer_id=a.dealer_id
WHERE tm.tm_store_type IN ('1','2') AND a.car_status IN ('0','2') GROUP BY 1)a)  GROUP BY 1)k

"""

cursor2 = conn2.cursor()
cursor2.execute(query2)

record2 = cursor2.fetchall()

columns2 = [col[0] for col in cursor2.description]

df2 = pd.DataFrame(record2, columns = columns2)


# In[74]:


final_data2 = df2.replace(np.nan,"")

finaldata2 = final_data2.astype('str')

kar2 = df2.to_records(index = False)

# data0 = [tuple(x) for x in kar]

# data = {'values' : data0}

sheet_range2 = 'MIS_Working_Sheet!L2' 


# In[75]:


service.spreadsheets().values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID,
        range = 'MIS_Working_Sheet!L:N', body={}).execute()
print("clearing the spreadsheet")


# In[76]:


# Insert dataset
def schema(kar, dimension: str = 'ROWS') -> dict:
    try:
        req_body = {
            'majorDimension': dimension,
            'values': kar
        }
        return req_body
    except Exception as e:
        print(e)
        return {}


# In[77]:


columns2 = schema([columns2])

service.spreadsheets().values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
        range = 'MIS_Working_Sheet!L1' , body = columns2, valueInputOption='USER_ENTERED').execute()
print("updating the spreadsheet table 2")

body_values2 = schema(record2)


# In[78]:


service.spreadsheets().values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
        range=sheet_range2, body = body_values2, valueInputOption='USER_ENTERED').execute()
print("updating the spreadsheet table 2")


# In[1]:


import time
ts = time.time()
import datetime
st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
print(str(st)+" ROI_GSheet")
print("*********************************")


# In[ ]:




