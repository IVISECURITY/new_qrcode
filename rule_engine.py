import aws_rds
import pandas as pd
import os,time
#all modules are imported for their specific task
from datetime import datetime
import requests

def get_schedule_data():
    file_name = 'schedule.csv'
    if(os.path.exists(file_name) and os.path.isfile(file_name)):
        os.remove(file_name)
        print("file deleted")
    a=aws_rds.get_schedule()
    df = pd.DataFrame(a,columns=['id','deviceId','deviceTypeId','siteId','assetId','startTime','endTime'])
    df.to_csv('schedule.csv',index=False,header=True)
    print("file_created")


def get_device_data():
    file_name = 'devices.csv'
    if(os.path.exists(file_name) and os.path.isfile(file_name)):
        os.remove(file_name)
        print("file deleted")
    a=aws_rds.get_device()
    df = pd.DataFrame(a,columns=['deviceId','deviceUnitId','deviceTypeId','siteId','tempRange','deviceName'])
    df.to_csv("devices.csv",index=False,header=True)
    print("file_created") 

def get_latlng_data():
    file_name = 'sites.csv'
    if(os.path.exists(file_name) and os.path.isfile(file_name)):
        os.remove(file_name)
        print("file deleted")
    a=aws_rds.get_latlng()
    df = pd.DataFrame(a,columns=['siteId','latitude','longitude'])
    df.to_csv('sites.csv',index=False,header=True)
    print("file_created")  

def get_rule_data():
    file_name = 'rules_master.csv'
    if(os.path.exists(file_name) and os.path.isfile(file_name)):
        os.remove(file_name)
        print("file deleted")
    a=aws_rds.get_rule()
    df = pd.DataFrame(a,columns=['ruleName','ruleId'])
    df.to_csv('rules_master.csv',index=False,header=True)
    print("file_created")  

def get_ruleengine_data():
    file_name = 'device_rules.csv'
    if(os.path.exists(file_name) and os.path.isfile(file_name)):
        os.remove(file_name)
        print("file deleted")
    a=aws_rds.get_ruleEngine()
    df = pd.DataFrame(a,columns=['id','siteId','ruleId','assetId','deviceTypeId','deviceId'])
    df.to_csv('device_rules.csv',index=False,header=True)
    print("file_created")


def get_asset_data():
    file_name = 'qr_assets.csv'
    if(os.path.exists(file_name) and os.path.isfile(file_name)):
        os.remove(file_name)
        print("file deleted")
    a=aws_rds.get_asset()
    df = pd.DataFrame(a,columns=['id','assetId'])
    df.to_csv('qr_assets.csv',index=False,header=True)
    print("file_created")    


  



def get_temp(temp_url,lat,lng,rng,api_key):
    day = time.strftime("%p")
    c =int(float(requests.get(temp_url.format(lat,lng,api_key)).json()['current']['temp'])-273.15)
    
    if 1 <= c <= rng:
        a = 'R1'
        
    elif rng+1 <= c <= rng * 2:
        a = 'R2'
        
    else:
        a = 'R3'
    
    return day+a



def get_call_data():
    get_schedule_data()
    get_device_data()
    get_latlng_data()
    get_rule_data()
    get_ruleengine_data()
    get_asset_data()
    return "updated"

# asdf = get_call_data()


df = pd.read_csv('device_rules.csv')
df1 = pd.read_csv('qr_assets.csv',index_col ="id")
df2 = pd.read_csv('rules_master.csv',index_col ="ruleName")
df3 = pd.read_csv('sites.csv',index_col ="siteId")
df4 = pd.read_csv('devices.csv')
df5 = pd.read_csv('schedule.csv')


def get_assetId(d_id,d_type,rule,site):
    db = df["assetId"][(df['siteId'] == site) & (df['ruleId'] == rule) & (df['deviceTypeId'] == d_type) & (df['deviceId'] == d_id)]
    vb = list(db)
    return vb[0]

def get_asset(a_id):
    db = df1.loc[a_id]
    vb = list(db)
    return vb[0]

def get_rule_id(r1):
    db = df2.loc[r1]
    vb = list(db)
    return vb[0]


def get_latlng(site):
    db = df3.loc[site]
    vb = list(db)
    return vb

    
def get_device(deviceId,siteId,deviceTypeId):
    db = df4["tempRange"][(df4['deviceId']==deviceId) & (df4['siteId'] == siteId) & (df4['deviceTypeId'] == deviceTypeId)]
    print(db)
    vb = list(db)
    return vb[0]


# def get_schedule(deviceId,siteId,deviceTypeId):
#     print(df5)
#     db = df5.loc[(df5['siteId'] == siteId) & (df5['deviceId'] == deviceId) & (df5['deviceTypeId'] == deviceTypeId)]
#     print(db)
#     vb = list(db.values)
#     return vb
def get_schedule(site,account,device_type):
    print(df5)
    db = df5.loc[(df5['siteId'] == site) & (df5['deviceId'] == account) & (df5['deviceTypeId'] == device_type)]
    print(db)
    vb = list(db.values)
    return vb




def main_rule(account,site,device_type,temp_url,api_key):
    print(temp_url)
    try:
        print(account,site,device_type)
        av = get_schedule(site,account,device_type)
        # av=get_schedule(site,account,device_type)
        print("schedule",av)
        if len(av) >= 1:
            for i in av:
                today =datetime.now()
                From_time = datetime.fromisoformat(i[-2])
                To_time = datetime.fromisoformat(i[-1])

                if From_time <= today <=  To_time:
                    a_id = i[-3]
                    break

                else :
                    data = get_device(account,site,device_type)
                    print(data)
                    latlng = get_latlng(site)
                    print(temp_url,latlng[0],latlng[1],data)
                    rule = get_temp(temp_url,latlng[0],latlng[1],data,api_key)
                    print(rule)
                    rule_id = get_rule_id(rule)
                    print(rule_id)
                    a_id = get_assetId(account,device_type,rule_id,site)
        else :
            data = get_device(account,site,device_type)
            print(data)
            latlng = get_latlng(site)
            print(temp_url,latlng[0],latlng[1],data)
            rule = get_temp(temp_url,latlng[0],latlng[1],data,api_key)
            print(rule)
            rule_id = get_rule_id(rule)
            print(rule_id)
            a_id = get_assetId(account,device_type,rule_id,site)



            
    except Exception as e:
        # defaultly gave image path through db
        print("this is exception",e)
        a_id = 1
    try:
        print(a_id,"hello")
        res = get_asset(a_id)
    except Exception as e:
        res = "exception......"+str(e)
    return res