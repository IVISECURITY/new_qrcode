import pymysql as p
import os
from dotenv import load_dotenv
load_dotenv()

host = os.environ.get('RDS_URL')
user = os.environ.get('RDS_USER')
password = os.environ.get('RDS_PASS')
database = os.environ.get('RDS_DB')
#port = int(os.environ.get('RDS_PORT'))
key=os.environ.get('API_KEY')
url=os.environ.get('URL')
device_type = os.environ.get('DEVICE_TYPE')

con=p.connect(host=host,user=user,password=password,database=database)
cur=con.cursor()

schedule_query = "select id,deviceId,deviceTypeId,siteId,assetId,startTime,endTime from schedule"
device_query = 'select deviceId,deviceUnitId,deviceTypeId,siteId,tempRange,deviceName from devices'
site_query = 'select siteId,latitude,longitude from sites'
rules_query = 'select ruleName,ruleId from rules_master'
rule_engine_query = "select * from device_rules"
assets_query = 'select id,assetId from qr_assets'


def get_schedule():
    con.ping(reconnect = True)
    cur.execute(schedule_query)
    res = cur.fetchall()
    return res

def get_device():
    con.ping(reconnect = True)
    cur.execute(device_query)
    res = cur.fetchall()
    return res


def get_latlng():
    con.ping(reconnect = True)
    cur.execute(site_query)
    res = cur.fetchall()
    return res

def get_rule():
    con.ping(reconnect = True)
    cur.execute(rules_query)
    res = cur.fetchall()
    return res 


def get_ruleEngine():
    con.ping(reconnect = True)
    cur.execute(rule_engine_query)
    res = cur.fetchall()
    return res        

def get_asset():
    con.ping(reconnect = True)
    cur.execute(assets_query)
    res = cur.fetchall()
    return res 





 




