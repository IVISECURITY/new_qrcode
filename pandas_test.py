import pandas as pd

df5 = pd.read_csv('schedule.csv')
# print(df4)

# df.loc[(df['column_name'] >= A) & (df['column_name'] <= B)],'deviceUnitId','deviceTypeId','siteId','temp_range','deviceName']['deviceId']

# def get_device(deviceId,siteId):
#     db = df4.loc[(df4['deviceUnitId']==deviceId) & (df4['siteId'] == siteId)]
#     # db = df4.loc[(df4['deviceUnitId']==deviceId) & (df4['siteId'] == siteId)]
#     print(db)
#     #db = df4.loc[device_id]
#     # vb = list(db)
#     return list(db.values)[0]

def get_schedule(siteId,deviceId,deviceTypeId):
    print(df5)
    db = df5.loc[(df5['siteId'] == siteId) & (df5['deviceId'] == deviceId) & (df5['deviceTypeId'] == deviceTypeId)]
    print(db)
    vb = list(db.values)
    return vb
av = get_schedule(1016,112,3)
print(av[0])
