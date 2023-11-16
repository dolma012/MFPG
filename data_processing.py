import argparse
import time
import pyspark
import os
import pandas as pd
from functools import reduce
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession

if __name__ == '__main__':
  spark = SparkSession.builder \
      .appName('Ie_project') \
      .master('local[*]') \
      .config('spark.executor.memory', '8g') \
      .config('spark.driver.memory', '8g') \
      .config('spark.driver.maxResultSize', '8g') \
      .getOrCreate()
  sc = spark.sparkContext

spark = SparkSession(sc)  
direc_status = './statusdata/'
diag_file = './umn_diagnostics_seen.csv'
direc_trip = './tripdata/'
device_file = './umn_geotab_deviceinfo.csv'
direc_log = './logrecord/'
log_files = os.listdir(direc_log)
trip_files = os.listdir(direc_trip)
status_files = os.listdir(direc_status)

diag_df = pd.read_csv(diag_file, sep=',', names=["row", "diagnostic", "name", "units", "interest_flag", "seen_with_analysis_vehicles_flag", "seen_with_analysis_vehicles_count"])
df = diag_df.iloc[1:]
diag_df = spark.createDataFrame(df)
diag_rdd = diag_df.rdd
removed_diagnostic_lst = diag_rdd.filter(lambda x: x['interest_flag'] == '0')
removed_diagnostic_lst = removed_diagnostic_lst.map(lambda x: x['diagnostic']).collect()


diag_rdd =diag_rdd.filter(lambda x: x['interest_flag'] != '0')
diagnostic_lst = diag_rdd.map(lambda x: x['diagnostic']).collect()
diag_df = diag_rdd.map(lambda x: x).toDF()


device_df = spark.read.csv(device_file, header=True)   
device_df = device_df.rdd
device_lst = device_df.map(lambda x : x['id']).collect()



import ast
from pyspark.sql import DataFrame
from functools import reduce
from pyspark.sql.functions import expr
#key is device_id -> statusDataFrame
deviceHashMapStatusData = {}
deviceHashMapLogData = {}

device = device_lst[0]


status_lst = []
curr_status_data = None
for i,file in enumerate(status_files):
    status_fp = open(os.path.join(direc_status, file))
    df = spark.read.csv(status_fp.name, header=True)
    json_schema = StructType([StructField("id", StringType())])

    df = df.withColumn("device", from_json(df["device"].cast(StringType()), json_schema)["id"])
    # reassignment of columns with dict in string to value of dict
    df = df.withColumn("diagnostic", from_json(df["diagnostic"].cast(StringType()), json_schema)["id"])
    status_lst.append(df)  
    if i == 1074/2:
        print("50 percent of status data loaded")
print("Completed loading status to dataframe")

log_lst = [] 
for i, file in enumerate(log_files):
    log_fp = open(os.path.join(direc_log, file))
    df = spark.read.csv(log_fp.name, header=True)
    json_schema = StructType([StructField("id", StringType())])
    df = df.withColumn("device", from_json(df["device"].cast(StringType()), json_schema)["id"])
    log_lst.append(df)
    if i == 227//2:
        print("50 percent of log data loaded")
print("Completed loading log data to dataframe")
log_df = reduce(DataFrame.unionAll, log_lst)
status_df = reduce(DataFrame.unionAll, status_lst)
deviceHashMapStatusData[device] = status_df
deviceHashMapLogData[device] = log_df

print(spark.conf.get("spark.driver.maxResultSize"))


import json
import csv
from pyspark.sql.functions import col

import json
import ast
from datetime import datetime
def csv_to_json(csv_file_path, json_file_path, start_stop_list):
    #create a dictionary
    data_dict = {}
 
    #Step 2
    #open a csv file handler
    with open(csv_file_path, encoding = 'utf-8') as csv_file_handler:
        csv_reader = csv.DictReader(csv_file_handler, delimiter=";")
 
        #convert each row into a dictionary
        #and add the converted data to the data_variable
        i=0
        for rows in csv_reader:
 
            #assuming a column named 'No'
            #to be the primary key
            key = start_stop_list[i]
            data_dict[key] = rows
            i+=1
 
    #Step 3
    with open(json_file_path, 'w', encoding = 'utf-8') as json_file_handler:
        #Step 4
        json_file_handler.write(json.dumps(data_dict, indent = 4))


for dev_i in range(len(device_lst)):
    curr_status = deviceHashMapStatusData[device_lst[0]]
    curr_log = deviceHashMapLogData[device_lst[0]]
    curr_log = curr_log.filter(curr_log.device== device_lst[dev_i])
    curr_log = curr_log.drop("id","device")
    status_data = curr_status.filter(curr_status.device == device_lst[dev_i])
    status_data = status_data.filter( col("diagnostic").isin(diagnostic_lst))

    status_data = status_data.drop("controller", "id", "version","device")                                                           
    status_data_pd = status_data.toPandas()
    log_data_pd = curr_log.toPandas()
    inner_join_stat_log = pd.merge(status_data_pd, log_data_pd, on=["dateTime"])
    json_file_path =f'filtered_status_{device_lst[dev_i]}.json'
    print(json_file_path)
    log_status_df = inner_join_stat_log.to_json(orient ="records")
    status_log_lst = ['dateTime', 'data', 'diagnostic', 'controller', 'version','latitude_longitude', 'speed']
    with open(json_file_path, 'w') as json_file:
        json.dump(log_status_df, json_file)
    trip_df = None
    for trip in trip_files:
        df = spark.read.csv(direc_trip+trip, header= True)
        json_schema = StructType([StructField("id", StringType())])
        trip_fp = df.withColumn("device", from_json(df["device"].cast(StringType()), json_schema)["id"])
        if trip_df is None:
            trip_df = trip_fp
        else:        
            trip_df = trip_df.union(trip_fp)

    trip_rdd = trip_df.rdd
    trip_filtered = trip_rdd.filter(lambda x: x["device"] == device_lst[dev_i])
    try:
        trip_df = trip_filtered.toDF()
        trip_json = trip_df.toJSON().collect()


        json_file_path =f'trip_{device_lst[dev_i]}.json'
        with open(json_file_path, 'w') as json_file:
            json.dump(trip_json, json_file)
        status_log_lst = ['dateTime', 'data', 'diagnostic', 'latitude_longitude', 'speed']            

        header_lst = ["afterHoursDistance","afterHoursDrivingDuration","afterHoursEnd","afterHoursStart","afterHoursStopDuration","distance","drivingDuration","engineHours","idlingDuration","isSeatBeltOff","maximumSpeed","nextTripStart","speedRange1","speedRange1Duration","speedRange2","speedRange2Duration","speedRange3","speedRange3Duration","start","stop","stopDuration","stopPoint","workDistance","workDrivingDuration","workStopDuration","device","driver","id","averageSpeed"
        ] + diagnostic_lst + status_log_lst

        json_file_path = f"filtered_status_{device_lst[dev_i]}.json"
        with open(json_file_path, "r") as json_file:
            data = json.load(json_file)

        json_file_path = f'trip_{device_lst[dev_i]}.json'  
        with open(json_file_path, "r") as json_file:
            trip_data = json.load(json_file)

        #dateTime-> [(diag, data)]
        #dateTime-> [speed]
        #dateTime-> [long,lat]

        dict_speed = {}
        dict_long_lat = {}
        dict_diag = {}
        dict_trip = {}
        duration = {}
        # add data to each row
        y = False



        for i in trip_data:
            k = ast.literal_eval(i)
            print(type(i))
            k_sta =k["start"][:25]
            k_sto=k["stop"][:25]
            k_start = str(datetime.strptime(k_sta[:-6], '%Y-%m-%d %H:%M:%S').date()) + ' ' + str(datetime.strptime(k_sta[:-6], '%Y-%m-%d %H:%M:%S').time())
            k_stop = str(datetime.strptime(k_sto[:-6], '%Y-%m-%d %H:%M:%S').date()) + ' ' + str(datetime.strptime(k_sto[:-6], '%Y-%m-%d %H:%M:%S').time())
            duration[k_start,k_stop] = k["drivingDuration"]
            if (k_start,k_stop) in dict_trip:
                print('incorrect')
                y = True
            dict_trip[k_start,k_stop] = k

        data = ast.literal_eval(data)
        for i in data:
        #     k = ast.literal_eval(i)
            k_d = i["dateTime"]
            k_d =  k_d[:25]
            k_d = str(datetime.strptime(k_d[:-6], '%Y-%m-%d %H:%M:%S').date()) + ' ' +  str(datetime.strptime(k_d[:-6], '%Y-%m-%d %H:%M:%S').time()) 

            if k_d not in dict_diag:
                dict_diag[k_d]= [(i["diagnostic"],i["data"])]
            else:
                dict_diag[k_d].append((i["diagnostic"],i["data"]))
            dict_speed[k_d] = i["speed"]
            dict_long_lat[k_d] = (i["latitude"], i["longitude"])


        start_stop_list = {}

        with open(f'device_{device_lst[dev_i]}.csv', 'w') as file: 
            dw = csv.DictWriter(file, delimiter=';',   fieldnames=header_lst) 
            dw.writeheader()
            dates = list(dict_diag.keys())
        # comparethe date to the trip_dict
            con = 0
            for trip in dict_trip:
                trip_data =  dict_trip[trip[0],trip[1]]        
                rows = [date for date in dates if datetime.strptime(date, '%Y-%m-%d %H:%M:%S').date() >= datetime.strptime(trip[0], '%Y-%m-%d %H:%M:%S').date()
                        and datetime.strptime(date, '%Y-%m-%d %H:%M:%S').time() >= datetime.strptime(trip[0], '%Y-%m-%d %H:%M:%S').time()
                        and datetime.strptime(date, '%Y-%m-%d %H:%M:%S').date() <= datetime.strptime(trip[1], '%Y-%m-%d %H:%M:%S').date()
                        and datetime.strptime(date, '%Y-%m-%d %H:%M:%S').time() <= datetime.strptime(trip[1], '%Y-%m-%d %H:%M:%S').time()]
                for date in rows:
                    if f'device_{device_lst[dev_i]}' not in start_stop_list:
                        start_stop_list[f'device_{device_lst[dev_i]}'] = [date]
                    start_stop_list[f'device_{device_lst[dev_i]}'].append(date)
                    row_diag = dict_diag[date]
                    print(row_diag)
                    count=0
                    for j,i in enumerate(header_lst):
                        if i == "speed":
                            file.write(dict_speed[date])
                        elif i == "latitude_longitude":
                            file.write(str(dict_long_lat[date]))
                        # if its not latitude or longitude or speed then check for diagnostic
                        elif [k for k in row_diag if k[0] == i]:

                            diag = [k for k in row_diag if k[0] == i]
                            if diag:
                                print("diag", diag[0])
                                file.write(diag[0][1])
                        else:
                            if i in dict_trip[trip[0],trip[1]]:


                                d = dict_trip[trip[0],trip[1]]

                                file.write(d[i])
                        if j < len(header_lst) - 1:
                            count+=1
                            file.write(";")


                    file.write("\n")


        csv_file_path = f"device_{device_lst[dev_i]}.csv"
        json_file_path = f"device_{device_lst[dev_i]}.json"

        csv_to_json(csv_file_path, json_file_path, start_stop_list[f"device_{device_lst[dev_i]}"])

    except:
        continue
        

