# !/usr/bin/python 
# coding:utf-8

import pandas as pd

from datetime import datetime

from math import radians, cos, sin, asin, sqrt, acos, degrees

# Minutes to Seconds Conversion
def MinToSec(time_string):
  # datetime_instance = datetime.strptime(time_string, "%Y/%m/%d %H:%M")
  hour = time_string.strftime('%H')
  min = time_string.strftime('%M')

  return int(hour) * 3600 + int(min) * 60

def haversine(lon1, lat1, lon2, lat2): # 经度1，纬度1，经度2，纬度2 （十进制度数）
        """
        Calculate the great circle distance between two points 
        on the earth (specified in decimal degrees)
        """
        # 将十进制度数转化为弧度
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
     
        # haversine公式
        dlon = lon2 - lon1 
        dlat = lat2 - lat1 
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a)) 
        r = 6378.145 # 地球平均半径，单位为公里
        return c * r 

def caculate_v(dist, t1, t2):
  return float(dist)/(((float(t2)-float(t1))/3600)+0.00001)

# 新增三個欄位 st_sec, ed_sec, dist
def process1(data):
  print('新增三個欄位 st_sec, ed_sec, dist 中')
  for index in range(len(data)):

    start_time_string = data.at[index, start_dt]
    end_time_string = data.at[index, end_dt]

    data.at[index, 'st_sec'] = MinToSec(start_time_string)
    data.at[index, 'ed_sec'] = MinToSec(end_time_string)

  # 算距離是拿第 i 筆與 i + 1 筆的經緯度換算出距離存放在 i + 1
  # 所以當 i 成為最後一筆資料還加一的時候就會跳錯 因此要額外判斷並排除
    if index != len(data) - 1:
      # 經度 1
      lon1 = data.at[index, longitude]
      # 緯度 1
      lat1 = data.at[index, latitude]
      # 經度 2
      lon2 = data.at[index + 1, longitude]
      # 緯度 2
      lat2 = data.at[index + 1, latitude]
      
      data.at[index + 1, 'dist'] = haversine(lon1, lat1, lon2, lat2)

  print('完成新增三個欄位 st_sec, ed_sec, dist')
  return data

# 新增一個欄位 v(kmh)
def process2(data):
  print('新增欄位 v(kmh)')
  # 新增一個欄位 v(kmh)
  for index in range(len(data) - 1):

    # 取出距離資料
    distance = data.at[index + 1, 'dist']

    # 算時間差
    t1 = data.at[index, 'ed_sec']
    t2 = data.at[index + 1, 'st_sec']

    # 將計算出來的速率填入新的欄位 v(kmh)
    data.at[index + 1, 'v(kmh)'] =  caculate_v(distance, t1, t2)

  print('完成新增欄位 v(kmh)')
  return data
  
# 處理速度超過 99 的資料
def process3(data):

  print('處理速度超過 99 的資料')
  data.drop(['index'], 1, inplace = True)
  start_index = 0

  while True:
    if start_index >= len(data) - 1:
      break
    # 如果速度大於 99 就要刪除這筆資料並與下一筆資料進行重新運算
    # 直到速度為正常
    if data.at[start_index + 1,'v(kmh)'] > 99:
      data.drop(index=[start_index + 1], inplace = True)
      data = data.reset_index()
      data.drop(['index'], 1, inplace = True)

      lon1 = data.at[start_index, longitude]
      lat1 = data.at[start_index, latitude]
      lon2 = data.at[start_index+1, longitude]
      lat2 = data.at[start_index+1, latitude]

      data.at[start_index + 1, 'dist'] =  haversine(lon1, lat1, lon2, lat2)

      dist = data.at[start_index + 1, 'dist']
      t1 = data.at[start_index, 'ed_sec']
      t2 = data.at[start_index + 1, 'st_sec']
      data.at[start_index + 1, 'v(kmh)'] =  caculate_v(dist, t1, t2)
    else:
      start_index = start_index + 1
  
  print('完成處理速度超過 99 的資料')
  return data

# 檔案輸出
def output_file(data, path):
  print('檔案輸出中')
  output = data
  outputpath = path
  output.to_excel(outputpath, index = False, header = True)

def read_file(prefix, file_name_list, file_type, output_prefix, output_enable:False):

  for file_name in file_name_list:
    # 組建出檔案路徑
    file_path = str(prefix) + str(file_name) + '.' + str(file_type)
    output_path = str(output_prefix) + str(file_name) + '_done.' + str(file_type)

    print('讀取的檔案路徑', file_path)
    print('輸出的檔案路徑', output_path)
    print('讀檔錯誤時請檢查此檔案路徑是否正確')

    if(str(file_type) == 'xlsx'):
      cellular_data = pd.read_excel(file_path)
    elif(str(file_type) == 'csv'):
      cellular_data = pd.read_csv(file_path)
    else:
      print('file_type 格式有誤')

    cellular_data = cellular_data.reset_index()
    cellular_data = process1(cellular_data)
    cellular_data = process2(cellular_data)
    cellular_data = process3(cellular_data)
  
    if output_enable:
      print('輸出檔案')
      output_file(cellular_data, output_path)

# =================== 主要程式開始 =========================
# 主要執行信令資料的整理以及運算出新的數據回存

# 一些參數是先定義
# prefix 為檔案路徑的前綴
# 一個完整的路徑會由 prefix + file_name + file_type 組成
prefix = r'C:\\Users\\henry\\Desktop\\Analytics\\data\\Original\\'
prefix_androiod = r'C:\\Users\\henry\\Desktop\\Analytics\\data\\Original\\androiod\\'

# 欲讀取的檔案名稱清單
file_name_list = ['01.05','01.06','01.07']
file_name_list_androiod = ['01.05 - andriod', '01.06 - andriod', '01.07 - andriod']

# 欲讀取的檔案類型
file_type = 'xlsx'

# 檔案輸出的位置 (已經預設輸出的檔名為 file_name_done.[file_type])
output_prefix = r'C:\\Users\\henry\Desktop\Analytics\data\Done\\'
output_prefix_androiod = r'C:\\Users\\henry\Desktop\Analytics\data\Done\\androiod\\'

# 欄位名稱定義
# 有時候資料的欄位名稱會被改動，為了防止每次欄位名稱不同都要再次修改用
start_dt = 'start_dt'
end_dt = 'end_dt'
longitude = 'lon'
latitude = 'lat'

# 呼叫函式 read_file 將參數丟入
# 最後一個參數為是否要將檔案輸出
# read_file(prefix, file_name_list, file_type, output_prefix, True)

phone_brand = 'androiod'
# phone_brand = 'iPhone'

if phone_brand == 'androiod':
  read_file(prefix_androiod, file_name_list_androiod, file_type, output_prefix_androiod, True)
elif phone_brand == 'iPhone':
  read_file(prefix, file_name_list, file_type, output_prefix, True)
