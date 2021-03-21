# !/usr/bin/python 
# coding:utf-8

import pandas as pd

from datetime import datetime

# 手機編號
phone_number_index = [7,8,9,10,'MI(小米)','怡秀','哲瑋']
# 手機號碼
phone_number_list = ['0974-342-496', '0974-296-447', '0974-314-641', '0974-314-490','0974-248-488','0921-407-460','0958-986-553']

# https://blog.csdn.net/weixin_44073728/article/details/111054157
data = pd.read_excel('./data_' + phone_number_list[0] + '_1.xlsx')

true_data = pd.read_excel('./true-data.xlsx')

true_data = true_data.groupby('手機編號').get_group(phone_number_index[0])
true_data = true_data.groupby('搭車日期').get_group('2019-09-24')
true_data = true_data.reset_index()
true_data.drop(['index'], 1, inplace = True)

print(true_data)

# 移動停留點判斷(順序應該再飆飄點前處理
# 設定一新空間來存整理過後之資料
new_space = pd.DataFrame(columns = list(data.columns[0:]))

for i in range(len(true_data)):
  o_time = true_data.at[i, '車輛出發時間(24小時制)']
  d_time = true_data.at[i, '車輛抵達時間(24小時制)']

  # 真實資料起始(小時)
  truedata_o_hour = o_time.strftime('%H')
  # 真實資料起始(分)
  truedata_o_Min = o_time.strftime('%M')

  # 真實資料結束(小時)
  truedata_d_Hour = d_time.strftime('%H')
  # 真實資料結束(分)
  truedata_d_Min = d_time.strftime('%M')

  print('第'+ str(i) +'真實資料出發時間', truedata_o_hour, truedata_o_Min)
  print('第'+ str(i) +'真實資料結束時間', truedata_d_Hour, truedata_d_Min)

  # 判斷第 i 筆真實資料的出發時間是在信令資料裡面的哪一筆

  befound = 1
  befound_index = 0
  temp_hour_index = []
  temp_befound_index = []
  while befound > 0:
    for j in range(len(data)):
      start_time_string = data.at[j, 'start_dt']
      datetime_instance = datetime.strptime(start_time_string, "%Y/%m/%d %H:%M")
      # 信令資料裡每 j 筆出發時間的小時
      cellular_o_hour = datetime_instance.strftime('%H')

      if j > befound_index and truedata_o_hour == cellular_o_hour:
        # 找到與真實資料相同小時的資料 將 j 也就是資料的 index 先存在 temp_hour_index
        befound_index = j
        temp_hour_index = temp_hour_index + [j]
    
    print(temp_hour_index)

    # 符合"小時"的 index 接續判斷"分鐘"是不是我們要的
    for hour_index in temp_hour_index:
      start_time_string = data.at[j, 'start_dt']
      datetime_instance = datetime.strptime(start_time_string, "%Y/%m/%d %H:%M")
      # 信令資料裡每 j 筆出發時間的分鐘
      cellular_o_min = datetime_instance.strftime('%M')

      if int(truedata_o_Min) - 3 <= int(cellular_o_min) <= int(truedata_o_Min) + 3:
        temp_befound_index = hour_index
        print('a1 ', hour_index)
        break
      # 如果 hour index 已經是最後一筆被暫存在 temp hour 的 index
      # 表示
      elif hour_index == temp_hour_index[-1]:
        print('a1 not found')
        befound = -1
    
    if befound < 0:
      break
  print('find a1 =', temp_befound_index)
