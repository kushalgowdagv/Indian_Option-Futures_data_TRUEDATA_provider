from truedata_ws.websocket.TD import TD
import pandas as pd
import datetime
import time

td_obj = TD('tdwsp296', 'rahul@296',live_port=None)

with open("final aug.txt","r") as myfile:
    option_contracts=myfile.read().splitlines()

# Extracting Options data from contracts imported from the file op_contracts.txt
options_data=pd.DataFrame()
for contract in option_contracts:
    try:
        for count,c in enumerate(contract):
            if c.isdigit():
                #print(count)
                break
        dates=contract[count:count+6] # extract date from contract symbol

        end_date=datetime.datetime(2022,int(dates[2:4].lstrip("0")),int(dates[4:6].lstrip("0")),15,30,0)
        bnf_daily = td_obj.get_historic_data(contract,start_time=datetime.datetime(2022,8,1,9,15,0),end_time=end_date)
        temp_df=pd.DataFrame(bnf_daily)
        temp_df["OptType"]=contract[-2:]

        #print(end_date)
        #print(contract)
        temp_df["datetime"]=pd.to_datetime(temp_df["time"], format='%Y-%m-%d %H:%M:%S')
        temp_df["Date"]=temp_df["datetime"].dt.strftime("%Y/%m/%d")
        temp_df["Time"]=temp_df["datetime"].dt.strftime("%H:%M:%S")
        
        temp_df["Expiry_Date"]=end_date.strftime("%Y/%m/%d")
        temp_df["Strike"]=contract[-7:-2]
        temp_df=temp_df.iloc[:,[9,10,12,7,11,1,2,3,4,5,6,0,8]]
        temp_df.drop(columns=["time","datetime"],inplace=True)
        temp_df.rename(columns={"o":"Open","h":"High","l":"Low","c":"Close","v":"Volume","oi":"Open Interest"},inplace=True)
        options_data=pd.concat([options_data,temp_df],ignore_index=True)
        
        
        
        #print(temp_df.tail())
    except:
        time.sleep(0.5)
        continue

# Extracting Futures data from Truedata

# --------------------- AUGUST 2022 FUT ----------------------------------
bnf_fut=td_obj.get_historic_data("BANKNIFTY22AUGFUT",start_time=datetime.datetime(2022,8,1,9,15,0),end_time=datetime.datetime(2022,8,31,15,30,0))
bnf_fut_data=pd.DataFrame(bnf_fut)
bnf_fut_data["datetime"]=pd.to_datetime(bnf_fut_data["time"], format='%Y-%m-%d %H:%M:%S')
bnf_fut_data["Date"]=bnf_fut_data["datetime"].dt.strftime("%Y/%m/%d")
bnf_fut_data["Time"]=bnf_fut_data["datetime"].dt.strftime("%H:%M:%S")
bnf_fut_data["OptType"]="NA"
bnf_fut_data["Strike"]=0
bnf_fut_data["temp_Date"]=pd.to_datetime("2022-08-25",format='%Y-%m-%d')
bnf_fut_data["Expiry_Date"]=bnf_fut_data["temp_Date"].dt.strftime("%Y/%m/%d")
bnf_fut_data.drop(columns=["time","datetime","temp_Date"],inplace=True)
bnf_fut_data=bnf_fut_data.iloc[:,[6,7,9,8,10,0,1,2,3,4,5]]
bnf_fut_data.rename(columns={"o":"Open","h":"High","l":"Low","c":"Close","v":"Volume","oi":"Open Interest"},inplace=True)

# Extracting Futures data from Truedata

# --------------------- SEPTEMBER 2022 FUT ----------------------------------
bnf_fut_1=td_obj.get_historic_data("BANKNIFTY22SEPFUT",start_time=datetime.datetime(2022,8,26,9,15,0),end_time=datetime.datetime(2022,9,1,15,31,0))
bnf_fut_data_1=pd.DataFrame(bnf_fut_1)
bnf_fut_data_1["datetime"]=pd.to_datetime(bnf_fut_data_1["time"], format='%Y-%m-%d %H:%M:%S')
bnf_fut_data_1["Date"]=bnf_fut_data_1["datetime"].dt.strftime("%Y/%m/%d")
bnf_fut_data_1["Time"]=bnf_fut_data_1["datetime"].dt.strftime("%H:%M:%S")
bnf_fut_data_1["OptType"]="NA"
bnf_fut_data_1["Strike"]=0
bnf_fut_data_1["temp_Date"]=pd.to_datetime("2022-09-29",format='%Y-%m-%d')
bnf_fut_data_1["Expiry_Date"]=bnf_fut_data_1["temp_Date"].dt.strftime("%Y/%m/%d")
bnf_fut_data_1.drop(columns=["time","datetime","temp_Date"],inplace=True)
bnf_fut_data_1=bnf_fut_data_1.iloc[:,[6,7,9,8,10,0,1,2,3,4,5]]
bnf_fut_data_1.rename(columns={"o":"Open","h":"High","l":"Low","c":"Close","v":"Volume","oi":"Open Interest"},inplace=True)


# CONCAT AUGUST FUTURES DATA TO OPTIONS DATA
options_data=pd.concat([options_data,bnf_fut_data],ignore_index=True)
# CONCAT SEPT FUTURES DATA TO OPTIONS DATA 
options_data=pd.concat([options_data,bnf_fut_data_1],ignore_index=True)

# SORT THE FINAL DATA BASED ON DATE AND TIME
options_data.sort_values(by=["Date","Time"],inplace=True)

options_data.to_csv("/home/rahul/BankNifty_Data_August.csv",index=False)