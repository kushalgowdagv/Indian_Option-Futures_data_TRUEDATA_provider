from truedata_ws.websocket.TD import TD
import time
import logging
import numpy as np
import pandas
import calendar
import datetime
from dateutil.relativedelta import * 
from dateutil import parser
import calendar 

username = 'tdws'
password = 'rahul'

td_obj = TD(username, password, live_port=None)

def get_expiry(start_date):
    final_output= {}
    #z = parser.parse(start_date)
    start = start_date.month
    final_output= {}
    for month in range(start,start+3):
        weekly_expiry_days= []
        for week in calendar.monthcalendar(start_date.year,month):
            if week[3] != 0:
                weekly_expiry_days.append(week[3])
        final_output[month] = weekly_expiry_days        
        #print("weekly_expiry_days", weekly_expiry_days)
    ex = final_output
    ex_month = list(ex.keys())
    ex_dates = []
    for mn in ex_month:
        for day in ex[mn]:
            ex_date = str(start_date.year)+"-"+str(mn).zfill(2)+"-"+str(day).zfill(2)
            #print(mn,day,ex_date)
            ex_dates.append(ex_date)
    return ex_dates 

def get_NSE_tickers():
    nseticker_dict = {}
    # url = "https://archives.nseindia.com/content/fo/sos_scheme.xls"
    # df = pandas.read_excel(url)
    # df.loc[len(df.index)] = ["NIFTY",50,0,0]
    # df.loc[len(df.index)] = ["BANKNIFTY",100,0,0]
    # for i in range(0,len(df)):
    #     nseticker_dict[df["Symbol"][i]] = [df["Applicable Step value"][i],df["No. of Strikes Provided\nIn the money - At the money - Out of the money"][i],df["No of additional strikes which may be enabled intraday"][i]]
    
# ------------------------ NOTE : WE WILL BE USING ONLY NIFTY AND BANKNIFTY FOR DATA EXTRACTION -----------------------------------------
    nseticker_dict['NIFTY'] = [50]
    nseticker_dict['BANKNIFTY'] = [100]
    return nseticker_dict 

def get_option_strikes(ticker,open_price):
    jump = round((get_NSE_tickers()[ticker][0]/100)*100)
    print("jump",jump)
    up_strike= []
    down_strike = []
    for i in range(jump,20*jump,jump):
        up_strike.append(open_price + i)
        down_strike.append(open_price -i)
        
    strikes = down_strike + up_strike
    return strikes

def generate_options_symbol(tickername,expirydate,strikeprice,op_type):
     
    symbol1 = tickername
    symbol2 = str(expirydate)
    symbol2a = symbol2[2:4]
    symbol2b = symbol2[5:7]
    symbol2c = symbol2[-2:]
    symbol3 =  str(strikeprice)   
    final_sym = symbol1+symbol2a+symbol2b+symbol2c+symbol3+op_type 
    return final_sym 

given_date= input("Enter a date:")
#start_date = datetime.date(2022,9,1) # convert into datetime.date
start_date = pandas.Timestamp(given_date).date()
end_date = start_date + relativedelta(days=1)
print(start_date,end_date) 
#################### get data commands ##################################################

#tickers_list = list(get_NSE_tickers().keys())
tickers_list = ["NIFTY","BANKNIFTY"]
final_df_call = pandas.DataFrame()
final_df_put = pandas.DataFrame()
final_df = pandas.DataFrame()
fut_final_df = pandas.DataFrame()
date = start_date

call_options_list=[]
put_options_list=[]
while date < end_date:
    for t in tickers_list:
        try:
            print("-------------------------------- STARTING --------------------------------")
            start_time=datetime.datetime(date.year,date.month,date.day,9,15,0)
            end_time = datetime.datetime(date.year,date.month,date.day,15,30,0)
            symbol = t+"-I"
            z = td_obj.get_historic_data(symbol,start_time=start_time,end_time=end_time,bar_size='eod')
            if z:
                print(z,t)
                open_price = round(z[0]["o"]/100)*100
                #print(open_price)
                futures = td_obj.get_historic_data(symbol,start_time=start_time,end_time=end_time)
                #if futures 
                df_fut = pandas.DataFrame(futures)
                df_fut = df_fut.rename(columns ={"time":"datetime","o":"open","h":"high","l":"low","c":"close","v":"volume","oi":"open_interest"})
                df_fut["Ticker"] = t
                df_fut["Expiry_dates"] = "NA"
                df_fut = df_fut.loc[:,["Ticker","Expiry_dates","datetime","open","high","low","close","volume","open_interest"]]
                fut_final_df = pandas.concat([fut_final_df,df_fut],ignore_index=True)
            #time.sleep(1)
            exp_count=0
            exp_list=get_expiry(date)
            print(exp_list,end=" ")
            for dt in get_expiry(date):
                print("EXPIRY DATE IN LOOP -----------------",dt)
                if exp_count >0:
                    break
                print(dt<date)
                print(dt," DataType:",type(dt))
                print(date," DataType:",type(date))
                if dt < date: 
                    continue
                for strike in get_option_strikes(t,open_price):
                    try:
                        #print(dt,strike)
                        ##### call data ####################################################################
                        sym_call = generate_options_symbol(t,dt,strike,"CE")
                        call_options_list.append(sym_call)
                        # op_data_call = td_obj.get_historic_data(sym_call,start_time=start_time,end_time=end_time)
                        # time.sleep(0.1)
                        # #print("...............................")
                        # #print(op_data_call)#time.sleep(1)
                        # if op_data_call:
                        #     df_call = pandas.DataFrame(op_data_call)
                        #     df_call = df_call.rename(columns ={"time":"datetime","o":"open","h":"high","l":"low","c":"close","v":"volume","oi":"open_interest"})
                        #     df_call["Ticker"] =  sym_call
                        #     df_call["Expiry_dates"] = dt
                        #     df_call["Underlying"] = t
                        #     df_call = df_call.loc[:,["Underlying","Ticker","Expiry_dates","datetime","open","high","low","close","volume","open_interest"]]
                        #     final_df = pandas.concat([final_df,df_call],ignore_index=True)

                        #### put data #######################################################################
                        sym_put = generate_options_symbol(t,dt,strike,"PE")
                        put_options_list.append(sym_put)
                        # op_data_put = td_obj.get_historic_data(sym_put,start_time=start_time,end_time=end_time)
                        # time.sleep(0.1)
                        # #print("...............................")
                        # #print(op_data_put)#time.sleep(1) 
                        # if op_data_put:
                        #     df_put = pandas.DataFrame(op_data_put)
                        #     df_put = df_put.rename(columns ={"time":"datetime","o":"open","h":"high","l":"low","c":"close","v":"volume","oi":"open_interest"})
                        #     df_put["Ticker"] =  sym_put
                        #     df_put["Expiry_dates"] = dt
                        #     df_put["Underlying"] = t
                        #     df_put = df_put.loc[:,["Underlying","Ticker","Expiry_dates","datetime","open","high","low","close","volume","open_interest"]]
                        #     final_df = pandas.concat([final_df,df_put],ignore_index=True)

                        #print(dt,strike,sym_call,sym_put)
                        # print("---------final_df-------------")
                        # print(final_df)
                        # print(fut_final_df)
                        exp_count+=1

                    except:
                        print("options data not found")
                        time.sleep(0.5)
                
        except Exception as e:
            print(repr(e))
            print("FUTURES DATA NOT FOUND")
            time.sleep(0.5)
            
    date+=relativedelta(days=1)  
    
#final_df.to_csv("/home/rahul/lab/Shubham/ipcque/true_data_test.csv", index=False)    
#fut_final_df.to_csv("/home/rahul/lab/Shubham/ipcque/true_data_test.csv", index=False)
print("LIST OF CALL OPTIONS")
print(call_options_list)
print("--------------------------------------------------------------------------------------------")
print("LIST OF PUT OPTIONS")
print(put_options_list)

# if final_df:
#     final_df.to_csv("options.csv",index = False)
#     fut_final_df.to_csv("futures.csv",index =False)

    
    #print(final_df)
#print(fut_final_df)
