import vnstock
import sqlite3
import pandas as pd
from datetime import datetime

def main():
    today = datetime.today().strftime('%Y-%m-%d')
    print(f'Today is {today}')
    conn = sqlite3.connect("C:/Users/nvdun/OneDrive/3.Programing and tools/0. Git repository/FinMarket_VN/My_proj/Supporting_proj/vnstock_utili/VN_stock_MasterDATA.db")
    cursor = conn.cursor()
    List_ = cursor.execute('SELECT ticker FROM Profile_').fetchall()
    List_TCK = []
    for TCK in List_:
        List_TCK.append(TCK[0])


    # Add Proxy price
    cols_name = ['ticker','DATE','Last_PRC','AVG_3_PRC','AVG_5_PRC','AVG_10_PRC','AVG_20_PRC','Last_VOL','AVG_3_VOL','AVG_5_VOL','AVG_10_VOL','AVG_20_VOL']
    Proxy_Prc_ = pd.DataFrame(columns=cols_name)
    for tck in List_TCK:
        DF_trading = vnstock.stock_historical_data(tck,start_date='2023-11-30',end_date=today,resolution = '1D')
        Date = DF_trading.iloc[-1,0]
        Last_CLS = DF_trading.iloc[-1,4]
        Last_Vol = DF_trading.iloc[-1,5]
        AVG_3_CLS = round(DF_trading.iloc[-3:,4].sum()/3,0)
        AVG_5_CLS = round(DF_trading.iloc[-5:,4].sum()/5,0)
        AVG_10_CLS = round(DF_trading.iloc[-10:,4].sum()/10,0)
        AVG_20_CLS = round(DF_trading.iloc[-20:,4].sum()/20,0)
        AVG_3_VOL = round(DF_trading.iloc[-3:,5].sum()/3,0)
        AVG_5_VOL = round(DF_trading.iloc[-5:,5].sum()/5,0)
        AVG_10_VOL = round(DF_trading.iloc[-10:,5].sum()/10,0)
        AVG_20_VOL = round(DF_trading.iloc[-20:,5].sum()/20,0)
        print(f'TCK: {tck}')
        data_arr = [tck,Date,Last_CLS,AVG_3_CLS,AVG_5_CLS,AVG_10_CLS,AVG_20_CLS,Last_Vol,AVG_3_VOL,AVG_5_VOL,AVG_10_VOL,AVG_20_VOL]
        new_row_df = pd.DataFrame([data_arr],columns= cols_name)
        Proxy_Prc_= pd.concat([Proxy_Prc_,new_row_df],ignore_index=True)
    print('\n\nFinish Updating price')
    Proxy_Prc_.to_sql('Proxy_PRC',conn,if_exists='replace',index=False)
    

if __name__ == '__main__':
    main()