import vnstock
import sqlite3
import pandas as pd
from datetime import datetime
import time

def main():
    today = datetime.today().strftime('%Y-%m-%d')
    print(f'Today is {today}')
    conn = sqlite3.connect("C:/Users/nvdun/OneDrive/3.Programing and tools/0. Git repository/FinMarket_VN/My_proj/Supporting_proj/vnstock_utili/VN_stock_MasterDATA.db")
    cursor = conn.cursor()
    List_ = cursor.execute('SELECT ticker FROM Profile_').fetchall()
    List_TCK = []
    for TCK in List_:
        List_TCK.append(TCK[0])
    print('Giving company TCK')
    tck = input()
    if (tck in List_TCK):
        print(f'Warning {tck} is in the DB')
        input()
        print('Input (y) to replace current record')
        rplc = input()
        if(rplc.lower() == 'y'):
            Remove_record(tck,cursor)
            conn.commit()
        else:    
            return
    print(f'New ticker {tck} ...')
    DF_Profile = vnstock.company_overview(tck)
    DF_ = vnstock.company_large_shareholders(tck)
    DF_.drop(DF_.index[-1:],inplace=True)
    Top_9_SHolder = DF_.sum(axis=0)[2]
    if(Top_9_SHolder>1): 
        Top_9_SHolder = 1
    DF_.drop(DF_.index[-4:],inplace=True)
    Top_5_SHolder = DF_.sum(axis=0)[2]
    if(Top_5_SHolder>1): 
        Top_5_SHolder = 1
    DF_Profile['T9_SHolder']=Top_9_SHolder
    DF_Profile['T5_SHolder']=Top_5_SHolder
    Free_float = DF_Profile.iloc[0,6]/DF_Profile.iloc[0,7]
    if(Free_float>1):
        Free_float = 1
    DF_Profile['Free_float'] = Free_float
    print(DF_Profile.transpose())
    print(f'Press enter to add {tck} to database')
    input()
    DF_Profile.to_sql('Profile_',conn,if_exists='append',index=False)

    # Add Balance sheet
    DF_Q_BS = pd.DataFrame()
    DF_FStatement = vnstock.financial_flow(tck,'balancesheet','quarterly',get_all=False)
    DF_FStatement.reset_index(inplace=True)
    DF_Q_BS = pd.concat([DF_Q_BS,DF_FStatement],ignore_index=True)
    DF_Q_BS.to_sql('BalanceSheet_Q',conn,if_exists='append',index=False)
    print('Added BalanceSheet quarterly ... ')
    DF_Y_BS = pd.DataFrame()
    DF_FStatement = vnstock.financial_flow(tck,'balancesheet','yearly',get_all=False)
    DF_FStatement.reset_index(inplace=True)
    DF_Y_BS = pd.concat([DF_Y_BS,DF_FStatement],ignore_index=True)
    DF_Y_BS.to_sql('BalanceSheet_Y',conn,if_exists='append',index=False)
    print('Added BalanceSheet annually ... \n')

    # Add Profit and Loss
    DF_Q_PnL = pd.DataFrame()
    DF_FStatement = vnstock.financial_flow(tck,'incomestatement','quarterly',get_all=False)
    DF_FStatement.reset_index(inplace=True)
    DF_Q_PnL = pd.concat([DF_Q_PnL,DF_FStatement],ignore_index=True)
    DF_Q_PnL.to_sql('PnL_Q',conn,if_exists='append',index=False)
    print('Added income statement quarterly ... ')
    DF_Y_PnL = pd.DataFrame()
    DF_FStatement = vnstock.financial_flow(tck,'incomestatement','yearly',get_all=False)
    DF_FStatement.reset_index(inplace=True)
    DF_Y_PnL = pd.concat([DF_Y_PnL,DF_FStatement],ignore_index=True)
    DF_Y_PnL.to_sql('PnL_Y',conn,if_exists='append',index = False)
    print('Added income statement annually ... \n')

    # Add Cashflow
    DF_Q_CF = pd.DataFrame()
    DF_FStatement = vnstock.financial_flow(tck,'cashflow','quarterly',get_all=False)
    DF_FStatement.reset_index(inplace=True)
    DF_Q_CF = pd.concat([DF_Q_CF,DF_FStatement],ignore_index=True)
    DF_Q_CF.to_sql('CF_Q',conn,if_exists='append',index = False)
    print('Added cash flow quarterly ...')
    DF_Y_CF = pd.DataFrame()
    DF_FStatement = vnstock.financial_flow(tck,'cashflow','yearly',get_all=False)
    DF_FStatement.reset_index(inplace=True)
    DF_Y_CF = pd.concat([DF_Y_CF,DF_FStatement],ignore_index=True)
    DF_Y_CF.to_sql('CF_Y',conn,if_exists='append',index = False)
    print('Added cash flow annually ... \n')

    # Add Proxy price
    cols_name = ['ticker','DATE','Last_PRC','AVG_3_PRC','AVG_5_PRC','AVG_10_PRC','AVG_20_PRC','Last_VOL','AVG_3_VOL','AVG_5_VOL','AVG_10_VOL','AVG_20_VOL']
    Proxy_Prc_ = pd.DataFrame(columns=cols_name)

    DF_trading = vnstock.stock_historical_data(tck,start_date='2023-02-14',end_date=today,resolution = '1D')
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
    Proxy_Prc_.to_sql('Proxy_PRC',conn,if_exists='append',index=False)
    print(f'Finish add {tck} to the database')
    
def Remove_record(Ticker: str,cursor: sqlite3.Cursor):
    tbl_list = ['Profile_','BalanceSheet_Q','BalanceSheet_Y','CF_Q','CF_Y','PnL_Q','PnL_Y','Proxy_PRC']
    sql = ""
    for tbl in tbl_list:
        sql = f"DELETE FROM {tbl} WHERE ticker = '{Ticker}';"
        print(sql)
        cursor.execute(sql)
        time.sleep(0.5)
    return

if __name__ == '__main__':
    main()