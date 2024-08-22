import sqlite3 
import numpy as np
import pandas as pd

def main():
    # Initiation 
    SYM_List = []
    # 1st database connection
    conn1 = sqlite3.connect('C:\\Users\\nvdun\\OneDrive\\Desktop\\Beta_database.db')
    cursor1 = conn1.cursor()
    # 2nd (write) database connection
    conn2 = sqlite3.connect('C:\\Users\\nvdun\\OneDrive\\Desktop\\Beta_adjusted_database.db')
    cursor2 = conn2.cursor()

    # Import SQL from 1st database to DF
    TBL_name = cursor1.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    Ticker_List = []
    for TBL in TBL_name:
        Ticker_List.append(TBL[0])
    for Ticker in Ticker_List:
        query = f'SELECT * FROM {Ticker}'
        Old_DF = pd.read_sql(query,conn1)
        #Convert Date obj into date datatype + Refine database with 2000 records 
        # Old_DF['Date'] = pd.to_datetime(Old_DF['Date'], format='%d/%m/%Y')
        Old_DF = Old_DF[:2000]
        NewDF = Process_DF(Old_DF)
        NewDF.to_sql(Ticker,conn2,if_exists='replace',index=False)
    print('DONE')
    input()
    return


def Process_DF(DF_:pd.DataFrame):
    # Re-adjusting close price
    DF_['Adj_Ratio'] = DF_.apply(lambda row: 1 if row['Adj_Cls'] == 0 else row['Adj_Cls']/row['Cls'],axis=1)
    DF_['Cls'] = round(DF_['Cls']*DF_['Adj_Ratio'],2)
    DF_['Opn'] = round(DF_['Opn']*DF_['Adj_Ratio'],2)
    DF_['Hgh'] = round(DF_['Hgh']*DF_['Adj_Ratio'],2)
    DF_['Low'] = round(DF_['Low']*DF_['Adj_Ratio'],2)
    DF_['Vol'] = round(DF_['Mtch_Vol'] + DF_['Ord_Vol'],2)
    print(DF_[['Date','Opn','Hgh','Low','Cls','Vol']])
    return DF_[['Date','Opn','Hgh','Low','Cls','Vol']]

    # _list_DF = []
    # Interval = list(range(1,11,1))
    # for num in Interval:
    #     Index = list(range(0,2000,num))
    #     _df_ = DF_.loc[Index]
    #     DF_cal = _df_[['Date','Adjusted_CLS']]
    #     DF_cal.to_csv('output.csv',index=False)
    #     # Sorting following from the oldest to the newest records
    #     DF_cal.sort_values(by='Date',inplace=True,ascending=True,ignore_index=True)
    #     # Calculate return each day + assign the first record as 0 return
    #     DF_cal['Diff'] = DF_cal['Adjusted_CLS'].diff()
    #     DF_cal.loc[0,'Diff'] = 0
    #     # Calculate return as percentage
    #     DF_cal['Return']=DF_cal['Diff']/DF_cal['Adjusted_CLS']
    #     # Calculate cumulative return
    #     DF_cal['Cuml'] = DF_cal['Adjusted_CLS']/DF_cal.loc[0,'Adjusted_CLS'] 
    #     # Assign highest value
    #     DF_cal['Highest'] = DF_cal['Cuml'].cummax()
    #     # Calculate drawdown
    #     DF_cal['DrawDown'] = (DF_cal['Cuml']-DF_cal['Highest'])/DF_cal['Highest']
    #     # Ranking all the records
    #     print(DF_cal)
    #     DF_VaR = DF_cal[['Date','Return']].sort_values(by='Return',ignore_index=True,ascending=False)
    #     DF_SHAPE = DF_VaR.shape[0]
    #     # Assign risk/gain level 
    #     VaR_pct = [1,0.99,0.95,0.90,0.1,0.05,0.01]
    #     VaR_idx = list(map(lambda x: int(x*DF_SHAPE-1), VaR_pct))
    #     VaR_idx.append(0)
    #     DF_VaR = DF_VaR.loc[VaR_idx,:]
    #     print(f'\n\nInterval = {num}')
    #     print(DF_VaR)
    return

if __name__ == "__main__":
    main()