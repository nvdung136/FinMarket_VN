import sqlite3 
import numpy as np
import pandas as pd

def main():
    # Initiation 
    SYM_List = []
    conn = sqlite3.connect('C:\\Users\\nvdun\\OneDrive\\Desktop\\Beta_database.db')
    cursor = conn.cursor()
    # Import SQL database to DF
    Ticker = 'hpg'
    query = f'SELECT * FROM {Ticker}'
    DF_ = pd.read_sql(query,conn)
    #Convert Date obj into date datatype + Refine database with 2000 records 
    DF_['Date'] = pd.to_datetime(DF_['Date'], format='%d/%m/%Y')
    DF_ = DF_[:2000]
    Process_DF(DF_)

def Process_DF(DF_:pd.DataFrame):
    # Re-adjusting close price
    DF_['Adjusted_CLS'] = DF_.apply(lambda row: row['Cls'] if row['Adj_Cls'] == 0 else row['Adj_Cls'],axis=1)
    _list_DF = []
    Interval = list(range(1,11,1))
    for num in Interval:
        Index = list(range(0,2000,num))
        _df_ = DF_.loc[Index]
        DF_cal = _df_[['Date','Adjusted_CLS']]
        DF_cal.to_csv('output.csv',index=False)
        # Sorting following from the oldest to the newest records
        DF_cal.sort_values(by='Date',inplace=True,ascending=True,ignore_index=True)
        # Calculate return each day + assign the first record as 0 return
        DF_cal['Diff'] = DF_cal['Adjusted_CLS'].diff()
        DF_cal.loc[0,'Diff'] = 0
        # Calculate return as percentage
        DF_cal['Return']=DF_cal['Diff']/DF_cal['Adjusted_CLS']
        # Calculate cumulative return
        DF_cal['Cuml'] = DF_cal['Adjusted_CLS']/DF_cal.loc[0,'Adjusted_CLS'] 
        # Assign highest value
        DF_cal['Highest'] = DF_cal['Cuml'].cummax()
        # Calculate drawdown
        DF_cal['DrawDown'] = (DF_cal['Cuml']-DF_cal['Highest'])/DF_cal['Highest']
        # Ranking all the records
        print(DF_cal)
        DF_VaR = DF_cal[['Date','Return']].sort_values(by='Return',ignore_index=True,ascending=False)
        DF_SHAPE = DF_VaR.shape[0]
        # Assign risk/gain level 
        VaR_pct = [1,0.99,0.95,0.90,0.1,0.05,0.01]
        VaR_idx = list(map(lambda x: int(x*DF_SHAPE-1), VaR_pct))
        VaR_idx.append(0)
        DF_VaR = DF_VaR.loc[VaR_idx,:]
        print(f'\n\nInterval = {num}')
        print(DF_VaR)
    return

if __name__ == "__main__":
    main()