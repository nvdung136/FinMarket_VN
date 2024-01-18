import sqlite3
from os import path as Path
from vnstock import stock_historical_data  
import pandas

# DB_path 
dir_path = Path.dirname(Path.realpath(__file__))

def DB_Connect():
    sub_path = Path.normpath('Database/Transaction_history/Day_Trade.db')
    path = Path.join(dir_path,sub_path)
    DB = sqlite3.connect(path)
    return DB

def CreateTable(DB_cursor,symbol):
    try:
        SQL_line = 'CREATE TABLE {}(_DATE TEXT,_OPEN INTEGER,_HIGH INTEGER,_LOW INTEGER,_CLOSE INTEGER,_VOLUME INTEGER)'.format(symbol)
        DB_cursor.execute(SQL_line)   
    except:
        print('Table already created')
    return

# Extract and Transform DATA to the pre-determined form
# For now, data source comes from vnstock (but later we should cover other source to find the most reliable one)
def Extract_n_Transform(Symbol):
    start_date = '2020-01-01'    
    end_date = '2023-08-03'
    Prim_df = stock_historical_data(Symbol,start_date,end_date,'1D','stock')
    Sec_df = Prim_df[["time","open","high","low","close","volume"]]
    Sec_df = Sec_df.rename(columns={'time': '_DATE', 'open': '_OPEN', 'high': '_HIGH', 'low': '_LOW', 'close': '_CLOSE',  'volume': '_VOLUME'})
    return Sec_df

def Full_flow(Conn_ :sqlite3.Connection,Cur_:sqlite3.Cursor,Symbol):
    CreateTable(Cur_,Symbol)
    print("Done initiation")
    DF = Extract_n_Transform(Symbol)
    print("Done extract and transform")
    print(DF)
    DF.to_sql(Symbol,Conn_,if_exists='append',index=False)
    print("Write finishes")
    # sql_line = 'SELECT * FROM {}'.format(Symbol)
    # Load_DB = Cur_.execute(sql_line)
    # for row in Load_DB:
    #     print(row)


# if(__name__):
#     DB = DB_Connect()
#     DB_cur = DB.cursor()
#     Full_flow(DB,DB_cur,'MWG')