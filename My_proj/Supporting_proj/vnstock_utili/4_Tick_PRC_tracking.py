import vnstock 
import pandas as pd
import sqlite3
from IPython.display import display
import time
import sys

# Connect to the database
conn_CW = sqlite3.connect("C:\\Users\\nvdun\\OneDrive\\Desktop\\Beta_database_des.db")
cursor_CW = conn_CW.cursor()

def main():
    Qu_CW,Qu_UND = Qu_line_Get()
    price_update(Qu_CW,Qu_UND)
    conn_CW.commit()
    return


def Qu_line_Get():
    query = f"SELECT Ticker FROM WATCH_LIST"
    cursor_CW.execute(query)
    CW_TCK_LST = cursor_CW.fetchall()
    query_UND = f"SELECT DISTINCT UNDER_TCK FROM WATCH_LIST"
    cursor_CW.execute(query_UND)
    _UND_TCK_ = cursor_CW.fetchall()
    Qu_CW = ""
    Qu_UND = ""
    for CW_TCK in CW_TCK_LST:
        Qu_CW += f"{CW_TCK[0]},"
    for TCK in _UND_TCK_:
        Qu_UND += f"{TCK[0]},"
    return Qu_CW,Qu_UND

def price_update(Qu_CW,Qu_UND):
    PRC_CW_DF = vnstock.trading.price_depth(Qu_CW)[['Mã CP','Giá khớp lệnh']]
    PRC_CW_DF = PRC_CW_DF.rename(columns={'Mã CP': 'Ticker','Giá khớp lệnh': 'Price' })
    PRC_CW_DF = PRC_CW_DF[PRC_CW_DF['Price'] != 0]
    update_CW = """
    UPDATE WATCH_LIST
    SET Price = :value1
    WHERE Ticker = :id
    """
    for index, row in PRC_CW_DF.iterrows():
        data = {'value1': row['Price'], 'id': row['Ticker']}
        conn_CW.execute(update_CW, data)
    
    PRC_UND_DF = vnstock.trading.price_depth(Qu_UND)[['Mã CP','Giá khớp lệnh']]
    PRC_UND_DF = PRC_UND_DF.rename(columns={'Mã CP': 'Ticker','Giá khớp lệnh': 'Price' })
    PRC_UND_DF['Price'] = PRC_UND_DF['Price']/1000  
    PRC_UND_DF = PRC_UND_DF[PRC_UND_DF['Price'] != 0]  
    update_UND = """
    UPDATE WATCH_LIST
    SET Spot_PRC = :value1
    WHERE UNDER_TCK = LOWER(:id)
    """
    for index, row in PRC_UND_DF.iterrows():
        data = {'value1': row['Price'], 'id': row['Ticker']}
        conn_CW.execute(update_UND, data)

    print(PRC_UND_DF)
    print(PRC_CW_DF)
    return

if __name__ == '__main__':
    while(True):
        main()
        time.sleep(5)