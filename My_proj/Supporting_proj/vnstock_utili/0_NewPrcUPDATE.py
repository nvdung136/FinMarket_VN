import vnstock 
import pandas as pd
import sqlite3
import importlib
from IPython.display import display
import time
import sys

# Connect to the database
conn_CW = sqlite3.connect("C:\\Users\\nvdun\\OneDrive\\Desktop\\_CW_Beta_database.db")
cursor_CW = conn_CW.cursor()

def main():
    module = importlib.import_module('1_2_OptionExpirationCheck_DONE')
    module.main()
    SYM_List = []

    print("press ENTER to UPDATE ALL DATABSE \nElse TYPE ticker name and ENTER")
    InPUT = input()
    # Read table names to list
    match InPUT:
        case '':
            TBL_name = cursor_CW.execute("SELECT name FROM sqlite_master WHERE type='table' EXCEPT SELECT 'CWList_MASTER' FROM sqlite_master WHERE type='table';").fetchall()
            for TBL in TBL_name:
                SYM_List.append(TBL[0])
        case '1':
            SYMS = ['hpg','mwg','pdr','tcb','fpt','msn']
            for SYM in SYMS:
                SYM_Norm = SYM.upper()
                SYM_List.append(SYM_Norm)
        case _:
            SYMS = InPUT.split(';')
            for SYM in SYMS:
                SYM_Norm = SYM.upper()
                SYM_List.append(SYM_Norm)

    for SYM in SYM_List:
        try:
            Qu_CW = Qu_line_Get(SYM)
            price_update(Qu_CW)
            print(f'Done update {SYM}')
        except:
            print(f'Error in updating {SYM}')
    print('Update done ... enter to exit')
    conn_CW.commit()
    conn_CW.close()
    input()
    return


def Qu_line_Get(SYM):
    query = f"SELECT Ticker FROM {SYM}"
    cursor_CW.execute(query)
    CW_TCK_LST = cursor_CW.fetchall()
    Qu_CW = ""
    for CW_TCK in CW_TCK_LST:
        Qu_CW += f"{CW_TCK[0]},"
    return Qu_CW

def price_update(Qu_CW,SYM):
    PRC_CW_DF = vnstock.trading.price_depth(Qu_CW)[['Mã CP','Giá khớp lệnh']]
    PRC_CW_DF = PRC_CW_DF.rename(columns={'Mã CP': 'Ticker','Giá khớp lệnh': 'Price' })
    PRC_CW_DF = PRC_CW_DF[PRC_CW_DF['Price'] != 0]
    update_CW = """
    UPDATE :SYM
    SET Price = :value1
    WHERE Ticker = :id
    """
    for index, row in PRC_CW_DF.iterrows():
        data = {'SYM':SYM, 'value1': row['Price'], 'id': row['Ticker']}
        conn_CW.execute(update_CW, data)
    return

if __name__ == '__main__':
    while(True):
        main()
        time.sleep(5)