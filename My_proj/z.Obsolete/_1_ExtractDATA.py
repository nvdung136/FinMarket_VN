from _0_Database_API import Full_flow,DB_Connect
import pandas as PD
from os import path as Path
import time
import re

dir_path = Path.dirname(Path.realpath(__file__))

def extract_VN100():
    file_path = Path.join(dir_path,Path.normpath('./Companies_List.csv'))
    # print(file_path)
    DFrame = PD.read_csv(file_path)
    DFrame = DFrame[['ticker','VN100']]
    VN100 = DFrame[DFrame['VN100'] == True]
    SymbolList = VN100['ticker'].squeeze()
    return SymbolList

if(__name__):
    DB = DB_Connect()
    DB_cur = DB.cursor()
    SymbolList = extract_VN100()
    ## Write data to the DB 
    # count = 1
    # for sym in SymbolList:
    #     print(count)
    #     Full_flow(DB,DB_cur,sym)
    #     time.sleep(3)
    #     count += 1 
    # print(sym)

    # Eliminate the duplicate record date
    for sym in SymbolList:
        print(sym)
        sqline = 'SELECT _DATE FROM {} GROUP BY _DATE HAVING count(_DATE) > 1'.format(sym)
        Dup_check = DB_cur.execute(sqline)
        for dup in Dup_check:
            DATE = str(dup)
            DATE = "'" + DATE[2:-3] + "'"
            sqline2 = 'SELECT * FROM {} WHERE _DATE = {}'.format(sym,DATE)
            dupline = DB_cur.execute(sqline2)
            volume = 0
            for line in dupline:
                if(line[5]>volume):
                    WrtObj = line
                    volume = line[5]
            DEL_sql = 'DELETE FROM {} WHERE _DATE = {}'.format(sym,DATE)
            DB_cur.execute(DEL_sql)
            WRT_sql = 'INSERT INTO {} VALUES {}'.format(sym,WrtObj)
            DB_cur.execute(WRT_sql)
            DB.commit()
    DB.close()