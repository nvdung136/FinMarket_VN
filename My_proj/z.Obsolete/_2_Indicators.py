import pandas as PD
import sqlite3 as sq3
from os import path as Path
from typing import Literal
from statistics import mean 
from matplotlib import pyplot as plt

path  = Path.normpath(r'C:\Users\nvdun\OneDrive\Programing and tools\0. Git repository\FinMarket_VN\My_proj\Database\Transaction_history\Day_Trade.db')
DB = sq3.connect(path)

def Get_DF(Sym,connection):
    Sym_DataFrame = PD.read_sql_query('SELECT * FROM {}'.format(Sym),connection)
    return Sym_DataFrame

_Types = Literal['all','_CLOSE','_OPEN','_HIGH','_LOW']

def Simple_MA(DF: PD.DataFrame,duration : int =10 ,_On_ : _Types = '_CLOSE'):
    if _On_ == 'all':
        Primative_DF = DF 
    else :
        Primative_DF = DF[['_DATE',_On_]]
    
    Middle_Series = Primative_DF.iloc[:,1].to_numpy().astype(float)
    Res_array = []
    for x in range(Middle_Series.size-(duration-1)):
        Temp_array = Middle_Series[x:x+(duration-1)]
        Res_array.append(mean(Temp_array))
    Res_DF = PD.DataFrame({'MovingAverage': Res_array})
    Fin_DF = PD.concat([Primative_DF,Res_DF],axis=1)
    Fin_DF['MovingAverage'] = Fin_DF['MovingAverage'].shift(duration - 1)
    Dim = Fin_DF.shape
    if (Dim[0]-(duration*10)) > 0: 
        plt_row = Dim[0] - (duration*10) 
    else:
        plt_row = 0 
    Fin_DF = Fin_DF.iloc[plt_row:,:]
    Fin_DF.plot(x='_DATE',y = [_On_,'MovingAverage'])
    plt.show()
    return

def main():
    print('Give stock symbol')
    sym = input()
    print('Duration ?')
    dur = input()
    sym_DF = Get_DF(str(sym),DB)
    Simple_MA(sym_DF,int(dur))
if(__name__):
    main()
