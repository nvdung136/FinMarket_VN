from vnstock import stock_historical_data  
import pandas

df =  stock_historical_data(symbol='FPT', 
                            start_date='2020-01-01', 
                            end_date='2023-08-03', resolution='1D', type='stock')
type(df["ticker"])