from d2l import tensorflow as d2l 
import tensorflow as tf 
import pandas as pd 
import numpy as np 
import seaborn as sns 
import IPython.display as dsp 
import pickle 
import tqdm 

import requests 
import concurrent.futures 
import json 

class LightningDataLoader(d2l.DataModule):
    
    def __init__(self, batch_size, filepath = "./Datasets/"\
                "lightning_strikes_with_cx_cy_corrected.csv", weather_call=False):
        
        self.save_hyperparameters()
        self.dataset = None 
        self.df = pd.read_csv(filepath)
        self.df['primary_key'] = self.df.apply(
            lambda X:"".join([X.date, X['cx-cy']]), axis=1)
        
        if self.weather_call: self.weather_data = self.weather_api(self.df, "2021-01-01", "2021-12-31")

@d2l.add_to_class(LightningDataLoader)
def weather_api(self, df, start_date, end_date):
    
    co_ordinates = [(cxcy.split('-')[0], cxcy.split('-')[1]) for cxcy in df['cx-cy'].unique()]
    dfs = []
    for cx, cy in tqdm.tqdm(co_ordinates):

        json_obj = json.loads(requests.get(f'https://archive-api.open-meteo.com/v1/archive?latitude='\
                                    '{cy}&longitude={cx}&start_date={start_date}&end_date={end_date}'\
                                    '&daily=weathercode,temperature_2m_max,temperature_2m_min,'\
                                    'temperature_2m_mean,apparent_temperature_max,apparent_'\
                                    'temperature_min,apparent_temperature_mean,sunrise,sunset,'\
                                    'shortwave_radiation_sum,precipitation_sum,rain_sum,snowfall_sum,'\
                                    'precipitation_hours,windspeed_10m_max,windgusts_10m_max,'\
                                    'winddirection_10m_dominant,et0_fao_evapotranspiration&timezone=Asia%2FSingapore').content)
        data = pd.DataFrame(json_obj['daily']) 
        data['cx'] = cx
        data['cy'] = cy
        dfs.append(data)

    return pd.concat(dfs)


@d2l.add_to_class(LightningDataLoader)
def data_compiler(self):
    self.df = pd.read_csv(self.filepath)
    self.df['primary_key'] = self.df.apply(
            lambda X:"".join([X.date, X['cx-cy']]), axis=1)
    self.weather_data['primary_key'] = self.weather_data.apply(
        lambda X: "".join([X.time, "-".join([X.cx, X.cy])]), axis=1)
    
    date_range = pd.date_range(start=self.df['date'].min(), 
                            end=self.df['date'].max(), freq='D')

    # Get all unique cx-cy combinations
    co_ordinates = self.df['cx-cy'].unique()

    # Create a DataFrame with all combinations of dates and cx-cy
    all_combinations = pd.MultiIndex.from_product(
        [date_range, co_ordinates], names=['date', 'cx-cy'])
    df_all_combinations = pd.DataFrame(index=all_combinations).reset_index()
    df_all_combinations['date'] = df_all_combinations[
        'date'].apply(lambda X: str(X)[:10])
    df_all_combinations['primary_key'] = df_all_combinations.apply(
        lambda X: "".join([X.date, X['cx-cy']]), axis=1)

    self.df = pd.merge(df_all_combinations, self.df[[
        "primary_key", "CurrentCLS"]],
         how='left', on='primary_key').fillna({'CurrentCLS': 0})
    
    self.current_per_key = self.df.groupby('primary_key')[
        'CurrentCLS'].sum().reset_index()
    self.dataset = pd.merge(self.weather_data, 
            self.current_per_key, on='primary_key', how = 'inner')
    self.dataset.drop(columns = ["weathercode", "sunrise", 
                "sunset", "snowfall_sum", "primary_key"], inplace=True)
    
    return self.dataset 

