import pandas as pd 
import json 
import requests 
import tqdm 

df = pd.read_csv('./Datasets/apr_to_may.csv')
df['cx-cy'] = df.apply(lambda record: str(record.cx) + '-' + str(record.cy), axis =1 )

def weatherCall():
    
    co_ordinates = [(cxcy.split('-')[0], cxcy.split('-')[1]) for cxcy in df['cx-cy'].unique()]
    dfs = []
    for cx, cy in tqdm.tqdm(co_ordinates):

        json_obj = json.loads(requests.get(f'https://archive-api.open-meteo.com/v1/archive?latitude={cy}&longitude={cx}&start_date=2023-01-01&end_date=2023-05-31&daily=weathercode,temperature_2m_max,temperature_2m_min,temperature_2m_mean,apparent_temperature_max,apparent_temperature_min,apparent_temperature_mean,sunrise,sunset,shortwave_radiation_sum,precipitation_sum,rain_sum,snowfall_sum,precipitation_hours,windspeed_10m_max,windgusts_10m_max,winddirection_10m_dominant,et0_fao_evapotranspiration&timezone=Asia%2FSingapore').content)
        data = pd.DataFrame(json_obj['daily']) 
        data['cx'] = cx
        data['cy'] = cy
        dfs.append(data)

    return pd.concat(dfs)

weather = weatherCall()
weather.to_csv('Datasets/weather_apr_to_may.csv', index=False)