import sys
import subprocess
import os 

from SharePointv2.Sharepoint_API import GRAPH_API

import pandas as pd
import numpy as np
import plotly.express as px
from bs4 import BeautifulSoup 
import requests
from datetime import datetime as dt

from RGGI_plant_analysis import RGGI_capacity
from Analyse_PJM_generation import clean_historical_generators,calculate_historical_PJM_share
from Analyse_PJM_capacity import run_full_relative_capacity_PJM

def run_full_generators_assessment_and_RGGI_PJM_proportion(location,read_latest):
    ## Capacity forecasts - retirements & additions
    historical_and_forecast,pjm_planned_retired,isne_planned_retired,nyis_planned_retired  = run_full_relative_capacity_PJM(location,read_latest=read_latest)
    rggi_share_pjm_capacity_fig = px.line(historical_and_forecast,x='Date',y='RGGI_share',color='PJM_tech',title='RGGI Share of PJM Capacity')
    monthly_capacity_change_pct = historical_and_forecast.groupby(['report_year','report_month','PJM_tech'])['RGGI_share'].last().unstack('PJM_tech').pct_change().fillna(0)
    
    ## Now looking at generator share
    gen_df = clean_historical_generators(location)

    pjm, rggi_share = calculate_historical_PJM_share(gen_df)
    
    last_3_year_monthly_avg = rggi_share.dropna(subset=['RGGI_share']).query('YEAR>2020 and YEAR<2024').reset_index().groupby(['month','PJM_tech'])['RGGI_share'].mean().unstack('PJM_tech')
    
    ## moDELLING FORWARD
    forward_forecast_df = pd.DataFrame()

    cumulative_capacity_change = (monthly_capacity_change_pct+1).cumprod()
    for x in range(2024,2031):
        #if x == 2024:
        mini_df = last_3_year_monthly_avg * cumulative_capacity_change.loc[x]
        mini_df=mini_df.fillna(0)
        mini_df['YEAR']=x
        forward_forecast_df = pd.concat([forward_forecast_df,mini_df])
        
    forward_forecast_df=forward_forecast_df.reset_index()
    forward_forecast_df['Date'] = (pd.to_datetime(forward_forecast_df['YEAR'].astype(str)+'/'+forward_forecast_df['month'].astype(str)+'/1',format='%Y/%m/%d')+pd.offsets.MonthEnd(0))
    forward_forecast_df.sort_values('Date')
    
    ### Filter out this year as YTD generator files missing some key generators, making proportion calcs incorrect

    forward_forecast_df = forward_forecast_df.query(f'Date>="{dt.today().year}"').groupby(['YEAR','month','Date']).last().stack('PJM_tech').to_frame('RGGI_share')
    full_time_series = pd.concat([rggi_share.query('Date<"2024"'),forward_forecast_df])
    rggi_share_pjm_generation_fig =  px.line(full_time_series.reset_index().pivot(index='Date',columns='PJM_tech',values='RGGI_share'),
                                        labels={'value':'RGGI Share PJM power generation by fuel'})
    rggi_share_pjm_generation_fig.update_layout(yaxis_tickformat='.2%')
    folder= GRAPH_API(location).find_uniquefolder('/Corporate/Shared Analysis/RGGI_ISO_power_data/PJM/EIA_data')
    resp = GRAPH_API(location).save_df_as_csv(full_time_series,os.environ['CCap_docs'],folder,'RGGI_share_PJM.csv')

    return rggi_share, rggi_share_pjm_capacity_fig,rggi_share_pjm_generation_fig,full_time_series,gen_df,pjm_planned_retired,isne_planned_retired,nyis_planned_retired 