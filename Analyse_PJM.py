import pandas as pd
import numpy as np
import plotly.express as px
from bs4 import BeautifulSoup 
import requests
from datetime import datetime as dt
import plotly.subplots as sp
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pandas.tseries.offsets import MonthEnd
from zipfile import ZipFile
from io import BytesIO
from urllib.request import urlopen

from RGGI_plant_analysis import RGGI_capacity

RGGI_states = ['CT', 'DE', 'ME', 'MD', 'MA', 'NH', 'NJ', 'NY', 'RI', 'VT'] 


tech_convert_dict = {'Petroleum Liquids':'Oil',
                    'Onshore Wind Turbine':'Wind',
       'Conventional Hydroelectric':'Hydro', 
       'Conventional Steam Coal':'Coal',
       'Natural Gas Fired Combined Cycle':'Gas', 
       'Natural Gas Steam Turbine':'Gas',
       'Natural Gas Fired Combustion Turbine':'Gas', 
       'Nuclear':'Nuclear',
       'Hydroelectric Pumped Storage':'Storage',
       'Natural Gas Internal Combustion Engine':'Gas', 
       'Batteries':'Storage',
       'Solar Photovoltaic':'Solar', 
       'Geothermal':'Other Renewables', 
       'Wood/Wood Waste Biomass':'Other Renewables',
       'Coal Integrated Gasification Combined Cycle':'Gas',
        'Other Gases':'Gas',
       'Petroleum Coke':'Coal', 
       'Municipal Solid Waste':'Other', 
       'Landfill Gas':'Other Renewables',
       'Natural Gas with Compressed Air Storage':'Gas', 
       'All Other':'Other',
       'Other Waste Biomass':'Other Renewables', 
       'Solar Thermal without Energy Storage':'Solar',
       'Other Natural Gas':'Gas', 
       'Solar Thermal with Energy Storage':'Solar',
       'Flywheels':'Other', 
       'Offshore Wind Turbine':'Wind', 
       'Hydrokinetic':'Hydro',
       'Other Energy Storage':'Storage'}

Fossil_tech = ['Coal','Gas','Oil']

#tCO2/MWh
simple_emissions_factor={'Coal': 1.046,
                           "Gas": 0.467,
                           "Oil": 0.867,
                           'Multiple Fuels':0.467*0.9+0.867*.1}

metric_tons_to_short_tons = 1.1023

def run_historical_RGGI_share(read_latest):
    time_series_historical_capacity_with_tech = RGGI_capacity().save_historical_capacity_per_plant(years_back=6,read_latest=read_latest)
    time_series_historical_capacity_with_tech = time_series_historical_capacity_with_tech.reset_index()
    time_series_historical_capacity_with_tech['PJM_tech'] = time_series_historical_capacity_with_tech['Technology'].map(tech_convert_dict)
    time_series_historical_capacity_with_tech['Fossil'] = np.where(time_series_historical_capacity_with_tech['PJM_tech'].isin(Fossil_tech),1,0)
    dual_fuels = time_series_historical_capacity_with_tech.groupby(['Plant ID','report_month','report_year'])['PJM_tech'].nunique()
    time_series_historical_capacity_with_tech = time_series_historical_capacity_with_tech.join(dual_fuels.to_frame('dual_fossil'),on=['Plant ID','report_month','report_year'])
    time_series_historical_capacity_with_tech['PJM_tech'] = np.where(time_series_historical_capacity_with_tech['dual_fossil']>1,'Multiple Fuels',time_series_historical_capacity_with_tech['PJM_tech'])
    time_series_historical_capacity_with_tech['RGGI_state'] = np.where(time_series_historical_capacity_with_tech['Plant State'].isin(RGGI_states),1,0)
    time_series_historical_capacity_with_tech['RGGI_eligible'] = np.where((time_series_historical_capacity_with_tech['RGGI_state']==1) & (time_series_historical_capacity_with_tech['Nameplate Capacity (MW)']>=25) ,1,0) #

    ### RGGI Share of PJM Capacity
    pjm_rggi_capacity_compare = time_series_historical_capacity_with_tech.query('`Balancing Authority Code`=="PJM"').groupby(['report_year','report_month','PJM_tech','RGGI_eligible'])['Nameplate Capacity (MW)'].sum().unstack('RGGI_eligible')
    pjm_rggi_capacity_compare = pjm_rggi_capacity_compare.reset_index()
    pjm_rggi_capacity_compare['Date'] = pd.to_datetime('01/'+pjm_rggi_capacity_compare['report_month'].astype(str)+'/'+pjm_rggi_capacity_compare['report_year'].astype(str),format='%d/%m/%Y')+MonthEnd(0)
    #pjm_rggi_capacity_compare['RGGI_share'] = pjm_rggi_capacity_compare[1]/pjm_rggi_capacity_compare[[1,0]].sum(axis=1)

    #rggi_share_of_pjm_historical  = pjm_rggi_capacity_compare.groupby(['report_year','report_month','Date','PJM_tech'])['RGGI_share'].mean().to_frame().fillna(0)

    return pjm_rggi_capacity_compare,time_series_historical_capacity_with_tech

def run_future_RGGI_share():
    next_update_time,recent_report,report_month,report_year,date_of_last_report  =RGGI_capacity().scrape_recent_EIA_860m(lagged_report=2)

    ## Current capacity & retirements
    plants = pd.read_excel(recent_report,sheet_name='Operating')
    header_col = plants[(plants.iloc[:,0]=='Entity ID')==True].index[0]
    plants = pd.read_excel(recent_report,sheet_name='Operating',header=header_col+1)
    plants['report_month'] = report_month
    plants['report_year'] = report_year
    plants = plants.dropna(subset=['Plant Name'])
    PJM_plants =  plants.query(f'`Balancing Authority Code`=="PJM" ')
    
    PJM_plants['PJM_tech'] = PJM_plants['Technology'].map(tech_convert_dict)
    PJM_plants['Fossil'] = np.where(PJM_plants['PJM_tech'].isin(Fossil_tech),1,0)
    dual_fuels = PJM_plants.groupby(['Plant ID','report_month','report_year'])['PJM_tech'].nunique()
    PJM_plants = PJM_plants.join(dual_fuels.to_frame('dual_fossil'),on=['Plant ID','report_month','report_year'])
    PJM_plants['PJM_tech'] = np.where(PJM_plants['dual_fossil']>1,'Multiple Fuels',PJM_plants['PJM_tech'])
    full_tech_list = PJM_plants.PJM_tech.unique()

    PJM_plants = RGGI_capacity().analyse_RGGI_capacity(PJM_plants)

    ### Planned additions
    planned = pd.read_excel(recent_report,sheet_name='Planned',header=2)
    planned = planned.dropna(subset=['Plant ID'])
    planned['PJM_tech'] = planned['Technology'].map(tech_convert_dict)  
    planned['Fossil'] = np.where(planned['PJM_tech'].isin(Fossil_tech),1,0)
    dual_fuels = planned.groupby(['Plant ID','Planned Operation Month','Planned Operation Year'])['PJM_tech'].nunique()
    planned = planned.join(dual_fuels.to_frame('dual_fossil'),on=['Plant ID','Planned Operation Month','Planned Operation Year'])
    planned['PJM_tech'] = np.where(planned['dual_fossil']>1,'Multiple Fuels',planned['PJM_tech'])


    PJM_planned = planned.query(f'`Balancing Authority Code`=="PJM"')
    PJM_planned['rggi_state'] = np.where(PJM_planned['Plant State'].isin(RGGI_states),1,0)
    RGGI_PJM_planned = PJM_planned.query('rggi_state==1 and `Nameplate Capacity (MW)`>25 ') # 
    additions,approved_additions,not_yet_approved_additions = RGGI_capacity().analyse_RGGI_planned_capacity_PJM(PJM_planned)
    pjm_rggi_additions,pjm_rggi_approved_additions,pjm_rggi_not_yet_approved_additions = RGGI_capacity().analyse_RGGI_planned_capacity_PJM(RGGI_PJM_planned)

    PJM_plants['rggi_state'] = np.where(PJM_plants['Plant State'].isin(RGGI_capacity().RGGI_states),1,0)

    PJM_RGGI_plants = PJM_plants.query('rggi_state==1 and `Nameplate Capacity (MW)`>25 ') #
    pjm_plus_retirements_plus_additions_timeseries, pjm_time_series_capacity_fig = RGGI_capacity().estimated_timeseries_capacity_PJM(PJM_plants,full_tech_list,additions,date_of_last_report,PJM_retiredates=True)
    
    rggi_pjm_rggi_plus_retirements_plus_additions_timeseries, rggi_pjm_time_series_capacity_fig = RGGI_capacity().estimated_timeseries_capacity_PJM(PJM_RGGI_plants,full_tech_list,pjm_rggi_additions,date_of_last_report,PJM_retiredates=True)

    """rggi_share_pjm = pjm_plus_retirements_plus_additions_timeseries.stack().to_frame('PJM').join(rggi_pjm_rggi_plus_retirements_plus_additions_timeseries.stack().to_frame('RGGI_PJM'))
    rggi_share_pjm = rggi_share_pjm.reset_index()
    rggi_share_pjm['PJM_tech'] = rggi_share_pjm['level_1'].map(tech_convert_dict)
    rggi_share_pjm = rggi_share_pjm.groupby(['level_0','PJM_tech'])[['PJM','RGGI_PJM']].sum()
    rggi_share_pjm['RGGI_share_PJM'] = rggi_share_pjm['RGGI_PJM']/rggi_share_pjm['PJM']
    rggi_share_pjm['RGGI_share_PJM'] = rggi_share_pjm['RGGI_share_PJM'].fillna(0) """

    return rggi_pjm_rggi_plus_retirements_plus_additions_timeseries,pjm_plus_retirements_plus_additions_timeseries,PJM_plants

def run_full_relative_capacity_PJM(read_latest):
    historical_rggi_share_pjm,test_df = run_historical_RGGI_share(read_latest=read_latest)
    historical_rggi_share_pjm['RGGI_share'] = historical_rggi_share_pjm[1]/historical_rggi_share_pjm[[1,0]].sum(axis=1)
    rggi_pjm_rggi_plus_retirements_plus_additions_timeseries,pjm_plus_retirements_plus_additions_timeseries,PJM_plants =  run_future_RGGI_share()
    rggi_share_pjm = pjm_plus_retirements_plus_additions_timeseries.iloc[1:].stack().to_frame('PJM').join(rggi_pjm_rggi_plus_retirements_plus_additions_timeseries.iloc[1:].stack().to_frame('RGGI_PJM'))
    rggi_share_pjm = rggi_share_pjm.reset_index()

    rggi_share_pjm['RGGI_share'] = rggi_share_pjm['RGGI_PJM']/rggi_share_pjm['PJM']
    rggi_share_pjm['report_month'] = rggi_share_pjm.level_0.dt.month
    rggi_share_pjm['report_year'] = rggi_share_pjm.level_0.dt.year
    rggi_share_pjm['level_0'] = rggi_share_pjm['level_0']+pd.offsets.MonthEnd(0)
    rggi_share_pjm = rggi_share_pjm.rename(columns={'level_1':'PJM_tech',
                                                    'level_0':'Date'})
    historical_and_forecast = pd.concat([historical_rggi_share_pjm,rggi_share_pjm[['Date','PJM_tech','RGGI_share','report_month','report_year']]],axis=0,join='outer')#.to_csv('RGGI_share_PJM.csv')

    return historical_and_forecast