import pandas as pd
import numpy as np
import plotly.express as px
from bs4 import BeautifulSoup 
import requests
from datetime import datetime as dt
import plotly.subplots as sp
from pandas.tseries.offsets import MonthEnd

from zipfile import ZipFile
from io import BytesIO
from urllib.request import urlopen
import pickle

from RGGI_plant_analysis import RGGI_capacity
from EIA_emissions_factors import download_EF_from_EIA
from SharePointv2.Sharepoint_API import GRAPH_API

RGGI_states = ['CT', 'DE', 'ME', 'MD', 'MA', 'NH', 'NJ', 'NY', 'RI', 'VT'] 


PJM_tech_dict = {'DFO':'Oil', 
                 'RFO':'Oil',
                 'WND':'Wind', 
                 'LFG':'Other Renewables', 
                 'PC':'Coal', 
                 'SUN':'Solar', 
                 'OBG':'Other Renewables', 
                 'GEO':'Other Renewables', 
                 'MWH':'Other', 
                 'OG':'Gas',
                'WO':'Oil', 
       'JF':'Oil', 
       'KER':'Oil', 
       'OTH':'Other', 
       'WC':'Coal', 
       'SGC':'Gas', 
       'OBS':'Other Renewables',
        'AB':'Other Renewables', 
        'TDF':'Other', 
        'BFG':'Gas',
        'MSB':'Other',
       'MSN':'Other Renewables', 
       'SC':'Coal',
       'SUB':'Coal',
       'LIG':'Coal',
       'BIT':'Coal','RC':'Coal','ANT':'Coal','NG':'Gas','PG':'Gas',
       'BLQ':'Other Renewables',
       'WH':'Other', 'WDS':'Other Renewables',
       'OBL':'Other Renewables', 'SLW':'Other Renewables', 
       'PUR':'Other', 'WDL':'Other Renewables', 'SGP':'Gas',
       'H2':'Hydro','WAT':'Hydro','BAT':'Storage','PS':'Storage','NUC':'Nuclear'}

Fossil_tech = ['Coal','Gas','Oil']

#tCO2/MWh
simple_emissions_factor={'Coal': 1.046,
                           "Gas": 0.467,
                           "Oil": 0.867,
                           'Multiple Fuels':0.467*0.9+0.867*.1}

metric_tons_to_short_tons = 1.1023

#Generation
def gather_historical_generation(years_back=5):
    eia_html = requests.get('https://www.eia.gov/electricity/data/eia923/')
    soup = BeautifulSoup(eia_html.content,'html.parser')
    release_dates= soup.find_all('div',class_='release-dates')

    next_update_time = release_dates[0].find_all('span',class_='date')[-1].text
    monthly_report_links = {}
    this_year =dt.today().year
    dates_to_observe = range(this_year-years_back,this_year+1)
    for x in soup.find_all('a', href=True):
        try:
            if int(x.attrs['title']) in dates_to_observe:
                monthly_report_links[x.attrs['title']] = x.attrs['href']
        except:
            pass
    link_df = pd.DataFrame.from_dict(monthly_report_links,orient='index').sort_index()

    mega_gen_fuel_df = pd.DataFrame()
    for year in link_df.index:
        resp = urlopen('https://www.eia.gov/electricity/data/eia923/'+link_df.loc[year][0])
        myzip = ZipFile(BytesIO(resp.read()))
        for  x in myzip.filelist:
            if 'EIA923_Schedules_2_3_4_5_M' in x.filename:
                print(x.filename)
                gen_file = pd.ExcelFile(myzip.open(x))
                gen_fuel_df = pd.read_excel(gen_file,sheet_name='Page 1 Generation and Fuel Data',header=5)
                gen_fuel_df = gen_fuel_df.dropna(subset=['Plant Name'])

                mega_gen_fuel_df = pd.concat([mega_gen_fuel_df,gen_fuel_df],axis=0)

    return mega_gen_fuel_df

### Capacity
def save_historical_capacity_per_plant(location):
    filestream = GRAPH_API(location).download_file('/Corporate/Shared Analysis/RGGI_ISO_power_data/PJM/EIA_data/full_capacity_series_with_tech.pkl')
    rggi_plants = pd.read_pickle(filestream)
    all_capacity = rggi_plants.groupby(['Plant ID','Energy Source Code','Prime Mover Code','Plant State','report_month', 'report_year'])['Nameplate Capacity (MW)'].sum().to_frame()
    return all_capacity



def clean_historical_generators(location):
    mega_gen_fuel_df = gather_historical_generation(years_back=5)
    time_series_historical_capacity =save_historical_capacity_per_plant(location)

    gen_cols = list(mega_gen_fuel_df.columns[['Netgen' in x for x in mega_gen_fuel_df.columns]])

    fuel_cons_columns = ['Quantity\nJanuary', 'Quantity\nFebruary',
       'Quantity\nMarch', 'Quantity\nApril', 'Quantity\nMay', 'Quantity\nJune',
       'Quantity\nJuly', 'Quantity\nAugust', 'Quantity\nSeptember',
       'Quantity\nOctober', 'Quantity\nNovember', 'Quantity\nDecember']

    mmbtu_cols = ['Tot_MMBtu\nJanuary', 'Tot_MMBtu\nFebruary', 'Tot_MMBtu\nMarch',
       'Tot_MMBtu\nApril', 'Tot_MMBtu\nMay', 'Tot_MMBtu\nJune',
       'Tot_MMBtu\nJuly', 'Tot_MMBtu\nAugust', 'Tot_MMBtu\nSeptember',
       'Tot_MMBtu\nOctober', 'Tot_MMBtu\nNovember', 'Tot_MMBtu\nDecember']

    mega_gen_fuel_df['RGGI_state'] = np.where(mega_gen_fuel_df['Plant State'].isin(RGGI_states),1,0)
    rggi_or_pjm = mega_gen_fuel_df[(mega_gen_fuel_df['RGGI_state']==1)| (mega_gen_fuel_df['Balancing\nAuthority Code']=='PJM')]
    
    rggi_or_pjm[gen_cols] = rggi_or_pjm[gen_cols].replace('.',0)
    rggi_or_pjm[fuel_cons_columns] = rggi_or_pjm[fuel_cons_columns].replace('.',0)
    rggi_or_pjm[mmbtu_cols] = rggi_or_pjm[mmbtu_cols].replace('.',0)

    mmbtu_df = rggi_or_pjm.groupby(['Plant Id','Plant State','Reported\nFuel Type Code','YEAR'])[mmbtu_cols].sum().stack().to_frame('Fuel_consumed_Mmbtu')
    mmbtu_df['month'] = [x[-1] for x in mmbtu_df.reset_index()['level_4'].str.split('\n')]
    mmbtu_df=mmbtu_df.reset_index()
    mmbtu_df['Date'] = pd.to_datetime('01/'+mmbtu_df['month']+'/'+mmbtu_df['YEAR'].astype(str))+MonthEnd(0)
    mmbtu_df['month'] = mmbtu_df['Date'].dt.month
    mmbtu_df = mmbtu_df.groupby(['Plant Id','Plant State','Date','Reported\nFuel Type Code'])['Fuel_consumed_Mmbtu'].sum().unstack('Reported\nFuel Type Code').fillna(0)#.add_suffix('_mmbtu')

    EF_df = download_EF_from_EIA(fuels_list=mmbtu_df.columns)

    plant_emissions = mmbtu_df.copy()

    for x in mmbtu_df.columns:
        plant_emissions[x] = mmbtu_df[x]*float(EF_df.loc[x].values[0])
    plant_emissions = (plant_emissions/1000)* 0.9071847
    plant_emissions = plant_emissions.stack().to_frame('Emissions_stCO2')
    rggi_or_pjm = rggi_or_pjm.groupby(['Plant Id','Plant Name','Reported\nFuel Type Code','Plant State', 'Balancing\nAuthority Code','RGGI_state','YEAR'])[gen_cols].sum().stack().to_frame('Generation MWh')
    rggi_or_pjm['month'] = [x[-1] for x in rggi_or_pjm.reset_index()['level_7'].str.split('\n')]
    rggi_or_pjm = rggi_or_pjm.reset_index()
    rggi_or_pjm['Date'] = pd.to_datetime('01/'+rggi_or_pjm['month']+'/'+rggi_or_pjm['YEAR'].astype(str),format='%d/%B/%Y')+MonthEnd(0)
    rggi_or_pjm['month'] = rggi_or_pjm['Date'].dt.month

    time_series_historical_capacity_byprime_mover = time_series_historical_capacity.reset_index().groupby(['Plant ID', 'Energy Source Code','Plant State','report_month','report_year']).sum()
    rggi_or_pjm_capacity = rggi_or_pjm.merge(time_series_historical_capacity_byprime_mover,left_on=['Plant Id','Reported\nFuel Type Code','Plant State','month','YEAR'],right_index=True,how='left')
    gap_fill = rggi_or_pjm_capacity.groupby(['Plant Id','Reported\nFuel Type Code','YEAR'])['Nameplate Capacity (MW)'].mean().to_frame('Gap_fill_capacity_year')
    rggi_or_pjm_capacity = rggi_or_pjm_capacity.merge(gap_fill,left_on=['Plant Id','Reported\nFuel Type Code','YEAR'],right_index=True,how='left')
    rggi_or_pjm_capacity['Nameplate Capacity (MW)'] = np.where((rggi_or_pjm_capacity['Generation MWh']>0) & (pd.isna(rggi_or_pjm_capacity['Nameplate Capacity (MW)'])),
                                                            rggi_or_pjm_capacity['Gap_fill_capacity_year'],rggi_or_pjm_capacity['Nameplate Capacity (MW)'])

    gap_fill = rggi_or_pjm_capacity.groupby(['Plant Id','Reported\nFuel Type Code'])['Nameplate Capacity (MW)'].mean().to_frame('Gap_fill_capacity_overall')
    rggi_or_pjm_capacity = rggi_or_pjm_capacity.merge(gap_fill,left_on=['Plant Id','Reported\nFuel Type Code'],right_index=True,how='left')

    rggi_or_pjm_capacity['Nameplate Capacity (MW)'] = np.where((rggi_or_pjm_capacity['Generation MWh']>0) & (pd.isna(rggi_or_pjm_capacity['Nameplate Capacity (MW)'])),
                                                            rggi_or_pjm_capacity['Gap_fill_capacity_overall'],rggi_or_pjm_capacity['Nameplate Capacity (MW)'])

    rggi_or_pjm_capacity = rggi_or_pjm_capacity.join(plant_emissions,on=['Plant Id','Plant State','Date','Reported\nFuel Type Code'])
    rggi_or_pjm_capacity['PJM_tech'] = rggi_or_pjm_capacity['Reported\nFuel Type Code'].map(PJM_tech_dict)
    rggi_or_pjm_capacity['Generation MWh_clipped'] = rggi_or_pjm_capacity['Generation MWh'].clip(0,9999999999999999999999999999999999)

    coal_dual_fuel = ((rggi_or_pjm_capacity.groupby(['Plant Id','Date','PJM_tech'])['Generation MWh_clipped'].sum().unstack('PJM_tech')[Fossil_tech].divide(rggi_or_pjm_capacity.groupby(['Plant Id','Date','PJM_tech'])['Generation MWh_clipped'].sum().unstack('PJM_tech')[Fossil_tech].sum(axis=1),axis=0)<0.9) &
             (rggi_or_pjm_capacity.groupby(['Plant Id','Date','PJM_tech'])['Generation MWh_clipped'].sum().unstack('PJM_tech')[Fossil_tech].divide(rggi_or_pjm_capacity.groupby(['Plant Id','Date','PJM_tech'])['Generation MWh_clipped'].sum().unstack('PJM_tech')[Fossil_tech].sum(axis=1),axis=0)>0.1))['Coal']

    gas_dual_fuel = ((rggi_or_pjm_capacity.groupby(['Plant Id','Date','PJM_tech'])['Generation MWh_clipped'].sum().unstack('PJM_tech')[Fossil_tech].divide(rggi_or_pjm_capacity.groupby(['Plant Id','Date','PJM_tech'])['Generation MWh_clipped'].sum().unstack('PJM_tech')[Fossil_tech].sum(axis=1),axis=0)<0.9) &
                (rggi_or_pjm_capacity.groupby(['Plant Id','Date','PJM_tech'])['Generation MWh_clipped'].sum().unstack('PJM_tech')[Fossil_tech].divide(rggi_or_pjm_capacity.groupby(['Plant Id','Date','PJM_tech'])['Generation MWh_clipped'].sum().unstack('PJM_tech')[Fossil_tech].sum(axis=1),axis=0)>0.1))['Gas']
    dual_fuel_df = rggi_or_pjm_capacity.groupby(['Plant Id','Date','PJM_tech'])['Generation MWh'].sum().unstack('PJM_tech')[coal_dual_fuel | gas_dual_fuel]
    dual_fuel_df['Dual_fuel']=1
    rggi_or_pjm_capacity = rggi_or_pjm_capacity.join(dual_fuel_df['Dual_fuel'],on=['Plant Id','Date'])
    rggi_or_pjm_capacity['PJM_tech'] = np.where(rggi_or_pjm_capacity['Dual_fuel']==1,'Multiple Fuels',rggi_or_pjm_capacity['PJM_tech'])

    return rggi_or_pjm_capacity

def calculate_historical_PJM_share(rggi_or_pjm_capacity):
    pjm = rggi_or_pjm_capacity[rggi_or_pjm_capacity['Balancing\nAuthority Code']=="PJM"]
    total_pjm_gen = pjm.groupby(['Date','PJM_tech'])['Generation MWh_clipped'].sum().to_frame('Total_PJM_generation')
    pjm = pjm.join(total_pjm_gen,on=['Date','PJM_tech'])
    pjm['Generation_share_PJM'] = pjm['Generation MWh_clipped']/pjm['Total_PJM_generation']
    rggi_share = pjm.groupby(['YEAR','month','Date','PJM_tech','RGGI_state'])['Generation_share_PJM'].sum().unstack('RGGI_state')
    rggi_share['RGGI_share'] = rggi_share[1]/rggi_share[[0,1]].sum(axis=1)


    return pjm, rggi_share



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
    
    PJM_plants['PJM_tech'] = PJM_plants['Energy Source Code'].map(PJM_tech_dict)
    PJM_plants['Fossil'] = np.where(PJM_plants['PJM_tech'].isin(Fossil_tech),1,0)
    dual_fuels = PJM_plants.groupby(['Plant ID','report_month','report_year'])['PJM_tech'].nunique()
    PJM_plants = PJM_plants.join(dual_fuels.to_frame('dual_fossil'),on=['Plant ID','report_month','report_year'])
    PJM_plants['PJM_tech'] = np.where(PJM_plants['dual_fossil']>1,'Multiple Fuels',PJM_plants['PJM_tech'])
    full_tech_list = PJM_plants.PJM_tech.unique()

    PJM_plants = RGGI_capacity().analyse_RGGI_capacity(PJM_plants)

    ### Planned additions
    planned = pd.read_excel(recent_report,sheet_name='Planned',header=2)
    planned = planned.dropna(subset=['Plant ID'])
    planned['PJM_tech'] = planned['Energy Source Code'].map(PJM_tech_dict)  
    planned['Fossil'] = np.where(planned['PJM_tech'].isin(Fossil_tech),1,0)
    dual_fuels = planned.groupby(['Plant ID','Planned Operation Month','Planned Operation Year'])['PJM_tech'].nunique()
    planned = planned.join(dual_fuels.to_frame('dual_fossil'),on=['Plant ID','Planned Operation Month','Planned Operation Year'])
    planned['PJM_tech'] = np.where(planned['dual_fossil']>1,'Multiple Fuels',planned['PJM_tech'])


    PJM_planned = planned.query(f'`Balancing Authority Code`=="PJM"')
    PJM_planned['rggi_state'] = np.where(PJM_planned['Plant State'].isin(RGGI_states),1,0)
    RGGI_PJM_planned = PJM_planned.query('rggi_state==1 ') # 
    additions,approved_additions,not_yet_approved_additions = RGGI_capacity().analyse_RGGI_planned_capacity_PJM(PJM_planned)
    pjm_rggi_additions,pjm_rggi_approved_additions,pjm_rggi_not_yet_approved_additions = RGGI_capacity().analyse_RGGI_planned_capacity_PJM(RGGI_PJM_planned)

    PJM_plants['rggi_state'] = np.where(PJM_plants['Plant State'].isin(RGGI_capacity().RGGI_states),1,0)

    PJM_RGGI_plants = PJM_plants.query('rggi_state==1 ') #
    pjm_plus_retirements_plus_additions_timeseries, pjm_time_series_capacity_fig = RGGI_capacity().estimated_timeseries_capacity_PJM(PJM_plants,full_tech_list,additions,date_of_last_report,PJM_retiredates=True)
    
    rggi_pjm_rggi_plus_retirements_plus_additions_timeseries, rggi_pjm_time_series_capacity_fig = RGGI_capacity().estimated_timeseries_capacity_PJM(PJM_RGGI_plants,full_tech_list,pjm_rggi_additions,date_of_last_report,PJM_retiredates=True)

    """rggi_share_pjm = pjm_plus_retirements_plus_additions_timeseries.stack().to_frame('PJM').join(rggi_pjm_rggi_plus_retirements_plus_additions_timeseries.stack().to_frame('RGGI_PJM'))
    rggi_share_pjm = rggi_share_pjm.reset_index()
    rggi_share_pjm['PJM_tech'] = rggi_share_pjm['level_1'].map(tech_convert_dict)
    rggi_share_pjm = rggi_share_pjm.groupby(['level_0','PJM_tech'])[['PJM','RGGI_PJM']].sum()
    rggi_share_pjm['RGGI_share_PJM'] = rggi_share_pjm['RGGI_PJM']/rggi_share_pjm['PJM']
    rggi_share_pjm['RGGI_share_PJM'] = rggi_share_pjm['RGGI_share_PJM'].fillna(0) """

    return rggi_pjm_rggi_plus_retirements_plus_additions_timeseries,pjm_plus_retirements_plus_additions_timeseries,PJM_plants