import pandas as pd
import numpy as np
import plotly.express as px
from bs4 import BeautifulSoup 
import requests

from RGGI_plant_analysis import RGGI_capacity



def run_recent_capacity_and_forecast_rggi(location):
    next_update_time,recent_report,report_month,report_year,date_of_last_report  =RGGI_capacity(location).scrape_recent_EIA_860m(lagged_report=1)
    plants = pd.read_excel(recent_report,sheet_name='Operating')
    header_col = plants[(plants.iloc[:,0]=='Entity ID')==True].index[0]
    plants = pd.read_excel(recent_report,sheet_name='Operating',header=header_col+1)
    plants['report_month'] = report_month
    plants['report_year'] = report_year
    plants = plants.dropna(subset=['Plant Name'])
    RGGI_plants =  plants.query(f'`Plant State`=={RGGI_capacity(location).RGGI_states}') # or `Balancing Authority Code`=="PJM" 
    full_tech_list = plants.Technology.unique()

    RGGI_plants = RGGI_capacity(location).analyse_RGGI_capacity(RGGI_plants)
    planned = pd.read_excel(recent_report,sheet_name='Planned',header=2)
    RGGI_planned = planned.query(f'`Plant State`=={RGGI_capacity(location).RGGI_states} ') #or `Balancing Authority Code`=="PJM"
    additions,approved_additions,not_yet_approved_additions = RGGI_capacity(location).analyse_RGGI_planned_capacity(RGGI_planned)
    
    planned_fossil_retirments_fig,all_planned_capacity_fig,approved_capacity_fig, not_yet_approved_capacity_fig,RE_total_vs_approved_fig,cumulative_retirements_fig,rggi_capacity_by_tech_fig = RGGI_capacity(location).RGGI_capacity_charts(RGGI_plants,additions,approved_additions,not_yet_approved_additions)

    rggi_plus_retirements_plus_additions_timeseries, time_series_capacity_fig = RGGI_capacity(location).estimated_timeseries_capacity(RGGI_plants,full_tech_list,additions,date_of_last_report,PJM_retiredates=True)
    time_series_capacity_fig = time_series_capacity_fig.update_layout(width=1000)

    return planned_fossil_retirments_fig,all_planned_capacity_fig,approved_capacity_fig, not_yet_approved_capacity_fig,RE_total_vs_approved_fig,rggi_plus_retirements_plus_additions_timeseries, time_series_capacity_fig,cumulative_retirements_fig,rggi_capacity_by_tech_fig