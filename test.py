import pandas as pd
import numpy as np
import plotly.express as px
from bs4 import BeautifulSoup 
import requests

from RGGI_plant_analysis import RGGI_capacity
from Analyse_PJM import run_historical_RGGI_share,run_future_RGGI_share
historical_rggi_share_pjm,test_df = run_historical_RGGI_share(read_latest=True)
historical_rggi_share_pjm['RGGI_share'] = historical_rggi_share_pjm[1]/historical_rggi_share_pjm[[1,0]].sum(axis=1)
rggi_pjm_rggi_plus_retirements_plus_additions_timeseries,pjm_plus_retirements_plus_additions_timeseries,PJM_plants =  run_future_RGGI_share()