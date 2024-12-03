import pandas as pd
import numpy as np
import plotly.express as px
from bs4 import BeautifulSoup 
import requests

from RGGI_plant_analysis import RGGI_capacity
from Analyse_PJM_V2 import clean_historical_generators,calculate_historical_PJM_share,run_future_RGGI_share
gen_df = clean_historical_generators(read_latest=True)
pjm, rggi_share = calculate_historical_PJM_share(gen_df)
rggi_pjm_rggi_plus_retirements_plus_additions_timeseries,pjm_plus_retirements_plus_additions_timeseries,PJM_plants = run_future_RGGI_share()