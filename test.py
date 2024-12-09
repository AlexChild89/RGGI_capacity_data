import pandas as pd
import numpy as np
import plotly.express as px
from bs4 import BeautifulSoup 
import requests

from RGGI_plant_analysis import RGGI_capacity
from Analyse_PJM_generation import clean_historical_generators,calculate_historical_PJM_share
from Analyse_PJM_capacity import run_full_relative_capacity_PJM
from datetime import datetime as dt
import sys
from statistics import NormalDist
import subprocess
import os 

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

from enviro_vars import set_environmental_variables
try:       
    set_environmental_variables()
except Exception as e:
    print(e)
    print('in online environment - no enviro_vars module')

############# ADD GIT TOKEN BELOW ############
git_token = os.environ['gittoken']
AUTHORITY = os.environ['azAuth']
CLIENT_ID = os.environ['azClientid']
SECRET = os.environ['AzAppSecret']
location =  {"authority": AUTHORITY,"client_id": CLIENT_ID,'secret':SECRET,"scope": [ "https://graph.microsoft.com/.default" ]}

##
github_repository = f"git+https://{git_token}@github.com/research-carbon-cap/SharePointv2.git@Azure_functions"

install(github_repository)


from SharePointv2.Sharepoint_API import GRAPH_API
from get_overall_generation_and_PJM_share import run_full_generators_assessment_and_RGGI_PJM_proportion
rggi_share, rggi_share_pjm_capacity_fig,rggi_share_pjm_generation_fig,full_time_series,gen_df,pjm_planned_retired,isne_planned_retired,nyis_planned_retired  = run_full_generators_assessment_and_RGGI_PJM_proportion(location,read_latest=True)