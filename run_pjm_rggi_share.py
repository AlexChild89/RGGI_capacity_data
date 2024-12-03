import pandas as pd
import numpy as np
import plotly.express as px
from bs4 import BeautifulSoup 
import requests
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

from RGGI_plant_analysis import RGGI_capacity
from Analyse_PJM_V2 import clean_historical_generators,calculate_historical_PJM_share,run_future_RGGI_share
from Analyse_PJM import run_full_relative_capacity_PJM

Alex_id = os.environ['EmailID'] 
toEmail = ['research@carbon-cap.com','alex.child@carbon-cap.com'] #
CCap_docs_folder = os.environ['CCap_docs']
host = os.environ['orderbook_host']
dbname = os.environ['dbname']
user = os.environ['orderbook_user']
password = os.environ['password']