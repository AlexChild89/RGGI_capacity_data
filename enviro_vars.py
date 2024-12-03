import os
import pandas as pd


def set_environmental_variables():
    #try:
    env_vars = pd.read_excel(fr'{os.environ["USERPROFILE"]}\Carbon Cap\CarbonCap - Documents\Corporate\Azure_keys\Container_app_enviro_vars.xlsx',index_col=0)
    env = 'prod'
    os.environ['azAuth'] = env_vars.loc['azAuth',env]
    os.environ['azClientid'] = env_vars.loc['azClientid',env]
    os.environ['AzAppSecret'] = env_vars.loc['AzAppSecret',env]
    os.environ['gittoken'] = env_vars.loc['gittoken',env]
    os.environ['CCap_docs'] = env_vars.loc['CCap_docs',env]
    os.environ['EmailID'] = env_vars.loc['EmailID',env]
    os.environ['host'] = env_vars.loc['price_host',env]
    os.environ['dbname'] = env_vars.loc['dbname',env]
    os.environ['user'] = env_vars.loc['user',env]
    os.environ['password'] = env_vars.loc['password',env]
    os.environ['orderbook_host'] = env_vars.loc['orderbook_host',env]
    os.environ['orderbook_user'] = env_vars.loc['orderbook_user',env]
    os.environ['power_host'] = env_vars.loc['host',env]
    os.environ['size_multiplier'] = '0.5'
    
    return print('Environment variables set!')
        

    #except Exception as e:
    #    print(f'Error occurred: {e}')
    #    print('unable to set environment variables - must be in online production')
