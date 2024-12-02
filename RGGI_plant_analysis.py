import pandas as pd
import numpy as np
import plotly.express as px
from bs4 import BeautifulSoup 
import requests
from datetime import datetime as dt
import plotly.subplots as sp

from PJM_retirements import gather_PJM_retirements_with_issues

class RGGI_capacity:
    def __init__(self):
        self.RGGI_states = ['CT', 'DE', 'ME', 'MD', 'MA', 'NH', 'NJ', 'NY', 'RI', 'VT'] 
        self.Fossil_tech =  ['Petroleum Liquids', 'Natural Gas Steam Turbine','Natural Gas Fired Combined Cycle',
                'Conventional Steam Coal','Natural Gas Fired Combustion Turbine','Other Gases']   
        
    def scrape_recent_EIA_860m(self,lagged_report=1):
        eia_html = requests.get('https://www.eia.gov/electricity/data/eia860m/')
        soup = BeautifulSoup(eia_html.content,'html.parser')
        release_dates= soup.find_all('div',class_='release-dates')

        next_update_time = pd.to_datetime(release_dates[0].find_all('span',class_='date')[-1].text,format='%B %d, %Y')

        monthly_report_links = {}
        for x in soup.find_all('a', href=True):
            try:
                if '860M' in x.attrs['title']:
                    report_date= pd.to_datetime(x.attrs['title'][9:],format='%B %Y')
                    monthly_report_links[report_date] = x.attrs['href']
            except:
                pass

        link_df = pd.DataFrame.from_dict(monthly_report_links,orient='index').sort_index()
        print(f'saving down data for {link_df.index[-lagged_report].strftime("%Y-%m-%d")}')
        report_month =link_df.index[-lagged_report].month
        report_year = link_df.index[-lagged_report].year
        recent_report = pd.ExcelFile('https://www.eia.gov'+link_df.iloc[-lagged_report].values[0],engine='openpyxl')

        date_of_last_report = link_df.iloc[-lagged_report].name

        return next_update_time,recent_report,report_month,report_year,date_of_last_report
    
    
    
    def analyse_all_capacity(self,recent_report,report_month,report_year):
        plants = pd.read_excel(recent_report,sheet_name='Operating')
        header_col = plants[(plants.iloc[:,0]=='Entity ID')==True].index[0]
        plants = pd.read_excel(recent_report,sheet_name='Operating',header=header_col+1)
        plants['report_month'] = report_month
        plants['report_year'] = report_year
        plants = plants.dropna(subset=['Plant Name'])
        RGGI_plants =  plants
        RGGI_plants['Fossil'] =  np.where(RGGI_plants['Technology'].isin(self.Fossil_tech),1,0)
        RGGI_plants['PlantName_GenID'] = RGGI_plants['Plant Name'] +' '+ RGGI_plants['Generator ID']
        RGGI_plants['Nameplate Capacity (MW)'] = RGGI_plants['Nameplate Capacity (MW)'].replace(' ',0)
        RGGI_plants['Nameplate Capacity (MW)'] =RGGI_plants['Nameplate Capacity (MW)'].astype(float)
        all_capacity = RGGI_plants.groupby(['Plant ID','Energy Source Code','Prime Mover Code','Plant State','report_month', 'report_year'])['Nameplate Capacity (MW)'].sum().to_frame()

        return all_capacity
    
    def analyse_all_capacity_with_tech(self,recent_report,report_month,report_year):
        plants = pd.read_excel(recent_report,sheet_name='Operating')
        header_col = plants[(plants.iloc[:,0]=='Entity ID')==True].index[0]
        plants = pd.read_excel(recent_report,sheet_name='Operating',header=header_col+1)
        plants['report_month'] = report_month
        plants['report_year'] = report_year
        plants = plants.dropna(subset=['Plant Name'])
        RGGI_plants =  plants
        RGGI_plants['Fossil'] =  np.where(RGGI_plants['Technology'].isin(self.Fossil_tech),1,0)
        RGGI_plants['PlantName_GenID'] = RGGI_plants['Plant Name'] +' '+ RGGI_plants['Generator ID']
        RGGI_plants['Nameplate Capacity (MW)'] = RGGI_plants['Nameplate Capacity (MW)'].replace(' ',0)
        RGGI_plants['Nameplate Capacity (MW)'] =RGGI_plants['Nameplate Capacity (MW)'].astype(float)
        #all_capacity = RGGI_plants.groupby(['Plant ID','Energy Source Code','Prime Mover Code','Technology','Balancing Authority Code','Plant State','report_month', 'report_year'])['Nameplate Capacity (MW)'].sum().to_frame()

        return RGGI_plants
    
    ### Capacity
    def save_historical_capacity_per_plant(self,years_back,read_latest):
        if read_latest ==False:
            time_series_historical_capacity = pd.DataFrame()
            for x in range(1,12*years_back):
                next_update_time,recent_report,report_month,report_year,date_of_last_report  =self.scrape_recent_EIA_860m(lagged_report=x)
                capacity = self.analyse_all_capacity_with_tech(recent_report,report_month,report_year)
                time_series_historical_capacity = pd.concat([time_series_historical_capacity,capacity],axis=0)

            time_series_historical_capacity.to_pickle('full_capacity_series_with_tech.pkl')
        else:
            time_series_historical_capacity = pd.read_pickle('full_capacity_series_with_tech.pkl')

        return time_series_historical_capacity
    
    def analyse_RGGI_capacity(self,RGGI_plants):
         #`Nameplate Capacity (MW)`>25 
        RGGI_plants['Fossil'] =  np.where(RGGI_plants['Technology'].isin(self.Fossil_tech),1,0)
        RGGI_plants['PlantName_GenID'] = RGGI_plants['Plant Name'] +' '+ RGGI_plants['Generator ID']

        ### Retirement Date
        RGGI_plants[['year','month']] = RGGI_plants[['Planned Retirement Year','Planned Retirement Month']]
        RGGI_plants['year'] = RGGI_plants['year'].astype(str).str.rstrip().replace('',2100).astype(int)
        RGGI_plants['month'] = RGGI_plants['month'].astype(str).str.rstrip().replace('',12).astype(int)
        RGGI_plants['day']=1
        RGGI_plants['retirement_date'] = pd.to_datetime(RGGI_plants[['year','month','day']])

        ## Adding in PJM exepcted retirement dates
        Future_RGGI_PJM_deactivations_with_issues = gather_PJM_retirements_with_issues()
        RGGI_plants = RGGI_plants.join(Future_RGGI_PJM_deactivations_with_issues['ProjectedDeactivationDate'].to_frame('PJM_projected_retirement_date'),on='PlantName_GenID')
        RGGI_plants['retirement_date_original'] =RGGI_plants['retirement_date']
        RGGI_plants['retirement_date'] = np.where(pd.isna(RGGI_plants['PJM_projected_retirement_date']),RGGI_plants['retirement_date'],RGGI_plants['PJM_projected_retirement_date'])

        


        return RGGI_plants
    
    def analyse_RGGI_planned_capacity(self,RGGI_planned):
        

        ## Date of operation start
        RGGI_planned[['year','month']] = RGGI_planned[['Planned Operation Year','Planned Operation Month']]
        RGGI_planned['year'] = RGGI_planned['year'].astype(str).str.rstrip().replace('',2100).astype(float)
        RGGI_planned['month'] = RGGI_planned['month'].astype(str).str.rstrip().replace('',12).astype(float)
        RGGI_planned['day']=1
        RGGI_planned['operational_date'] = pd.to_datetime(RGGI_planned[['year','month','day']])

        approved_status_list = ['(U) Under construction, less than or equal to 50 percent complete',
       '(TS) Construction complete, but not yet in commercial operation',
       '(V) Under construction, more than 50 percent complete',
       '(T) Regulatory approvals received. Not under construction',
       ]
        not_yet_approved_status_list = ['(P) Planned for installation, but regulatory approvals not initiated',
            '(L) Regulatory approvals pending. Not under construction',
            '(OT) Other']
        full_status_list = ['(U) Under construction, less than or equal to 50 percent complete',
            '(TS) Construction complete, but not yet in commercial operation',
            '(V) Under construction, more than 50 percent complete',
            '(T) Regulatory approvals received. Not under construction',
            '(P) Planned for installation, but regulatory approvals not initiated',
            '(L) Regulatory approvals pending. Not under construction',
            '(OT) Other']
        
        additions = RGGI_planned.query(f"Status=={full_status_list}").pivot_table(index='operational_date',columns='Technology',values='Nameplate Capacity (MW)',aggfunc=np.sum).fillna(0)
        approved_additions = RGGI_planned.query(f"Status=={approved_status_list}").pivot_table(index='operational_date',columns='Technology',values='Nameplate Capacity (MW)',aggfunc=np.sum).fillna(0)
        not_yet_approved_additions = RGGI_planned.query(f"Status=={not_yet_approved_status_list}").pivot_table(index='operational_date',columns='Technology',values='Nameplate Capacity (MW)',aggfunc=np.sum).fillna(0)

        return additions,approved_additions,not_yet_approved_additions
    
    def analyse_RGGI_planned_capacity_PJM(self,RGGI_planned):
        

        ## Date of operation start
        RGGI_planned[['year','month']] = RGGI_planned[['Planned Operation Year','Planned Operation Month']]
        RGGI_planned['year'] = RGGI_planned['year'].astype(str).str.rstrip().replace('',2100).astype(float)
        RGGI_planned['month'] = RGGI_planned['month'].astype(str).str.rstrip().replace('',12).astype(float)
        RGGI_planned['day']=1
        RGGI_planned['operational_date'] = pd.to_datetime(RGGI_planned[['year','month','day']])

        approved_status_list = ['(U) Under construction, less than or equal to 50 percent complete',
       '(TS) Construction complete, but not yet in commercial operation',
       '(V) Under construction, more than 50 percent complete',
       '(T) Regulatory approvals received. Not under construction',
       ]
        not_yet_approved_status_list = ['(P) Planned for installation, but regulatory approvals not initiated',
            '(L) Regulatory approvals pending. Not under construction',
            '(OT) Other']
        full_status_list = ['(U) Under construction, less than or equal to 50 percent complete',
            '(TS) Construction complete, but not yet in commercial operation',
            '(V) Under construction, more than 50 percent complete',
            '(T) Regulatory approvals received. Not under construction',
            '(P) Planned for installation, but regulatory approvals not initiated',
            '(L) Regulatory approvals pending. Not under construction',
            '(OT) Other']
        
        additions = RGGI_planned.query(f"Status=={full_status_list}").pivot_table(index='operational_date',columns='PJM_tech',values='Nameplate Capacity (MW)',aggfunc=np.sum).fillna(0)
        approved_additions = RGGI_planned.query(f"Status=={approved_status_list}").pivot_table(index='operational_date',columns='PJM_tech',values='Nameplate Capacity (MW)',aggfunc=np.sum).fillna(0)
        not_yet_approved_additions = RGGI_planned.query(f"Status=={not_yet_approved_status_list}").pivot_table(index='operational_date',columns='PJM_tech',values='Nameplate Capacity (MW)',aggfunc=np.sum).fillna(0)

        return additions,approved_additions,not_yet_approved_additions


    def RGGI_capacity_charts(self,RGGI_plants,additions,approved_additions,not_yet_approved_additions):

        figure1 = px.bar(RGGI_plants.query('Fossil==1 and retirement_date<"2100-01-01"'),x='retirement_date',color='Technology',y='Nameplate Capacity (MW)',barmode='group',hover_data=['Plant State','Plant Name'],
            title='RGGI Planned Fossil Retirements up until 2100')
        figure2 = px.bar(RGGI_plants.query('Fossil==1 and retirement_date_original<"2100-01-01"'),x='retirement_date_original',color='Technology',y='Nameplate Capacity (MW)',barmode='group',hover_data=['Plant State','Plant Name'],
            title='PJM-adjusted RGGI Planned Fossil Retirements up until 2100')
        figure2 = figure2.update_layout(showlegend=False)
        figure1_traces = []
        figure2_traces = []
        for trace in range(len(figure1["data"])):
            figure1_traces.append(figure1["data"][trace])
        for trace in range(len(figure2["data"])):
            figure2_traces.append(figure2["data"][trace])

        #Create a 1x2 subplot
        planned_fossil_retirments_fig = sp.make_subplots(rows=1, cols=2,column_titles=['PJM Adjusted RGGI Retirements','EIA RGGI  Retirements'],row_titles=['Capacity MW']) 

        # Get the Express fig broken down as traces and add the traces to the proper plot within in the subplot
        for traces in figure1_traces:
            traces['showlegend']=False
            planned_fossil_retirments_fig.append_trace(traces, row=1, col=1)
        for traces in figure2_traces:
            planned_fossil_retirments_fig.append_trace(traces, row=1, col=2)


        rggi_capacity_by_tech = RGGI_plants.groupby('Technology')['Nameplate Capacity (MW)'].sum()
        rggi_capacity_by_tech_fig = px.bar(rggi_capacity_by_tech)
        
        all_planned_capacity_fig = px.area(additions.cumsum(),labels={'value':'Capacity (MW)'},title='RGGI Planned Capacity At all stages',width=900)
        approved_capacity_fig = px.area(approved_additions.cumsum(),labels={'value':'Capacity (MW)'},title='Approved RGGI State Capacity Pipeline',width=900)
        not_yet_approved_capacity_fig = px.area(not_yet_approved_additions.cumsum(),labels={'value':'Capacity (MW)'},title='Not Yet Approved RGGI State Capacity Pipeline',width=900)

        return planned_fossil_retirments_fig,all_planned_capacity_fig,approved_capacity_fig, not_yet_approved_capacity_fig
    
    def estimated_timeseries_capacity(self,RGGI_plants,full_tech_list,additions,date_of_last_report,PJM_retiredates=True):
        today_string = date_of_last_report.strftime('%Y-%m-%d').split('-')
        time_span = pd.date_range(start=(date_of_last_report).strftime('%Y-%m-%d'),end='2030-12-01',freq='MS') #
        full_capacity_time_series = pd.DataFrame(index=time_span,columns=full_tech_list)

        rggi_capacity_by_tech = RGGI_plants.groupby('Technology')['Nameplate Capacity (MW)'].sum()
        rggi_capacity_by_tech_today = rggi_capacity_by_tech.to_frame().transpose()
        rggi_capacity_by_tech_today.index.name='Date'

        rggi_capacity_by_tech_today.index = [f'{today_string[0]}-{today_string[1]}-01']
        rggi_capacity_by_tech_today.index  = pd.to_datetime(rggi_capacity_by_tech_today.index)
        full_capacity_time_series = pd.concat([full_capacity_time_series,rggi_capacity_by_tech_today],axis=0).sort_index()
        full_capacity_time_series = full_capacity_time_series.fillna(0)

        if PJM_retiredates == True:
            RGGI_retirements = RGGI_plants.query('Fossil==1 and retirement_date<"2100-01-01"').pivot_table(index='retirement_date',columns='Technology',values='Nameplate Capacity (MW)',aggfunc=np.sum)
        else:
            RGGI_retirements = RGGI_plants.query('Fossil==1 and retirement_date_original<"2100-01-01"').pivot_table(index='retirement_date',columns='Technology',values='Nameplate Capacity (MW)',aggfunc=np.sum)
        
        rggi_plus_retirements = pd.concat([full_capacity_time_series,-RGGI_retirements],axis=0).sort_index().fillna(0)
        rggi_plus_retirements_plus_additions = rggi_plus_retirements.add(additions.loc[rggi_plus_retirements.index[0]:rggi_plus_retirements.index[-1]],fill_value=0)
        rggi_plus_retirements_plus_additions_timeseries = rggi_plus_retirements_plus_additions.cumsum()
        time_series_capacity_fig = px.area(rggi_plus_retirements_plus_additions_timeseries,labels={'value':'Capacity MW',
                                                              'index':'Date'},
            title='RGGI Time Series Capacity with Retirements & Additions')
        
        return rggi_plus_retirements_plus_additions_timeseries, time_series_capacity_fig
    
    def estimated_timeseries_capacity_PJM(self,RGGI_plants,full_tech_list,additions,date_of_last_report,PJM_retiredates=True):
        today_string = date_of_last_report.strftime('%Y-%m-%d').split('-')
        time_span = pd.date_range(start=(date_of_last_report).strftime('%Y-%m-%d'),end='2030-12-01',freq='MS') #
        full_capacity_time_series = pd.DataFrame(index=time_span,columns=full_tech_list)

        rggi_capacity_by_tech = RGGI_plants.groupby('PJM_tech')['Nameplate Capacity (MW)'].sum()
        rggi_capacity_by_tech_today = rggi_capacity_by_tech.to_frame().transpose()
        rggi_capacity_by_tech_today.index.name='Date'

        rggi_capacity_by_tech_today.index = [f'{today_string[0]}-{today_string[1]}-01']
        rggi_capacity_by_tech_today.index  = pd.to_datetime(rggi_capacity_by_tech_today.index)
        full_capacity_time_series = pd.concat([full_capacity_time_series,rggi_capacity_by_tech_today],axis=0).sort_index()
        full_capacity_time_series = full_capacity_time_series.fillna(0)

        if PJM_retiredates == True:
            RGGI_retirements = RGGI_plants.query('Fossil==1 and retirement_date<"2100-01-01"').pivot_table(index='retirement_date',columns='PJM_tech',values='Nameplate Capacity (MW)',aggfunc=np.sum)
        else:
            RGGI_retirements = RGGI_plants.query('Fossil==1 and retirement_date_original<"2100-01-01"').pivot_table(index='retirement_date',columns='PJM_tech',values='Nameplate Capacity (MW)',aggfunc=np.sum)
        RGGI_retirements.index = RGGI_retirements.index+pd.offsets.MonthBegin(0)
        rggi_plus_retirements = full_capacity_time_series.add(-RGGI_retirements,fill_value=0) #pd.concat([full_capacity_time_series,-RGGI_retirements],axis=0).sort_index().fillna(0)
        rggi_plus_retirements_plus_additions = rggi_plus_retirements.add(additions.loc[rggi_plus_retirements.index[0]:rggi_plus_retirements.index[-1]],fill_value=0)
        rggi_plus_retirements_plus_additions_timeseries = rggi_plus_retirements_plus_additions.cumsum()
        time_series_capacity_fig = px.area(rggi_plus_retirements_plus_additions_timeseries,labels={'value':'Capacity MW',
                                                              'index':'Date'},
            title='RGGI Time Series Capacity with Retirements & Additions')
        
        return rggi_plus_retirements_plus_additions_timeseries, time_series_capacity_fig