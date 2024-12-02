import pandas as pd
import numpy as np

def gather_PJM_retirements_with_issues():
    PJM_deactivations = pd.read_xml('https://www.pjm.com/pub/planning/downloads/xml/GenDeactivationUnits.xml')
    RGGI_pjm_States = ['New Jersey','Delaware','Maryland']
    PJM_deactivations['RGGI']  =np.where(PJM_deactivations.State.isin(RGGI_pjm_States),1,0)
    RGGI_PJM_deactivations = PJM_deactivations.query('RGGI==1 and Capacity>=25')
    RGGI_PJM_deactivations.Status.unique()
    RGGI_PJM_deactivations['WithdrawnDeactivationDate'] = pd.to_datetime(RGGI_PJM_deactivations['WithdrawnDeactivationDate'],format='%m/%d/%Y')
    
    withdrawn_deactivations_since_2022 = RGGI_PJM_deactivations.query('Status=="Withdrawn Deactivation" and WithdrawnDeactivationDate>"2022-01-01"')
    
    Future_RGGI_PJM_deactivations_with_issues = RGGI_PJM_deactivations.query('Status=="Future Deactivation" and ReliabilityAnalysis=="Issue identified"')
    
    id_dict_EIA= {'Brandon Shores 1':'Brandon Shores 1',
                'Brandon Shores 2':'Brandon Shores 2',
                'Wagner 3': 'Herbert A Wagner 3',
                'Wagner 4': 'Herbert A Wagner 4',
                'Indian River 4':'Indian River Generating Station 4'}
    
    Future_RGGI_PJM_deactivations_with_issues['EIA_UnitName'] = Future_RGGI_PJM_deactivations_with_issues['UnitName'].map(id_dict_EIA)
    Future_RGGI_PJM_deactivations_with_issues = Future_RGGI_PJM_deactivations_with_issues.set_index('EIA_UnitName')
    Future_RGGI_PJM_deactivations_with_issues['ProjectedDeactivationDate'] =pd.to_datetime(Future_RGGI_PJM_deactivations_with_issues['ProjectedDeactivationDate'],format='%m/%d/%Y')

    return Future_RGGI_PJM_deactivations_with_issues