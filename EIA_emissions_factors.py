import pandas as pd

def download_EF_from_EIA(fuels_list):
    emissions_factors = pd.read_html('https://www.eia.gov/environment/emissions/co2_vol_mass.php',index_col=0)[0]
    emissions_factors = emissions_factors.iloc[1:]
    EF_dict = {'PG':'Propane',
            'DFO':'Diesel and Home Heating Fuel (Distillate Fuel Oil)', 'KER':'Kerosene',
            'WC':'Coal (All types)','BFG':'Natural Gas','NG':'Natural Gas','OG':'Natural Gas',
            'RFO':'Residual Heating Fuel (Businesses only)','JF':'Jet Fuel','PC':'Petroleum coke',
                'ANT':'Anthracite','BIT':'Bituminous','RC':'Subbituminous','SC':'Subbituminous','SUB':'Subbituminous',
                'LIG':'Lignite','TDF':'Tire-derived fuelb','WO':'Waste oilb'
            }
    EF_df = pd.DataFrame.from_dict(EF_dict,orient='index')
    EF_df = EF_df.join(emissions_factors['Kilograms CO2'],on=0)['Per Million Btu'].to_frame('KgCO2_perMmbtu')

    full_ef_df = pd.DataFrame(index=fuels_list)
    full_ef_df = full_ef_df.join(EF_df).fillna(0)
    return full_ef_df