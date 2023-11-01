import pandas as pd
import snowflake.connector
from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import *
from snowflake.snowpark.context import get_active_session
import streamlit as st
import plotly
import plotly.express as px
from streamlit_plotly_events import plotly_events

print('TEST 1!')

from datetime import datetime, timedelta

#SETTING UP CONNECTION TO SNOWFLAKE TO EXTRACT DATA FROM TABLES
conn = st.experimental_connection('snowpark', user='svarma@oneweb.net',
account='oneweb_operations_uk.eu-west-2.aws',
warehouse='SSDH_USER_WH',
database='DATAOPS_GN_FB_SV_POLAR_TEST',
schema='GN_CALCULATION',
authenticator='externalbrowser',
role='TENANT_DEV')

print('TEST 2!')

#GETTING GN ID DATA AND CACHING IT FOR FASTER PERFORMANCE
@st.cache_data(ttl=9999)  # Cache the query result for 9999 seconds
def get_data_gn():
    query = f"SELECT * FROM CALC_DM_CM_PROD_SNPS"
    gnmames = conn.query(query)
    return gnmames

gnnames = get_data_gn()

print(gnnames)

#GETTING POLAR PLOT DATA AND CACHING IT FOR FASTER PERFORMANCE
@st.cache_data(ttl=9999)  # Cache the query result for 9999 seconds
def get_data(start_date_str, end_date_str, inital_gnid):
    query = f"SELECT * FROM POLARTEST WHERE DATE BETWEEN '{selected_start_date}' AND '{selected_end_date}' AND GNID = '{initial_gnid}'"
    df = conn.query(query)
    print('lauren test')
    df['DATE'] = pd.to_datetime(df['DATE'])
    return df

#CONTRAINING THE CODE - USER TELLS THE QUERY WHAT GN TO USE
initial_gnid = st.selectbox("Select a GNID", gnnames['GN_ID'].unique())
st.write(initial_gnid)

st.title('Polar Plot :satellite_antenna: :rocket:')

yesterday = datetime.today() - timedelta(days=4)
thirty_days_ago = yesterday - timedelta(days=30)

#SETTING A DATE RANGE - THIS ALSO CONSTRAINS THE CODE
st.write('The app runs slower the larger the date range you choose is - maybe try intervals of 3 days?')
selected_start_date = st.date_input("Select the start date", min_value=thirty_days_ago, max_value=yesterday, value=thirty_days_ago)
selected_end_date = st.date_input("Select the end date", min_value=thirty_days_ago, max_value=yesterday, value=yesterday)

if selected_start_date <= selected_end_date:

    start_date_str = selected_start_date.strftime('%Y-%m-%d')
    end_date_str = selected_end_date.strftime('%Y-%m-%d')

    print('TEST 3!')
    print('loading data........')
    filtered_data = get_data(start_date_str, end_date_str, initial_gnid)
    print('shape of entire df spanning a date range')
    print(filtered_data.shape)

    #SLICERS TO USER INPUT
    with st.sidebar:
        '''
        adding everything slicer in sidebar for aesthetics
        '''
        selected_mea = st.multiselect("Select MEA Profile", filtered_data['MEAPROFILE'].unique())
        selected_sap_id = st.multiselect("Select SAP ID(s)", filtered_data['SAPID'].unique())
        sapdf = filtered_data[filtered_data['SAPID'].isin(selected_sap_id)]
        sat_id = st.multiselect("Select Satellite ID(s)", sapdf['SATELLITE_ID'].unique())
        categorization = st.multiselect("Select Categorization", sapdf['CATEGORIZATION_N'].unique())
        tracking_mode = st.multiselect("Select Tracking Mode", sapdf['TRACKINGMODE'].unique())
        rssi_drops = st.multiselect("Does RSSI Drop?", sapdf['RSSI_DROPS'].unique())
        print('shape of entire df spanning a date range (should be the same, we havent filtered it yet, just registering clicks)')
        print(filtered_data.shape)
        if selected_mea or selected_sap_id or categorization or sat_id or tracking_mode:
            if selected_mea:
                filtered_data = filtered_data[filtered_data['MEAPROFILE'].isin(selected_mea)]
            if selected_sap_id:
                sapdf = sapdf[sapdf['SAPID'].isin(selected_sap_id)]
            if sat_id:
                sapdf = sapdf[sapdf['SATELLITE_ID'].isin(sat_id)]
            if categorization:
                sapdf = sapdf[sapdf['CATEGORIZATION_N'].isin(categorization)]
            if tracking_mode:
                sapdf = sapdf[sapdf['TRACKINGMODE'].isin(tracking_mode)]
            if rssi_drops:
                sapdf = sapdf[sapdf['RSSI_DROPS'].isin(rssi_drops)]

        print('shape of filtered data - filtere dby sap, sat, mea, etc etc so should be lower than before)')
        print(sapdf.shape)

        st.subheader("Azimuth and Elevation Slicers")
        '''az and el double ended slicers'''
        azimuth_range = st.slider("Azimuth Range", 0, 360, (0, 360), 1)
        elevation_range = st.slider("Elevation Range", 0, 90, (0, 90), 1)

        min_azimuth, max_azimuth = azimuth_range
        min_elevation, max_elevation = elevation_range

        filtered_data_az = sapdf[(sapdf['AZPOSSRSF'] >= min_azimuth) & (sapdf['AZPOSSRSF'] <= max_azimuth)]
        print('shape of filtered data - filtered elevation')
        filtered_data_elv = filtered_data_az[(filtered_data_az['ELPOSSRSF'] >= min_elevation) & (filtered_data_az['ELPOSSRSF'] <= max_elevation)]
        print(filtered_data_elv.shape)

        def counting(value):
            '''
            create a count function that counts number of cidsg
            assuming all cidsg are unique and 1:1 map to categorization, we can get
            the length of values categorizations in the df, meaning we have 
            the counts of categorizations... checked with PBI and it matches
            '''
            filtered_rows = filtered_data_elv[filtered_data_elv["CATEGORIZATION_N"] == str(value)]
            value_counts = filtered_rows['CONTACT_ID_SATELLITE_GATEWAYSEC'].value_counts()
            print(value)
            print(len(value_counts))
            print()
            print()
            return len(value_counts)
    
    print('onto plotting now!')
    '''
    Here, we build the polar plot
    - give it hover tool tips (can change to include many others)
    '''
    fig = px.scatter_polar(
        filtered_data_elv,
        r='ELPOSSRSF',
        theta='AZPOSSRSF',
        color='RSSI', hover_data=["CONTACT_ID_SATELLITE_GATEWAYSEC", "TIMESTAMP", "SATELLITE_ID",
                                   "SAPID", "SRSF_START_TIME", "SRSF_END_TIME", "CATEGORIZATION_N"],
        color_continuous_scale= 'RdYlGn', range_color=[0, 25]
    )
    
    fig.update_traces(marker=dict(size=3), selector=dict(mode='markers'))
    fig.update_polars(radialaxis_range=[90, 0])
    fig.update_layout(title="Azimuth vs. Elevation")

    st.plotly_chart(fig)

    st.dataframe(filtered_data_elv, height= 250, hide_index= True)

    countofgbs = counting("Good Beacon Signal")
    countofd = counting("Degraded")
    countofsu = counting("Satellite Unavailable")
    countofla = counting("Late Allocation")
    countofr = counting("Reallocation")
    countofnc = counting("No Contact")
    countoffc = counting("SAP Failed Contact")
    countofaf = counting("Allocation Failed")

    '''writing counts to streamlit'''

    st.write('Count of Good Beacon Signal: ', countofgbs)
    st.write('Count of Degraded Contacts: ', countofd)
    st.write('Count of Satellite Unavailable Contacts: ', countofsu)
    st.write('Count of Late Allocations: ', countofla)
    st.write('Count of Reallocations: ', countofr)
    st.write('Count of No Contacts: ', countofnc)
    st.write('Count of Failed Contacts: ', countoffc)
    st.write('Count of Allocation Failures: ', countofaf)
