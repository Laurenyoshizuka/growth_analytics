
import streamlit as st
import sqlite3
import os
import requests
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import itertools
import numpy as np
import re
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

page = st.query_params.get('page', [''])[0]


@st.cache_data
def load_data():


    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = os.path.dirname(base_path)
    
    data_folder = os.path.join(base_path, 'data')
    db_folder = os.path.join(base_path, 'db')

    if not os.path.exists(data_folder):
        st.error(f"Data folder not found at {data_folder}")
        return

    if not os.path.exists(db_folder):
        os.makedirs(db_folder)

    db_path = os.path.join(db_folder, 'database.db')
    duckdb_path = os.path.join(db_folder, 'duckdb.db')

    data_frames = {}

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        csv_files = [f for f in os.listdir(data_folder) if f.endswith('.csv')]

        for file_name in csv_files:
            file_path = os.path.join(data_folder, file_name)

            if not os.path.exists(file_path):
                st.error(f"File not found: {file_path}")
                continue  

            try:
                table_name = os.path.splitext(file_name)[0]

                # Special handling for market_data
                if table_name == "market":
                    df = pd.read_csv(file_path, skiprows=4).dropna(axis=1, how="all")
                    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
                    table_exists = cursor.fetchone()
                    
                    if not table_exists:
                        df.to_sql(table_name, conn, if_exists='replace', index=False)
                        if page == 'eda.py':
                            st.success(f"Market Data loaded and stored as '{table_name}' table successfully!")
                    else:       
                        if page == 'eda.py':
                            st.info(f"Table '{table_name}' already exists, skipping.")
                
                else:
                    df = pd.read_csv(file_path)
                    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
                    table_exists = cursor.fetchone()
                    
                    if not table_exists:
                        df.to_sql(table_name, conn, if_exists='replace', index=False)
                        if page == 'eda.py':
                            st.success(f"Data from {file_name} loaded and stored as '{table_name}' table successfully!")
                    else:
                        if page == 'eda.py':
                            st.info(f"Table '{table_name}' already exists, skipping.")

                data_frames[table_name] = df

            except Exception as e:
                if page == 'eda.py':
                    st.error(f"Error loading {file_name}: {e}")
        
        
        
        # try:
        #     ddb_conn = duckdb.connect(duckdb_path)
            
        #     # Process JSON files
        #     json_files = [f for f in os.listdir(data_folder) if f.endswith('.json')]
            
        #     # Special handling for Pixel.json
        #     pixel_file = os.path.join(data_folder, "Pixel.json")
        #     if os.path.exists(pixel_file):
        #         st.header("Processing Pixel Data")
                
        #         # First analyze the format
        #         analysis = analyze_pixel_data(pixel_file)
                
        #         # Then process with the robust parser
        #         df_pixel = process_pixel_json_robust(pixel_file, "Pixel", ddb_conn, data_folder)
        #         if df_pixel is not None:
        #             data_frames["Pixel"] = df_pixel
                
        #         # Remove Pixel.json from the list of files to process
        #         json_files = [f for f in json_files if f.lower() != "pixel.json"]
            
        #     # Process remaining JSON files
        #     for file_name in json_files:
        #         file_path = os.path.join(data_folder, file_name)
                
        #         if not os.path.exists(file_path):
        #             st.error(f"File not found: {file_path}")
        #             continue

        #         if os.path.getsize(file_path) == 0:
        #             st.error(f"Error: {file_name} is empty!")
        #             continue
                
        #         table_name = os.path.splitext(file_name)[0]
                
        #         # Continue with your standard JSON processing for other files
        #         # [Original JSON processing code would go here]
        #         # ...
            
        #     # Close DuckDB connection
        #     ddb_conn.close()
            
        # except Exception as e:
        #     st.error(f"DuckDB error: {str(e)}")
        #     import traceback
        #     st.error(traceback.format_exc())
        
        conn.commit()
        conn.close()

    except sqlite3.OperationalError as e:
        st.error(f"Error while accessing the database: {e}")

    return data_frames

def clean_df(df):
    for col in df.columns:
        if df[col].dtype == 'object':
            if col == 'CAMPAIGN_GROUP':
                continue
            if df[col].str.contains(r'\d{4}[-/]\d{2}[-/]\d{2}', regex=True).any():
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                continue
            if col in ['PLATFORM', 'GMV_CATEGORY', 'COUNTRY']:
                continue
            df[col] = df[col].replace({'[\$,]': ''}, regex=True).astype(int)
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.drop_duplicates()
    return df


def run_eda():
    st.title('Exploratory Data Analysis (EDA)')
    with st.expander("✨ Overview"):
        st.write("""
        This app is divided into 3 pages:
        1. **EDA**  ℹ️ Use the sidebar navigation to select a dataset to explore
        2. **Outbound Sizing**
        3. **Attribution Model**
        """)

    data_frames = load_data()
    dataset_map = {'outbound': 'Outbound Campaigns', 
                   'market': 'Market Data', 
                   'tenants': 'Tenants',
                   'pixel': 'Pixel Data'
                   }
    reverse_map = {v: k for k, v in dataset_map.items()}
    data_choice = st.sidebar.selectbox('Select Dataset', dataset_map.values())
    selected_key = reverse_map.get(data_choice)
    st.markdown("---")

    #####################
    # Outbound Data EDA #
    #####################
    if selected_key == 'outbound':
        outbound_data = data_frames['outbound']
        outbound_data = clean_df(outbound_data)

        # Adding campgin length col
        campaign_length = (pd.to_datetime(outbound_data['CAMPAIGN_LAST_DATE'], errors='coerce') - 
                        pd.to_datetime(outbound_data['CAMPAIGN_START_DATE'], errors='coerce')).dt.days
        last_date_index = outbound_data.columns.get_loc('CAMPAIGN_LAST_DATE')

        if 'CAMPAIGN_LENGTH' in outbound_data.columns:
            outbound_data.drop('CAMPAIGN_LENGTH', axis=1, inplace=True)

        outbound_data.insert(last_date_index + 1, 'CAMPAIGN_LENGTH', campaign_length)
  
        st.sidebar.markdown('---')

        st.header('Outbound Campaign Data')

        metric_columns = [
            'NB_EMAILS', 'NB_CONTACTS_TOUCHED', 'NB_COMPANIES_TOUCHED', 'TOTAL_NB_CLICKS',
            'TOTAL_NB_POSITIVE_REPLIES_PER_CAMPAIGN', 'TOTAL_NB_NEGATIVE_REPLIES_PER_CAMPAIGN',
            'NB_COMPANIES_CLICKED', 'NB_CUSTOMERS_FROM_OB_ALL_TIME', 'PIPELINE_OPP_AMOUNT_FROM_OB_ALL_TIME',
            'NEW_ARR_FROM_OB_ALL_TIME', 'NB_COMPANIES_TOUCHED_ICP', 'NB_COMPANIES_CLICKED_ICP',
            'NB_COMPANIES_REPLIED_ICP', 'NB_COMPANIES_REPLIED_POSITIVE_ICP',
            'NB_COMPANIES_CLICKED_ICP.1', 'NB_COMPANIES_REPLIED_ICP.1', 'NB_COMPANIES_REPLIED_POSITIVE_ICP.1'
        ]

        campaign_groups = sorted(outbound_data['CAMPAIGN_GROUP'].unique())
        st.sidebar.subheader('Filter Outbound Campaigns')
        selected_campaign_group = st.sidebar.selectbox('Select Campaign Group', ['All'] + campaign_groups, help='Select a campaign group to filter the data (applies to data summary, but not to visualizations)')

        st.sidebar.subheader('Select Metric to Visualize')
        selected_metric = st.sidebar.selectbox('Select Metric', metric_columns, help='Select a metric to visualize')

        query = ""
        if selected_campaign_group != 'All':
            query += f"CAMPAIGN_GROUP == '{selected_campaign_group}'"

        filtered_outbound_data = outbound_data.query(query) if query else outbound_data
        filtered_outbound_data = filtered_outbound_data.iloc[:, :-3]

        st.dataframe(filtered_outbound_data, hide_index=True)

        # Descriptive Statistics
        st.subheader('Descriptive Statistics')
        st.write(filtered_outbound_data.describe())

        # Summary of unique values
        unique_values = filtered_outbound_data.nunique().reset_index()
        unique_values.columns = ['Column', 'Unique Values']
        unique_values['Unique Values'] = unique_values['Unique Values'].astype(str)
        unique_values = unique_values.set_index('Column')
        st.subheader('Unique Values per Column')
        st.dataframe(unique_values, use_container_width=True)
        
        st.markdown("---") 

        # Visualizations by Campaign Group
        st.subheader(f'Campaign Performance by {selected_metric}')
        outbound_data = outbound_data.sort_values(by=[selected_metric], ascending=False)
        fig = px.bar(
            outbound_data,
            x='CAMPAIGN_GROUP',
            y=selected_metric,
            title=f'{selected_metric} per Campaign',
            labels={'{selected_metric}': 'Total Clicks', 'CAMPAIGN_GROUP': 'Campaign'},
        )
        st.plotly_chart(fig, use_container_width=True) 

        # Visualizations of Outbound Data over Time
        outbound_data['CAMPAIGN_START_DATE'] = pd.to_datetime(outbound_data['CAMPAIGN_START_DATE'], errors='coerce').dt.date
        outbound_data['CAMPAIGN_LAST_DATE'] = pd.to_datetime(outbound_data['CAMPAIGN_LAST_DATE'], errors='coerce').dt.date

        if selected_metric in outbound_data.columns:
            filtered_data = outbound_data.dropna(subset=['CAMPAIGN_START_DATE', 'CAMPAIGN_LAST_DATE', selected_metric, 'CAMPAIGN_GROUP'])

            fig = go.Figure()
            color_cycle = itertools.cycle(px.colors.qualitative.Set3 + px.colors.qualitative.Dark24)
            campaign_colors = {group: next(color_cycle) for group in filtered_data['CAMPAIGN_GROUP'].unique()}
            line_styles = itertools.cycle(["solid", "dot", "dash", "longdash", "dashdot"])

            for campaign_group, group_data in filtered_data.groupby('CAMPAIGN_GROUP'):
                campaign_data = group_data[['CAMPAIGN_START_DATE', 'CAMPAIGN_LAST_DATE', selected_metric]]
                for _, row in group_data.iterrows():
                    fig.add_trace(go.Scatter(
                        x=[row['CAMPAIGN_START_DATE'], row['CAMPAIGN_LAST_DATE']],
                        y=[row[selected_metric], row[selected_metric]],
                        mode='lines+markers',
                        name=campaign_group,
                        line=dict(color=campaign_colors[campaign_group], dash=next(line_styles)),
                        hoverinfo="text",
                        hovertext=campaign_data.apply(lambda row: f"Start: {row['CAMPAIGN_START_DATE']}<br>End: {row['CAMPAIGN_LAST_DATE']}<br>{selected_metric}: {row[selected_metric]}", axis=1)
                    ))
            fig.update_layout(
                title=f"{selected_metric} Over Time by Campaign Group",
                xaxis_title="Campaign Date Range",
                yaxis_title=selected_metric,
                xaxis=dict(
                    tickangle=-45,
                    rangeslider=dict(
                        visible=True,
                        thickness=0.05,
                    ),
                    type='date',
                ),
                hovermode="x unified",
                height=800 
            )
            st.plotly_chart(fig, use_container_width=True) 

        fig = px.scatter(
            outbound_data,
            x='CAMPAIGN_LENGTH',
            y=selected_metric,
            color='CAMPAIGN_GROUP',
            title=f'{selected_metric} vs Campaign Length',
            labels={
                'CAMPAIGN_LENGTH': 'Campaign Length (days)',
                selected_metric: selected_metric,
                'CAMPAIGN_GROUP': 'Campaign Group'
            },
            hover_data=['CAMPAIGN_START_DATE', 'CAMPAIGN_LAST_DATE']
        )
        st.plotly_chart(fig, use_container_width=True)

        if 'CAMPAIGN_START_DATE' in outbound_data.columns:
            outbound_data['CAMPAIGN_START_DATE'] = pd.to_datetime(outbound_data['CAMPAIGN_START_DATE'], errors='coerce')
            campaigns_by_start = outbound_data.groupby(outbound_data['CAMPAIGN_START_DATE'].dt.month)[selected_metric].sum().reset_index()
            fig = px.bar(campaigns_by_start, x='CAMPAIGN_START_DATE', y=selected_metric, title=f'Campaigns by Start Month ({selected_metric})')
            st.plotly_chart(fig, use_container_width=True) 


    ######################
    # Marketing Data EDA #
    ######################
    elif selected_key == 'market':
        market_data = data_frames.get('market')
        market_data = clean_df(market_data)

        if market_data is None or market_data.empty:
            st.error("Market data is missing or empty. Please check your data source.")
            return

        st.header('Market Data')

        platforms = market_data['PLATFORM'].dropna().unique() if 'PLATFORM' in market_data else []
        gmv_categories = market_data['GMV_CATEGORY'].dropna().unique() if 'GMV_CATEGORY' in market_data else []
        countries = sorted(market_data['COUNTRY'].dropna().unique()) if 'COUNTRY' in market_data else []

        st.sidebar.subheader('Filter Market Data')
        selected_platform = st.sidebar.selectbox('Select Platform', ['All'] + list(platforms))
        gmv_categories = list(gmv_categories)
        selected_gmv_category = st.sidebar.multiselect('Select GMV Categories',list(gmv_categories), default=list(gmv_categories))
        selected_country = st.sidebar.selectbox('Select Country', ['All'] + list(countries))

        query = []
        if selected_platform != 'All':
            query.append(f"PLATFORM == '{selected_platform}'")
        if selected_gmv_category != 'All':
            query.append(f"GMV_CATEGORY in {selected_gmv_category}")
        if selected_country != 'All':
            query.append(f"COUNTRY == '{selected_country}'")

        query_string = " & ".join(query)
        

        filtered_market_data = market_data.query(query_string) if query_string else market_data
        st.write(filtered_market_data)

        # Descriptive Statistics
        st.subheader('Descriptive Statistics')
        st.write(filtered_market_data.describe())

        # Summary of unique values
        unique_values = filtered_market_data.nunique().reset_index()
        unique_values.columns = ['Column', 'Unique Values']
        unique_values['Unique Values'] = unique_values['Unique Values'].astype(str)
        unique_values = unique_values.set_index('Column')
        st.subheader('Unique Values per Column')
        st.dataframe(unique_values, use_container_width=True)

        st.markdown("---")

        # Total GMV per GMV Category
        st.markdown("###### Total GMV per GMV Category")
        filtered_market_data['TOTAL_GMV'] = filtered_market_data['TOTAL_GMV'].replace('[\$,]', '', regex=True).astype(float)
        st.bar_chart(filtered_market_data.groupby('GMV_CATEGORY')['TOTAL_GMV'].sum())

        # AVG_GMV by GMV_CATEGORY
        log_transform = st.checkbox("Apply Log Transformation to AVG_GMV for Boxplots", value=False)
        filtered_market_data['AVG_GMV'] = filtered_market_data['AVG_GMV'].replace('[\$,]', '', regex=True).astype(float)
        if log_transform:
            filtered_market_data['AVG_GMV'] = filtered_market_data['AVG_GMV'].apply(lambda x: np.log1p(x) if x > 0 else None)
            y_axis_title = "Log(1 + AVG_GMV)"
            title = "AVG_GMV by GMV_CATEGORY (Log Transformed)"
        else:
            y_axis_title = "AVG_GMV"
            title = "AVG_GMV by GMV_CATEGORY"
        fig = px.box(
            filtered_market_data, 
            x='GMV_CATEGORY', 
            y='AVG_GMV', 
            title=title,
            labels={'AVG_GMV': y_axis_title}
        )
        st.plotly_chart(fig, use_container_width=True)

        # NB_DOMAINS vs TOTAL_GMV by GMV_CATEGORY
        filtered_market_data['NB_DOMAINS'] = pd.to_numeric(filtered_market_data['NB_DOMAINS'], errors='coerce')
        fig = px.scatter(filtered_market_data, x='NB_DOMAINS', y='TOTAL_GMV',
                        color='GMV_CATEGORY',
                        title='NB_DOMAINS vs TOTAL_GMV by GMV_CATEGORY',
                        labels={'NB_DOMAINS': 'Number of Domains', 'TOTAL_GMV': 'Total GMV'})
        st.plotly_chart(fig, use_container_width=True)

        # Top Platforms by Total GMV
        top_platforms = filtered_market_data.groupby('PLATFORM')['TOTAL_GMV'].sum().nlargest(10).reset_index()
        fig = px.bar(top_platforms, x='PLATFORM', y='TOTAL_GMV', title='Top 10 Platforms by Total GMV')
        st.plotly_chart(fig, use_container_width=True)

        if selected_platform != 'All':
            platform_filtered_data = filtered_market_data[filtered_market_data['PLATFORM'] == selected_platform]
        else:
            platform_filtered_data = filtered_market_data

        top_countries = (
            platform_filtered_data.groupby('COUNTRY')['TOTAL_GMV']
            .sum()
            .nlargest(10)
            .reset_index()
        )

        # Top Countries by Total GMV
        fig = px.bar(
            top_countries, 
            x='COUNTRY', 
            y='TOTAL_GMV', 
            title=f'Top 10 Countries by Total GMV ({selected_platform})' if selected_platform != 'All' else 'Top 10 Countries by Total GMV',
            labels={'TOTAL_GMV': 'Total GMV ($)', 'COUNTRY': 'Country'}
        )
        st.plotly_chart(fig, use_container_width=True)


   #####################
    # Tenants Data EDA #
    ####################
    elif selected_key == 'tenants':
        tenants_data = data_frames.get('tenants')
        
        if tenants_data is None or tenants_data.empty:
            st.error("Tenants data is missing or empty. Please check your data source.")
            return

        st.header('Tenants Data')
        st.write(tenants_data.head())

        # Summary of unique values
        unique_values = tenants_data.nunique().reset_index()
        unique_values.columns = ['Column', 'Unique Values']
        unique_values['Unique Values'] = unique_values['Unique Values'].astype(str)
        unique_values = unique_values.set_index('Column')
        st.subheader('Unique Values per Column')
        st.dataframe(unique_values, use_container_width=True)

        # Unique stores per Tenant
        unique_stores = tenants_data.groupby('TENANT_ID')['DATASOURCE_ID'].nunique().reset_index()
        unique_stores = unique_stores.sort_values(by='DATASOURCE_ID', ascending=False)
        top_10_unique_stores = unique_stores.head(10)
        fig = px.bar(top_10_unique_stores, x='TENANT_ID', y='DATASOURCE_ID', 
             labels={'TENANT_ID': 'Tenant ID', 'DATASOURCE_ID': 'Number of Unique Stores'},
             title='Top 10 Tenants by Number of Stores',
             color='DATASOURCE_ID', color_continuous_scale='Viridis')
        st.plotly_chart(fig)

        
    ##################
    # PIxel Data EDA #
    ##################
    elif selected_key == 'pixel':
        orders_per_month_per_store = data_frames.get('orders_month_store')
        attribution_model_90 = data_frames.get('attribution_model_90')
        attribution_model_180 = data_frames.get('attribution_model_180')
        attribution_cjm = data_frames.get('attribution_cjm')

        st.write("Orders per Month per Store:")
        st.dataframe(orders_per_month_per_store,  use_container_width=True, hide_index=True)
        st.write("Attribution Model (2 different lookback window legths):")
        st.write('90-day window')
        st.dataframe(attribution_model_90, use_container_width=True, hide_index=True)
        st.write('180-day window')
        st.dataframe(attribution_model_180, use_container_width=True, hide_index=True)
        st.write('Multi-touch Customer Journey Dataset')
        st.dataframe(attribution_cjm, use_container_width=True, hide_index=True)

    return data_frames