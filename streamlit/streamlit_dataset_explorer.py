# Copyright 2026 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config

# Import python packages
import streamlit as st
import pandas as pd

from snowflake.snowpark import Session
import numpy as np
import pandas as pd

import json

from snowflake.snowpark.functions import col

from typing import Any, List, Dict, Tuple, Mapping, cast

st.set_page_config(layout="wide")

st.markdown("""
<style>
    .stContainer > div {
        overflow: visible !important;
    }
    [data-testid="stVerticalBlock"] > div:has(iframe) {
        height: 95vh !important;
        min-height: 900px !important;
        display: flex !important;
        justify-content: center !important;
        align-items: flex-start !important;
    }
    iframe {
        height: 100% !important;
        min-height: 900px !important;
    }
</style>
""", unsafe_allow_html=True)

# We can also use Snowpark for our analyses!
from snowflake.snowpark.context import get_active_session
session = get_active_session()

MAX_COLUMNS_TO_PROFILE = 10

@st.cache_data()
def get_query_column_stats(table_name:str, columns:List[str], filter:str|None = None)->Dict[str,Dict]:
    columns = columns[:MAX_COLUMNS_TO_PROFILE]

    column_sql = f"""SELECT COLUMN_NAME, DATA_TYPE 
        FROM AME_AD_SALES_DEMO.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION"""
    column_data_types_df = session.sql(column_sql).collect()
    column_data_types = {_.COLUMN_NAME:_.DATA_TYPE for _ in column_data_types_df }
    
    def create_column_sql(column:str)->str:
        data_type = column_data_types[column]
        bucket_count=20
        result = f"""
            COUNT(DISTINCT {column}) as {column}_UNIQUE,"""
        
        if data_type in ['FLOAT','NUMBER','TIMESTAMP_LTZ','DECIMAL', 'DATE']:
            result = result + f"""
            (SELECT MIN({column}) FROM {table_name}) as {column}_MIN,
            (SELECT MAX({column}) FROM {table_name}) as {column}_MAX,"""
        if data_type in ['TEXT']:
            result = result + f"""
            (SELECT MIN({column}) FROM {table_name}) as {column}_MIN,
            (SELECT MAX({column}) FROM {table_name}) as {column}_MAX,"""
        else:
            result = result + f"""
            NULL as {column}_MIN,
            NULL as {column}_MAX,"""
        if data_type in ['FLOAT','NUMBER','DECIMAL', 'DATE', 'TEXT']:
            result = result + f"""
                NULL as {column}_DISTRIBUTION,"""
        else:
             result = result + f"""
                NULL as  {column}_DISTRIBUTION,"""

        result = result + f"""
            (SELECT ARRAY_AGG(DISTINCT {column}) FROM {table_name} SAMPLE (20 ROWS)) as {column}_SAMPLES,"""
        

        return result
    
    columns_distinct_sql = ''.join([create_column_sql(c) for c in columns])
    sql = f"""
    SELECT 
        COUNT(*) as ALL_ROWS_COUNT,
        {columns_distinct_sql}
    FROM {table_name}
    """
    if filter:
        sql = sql + f"""
    WHERE {filter}
    """
    
    column_stats = session.sql(sql).collect()
    column_stats_results = column_stats[0]
    all_rows_count = column_stats_results.ALL_ROWS_COUNT
    return {_:{
            'unique':column_stats_results[f'{_}_UNIQUE'], 
            'uniqueness': column_stats_results[f'{_}_UNIQUE'] / all_rows_count,
            'min':column_stats_results[f'{_}_MIN'], 
            'max':column_stats_results[f'{_}_MAX'], 
            'samples':column_stats_results[f'{_}_SAMPLES'], 
            'distribution':column_stats_results[f'{_}_DISTRIBUTION'], 
            } for _ in columns}

@st.cache_data()
def get_table_sample(table_name:str):
    df_sample = session.table(table_name).sample(n=30).collect()
    return df_sample


@st.cache_data()
def get_tables():
    df_tables = session.sql('''SELECT TABLE_CATALOG AS DATABASE_NAME, TABLE_SCHEMA AS SCHEMA_NAME, TABLE_NAME, COMMENT
    FROM AME_AD_SALES_DEMO.INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_CATALOG = 'AME_AD_SALES_DEMO'
      AND TABLE_SCHEMA IN ('INGEST','HARMONIZED', 'ANALYSE','DATA_SHARING')''').select([col(f'"{_.strip().upper()}"').alias(_.upper()) for _ in 'database_name,schema_name,table_name,comment'.split(',')]).collect()
    
    return {'.'.join([_.SCHEMA_NAME, _.TABLE_NAME]):{
        'table_full_name':'.'.join([_.DATABASE_NAME, _.SCHEMA_NAME, _.TABLE_NAME]),
        'comment': _.COMMENT
    } for _ in df_tables}

table_details = get_tables()

# --- Base64 Icon (Used for all nodes) ---
# NOTE: Using the reliable house icon Base64 string from previous steps for visibility.
WORKING_BASE64_ICON = (
    "data:image/png;base64,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"
)


# --- 1. Fetch Table Dependencies Dynamically ---
st.cache_data()
def get_table_lineage_list():
    lineage_map_df = session.sql('SELECT * FROM APPS.ACCOUNT_USAGE_CREATE_TABLE_AS_SELECT_VW').collect()
    lineage_map = [(_.TARGET_TABLE_NAME, json.loads(_.SOURCE_TABLES)) for _ in lineage_map_df]
    database_name = session.get_current_database().strip('"')
    lineage_map_list = [(target_table_name.replace(f'{database_name}.', ''), [source_table_name.replace(f'{database_name}.', '') for source_table_name in source_table_names]) for target_table_name, source_table_names in lineage_map]    
    return lineage_map_list

LINEAGE_MAP = get_table_lineage_list()

# Single arrow relationships (target -> source) - kept as fallback for direct transformations
SINGLE_RELATIONSHIPS = []


# --- 2. Helper Function to Build Nodes and Edges ---

def build_graph_data(lineage_map, single_relationships):
    """Converts the lineage map and single relationships into agraph Node and Edge objects."""
    all_nodes = set()
    edges = []
    
    # Process Many-to-One Relationships
    for target, sources in lineage_map:
        if target in table_details:
            all_nodes.add(target)
            for source in sources:
                if source in table_details:
                    all_nodes.add(source)
                    edges.append(Edge(
                        source=source,
                        target=target,
                        label="", # Keeping edge labels clean for clarity
                        color="#0056B3", # Blue for main lineage flows
                        width=2,
                        type="arrow"
                    ))

    # Process Single Relationships (A -> B, based on your notation)
    for target, source in single_relationships:
        all_nodes.add(target)
        all_nodes.add(source)
        edges.append(Edge(
            source=source,
            target=target,
            label="",
            color="#FF9900", # Orange for direct transformations
            width=2,
            type="arrow"
        ))
        
    # Create Node Objects with image
    nodes = []
    for node_id in all_nodes:
        # Assign a different color/shape based on the prefix for better visualization
        node_color = "#3399FF" if node_id.startswith("INGEST") else "#00CC99"
        node_label = node_id.replace("_", " ").split(".")[-1] # Clean up label
        node_title = table_details[node_id]['comment']

        nodes.append(Node(
            id=node_id,
            label=node_label,
            title=node_title,
            size=20,
            shape='image',
            image=WORKING_BASE64_ICON,
            color=node_color # Use color as fill if the image doesn't override it
        ))
        
    return nodes, edges

# Build the data
agraph_nodes, agraph_edges = build_graph_data(LINEAGE_MAP, SINGLE_RELATIONSHIPS)


# --- 3. Configure the Graph ---

config = Config(
    width="100%",
    height=1600,
    directed=True,
    physics=False,
    
    # --- Layout Configuration (Consolidated for Hierarchical) ---
    layout={
        "clustering": {"enabled": True},
        "hierarchical": {
            "enabled": True, 
            "levelSeparation": 200,
            "nodeSpacing": 100,
            "direction": "LR",
            "sortMethod": "directed",
        }
    },
    
    # --- Edge Styling ---
    nodeHighlightBehavior=True,
    highlightColor="#FFD700", # Gold highlight
    edges={
        "hoverWidth": 0.5, 
        "selectWidth": 0.5,
        "smooth": {
            "enabled": True,  # Enable smoothing
            "type": "cubicBezier" # Choose the type of curve (e.g., dynamic or cubicBezier)
        }
    },
)

# --- 4. Render the Component in Streamlit ---

st.title("Data Explorer")
st.subheader("Data Flow from Ingestion to Harmonized Aggregates")
with st.container(border=True):
    return_value = agraph(
        nodes=agraph_nodes, 
        edges=agraph_edges, 
        config=config
    )

# Optional: Display the return value
if return_value:
    with st.container(border=True):
        st.toast(f"Table: **{return_value}**", icon=None, duration="short")
    
        table_detail = table_details[return_value]    
        table_full_name = table_detail['table_full_name']
        table_comment =  table_detail['comment']
        columns = session.table(table_full_name).columns
    
        stats = get_query_column_stats(table_full_name, columns, '')
        samples = get_table_sample(table_full_name)
    
        st.subheader(table_full_name)
        st.markdown(f'_{table_comment}_')

        tabs = st.tabs(['Distribution', 'Sample'])
        with tabs[0]:
            dist = [[_norm/sum(_bucket) for _norm in _bucket] for _bucket in[([_bucket['COUNT'] for _bucket in json.loads(stats[_stat]['distribution'])] if stats[_stat]['distribution'] else []) for _stat in stats]]
            def render_sample(sample:list)->str:
                return ', '.join([f'{_}' for _ in sample])
                    
            stats_samples = [render_sample(_x) for _x in [json.loads(stats[_]['samples']) for _ in stats if stats[_]['samples']]]
            editor_columns_df = pd.DataFrame(
                {
                    'column': [_ for _ in stats],
                    'unique': [stats[_]['unique'] for _ in stats],
                    'min': [stats[_]['min'] for _ in stats],
                    'max': [stats[_]['max'] for _ in stats],
                    'samples': stats_samples,
                    'dist': dist
                }
            )
            
            st.data_editor(editor_columns_df, 
                num_rows="fixed", hide_index=True, use_container_width=True, height=500,
                column_config={
                                "column": st.column_config.Column(
                                    "Attribute",
                                    help="Attribute name",
                                    width="medium",
                                    required=True,
                                    disabled=True),
                                "unique": st.column_config.Column(
                                    "Unique values",
                                    help="Unique values count",
                                    width="small",
                                    required=True,
                                    disabled=True),
                                "min": st.column_config.Column(
                                    "Minimum value",
                                    help="Min value",
                                    width="small",
                                    required=True,
                                    disabled=True),
                                "max": st.column_config.Column(
                                    "Maximun value",
                                    help="Max value",
                                    width="small",
                                    required=True,
                                    disabled=True),
                                "dist": st.column_config.BarChartColumn(
                                    "Distribution",
                                    help="The number of distinct values in this column",
                                    width="medium",
                                    y_min=0.0,
                                    y_max=1.0),
                                })    
        with tabs[1]:
            st.data_editor(samples)