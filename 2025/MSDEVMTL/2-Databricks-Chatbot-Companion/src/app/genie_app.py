"""
"""

import logging
from dotenv import load_dotenv
import os
import streamlit as st
import folium
from streamlit_folium import st_folium
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import *
import pandas as pd

import genie_utils as utils

# Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Local Map Location
DEFAULT_MAP_LOCATION = [53.542719, -113.494685]

# Databricks Genie Setup
load_dotenv(".env_local")
load_dotenv(".env", override=False)

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID")

# Page Layout
st.set_page_config(page_title="Genie Chatbot - Your Data Companion", layout="wide")
chat_col, map_col = st.columns(2)

## Session State management
if "conversation_id" not in st.session_state:
    utils.clear_genie_state()

with chat_col: 
    st.header("💬 Ask your Yelp Data Companion: ")

    msg_container = st.container(height=500)
        
    # Chat history
    for message in st.session_state.chat_history:
        with msg_container.chat_message("user"):
            st.markdown(message["question"])
        with msg_container.chat_message("assistant"):
            if message["text"] is not None:
                st.markdown(message["text"])
            # if message["query"] is not None:
            #     st.code(message["query"], language="sql")
            if message["df"] is not None:
                st.dataframe(message["df"])
            
    # User Input
    if user_prompt:= st.chat_input("Ask Genie..."):
        with msg_container.chat_message("user"):
            st.markdown(user_prompt)

        with st.spinner("Genie is thinking..."):
            # Connect to Workspace and Genie
            ws = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
            genie = ws.genie

            if st.session_state.conversation_id is None:
                # Start a Conversation
                conversation = genie.start_conversation_and_wait(
                    space_id=GENIE_SPACE_ID,
                    content=user_prompt
                )

                st.session_state.conversation_id = conversation.conversation_id
            else:
                # Continue a Conversation
                conversation = genie.create_message_and_wait(
                    space_id=GENIE_SPACE_ID,
                    conversation_id=st.session_state.conversation_id,
                    content=user_prompt
                )

            # Retrieve response
            text, sql_query, pd_df = utils.process_genie_response(ws,conversation.attachments)

            with msg_container.chat_message("assistant"):
                if text is not None:
                    st.markdown(text)
                # if sql_query is not None:
                #     st.code(sql_query, language="sql")
                if pd_df is not None:
                    st.dataframe(pd_df)

            # Filter and extract data for the Map
            if pd_df is not None:
                map_df = utils.extract_data_for_map(pd_df)
            else:
                map_df = None

            st.session_state.map_df = map_df

            # Save to history
            st.session_state.chat_history.append({
                "question": user_prompt,
                "text": text,
                "query": sql_query,
                "df": pd_df
            })

with map_col: 
    # --- Right Column: Map View ---
    st.header("📍 Business Insights Map")

    with st.container(height=500, border=None, key="map_container"):

        map_df = st.session_state.map_df
        m = folium.Map(location=DEFAULT_MAP_LOCATION, zoom_start=10)

        if map_df is not None and not map_df.empty:
            
            # Limit to show max 10 rows
            map_df = map_df.head(10)

            # Initialize map centered
            m = folium.Map(
                location=[map_df["latitude"].mean(), map_df["longitude"].mean()], 
                zoom_start=utils.map_zoom(
                    latitude_max=map_df["latitude"].max(),
                    latitude_min=map_df["latitude"].min(),
                    longitude_max=map_df["longitude"].max(),
                    longitude_min=map_df["longitude"].min()
                )
            )

            # Add markers with styled popups
            for _, row in map_df.iterrows():
                folium.Marker(
                    location=[row["latitude"], row["longitude"]],
                    icon=folium.Icon(color='red', icon='info-sign'),
                    popup=folium.Popup(
                        utils.marker_popup_html(
                            business_name=row["name"], 
                            general_text=row.get("summary"),
                            review_text=row.get("review_text"),
                            tip_text=row.get("tip_text")
                        ),
                        max_width=250, 
                        sticky=True, 
                        show=True
                    )
                ).add_to(m)
        else:
            folium.TileLayer(
                tiles='CartoDB Positron',
                attr='CartoDB',
                name='Greyed Out',
                control=False
            ).add_to(m)

            # Disable user interaction
            m.options.update({
                'scrollWheelZoom': False,
                'dragging': False,
                'zoomControl': False
            })

        st_folium(m, width=675, height=450)

    # Start a new Conversation button
    if st.button("Start a new Conversation"):
        utils.clear_genie_state()
        st.rerun()

