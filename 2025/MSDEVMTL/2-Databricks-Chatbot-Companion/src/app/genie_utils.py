import logging
from typing import Optional, List, Tuple
import streamlit as st
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import *


# Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Static
MAP_ZOOM_LEVELS = [
    (0.01, 14),
    (0.05, 13),
    (0.1, 12),
    (0.5, 11),
    (1.0, 8),
    (float("inf"), 6)  # default fallback
]

def clear_genie_state() -> None:
    st.session_state.conversation_id = None
    st.session_state.chat_history = []
    st.session_state.map_df = None

def get_query_result(workspace: WorkspaceClient, statement_id: str) -> pd.DataFrame:
    result = workspace.statement_execution.get_statement(statement_id)
    columns=[i.name for i in result.manifest.schema.columns]
    return pd.DataFrame(result.result.data_array, columns=columns)

def process_genie_response(workspace: WorkspaceClient, attachments: List[GenieAttachment]) -> Tuple[Optional[str], Optional[str], Optional[pd.DataFrame]]:
    result_text = None
    result_query = None
    result_df = None

    for attachment in attachments:
        if attachment.text:
            result_text = attachment.text.content
        elif attachment.query:
            result_query = attachment.query.query
            result_df = get_query_result(workspace, attachment.query.statement_id)

    return result_text, result_query, result_df

def extract_data_for_map(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    required_fields = {"name", "latitude", "longitude"}
    optional_fields = {"tip_text", "review_text", "summary"}

    col_map = {col.lower(): col for col in df.columns}
    existing_cols = set(col_map.keys())

    if not required_fields.issubset(existing_cols):
        return None

    # Select matching columns
    selected_cols = [col_map[col] for col in required_fields]
    selected_cols += [col_map[col] for col in optional_fields if col in existing_cols]

    filtered_df = df[selected_cols].copy()

    # Enforce float for latitude/longitude
    for col in ["latitude", "longitude"]:
        true_col = col_map[col]
        filtered_df[true_col] = pd.to_numeric(filtered_df[true_col], errors="coerce").astype("float")

    # Enforce str type for text fields (fill NaNs to keep dtype clean)
    for col in ["name", "tip_text", "review_text", "summary"]:
        if col in col_map:
            true_col = col_map[col]
            filtered_df[true_col] = filtered_df[true_col].astype(str).fillna("")

    return filtered_df

def map_zoom(latitude_min: float, latitude_max: float, longitude_min: float, longitude_max: float) -> int:
    lat_range = latitude_max - latitude_min
    long_range = longitude_max - longitude_min

    max_range = max(lat_range, long_range)

    return next(zoom for threshold, zoom in MAP_ZOOM_LEVELS if max_range < threshold)

def marker_popup_html(business_name: str, review_text: Optional[str] = None, tip_text: Optional[str] = None, general_text: Optional[str] = None) -> str:
    popup_html = f"""
        <div style="
            padding: 3px 3px;
            background-color: white;
            color: black;
            border-radius: 10px;
            font-size: 14px;
            max-width: 200px;">
            <div style="font-weight: bold;">{business_name}</div>
    """

    if general_text:
        popup_html += f"""
            <div style="margin-top: 8px;">
                <div style="font-size: 10px;">{general_text}</div>
            </div>
        """
    
    if review_text:
        popup_html += f"""
            <div style="margin-top: 8px;">
                <span style="
                    background-color: #22c55e;
                    color: white;
                    font-size: 10px;
                    padding: 2px 6px;
                    border-radius: 6px;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;">
                    Review
                </span>
                <div style="margin-top: 4px;">{review_text}</div>
            </div>
        """

    if tip_text:
        popup_html += f"""
            <div style="margin-top: 6px;">
                <span style="
                    background-color: #4C6EF5;
                    color: white;
                    font-size: 10px;
                    padding: 2px 6px;
                    border-radius: 6px;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;">
                    Tip
                </span>
                <div style="margin-top: 4px;">{tip_text}</div>
            </div>            
        """

    popup_html += """
        </div>
    """

    return popup_html