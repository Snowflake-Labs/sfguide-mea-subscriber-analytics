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


"""Streamlit app to review INGEST schema tables and metadata."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session


DATABASE = "AME_AD_SALES_DEMO"
SCHEMA = "INGEST"

st.set_page_config(page_title="INGEST Data Explorer", layout="wide")


@st.cache_resource
def get_session():
    return get_active_session()


def run_query(sql: str) -> pd.DataFrame:
    session = get_session()
    return session.sql(sql).to_pandas()


@st.cache_data(show_spinner=False)
def get_connected_sources() -> pd.DataFrame:
    try:
        return run_query(f"SHOW STAGES IN SCHEMA {DATABASE}.{SCHEMA}")
    except Exception:
        return pd.DataFrame(columns=["name", "type", "url", "comment", "created_on"])


@st.cache_data(show_spinner=False)
def get_ingest_tables() -> pd.DataFrame:
    sql = f"""
        SELECT
            table_name,
            row_count,
            bytes,
            created,
            last_altered,
            table_type
        FROM {DATABASE}.information_schema.tables
        WHERE table_schema = '{SCHEMA}'
        ORDER BY table_name
    """
    return run_query(sql)


@st.cache_data(show_spinner=False)
def get_table_columns(table_name: str) -> pd.DataFrame:
    sql = f"""
        SELECT column_name, data_type, is_nullable, comment
        FROM {DATABASE}.information_schema.columns
        WHERE table_schema = '{SCHEMA}'
          AND table_name = '{table_name.upper()}'
        ORDER BY ordinal_position
    """
    return run_query(sql)


def qualify(table_name: str) -> str:
    return f"{DATABASE}.{SCHEMA}.{table_name}"


def summarize_column(table_name: str, column: str, data_type: str) -> Dict[str, Optional[str]]:
    qualified = qualify(table_name)
    column_quoted = f'"{column}"'
    dtype = data_type.upper()
    base_dtype = dtype.split("(")[0]

    is_semi_structured = base_dtype in {"OBJECT", "VARIANT", "ARRAY"}
    is_numeric = base_dtype in {"NUMBER", "DECIMAL", "INT", "INTEGER", "FLOAT", "DOUBLE", "REAL", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"}

    min_expr = f"MIN({column_quoted})" if not is_semi_structured else "NULL"
    max_expr = f"MAX({column_quoted})" if not is_semi_structured else "NULL"
    avg_expr = f"AVG({column_quoted}::DOUBLE)" if is_numeric else "NULL"

    summary_sql = f"""
        SELECT
            {min_expr} AS min_value,
            {max_expr} AS max_value,
            {avg_expr} AS avg_value,
            COUNT(*) AS total_count,
            COUNT(DISTINCT {column_quoted}) AS distinct_count,
            COUNT_IF({column_quoted} IS NULL) AS null_count
        FROM {qualified}
    """

    summary_df = run_query(summary_sql)
    summary = summary_df.iloc[0].to_dict() if not summary_df.empty else {}

    distinct_count = summary.get("DISTINCT_COUNT") or 0
    sample_values: List[str] = []
    if distinct_count <= 20:
        sample_sql = f"""
            SELECT DISTINCT {column_quoted} AS value
            FROM {qualified}
            WHERE {column_quoted} IS NOT NULL
            ORDER BY value
            LIMIT 20
        """
        sample_df = run_query(sample_sql)
        sample_values = sample_df["VALUE"].astype(str).tolist()

    summary["sample_values"] = ", ".join(sample_values) if sample_values else None
    return summary


def render_sources_section():
    st.header("Connected Sources")
    sources_df = get_connected_sources()
    if sources_df.empty:
        st.info("No stages registered in the INGEST schema.")
        return

    display_cols = [
        col
        for col in ["NAME", "TYPE", "URL", "COMMENT", "CREATED_ON"]
        if col in sources_df.columns
    ]
    st.dataframe(sources_df[display_cols], use_container_width=True)


def render_tables_overview() -> str:
    st.header("INGEST Tables Overview")
    tables_df = get_ingest_tables()
    if tables_df.empty:
        st.warning("No tables found in the INGEST schema.")
        return ""

    summary_cols = [
        "TABLE_NAME",
        "TABLE_TYPE",
        "ROW_COUNT",
        "BYTES",
        "CREATED",
        "LAST_ALTERED",
    ]
    st.dataframe(tables_df[summary_cols], use_container_width=True)

    selection = st.selectbox(
        "Select a table to inspect",
        options=tables_df["TABLE_NAME"].tolist(),
    )
    return selection


def render_table_details(table_name: str):
    st.subheader(f"Table Detail: {table_name}")
    qualified = qualify(table_name)

    columns_df = get_table_columns(table_name)
    st.markdown("#### Columns")
    st.dataframe(columns_df, use_container_width=True)

    with st.spinner("Analyzing column statistics..."):
        summary_rows = []
        for _, row in columns_df.iterrows():
            column_name = row["COLUMN_NAME"]
            data_type = row["DATA_TYPE"]
            stats = summarize_column(table_name, column_name, data_type)
            summary_rows.append(
                {
                    "column": column_name,
                    "data_type": data_type,
                    "min": stats.get("MIN_VALUE"),
                    "max": stats.get("MAX_VALUE"),
                    "avg": stats.get("AVG_VALUE"),
                    "distinct": stats.get("DISTINCT_COUNT"),
                    "nulls": stats.get("NULL_COUNT"),
                    "sample_values": stats.get("sample_values"),
                }
            )

    st.markdown("#### Column Summary")
    st.dataframe(pd.DataFrame(summary_rows), use_container_width=True)

    st.markdown("#### Sample Rows")
    sample_sql = f"SELECT * FROM {qualified} LIMIT 50"
    sample_df = run_query(sample_sql)
    if sample_df.empty:
        st.info("Table has no data.")
    else:
        st.dataframe(sample_df, use_container_width=True)


def main():
    st.title("INGEST Data Explorer")
    st.caption("Review connected sources and table metadata within the INGEST schema.")

    render_sources_section()
    st.divider()
    table = render_tables_overview()
    if table:
        render_table_details(table)


if __name__ == "__main__":
    main()

