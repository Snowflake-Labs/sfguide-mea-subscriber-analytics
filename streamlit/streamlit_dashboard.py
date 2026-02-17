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


import json
from typing import Any, Dict, List

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session


st.set_page_config(page_title="Analytics Assistant", layout="wide")

DATABASE = "AME_AD_SALES_DEMO"
SCHEMA = "ANALYSE"
SEMANTIC_VIEW = "AME_AD_SALES_SEMANTIC_VIEW"



@st.cache_resource
def get_session():
    return get_active_session()


def run_query(sql: str) -> pd.DataFrame:
    session = get_session()
    df = session.sql(sql).to_pandas()
    return df


def _escape(value: str) -> str:
    return value.replace("'", "''")


def _like_pattern(value: str) -> str:
    return f"%{_escape(value)}%"


def _format_in_clause(values) -> str:
    escaped = [f"'{_escape(str(v))}'" for v in values]
    return f"({', '.join(escaped)})"


def render_subscriber_view():
    st.header("Subscriber Explorer")
    col1, col2, col3 = st.columns([2, 2, 1])
    search_term = col1.text_input("Search (name, email, persona)")
    selected_tier = col2.multiselect(
        "Subscription Tier",
        options=run_query("SELECT DISTINCT tier FROM AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED ORDER BY 1")["TIER"].tolist(),
    )
    persona_options = run_query(
        "SELECT DISTINCT persona FROM AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS ORDER BY 1"
    )["PERSONA"].dropna().tolist()
    persona_filter = col3.multiselect("Persona", options=persona_options)

    base_sql = """
        SELECT
            spe.profile_id,
            spe.unique_id,
            spe.full_name,
            spe.email,
            spe.tier,
            abl.persona,
            abl.avg_site_visits_per_month,
            abl.login_frequency_per_week,
            abl.total_events,
            spe.lad_code,
            spe.area_name,
            spe.income_level,
            spe.education_level,
            churn.predicted_churn_prob,
            churn.churn_risk_segment,
            ltv.predicted_ltv
        FROM AME_AD_SALES_DEMO.HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED spe
        LEFT JOIN AME_AD_SALES_DEMO.HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS abl
            ON abl.unique_id = spe.unique_id
        LEFT JOIN AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_CHURN_RISK churn
            ON churn.profile_id = spe.profile_id
        LEFT JOIN AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_LTV_SCORES ltv
            ON ltv.profile_id = spe.profile_id
        WHERE 1 = 1
    """

    filters = []
    if search_term:
        pattern = _like_pattern(search_term)
        filters.append(
            f" AND (full_name ILIKE '{pattern}' OR email ILIKE '{pattern}'"
            f" OR persona ILIKE '{pattern}')"
        )
    if selected_tier:
        filters.append(f" AND tier IN {_format_in_clause(selected_tier)}")
    if persona_filter:
        filters.append(f" AND persona IN {_format_in_clause(persona_filter)}")

    df = run_query(base_sql + "".join(filters) + " LIMIT 500")
    df = df.rename(columns={
        "PREDICTED_CHURN_PROB": "churn_prob",
        "PREDICTED_LTV": "predicted_ltv"
    })

    st.dataframe(df, use_container_width=True)

    selected_profile = st.selectbox(
        "Drill-down to profile_id",
        options=df["PROFILE_ID"].tolist() if not df.empty else [],
        index=0 if not df.empty else None,
    )

    if selected_profile:
        escaped_profile = _escape(selected_profile)
        detail_sql = f"""
            SELECT *
            FROM AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_FEATURES
            WHERE PROFILE_ID = '{escaped_profile}'
        """
        detail_df = run_query(detail_sql)

        ltv_sql = f"""
            SELECT PREDICTED_LTV, LTV_TARGET
            FROM AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_LTV_SCORES
            WHERE PROFILE_ID = '{escaped_profile}'
        """
        ltv_df = run_query(ltv_sql)

        churn_sql = f"""
            SELECT PREDICTED_CHURN_PROB, CHURN_RISK_SEGMENT
            FROM AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_CHURN_RISK
            WHERE PROFILE_ID = '{escaped_profile}'
        """
        churn_df = run_query(churn_sql)

        st.subheader("Subscriber Feature Detail")
        st.json(detail_df.to_dict(orient="records"))

        metric_cols = st.columns(2)
        if not ltv_df.empty:
            metric_cols[0].metric(
                "Predicted LTV",
                f"{ltv_df.iloc[0]['PREDICTED_LTV']:.2f}",
                delta=f"Target {ltv_df.iloc[0]['LTV_TARGET']:.2f}"
            )
        if not churn_df.empty:
            metric_cols[1].metric(
                "Churn Probability",
                f"{churn_df.iloc[0]['PREDICTED_CHURN_PROB']:.2%}",
                delta=churn_df.iloc[0]['CHURN_RISK_SEGMENT']
            )


def render_ads_performance_view():
    st.header("Ad Sales Performance")
    col1, col2, col3 = st.columns(3)

    campaign_filter = col1.multiselect(
        "Campaign",
        run_query(
            "SELECT DISTINCT campaign_id FROM AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE ORDER BY 1"
        )["CAMPAIGN_ID"].tolist(),
    )
    category_filter = col2.multiselect(
        "Content Category",
        run_query(
            "SELECT DISTINCT content_category FROM AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE ORDER BY 1"
        )["CONTENT_CATEGORY"].tolist(),
    )
    date_range = col3.date_input(
        "Reporting Window",
        value=None,
        help="Filter report_month between the selected dates",
    )

    sql = """
        SELECT
            report_month,
            campaign_id,
            advertiser_name,
            content_category,
            impressions,
            clicks,
            ctr,
            spend,
            ecpm
        FROM AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE
        WHERE 1 = 1
    """
    if campaign_filter:
        sql += f" AND campaign_id IN {_format_in_clause(campaign_filter)}"
    if category_filter:
        sql += f" AND content_category IN {_format_in_clause(category_filter)}"
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
        if start_date and end_date:
            sql += f" AND report_month BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'"

    sql += " ORDER BY report_month"
    df = run_query(sql)

    if df.empty:
        st.info("No matching records for the selected filters.")
        return

    st.line_chart(df.set_index("REPORT_MONTH")["ECPM"], height=300)
    st.line_chart(df.set_index("REPORT_MONTH")["CTR"], height=300)
    st.dataframe(df, use_container_width=True)


def render_events_view():
    st.header("Journey & Ad Event Explorer")
    tab1, tab2 = st.tabs(["Clickstream Events", "Ad Events"])

    with tab1:
        persona_options = run_query(
            "SELECT DISTINCT persona FROM AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_HISTORY ORDER BY 1"
        )["PERSONA"].dropna().tolist()
        persona_filter = st.multiselect("Persona", persona_options)
        tier_options = run_query(
            "SELECT DISTINCT tier FROM AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_HISTORY ORDER BY 1"
        )["TIER"].dropna().tolist()
        tier_filter = st.multiselect("Tier", tier_options)

        sql = """
            SELECT
                h.persona,
                h.tier,
                COALESCE(cf.primary_content_type, 'unknown') AS primary_content_type,
                SUM(h.clickstream_events) AS clickstream_events,
                SUM(h.clickstream_active_days) AS active_days,
                SUM(h.behavioural_events) AS behavioural_events,
                AVG(
                    CASE
                        WHEN h.clickstream_active_days > 0 THEN h.clickstream_events / h.clickstream_active_days
                        ELSE NULL
                    END
                ) AS avg_events_per_active_day
            FROM AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_HISTORY h
            LEFT JOIN AME_AD_SALES_DEMO.ANALYSE.FE_SUBSCRIBER_CONTENT_FEATURES cf
                ON cf.unique_id = h.unique_id
            WHERE 1 = 1
        """
        if persona_filter:
            sql += f" AND persona IN {_format_in_clause(persona_filter)}"
        if tier_filter:
            sql += f" AND tier IN {_format_in_clause(tier_filter)}"
        sql += " GROUP BY 1,2,3 ORDER BY 1,2,3"
        df = run_query(sql)
        if df.empty:
            st.info("No journey metrics for the selected filters.")
        else:
            heatmap_source = df.pivot_table(
                index="PERSONA",
                columns="PRIMARY_CONTENT_TYPE",
                values="CLICKSTREAM_EVENTS",
                aggfunc="sum",
                fill_value=0,
            )
            st.subheader("Clickstream Intensity by Persona & Primary Content Type")
            st.dataframe(heatmap_source, use_container_width=True)
            bar_data = df.groupby("PRIMARY_CONTENT_TYPE")["BEHAVIOURAL_EVENTS"].sum()
            st.bar_chart(bar_data, height=300)
            st.dataframe(df, use_container_width=True)

    with tab2:
        campaign_filter = st.multiselect(
            "Campaign",
            run_query(
                "SELECT DISTINCT campaign_id FROM AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG ORDER BY 1"
            )["CAMPAIGN_ID"].tolist(),
        )
        persona_filter = st.multiselect(
            "Target Persona",
            run_query(
                "SELECT DISTINCT value::STRING AS persona"
                " FROM AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG, LATERAL FLATTEN(input => target_personas)"
                " WHERE value IS NOT NULL ORDER BY 1"
            )["PERSONA"].tolist(),
        )
        sql = """
            SELECT
                report_date,
                campaign_id,
                content_category,
                impressions,
                clicks,
                ctr,
                spend,
                effective_cpm
            FROM AME_AD_SALES_DEMO.HARMONIZED.AD_PERFORMANCE_DAILY_AGG
            WHERE 1 = 1
        """
        if campaign_filter:
            sql += f" AND campaign_id IN {_format_in_clause(campaign_filter)}"
        if persona_filter:
            persona_conditions = " OR ".join(
                [f"ARRAY_CONTAINS('{p}'::VARIANT, target_personas)" for p in persona_filter]
            )
            sql += f" AND ({persona_conditions})"
        sql += " ORDER BY report_date"
        df = run_query(sql)
        if df.empty:
            st.info("No ad delivery metrics for the selected filters.")
        else:
            metric_df = df.set_index("REPORT_DATE")
            st.subheader("Impressions & Spend by Day")
            st.line_chart(metric_df["IMPRESSIONS"], height=250)
            st.line_chart(metric_df["SPEND"], height=250)
            st.subheader("Effective CPM & CTR")
            st.line_chart(metric_df["EFFECTIVE_CPM"], height=250)
            st.line_chart(metric_df["CTR"], height=250)
            st.dataframe(df, use_container_width=True)


def send_analyst_message(messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    import _snowflake
    
    request_body = {
        "messages": messages,
        "semantic_view": f"{DATABASE}.{SCHEMA}.{SEMANTIC_VIEW}",
    }
    
    resp = _snowflake.send_snow_api_request(
        "POST",
        "/api/v2/cortex/analyst/message",
        {},
        {},
        request_body,
        {},
        30000,
    )
    
    if resp["status"] < 400:
        return json.loads(resp["content"])
    else:
        raise Exception(f"Failed request with status {resp['status']}: {resp['content']}")


def display_analyst_content(content: List[Dict[str, str]], message_index: int = 0) -> None:
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            with st.expander("Suggested questions", expanded=True):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(suggestion, key=f"sug_{message_index}_{suggestion_index}"):
                        st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            with st.expander("SQL Query", expanded=False):
                st.code(item["statement"], language="sql")
            with st.expander("Results", expanded=True):
                try:
                    df = run_query(item["statement"])
                    if len(df.index) > 1:
                        data_tab, chart_tab = st.tabs(["Data", "Chart"])
                        data_tab.dataframe(df, use_container_width=True)
                        if len(df.columns) > 1:
                            chart_df = df.set_index(df.columns[0])
                            chart_tab.bar_chart(chart_df)
                        else:
                            chart_tab.info("Need at least 2 columns for chart")
                    else:
                        st.dataframe(df, use_container_width=True)
                except Exception as e:
                    st.error(f"Error running query: {e}")


def render_ask_ai_view():
    st.markdown(
        "Ask questions about your subscriber data, churn risk, lifetime value, and ad performance."
    )
    
    if "analyst_messages" not in st.session_state:
        st.session_state.analyst_messages = []
    if "active_suggestion" not in st.session_state:
        st.session_state.active_suggestion = None
    
    if st.button("Clear conversation"):
        st.session_state.analyst_messages = []
        st.session_state.active_suggestion = None
        st.rerun()
    
    st.markdown("**Try one of these to get started:**")
    sample_questions = [
        "Which personas have the highest churn risk among Premium subscribers, and what's their average predicted LTV?",
        "Identify high-value subscribers at risk of churning — show their tier, persona, and watch time trends",
        "Compare engagement metrics like watch time, login frequency, and session completion between active and inactive subscribers",
        "Which content categories drive the most ad revenue per subscriber, and how does that vary by income level?",
    ]
    cols = st.columns(2)
    for i, question in enumerate(sample_questions):
        if cols[i % 2].button(question, key=f"sample_{i}"):
            st.session_state.active_suggestion = question
            st.rerun()
    
    chat_container = None
    if st.session_state.analyst_messages:
        msg_count = len(st.session_state.analyst_messages)
        container_height = min(300 + (msg_count * 150), 900)
        chat_container = st.container(height=container_height)
        with chat_container:
            for message_index, message in enumerate(st.session_state.analyst_messages):
                role = "assistant" if message["role"] == "analyst" else "user"
                with st.chat_message(role):
                    if role == "user":
                        st.markdown(message["content"][0]["text"])
                    else:
                        display_analyst_content(message["content"], message_index)
    
    prompt = st.chat_input("Ask a question about your subscriber data...")
    
    if st.session_state.active_suggestion:
        prompt = st.session_state.active_suggestion
        st.session_state.active_suggestion = None
    
    if prompt:
        st.session_state.analyst_messages.append({
            "role": "user",
            "content": [{"type": "text", "text": prompt}]
        })
        
        if chat_container is None:
            msg_count = len(st.session_state.analyst_messages)
            container_height = min(300 + (msg_count * 150), 900)
            chat_container = st.container(height=container_height)
        
        with chat_container:
            with st.chat_message("user"):
                st.markdown(prompt)
            
            with st.chat_message("assistant"):
                with st.spinner("Thinking..."):
                    try:
                        response = send_analyst_message(st.session_state.analyst_messages)
                        content = response["message"]["content"]
                        st.session_state.analyst_messages.append({
                            "role": "analyst",
                            "content": content,
                            "request_id": response.get("request_id")
                        })
                        display_analyst_content(content, len(st.session_state.analyst_messages))
                    except Exception as e:
                        st.error(f"Error: {e}")
                        st.session_state.analyst_messages.pop()


def main():
    st.title("Analytics Assistant")
    st.sidebar.write("Session obtained via get_active_session().")

    view = st.sidebar.radio(
        "Dashboard",
        options=["Subscribers", "Ad Performance", "Journeys & Ads", "Analytics Assistant"],
    )

    if view == "Analytics Assistant":
        render_ask_ai_view()
    elif view == "Subscribers":
        render_subscriber_view()
    elif view == "Ad Performance":
        render_ads_performance_view()
    else:
        render_events_view()


if __name__ == "__main__":
    main()
