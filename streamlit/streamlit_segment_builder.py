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


"""Streamlit audience segmentation builder scaffold."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from uuid import uuid4

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

try:
    from streamlit_sortables import sort_items  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    sort_items = None


st.set_page_config(page_title="Audience Segment Builder", layout="wide")

DATABASE = "AME_AD_SALES_DEMO"


@st.cache_resource
def get_session():
    """Return the active Snowflake session provided by Snowflake-hosted Streamlit."""
    return get_active_session()


def run_query(sql: str) -> pd.DataFrame:
    session = get_session()
    return session.sql(sql).to_pandas()


@dataclass
class AttributeDefinition:
    name: str
    label: str
    data_type: str
    source_table: str
    description: Optional[str] = None
    stats: Optional[Dict[str, str]] = None

    @property
    def key(self) -> str:
        return f"{self.source_table}.{self.name}"


ATTRIBUTE_SOURCES: Dict[str, Sequence[Dict[str, Iterable[str]]]] = {
    "Profile": (
        {
            "table": "HARMONIZED.SUBSCRIBER_PROFILE_ENRICHED",
            "include": [
                "PROFILE_ID",
                "FULL_NAME",
                "EMAIL",
                "TIER",
                "LAD_CODE",
                "LAD_NAME",
                "COUNTRY",
                "DIGITAL_MEDIA_PROPENSITY",
            ],
        },
    ),
    "Demographics": (
        {
            "table": "DATA_SHARING.DEMOGRAPHICS_PROFILES",
            "include": [
                "AGE",
                "AGE_BAND",
                "INCOME_LEVEL",
                "EDUCATION_LEVEL",
                "MARITAL_STATUS",
                "FAMILY_STATUS",
                "BEHAVIORAL_DIGITAL_MEDIA_CONSUMPTION_INDEX",
                "BEHAVIORAL_FINANCIAL_INVESTMENT_INTEREST",
            ],
        },
    ),
    "Engagement": (
        {
            "table": "HARMONIZED.AGGREGATED_BEHAVIORAL_LOGS",
            "include": [
                "AVG_SITE_VISITS_PER_MONTH",
                "LOGIN_FREQUENCY_PER_WEEK",
                "CONTENT_VIEWS_COUNT",
                "PRIMARY_CONTENT_TAG",
                "PERSONA",
            ],
        },
    ),
    "Ad Interactions": (
        {
            "table": "HARMONIZED.AD_PERFORMANCE_DAILY_AGG",
            "include": [
                "CAMPAIGN_ID",
                "CONTENT_CATEGORY",
                "IMPRESSION_WEIGHT",
                "OBSERVED_UNIQUE_SUBSCRIBERS",
                "EFFECTIVE_CPM",
            ],
        },
    ),
    "Model Scores": (
        {
            "table": "ANALYSE.FE_SUBSCRIBER_CHURN_RISK",
            "include": [
                "PREDICTED_CHURN_PROB",
                "CHURN_RISK_SEGMENT",
            ],
        },
        {
            "table": "ANALYSE.FE_SUBSCRIBER_LTV_SCORES",
            "include": [
                "PREDICTED_LTV",
                "LTV_TARGET",
            ],
        },
    ),
}


def _parse_table_name(fully_qualified: str) -> Dict[str, str]:
    parts = fully_qualified.split(".")
    if len(parts) == 2:
        return {"schema": parts[0], "table": parts[1]}
    if len(parts) == 3:
        return {"database": parts[0], "schema": parts[1], "table": parts[2]}
    raise ValueError(f"Unexpected table reference: {fully_qualified}")


def _information_schema_query(schema: str, table: str, database: str = DATABASE) -> str:
    return (
        f"SELECT column_name, data_type, comment"
        f" FROM {database}.information_schema.columns"
        f" WHERE table_schema = '{schema.upper()}'"
        f"   AND table_name = '{table.upper()}'"
        f" ORDER BY ordinal_position"
    )


@st.cache_data(show_spinner=False)
def load_attribute_metadata() -> Dict[str, List[AttributeDefinition]]:
    palette: Dict[str, List[AttributeDefinition]] = {group: [] for group in ATTRIBUTE_SOURCES}

    for group, table_confs in ATTRIBUTE_SOURCES.items():
        for table_conf in table_confs:
            table_name = table_conf["table"]
            include = set(col.upper() for col in table_conf.get("include", []))
            exclude = set(col.upper() for col in table_conf.get("exclude", []))

            parsed = _parse_table_name(table_name)
            schema = parsed.get("schema")
            table = parsed.get("table")
            database = parsed.get("database", DATABASE)

            cols_df = run_query(_information_schema_query(schema, table, database))

            for _, row in cols_df.iterrows():
                col_name = row["COLUMN_NAME"].upper()
                if include and col_name not in include:
                    continue
                if col_name in exclude:
                    continue

                display_label = col_name.replace("_", " ").title()
                palette[group].append(
                    AttributeDefinition(
                        name=col_name,
                        label=display_label,
                        data_type=row["DATA_TYPE"],
                        source_table=f"{database}.{schema}.{table}",
                        description=row.get("COMMENT"),
                    )
                )

        palette[group].sort(key=lambda attr: attr.label)

    return palette


def build_attribute_index(palette: Dict[str, List[AttributeDefinition]]) -> Dict[str, AttributeDefinition]:
    index: Dict[str, AttributeDefinition] = {}
    for attributes in palette.values():
        for attr in attributes:
            index[attr.key] = attr
    return index


def create_group_node(label: str = "Group", *, is_root: bool = False) -> Dict:
    return {
        "id": str(uuid4()),
        "type": "group",
        "name": label,
        "operator": "AND",
        "negated": False,
        "children": [],
        "is_root": is_root,
    }


def create_condition_node(attribute_key: str, attribute: AttributeDefinition) -> Dict:
    return {
        "id": str(uuid4()),
        "type": "condition",
        "attribute": attribute_key,
        "operator": default_operator(attribute.data_type),
        "value": "",
    }


def get_segment_tree() -> Dict:
    if "segment_tree" not in st.session_state:
        st.session_state["segment_tree"] = create_group_node(label="Root", is_root=True)
    return st.session_state["segment_tree"]


def set_segment_tree(tree: Dict) -> None:
    st.session_state["segment_tree"] = tree


def find_group(node: Dict, group_id: str) -> Optional[Dict]:
    if node.get("id") == group_id and node.get("type") == "group":
        return node
    for child in node.get("children", []):
        if child.get("type") == "group":
            result = find_group(child, group_id)
            if result:
                return result
    return None


def remove_child(parent: Dict, child_id: str) -> None:
    parent["children"] = [child for child in parent.get("children", []) if child.get("id") != child_id]


def list_group_paths(node: Dict, path: Optional[List[str]] = None) -> List[Tuple[str, str]]:
    path = path or []
    label = node.get("name", "Group")
    current_path = path + [label]
    results = [(node["id"], " > ".join(current_path))]
    for child in node.get("children", []):
        if child.get("type") == "group":
            results.extend(list_group_paths(child, current_path))
    return results


def default_operator(data_type: str) -> str:
    data_type = data_type.upper()
    if data_type in {"NUMBER", "FLOAT", "DOUBLE", "INT", "INTEGER", "DECIMAL"}:
        return ">="
    if data_type in {"BOOLEAN"}:
        return "="
    if data_type.endswith("DATE") or data_type.endswith("TIME"):
        return "BETWEEN"
    return "="


def operator_options(data_type: str) -> List[str]:
    data_type = data_type.upper()
    if data_type in {"NUMBER", "FLOAT", "DOUBLE", "INT", "INTEGER", "DECIMAL"}:
        return ["=", "!=", ">", ">=", "<", "<=", "BETWEEN"]
    if data_type in {"BOOLEAN"}:
        return ["=", "!="]
    if data_type.endswith("DATE") or data_type.endswith("TIME"):
        return ["=", "!=", "BETWEEN", ">", "<"]
    return ["=", "!=", "IN", "NOT IN", "CONTAINS", "NOT CONTAINS", "STARTS WITH", "ENDS WITH"]


def render_palette(
    palette: Dict[str, List[AttributeDefinition]],
    attribute_index: Dict[str, AttributeDefinition],
    selected_group_id: str,
    group_labels: Dict[str, str],
) -> None:
    st.subheader("Attribute Palette")
    search = st.text_input("Filter attributes", placeholder="Search by name or table")
    st.caption(f"Adding to: {group_labels.get(selected_group_id, 'Root')}")
    for group, attributes in palette.items():
        with st.expander(group, expanded=True):
            for attr in attributes:
                if search and search.lower() not in attr.label.lower():
                    continue
                details = [f"`{attr.data_type}`", attr.source_table]
                if attr.description:
                    details.append(attr.description)
                info_col, action_col = st.columns([4, 1])
                info_col.markdown("- **{}**  {}".format(attr.label, " • ".join(details)))
                if action_col.button("Add", key=f"add-{attr.key}"):
                    group = find_group(get_segment_tree(), selected_group_id)
                    if group is not None:
                        group["children"].append(create_condition_node(attr.key, attr))
                        set_segment_tree(get_segment_tree())
                        st.rerun()


def node_display_label(node: Dict, attribute_index: Dict[str, AttributeDefinition]) -> str:
    if node.get("type") == "group":
        name = node.get("name", "Group")
        count = len(node.get("children", []))
        return f"Group • {name} ({count} items)"

    attr_key = node.get("attribute")
    attr = attribute_index.get(attr_key)
    attr_label = attr.label if attr else attr_key
    value = node.get("value") or "…"
    operator = node.get("operator", "=")
    return f"{attr_label} {operator} {value}"


def render_condition(
    condition: Dict,
    attribute_index: Dict[str, AttributeDefinition],
    parent_group: Dict,
) -> None:
    attr_keys = list(attribute_index.keys())
    if not attr_keys:
        st.warning("No attributes available to configure conditions.")
        return

    current_attr_key = condition.get("attribute", attr_keys[0])
    if current_attr_key not in attribute_index:
        current_attr_key = attr_keys[0]
        condition["attribute"] = current_attr_key

    attr_col, op_col, value_col, action_col = st.columns([3, 2, 3, 1])
    selected_attr = attr_col.selectbox(
        "Attribute",
        options=attr_keys,
        index=attr_keys.index(current_attr_key) if current_attr_key in attr_keys else 0,
        format_func=lambda key: attribute_index[key].label,
        key=f"attr-{condition['id']}",
    )
    if selected_attr != condition.get("attribute"):
        attr_def = attribute_index[selected_attr]
        condition["attribute"] = selected_attr
        condition["operator"] = default_operator(attr_def.data_type)
        condition["value"] = ""

    attr_def = attribute_index.get(condition["attribute"])
    data_type = attr_def.data_type if attr_def else "STRING"
    operators = operator_options(data_type)

    operator = op_col.selectbox(
        "Operator",
        options=operators,
        index=operators.index(condition.get("operator", operators[0]))
        if condition.get("operator") in operators
        else 0,
        key=f"op-{condition['id']}",
    )
    condition["operator"] = operator

    placeholder = "Comma separated" if operator in {"IN", "NOT IN"} else "Enter value"
    if operator == "BETWEEN":
        placeholder = "lower,upper"

    value = value_col.text_input(
        "Value",
        value=str(condition.get("value", "")),
        placeholder=placeholder,
        key=f"val-{condition['id']}",
    )
    condition["value"] = value

    if action_col.button("Remove", key=f"cond-del-{condition['id']}"):
        remove_child(parent_group, condition["id"])
        set_segment_tree(get_segment_tree())
        st.rerun()


def render_group(
    group: Dict,
    attribute_index: Dict[str, AttributeDefinition],
    selected_group_id: str,
    parent: Optional[Dict] = None,
) -> None:
    header_cols = st.columns([3, 2, 1, 1])
    group["name"] = header_cols[0].text_input(
        "Group name",
        value=group.get("name", "Group"),
        key=f"group-name-{group['id']}",
    )
    group["operator"] = header_cols[1].radio(
        "Logic",
        options=["AND", "OR"],
        index=["AND", "OR"].index(group.get("operator", "AND")),
        horizontal=True,
        key=f"group-logic-{group['id']}",
    )
    group["negated"] = header_cols[2].checkbox(
        "NOT",
        value=group.get("negated", False),
        key=f"group-not-{group['id']}",
    )
    if not group.get("is_root") and header_cols[3].button("Delete", key=f"group-del-{group['id']}"):
        if parent is not None:
            remove_child(parent, group["id"])
            set_segment_tree(get_segment_tree())
            st.rerun()

    action_cols = st.columns([1, 1, 2])
    if action_cols[0].button("Add subgroup", key=f"group-add-sub-{group['id']}"):
        group["children"].append(create_group_node(label="Nested Group"))
        set_segment_tree(get_segment_tree())
        st.rerun()
    if action_cols[1].button("Add condition", key=f"group-add-cond-{group['id']}"):
        if attribute_index:
            first_attr = next(iter(attribute_index.values()))
            group["children"].append(create_condition_node(first_attr.key, first_attr))
            set_segment_tree(get_segment_tree())
            st.rerun()
    action_cols[2].markdown("✅ Active group" if group["id"] == selected_group_id else "")

    children = group.get("children", [])
    if sort_items and len(children) > 1:
        labels = [
            f"{child['type']}::{child['id']}::{node_display_label(child, attribute_index)}"
            for child in children
        ]
        reordered = sort_items(labels, direction="vertical", key=f"sort-{group['id']}")
        if reordered and reordered != labels:
            order_map = {item.split("::")[1]: idx for idx, item in enumerate(reordered)}
            children.sort(key=lambda child: order_map.get(child["id"], 0))
            group["children"] = children
            set_segment_tree(get_segment_tree())
            st.rerun()

    for child in list(children):
        if child.get("type") == "group":
            with st.container():
                st.markdown(f"### {node_display_label(child, attribute_index)}")
                render_group(child, attribute_index, selected_group_id, parent=group)
        else:
            with st.container():
                st.markdown(f"**Condition:** {node_display_label(child, attribute_index)}")
                render_condition(child, attribute_index, group)


def render_canvas(
    attribute_index: Dict[str, AttributeDefinition],
    group_labels: Dict[str, str],
) -> None:
    st.subheader("Segment Canvas")
    tree = get_segment_tree()
    group_paths = list_group_paths(tree)
    if not group_paths:
        st.info("No groups available. Add a group to get started.")
        return

    selected_group_id = st.session_state.get("selected_group_id", tree["id"])
    if selected_group_id not in group_labels:
        selected_group_id = tree["id"]

    options = [gid for gid, _ in group_paths]
    index = options.index(selected_group_id) if selected_group_id in options else 0
    selected_group_id = st.selectbox(
        "Active group",
        options=options,
        index=index,
        format_func=lambda gid: group_labels[gid],
        key="select-active-group",
    )
    st.session_state["selected_group_id"] = selected_group_id

    controls = st.columns([1, 1, 3])
    if controls[0].button("Add subgroup to active", key="add-sub-active"):
        target = find_group(tree, selected_group_id)
        if target is not None:
            target["children"].append(create_group_node(label="Nested Group"))
            set_segment_tree(tree)
            st.rerun()
    if controls[1].button("Clear conditions", key="clear-tree"):
        tree["children"] = []
        set_segment_tree(tree)
        st.rerun()

    render_group(tree, attribute_index, selected_group_id, parent=None)


def render_metrics_panel() -> None:
    st.subheader("Live Segment Metrics")
    metric_cols = st.columns(3)
    metric_cols[0].metric("Matched Subscribers", "--", "+0%")
    metric_cols[1].metric("Share of Base", "--", "0.0%")
    metric_cols[2].metric("Average LTV", "--")
    st.write("Preview charts will appear here once conditions are configured.")


def main() -> None:
    st.title("Audience Segment Builder")
    st.caption("Build audience definitions from harmonized & analyse datasets")

    palette = load_attribute_metadata()
    attribute_index = build_attribute_index(palette)
    tree = get_segment_tree()
    group_paths = list_group_paths(tree)
    group_labels = {gid: label for gid, label in group_paths}

    if "selected_group_id" not in st.session_state:
        st.session_state["selected_group_id"] = tree["id"]
    elif st.session_state["selected_group_id"] not in group_labels:
        st.session_state["selected_group_id"] = tree["id"]

    palette_col, canvas_col = st.columns([1, 2])
    with palette_col:
        render_palette(palette, attribute_index, st.session_state["selected_group_id"], group_labels)

    with canvas_col:
        render_canvas(attribute_index, group_labels)

    st.divider()
    render_metrics_panel()


if __name__ == "__main__":
    main()

