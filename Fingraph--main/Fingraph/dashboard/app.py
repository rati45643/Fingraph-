import os
import sys
import datetime
import logging
from typing import Dict, Any, List, Optional

import streamlit as st
import pandas as pd
from neo4j import GraphDatabase, Driver
from pyvis.network import Network
import streamlit.components.v1 as components

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FinGraph-Dashboard")

# Neo4j Default Connection
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

# Page Configuration
st.set_page_config(
    page_title="FinGraph | AML Investigation Dashboard",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ------------------------------------------------------------------------------
# 1. Neo4j Database Connection & Caching (Read-Only)
# ------------------------------------------------------------------------------
@st.cache_resource
def get_neo4j_driver(uri: str, user: str, password: str) -> Optional[Driver]:
    """Initializes and caches the Neo4j driver connection."""
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password), connection_timeout=5.0)
        driver.verify_connectivity()
        return driver
    except Exception as e:
        logger.error(f"Failed to connect to Neo4j at {uri}: {e}")
        return None

def check_connection(driver: Optional[Driver]) -> bool:
    if not driver:
        return False
    try:
        driver.verify_connectivity()
        return True
    except Exception:
        return False

# ------------------------------------------------------------------------------
# 2. Data Retrieval Queries (Read-Only)
# ------------------------------------------------------------------------------
@st.cache_data(ttl=15)
def fetch_kpi_summary(_driver: Driver) -> Dict[str, int]:
    """Retrieves high-level counts and KPI metrics from Neo4j."""
    query = """
    CALL {
        MATCH (a:Account) RETURN count(a) AS total_accounts
    }
    CALL {
        MATCH (t:Transaction) RETURN count(t) AS total_transactions
    }
    CALL {
        MATCH (a:Account) WHERE a.risk_level IN ['HIGH', 'CRITICAL'] RETURN count(a) AS high_risk_accounts
    }
    CALL {
        MATCH (t:Transaction) WHERE t.is_suspicious = true RETURN count(t) AS suspicious_transactions
    }
    RETURN total_accounts, total_transactions, high_risk_accounts, suspicious_transactions
    """
    try:
        with _driver.session() as session:
            record = session.run(query).single()
            if record:
                return {
                    "total_accounts": record["total_accounts"] or 0,
                    "total_transactions": record["total_transactions"] or 0,
                    "high_risk_accounts": record["high_risk_accounts"] or 0,
                    "suspicious_transactions": record["suspicious_transactions"] or 0,
                }
    except Exception as e:
        logger.error(f"Error fetching KPI summary: {e}")
    return {"total_accounts": 0, "total_transactions": 0, "high_risk_accounts": 0, "suspicious_transactions": 0}

@st.cache_data(ttl=15)
def fetch_all_accounts(_driver: Driver) -> pd.DataFrame:
    """Retrieves all Account nodes with owner, bank, Week 2 risk, and Week 3 GDS metrics."""
    query = """
    MATCH (a:Account)
    OPTIONAL MATCH (p:Person)-[:OWNS]->(a)
    OPTIONAL MATCH (b:Bank)-[:HOSTS]->(a)
    RETURN a.account_id AS account_id,
           a.account_type AS account_type,
           coalesce(p.name, 'Unknown') AS owner_name,
           coalesce(b.name, 'Unknown') AS bank_name,
           coalesce(a.risk_score, 0.0) AS risk_score,
           coalesce(a.risk_level, 'LOW') AS risk_level,
           coalesce(a.pagerank_score, 0.0) AS pagerank_score,
           a.wcc_component AS wcc_component,
           a.louvain_community AS louvain_community,
           a.last_risk_assessed AS last_risk_assessed
    ORDER BY a.risk_score DESC, a.pagerank_score DESC
    """
    try:
        with _driver.session() as session:
            data = session.run(query).data()
            return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Error fetching accounts: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=15)
def fetch_transactions(_driver: Driver, limit: int = 200) -> pd.DataFrame:
    """Retrieves transaction records with sender, receiver, amounts, and flags."""
    query = """
    MATCH (src:Account)-[:SENDS]->(t:Transaction)-[:TRANSFERRED_TO]->(dst:Account)
    RETURN t.transaction_id AS transaction_id,
           src.account_id AS source_account,
           dst.account_id AS dest_account,
           t.amount AS amount,
           t.timestamp AS timestamp,
           coalesce(t.is_suspicious, false) AS is_suspicious
    ORDER BY t.timestamp DESC
    LIMIT $limit
    """
    try:
        with _driver.session() as session:
            data = session.run(query, limit=limit).data()
            if data:
                df = pd.DataFrame(data)
                # Convert epoch ms to readable datetime string
                df["formatted_time"] = df["timestamp"].apply(
                    lambda ts: datetime.datetime.fromtimestamp(ts / 1000.0, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                    if pd.notnull(ts) and ts > 0 else "N/A"
                )
                return df
    except Exception as e:
        logger.error(f"Error fetching transactions: {e}")
    return pd.DataFrame()

@st.cache_data(ttl=15)
def fetch_network_edges(
    _driver: Driver,
    selected_account: Optional[str] = None,
    depth: int = 1,
    risk_filters: Optional[List[str]] = None,
    community_filter: Optional[int] = None,
    limit: int = 150
) -> List[Dict[str, Any]]:
    """
    Retrieves direct transfer graph edges based on filters or neighbor expansion around an account.
    """
    if selected_account:
        if depth == 1:
            query = """
            MATCH (src:Account)-[:SENDS]->(t:Transaction)-[:TRANSFERRED_TO]->(dst:Account)
            WHERE src.account_id = $acc_id OR dst.account_id = $acc_id
            RETURN src.account_id AS source,
                   dst.account_id AS target,
                   t.transaction_id AS tx_id,
                   t.amount AS amount,
                   t.timestamp AS timestamp,
                   coalesce(t.is_suspicious, false) AS is_suspicious,
                   src.risk_level AS src_risk,
                   dst.risk_level AS dst_risk,
                   src.risk_score AS src_score,
                   dst.risk_score AS dst_score,
                   src.pagerank_score AS src_pr,
                   dst.pagerank_score AS dst_pr,
                   src.louvain_community AS src_comm,
                   dst.louvain_community AS dst_comm
            LIMIT $limit
            """
        else: # 2-hop exploration
            query = """
            MATCH (center:Account {account_id: $acc_id})
            MATCH path = (center)-[:SENDS|TRANSFERRED_TO*2..4]-(other:Account)
            WITH relationships(path) AS rels, nodes(path) AS nds
            UNWIND range(0, size(nds)-2) AS idx
            WITH nds[idx] AS n1, nds[idx+1] AS n2
            MATCH (src:Account)-[:SENDS]->(t:Transaction)-[:TRANSFERRED_TO]->(dst:Account)
            WHERE (src = n1 AND dst = n2) OR (src = n2 AND dst = n1)
            RETURN DISTINCT src.account_id AS source,
                            dst.account_id AS target,
                            t.transaction_id AS tx_id,
                            t.amount AS amount,
                            t.timestamp AS timestamp,
                            coalesce(t.is_suspicious, false) AS is_suspicious,
                            src.risk_level AS src_risk,
                            dst.risk_level AS dst_risk,
                            src.risk_score AS src_score,
                            dst.risk_score AS dst_score,
                            src.pagerank_score AS src_pr,
                            dst.pagerank_score AS dst_pr,
                            src.louvain_community AS src_comm,
                            dst.louvain_community AS dst_comm
            LIMIT $limit
            """
        params = {"acc_id": selected_account, "limit": limit}
    else:
        query = """
        MATCH (src:Account)-[:SENDS]->(t:Transaction)-[:TRANSFERRED_TO]->(dst:Account)
        WHERE ($risk_filters IS NULL OR src.risk_level IN $risk_filters OR dst.risk_level IN $risk_filters)
          AND ($comm_filter IS NULL OR src.louvain_community = $comm_filter OR dst.louvain_community = $comm_filter)
        RETURN src.account_id AS source,
               dst.account_id AS target,
               t.transaction_id AS tx_id,
               t.amount AS amount,
               t.timestamp AS timestamp,
               coalesce(t.is_suspicious, false) AS is_suspicious,
               src.risk_level AS src_risk,
               dst.risk_level AS dst_risk,
               src.risk_score AS src_score,
               dst.risk_score AS dst_score,
               src.pagerank_score AS src_pr,
               dst.pagerank_score AS dst_pr,
               src.louvain_community AS src_comm,
               dst.louvain_community AS dst_comm
        LIMIT $limit
        """
        params = {
            "risk_filters": risk_filters if risk_filters else None,
            "comm_filter": community_filter,
            "limit": limit
        }

    try:
        with _driver.session() as session:
            return session.run(query, **params).data()
    except Exception as e:
        logger.error(f"Error fetching network edges: {e}")
        return []

# ------------------------------------------------------------------------------
# 3. Interactive Pyvis Network Graph Visualizer
# ------------------------------------------------------------------------------
def get_risk_color(risk_level: Optional[str]) -> str:
    """Returns visual color mapped to existing Week 2 risk_level."""
    mapping = {
        "CRITICAL": "#e74c3c",  # Red
        "HIGH": "#e67e22",      # Orange
        "MEDIUM": "#f39c12",    # Yellow/Gold
        "LOW": "#2ecc71"        # Green
    }
    return mapping.get(str(risk_level).upper(), "#95a5a6")

def build_pyvis_network(
    edges_data: List[Dict[str, Any]],
    accounts_lookup: Dict[str, Dict[str, Any]],
    highlight_account: Optional[str] = None
) -> Network:
    """Constructs a responsive interactive Pyvis network graph."""
    net = Network(height="620px", width="100%", bgcolor="#1a1a24", font_color="#f5f6fa", directed=True)
    net.barnes_hut(gravity=-3000, central_gravity=0.3, spring_length=130, spring_strength=0.05, damping=0.09)

    added_nodes = set()

    for item in edges_data:
        src = item["source"]
        dst = item["target"]
        amount = float(item["amount"])
        is_sus = bool(item["is_suspicious"])
        tx_id = item["tx_id"]
        formatted_date = datetime.datetime.fromtimestamp(
            item["timestamp"] / 1000.0, tz=datetime.timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S") if item.get("timestamp") else "N/A"

        # Add Source Node
        if src not in added_nodes:
            src_info = accounts_lookup.get(src, {})
            src_risk = src_info.get("risk_level", item.get("src_risk", "LOW"))
            src_score = src_info.get("risk_score", item.get("src_score", 0.0))
            src_pr = src_info.get("pagerank_score", item.get("src_pr", 0.0))
            src_comm = src_info.get("louvain_community", item.get("src_comm", "N/A"))
            src_owner = src_info.get("owner_name", "Unknown")

            # Size scaled by PageRank
            node_size = 16 + min(30, int((src_pr or 0) * 35))
            color = "#9b59b6" if src == highlight_account else get_risk_color(src_risk)
            border_width = 4 if src == highlight_account else 2

            tooltip = (
                f"<b>Account:</b> {src}<br>"
                f"<b>Owner:</b> {src_owner}<br>"
                f"<b>Risk Level:</b> {src_risk} ({src_score:.1f})<br>"
                f"<b>PageRank:</b> {src_pr:.4f}<br>"
                f"<b>Louvain Comm:</b> {src_comm}"
            )
            net.add_node(
                src,
                label=src,
                title=tooltip,
                color=color,
                size=node_size,
                borderWidth=border_width,
                shape="dot"
            )
            added_nodes.add(src)

        # Add Destination Node
        if dst not in added_nodes:
            dst_info = accounts_lookup.get(dst, {})
            dst_risk = dst_info.get("risk_level", item.get("dst_risk", "LOW"))
            dst_score = dst_info.get("risk_score", item.get("dst_score", 0.0))
            dst_pr = dst_info.get("pagerank_score", item.get("dst_pr", 0.0))
            dst_comm = dst_info.get("louvain_community", item.get("dst_comm", "N/A"))
            dst_owner = dst_info.get("owner_name", "Unknown")

            node_size = 16 + min(30, int((dst_pr or 0) * 35))
            color = "#9b59b6" if dst == highlight_account else get_risk_color(dst_risk)
            border_width = 4 if dst == highlight_account else 2

            tooltip = (
                f"<b>Account:</b> {dst}<br>"
                f"<b>Owner:</b> {dst_owner}<br>"
                f"<b>Risk Level:</b> {dst_risk} ({dst_score:.1f})<br>"
                f"<b>PageRank:</b> {dst_pr:.4f}<br>"
                f"<b>Louvain Comm:</b> {dst_comm}"
            )
            net.add_node(
                dst,
                label=dst,
                title=tooltip,
                color=color,
                size=node_size,
                borderWidth=border_width,
                shape="dot"
            )
            added_nodes.add(dst)

        # Add Directed Transfer Edge
        edge_color = "#e74c3c" if is_sus else "#3498db"
        edge_width = 3.0 if is_sus else 1.5
        edge_label = f"${amount:,.0f}"
        edge_tooltip = (
            f"<b>Transaction ID:</b> {tx_id}<br>"
            f"<b>Amount:</b> ${amount:,.2f}<br>"
            f"<b>Timestamp:</b> {formatted_date}<br>"
            f"<b>Suspicious Flag:</b> {'🚨 YES' if is_sus else '✅ No'}"
        )
        net.add_edge(
            src,
            dst,
            title=edge_tooltip,
            label=edge_label,
            color=edge_color,
            width=edge_width,
            arrows="to",
            smooth={"type": "curvedCW", "roundness": 0.15}
        )

    return net

# ------------------------------------------------------------------------------
# 4. Main Dashboard Application
# ------------------------------------------------------------------------------
def main():
    # Sidebar - Database Connection Status & Controls
    st.sidebar.title("🛡️ FinGraph AML")
    st.sidebar.caption("Anti-Money Laundering & Graph Analytics")
    st.sidebar.markdown("---")

    driver = get_neo4j_driver(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    is_connected = check_connection(driver)

    if not is_connected:
        st.sidebar.error(f"❌ Neo4j Disconnected ({NEO4J_URI})")
        st.error(
            "⚠️ **Could not establish connection to Neo4j.**\n\n"
            "Please ensure the Neo4j container is running at `bolt://localhost:7687` with credentials `neo4j/password`."
        )
        if st.sidebar.button("🔄 Retry Connection"):
            st.cache_resource.clear()
            st.rerun()
        return

    st.sidebar.success("✅ Neo4j Live Connected")
    if st.sidebar.button("🔄 Refresh Data Cache"):
        st.cache_data.clear()
        st.rerun()

    # Load Data
    kpi = fetch_kpi_summary(driver)
    accounts_df = fetch_all_accounts(driver)
    tx_df = fetch_transactions(driver, limit=300)

    # Empty State Handling
    if accounts_df.empty:
        st.warning(
            "⚠️ **Neo4j database is connected but contains no Account nodes.**\n\n"
            "To populate the graph with realistic financial streams, please run the data generator or GDS tests:\n"
            "```powershell\n"
            "python Fingraph\\gds_analytics\\test_gds_pipeline.py\n"
            "```"
        )
        return

    accounts_lookup = {row["account_id"]: row for row in accounts_df.to_dict(orient="records")}

    # --------------------------------------------------------------------------
    # Top Header & KPI Summary Cards
    # --------------------------------------------------------------------------
    st.title("🔍 FinGraph AML Investigation & Graph Analytics")
    st.caption("Week 3: Real-Time Graph Topology, PageRank, Louvain Communities & Entity Investigation")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("🏦 Total Accounts", f"{kpi['total_accounts']:,}")
    with col2:
        st.metric("💸 Total Transactions", f"{kpi['total_transactions']:,}")
    with col3:
        st.metric(
            "🚨 High/Critical Risk Accounts",
            f"{kpi['high_risk_accounts']:,}",
            help="Accounts with Week 2 risk_level in HIGH or CRITICAL"
        )
    with col4:
        st.metric(
            "🚩 Suspicious Transactions",
            f"{kpi['suspicious_transactions']:,}",
            help="Transactions flagged by AML pattern simulation"
        )

    st.markdown("---")

    # --------------------------------------------------------------------------
    # Sidebar Filters & Search
    # --------------------------------------------------------------------------
    st.sidebar.subheader("🔍 Investigation Controls")

    # Account Selector
    account_options = ["-- None (All Network) --"] + list(accounts_df["account_id"])
    selected_account_raw = st.sidebar.selectbox(
        "Focus on Specific Account:",
        options=account_options,
        index=0,
        help="Select an account to focus the graph and inspect entity details"
    )
    selected_account = None if selected_account_raw.startswith("--") else selected_account_raw

    # Neighborhood Depth Expansion
    if selected_account:
        depth = st.sidebar.radio(
            "Neighborhood Exploration Radius:",
            options=[1, 2],
            format_func=lambda x: f"{x}-Hop Distance",
            index=0
        )
    else:
        depth = 1

    # Risk Level Filter
    available_risk_levels = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    selected_risks = st.sidebar.multiselect(
        "Filter by Week 2 Risk Level:",
        options=available_risk_levels,
        default=available_risk_levels
    )

    # Louvain Community Filter
    communities = sorted([int(c) for c in accounts_df["louvain_community"].dropna().unique()])
    comm_options = ["All Communities"] + communities
    selected_comm_raw = st.sidebar.selectbox("Filter by Louvain Community:", options=comm_options, index=0)
    selected_comm = None if selected_comm_raw == "All Communities" else int(selected_comm_raw)

    max_txs = st.sidebar.slider("Max Graph Edges to Display:", min_value=10, max_value=300, value=100, step=10)

    # --------------------------------------------------------------------------
    # Main Tabs
    # --------------------------------------------------------------------------
    tab_graph, tab_inspector, tab_txs, tab_communities = st.tabs([
        "🌐 Interactive Graph View",
        "👤 Account Inspector",
        "📊 Transactions Ledger",
        "👥 Louvain Communities"
    ])

    # --------------------------------------------------------------------------
    # TAB 1: Network Graph Visualization
    # --------------------------------------------------------------------------
    with tab_graph:
        st.subheader("Interactive Financial Transfer Network")
        st.caption(
            "Nodes are color-coded by **Risk Level** and scaled by **PageRank Centrality**. "
            "Red edges indicate **Suspicious Transactions**."
        )

        edges_data = fetch_network_edges(
            driver,
            selected_account=selected_account,
            depth=depth,
            risk_filters=selected_risks,
            community_filter=selected_comm,
            limit=max_txs
        )

        if not edges_data:
            st.info("ℹ️ No transactions match the current filter criteria.")
        else:
            with st.spinner("Rendering graph visualization..."):
                pyvis_net = build_pyvis_network(edges_data, accounts_lookup, highlight_account=selected_account)
                html_content = pyvis_net.generate_html()
                components.html(html_content, height=650, scrolling=True)

        # Legend
        leg1, leg2, leg3, leg4, leg5 = st.columns(5)
        with leg1:
            st.markdown("🔴 **CRITICAL Risk** (`>= 75`)")
        with leg2:
            st.markdown("🟠 **HIGH Risk** (`50 - 74`)")
        with leg3:
            st.markdown("🟡 **MEDIUM Risk** (`25 - 49`)")
        with leg4:
            st.markdown("🟢 **LOW Risk** (`< 25`)")
        with leg5:
            st.markdown("🟣 **Focused Account**")

    # --------------------------------------------------------------------------
    # TAB 2: Account Inspector
    # --------------------------------------------------------------------------
    with tab_inspector:
        st.subheader("Account Deep-Dive & AML Audit Profile")

        if not selected_account:
            st.info("👈 Please select an account from the sidebar or choose one below to view its complete profile.")
            chosen = st.selectbox("Quick Select Account:", options=list(accounts_df["account_id"]), index=0)
            target_acc = chosen
        else:
            target_acc = selected_account

        acc_row = accounts_lookup.get(target_acc, {})
        if acc_row:
            c_left, c_right = st.columns([1, 1])

            with c_left:
                st.markdown("#### 📋 Core Entity Details")
                st.write(f"**Account ID:** `{acc_row.get('account_id')}`")
                st.write(f"**Account Type:** {acc_row.get('account_type', 'N/A')}")
                st.write(f"**Owner (Person):** {acc_row.get('owner_name', 'N/A')}")
                st.write(f"**Hosting Bank:** {acc_row.get('bank_name', 'N/A')}")

            with c_right:
                st.markdown("#### 🔬 Graph Analytics & Risk Metrics")
                r_level = acc_row.get('risk_level', 'LOW')
                r_score = acc_row.get('risk_score', 0.0)
                st.write(f"**Week 2 Risk Level:** **:{'red' if r_level in ['HIGH', 'CRITICAL'] else 'green'}[{r_level}]**")
                st.write(f"**Week 2 Risk Score:** `{r_score:.1f} / 100.0`")
                st.write(f"**GDS PageRank Centrality:** `{acc_row.get('pagerank_score', 0.0):.6f}`")
                st.write(f"**GDS Louvain Community:** `Community #{acc_row.get('louvain_community', 'N/A')}`")
                st.write(f"**GDS WCC Component:** `Component #{acc_row.get('wcc_component', 'N/A')}`")

            st.markdown("---")
            st.markdown("#### 📜 Associated Transactions")

            # Outbound
            out_txs = tx_df[tx_df["source_account"] == target_acc]
            in_txs = tx_df[tx_df["dest_account"] == target_acc]

            tab_out, tab_in = st.tabs([f"📤 Outbound Transfers ({len(out_txs)})", f"📥 Inbound Transfers ({len(in_txs)})"])
            with tab_out:
                if out_txs.empty:
                    st.write("No outbound transactions recorded.")
                else:
                    st.dataframe(
                        out_txs[["transaction_id", "dest_account", "amount", "formatted_time", "is_suspicious"]].rename(
                            columns={"dest_account": "Beneficiary Account", "amount": "Amount ($)", "formatted_time": "Timestamp", "is_suspicious": "Suspicious Flag"}
                        ),
                        use_container_width=True
                    )

            with tab_in:
                if in_txs.empty:
                    st.write("No inbound transactions recorded.")
                else:
                    st.dataframe(
                        in_txs[["transaction_id", "source_account", "amount", "formatted_time", "is_suspicious"]].rename(
                            columns={"source_account": "Sender Account", "amount": "Amount ($)", "formatted_time": "Timestamp", "is_suspicious": "Suspicious Flag"}
                        ),
                        use_container_width=True
                    )

    # --------------------------------------------------------------------------
    # TAB 3: Transactions Ledger
    # --------------------------------------------------------------------------
    with tab_txs:
        st.subheader("Global Transaction Audit Ledger")
        st.caption("Chronological stream of financial transfers with suspicious behavior flags.")

        if tx_df.empty:
            st.info("No transactions available in the database.")
        else:
            col_search, col_filter = st.columns([2, 1])
            with col_search:
                search_query = st.text_input("🔍 Search by Account ID or Transaction ID:", "")
            with col_filter:
                only_suspicious = st.checkbox("Show Only Suspicious Transactions", value=False)

            filtered_tx = tx_df.copy()
            if only_suspicious:
                filtered_tx = filtered_tx[filtered_tx["is_suspicious"] == True]
            if search_query:
                filtered_tx = filtered_tx[
                    filtered_tx["transaction_id"].str.contains(search_query, case=False, na=False) |
                    filtered_tx["source_account"].str.contains(search_query, case=False, na=False) |
                    filtered_tx["dest_account"].str.contains(search_query, case=False, na=False)
                ]

            st.dataframe(
                filtered_tx[["transaction_id", "source_account", "dest_account", "amount", "formatted_time", "is_suspicious"]].rename(
                    columns={
                        "transaction_id": "Transaction ID",
                        "source_account": "Source Account",
                        "dest_account": "Destination Account",
                        "amount": "Amount ($)",
                        "formatted_time": "Timestamp",
                        "is_suspicious": "Suspicious"
                    }
                ),
                use_container_width=True
            )

    # --------------------------------------------------------------------------
    # TAB 4: Louvain Communities & Clusters
    # --------------------------------------------------------------------------
    with tab_communities:
        st.subheader("Louvain Community Detection & Syndicate Clustering")
        st.caption("Aggregated graph partitions highlighting concentrated clusters of financial activity.")

        if accounts_df["louvain_community"].isna().all():
            st.info("ℹ️ Louvain community data has not been computed yet. Please run `gds_runner.py`.")
        else:
            comm_summary = accounts_df.groupby("louvain_community").agg(
                member_count=("account_id", "count"),
                avg_risk_score=("risk_score", "mean"),
                high_risk_count=("risk_level", lambda s: (s.isin(["HIGH", "CRITICAL"])).sum()),
                max_pagerank=("pagerank_score", "max")
            ).reset_index().rename(columns={
                "louvain_community": "Community ID",
                "member_count": "Total Members",
                "avg_risk_score": "Avg Risk Score",
                "high_risk_count": "High/Critical Risk Accounts",
                "max_pagerank": "Top PageRank Centrality"
            })

            comm_summary = comm_summary.sort_values(by=["High/Critical Risk Accounts", "Avg Risk Score"], ascending=False)
            st.dataframe(comm_summary, use_container_width=True)

if __name__ == "__main__":
    main()
