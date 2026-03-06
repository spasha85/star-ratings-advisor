import streamlit as st
import snowflake.connector
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="Star Ratings AI Advisor",
    page_icon="⭐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# CUSTOM CSS — Premium healthcare design
# ============================================================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap');
    
    /* Global */
    .stApp {
        font-family: 'Plus Jakarta Sans', sans-serif;
    }
    
    /* Header styling */
    .hero-header {
        background: linear-gradient(135deg, #0f172a 0%, #1e3a5f 50%, #0d9488 100%);
        padding: 2rem 2.5rem;
        border-radius: 20px;
        margin-bottom: 1.5rem;
        position: relative;
        overflow: hidden;
    }
    .hero-header::before {
        content: '';
        position: absolute;
        top: -50%;
        right: -20%;
        width: 400px;
        height: 400px;
        background: radial-gradient(circle, rgba(13,148,136,0.3) 0%, transparent 70%);
        border-radius: 50%;
    }
    .hero-header h1 {
        color: #ffffff;
        font-size: 2.2rem;
        font-weight: 800;
        margin: 0;
        letter-spacing: -0.5px;
    }
    .hero-header p {
        color: #94a3b8;
        font-size: 1rem;
        margin-top: 0.5rem;
    }
    
    /* KPI Cards */
    .kpi-card {
        background: linear-gradient(145deg, #ffffff 0%, #f8fafc 100%);
        border: 1px solid #e2e8f0;
        border-radius: 16px;
        padding: 1.5rem;
        text-align: center;
        box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05), 0 2px 4px -2px rgba(0,0,0,0.05);
        transition: transform 0.2s, box-shadow 0.2s;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 25px -5px rgba(0,0,0,0.1);
    }
    .kpi-value {
        font-size: 2.5rem;
        font-weight: 800;
        background: linear-gradient(135deg, #0d9488, #0f766e);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        line-height: 1.2;
    }
    .kpi-value-warning {
        font-size: 2.5rem;
        font-weight: 800;
        background: linear-gradient(135deg, #f59e0b, #d97706);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        line-height: 1.2;
    }
    .kpi-value-danger {
        font-size: 2.5rem;
        font-weight: 800;
        background: linear-gradient(135deg, #ef4444, #dc2626);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        line-height: 1.2;
    }
    .kpi-label {
        font-size: 0.85rem;
        font-weight: 600;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-top: 0.5rem;
    }
    
    /* Section headers */
    .section-header {
        font-size: 1.3rem;
        font-weight: 700;
        color: #0f172a;
        margin: 1.5rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 3px solid #0d9488;
        display: inline-block;
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%);
    }
    [data-testid="stSidebar"] * {
        color: #e2e8f0 !important;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 12px;
        padding: 10px 24px;
        font-weight: 600;
    }
    
    /* Dataframe */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
    }
    
    /* Footer */
    .footer {
        text-align: center;
        padding: 1.5rem;
        color: #94a3b8;
        font-size: 0.8rem;
        border-top: 1px solid #e2e8f0;
        margin-top: 2rem;
    }
    .footer a { color: #0d9488; text-decoration: none; }
</style>
""", unsafe_allow_html=True)


# ============================================================
# CONNECTION
# ============================================================
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        role=st.secrets["snowflake"]["role"],
    )

def run_query(query):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

def run_query_df(query):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=columns)

CHART_COLORS = ["#0d9488", "#1e3a5f", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899", "#06b6d4", "#84cc16", "#f97316"]


# ============================================================
# HERO HEADER
# ============================================================
st.markdown("""
<div class="hero-header">
    <h1>⭐ Star Ratings AI Advisor</h1>
    <p>Medicare Advantage Quality Intelligence — Powered by Claude AI on Snowflake Cortex</p>
</div>
""", unsafe_allow_html=True)


# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    st.markdown("### 🏥 Horizon Health Advantage")
    st.caption("Contract H1234 · HMO-POS · IL, IN, WI")
    st.divider()
    
    try:
        stats = run_query("""
            SELECT COUNT(*),
                   SUM(CASE WHEN TOTAL_OPEN_GAPS > 0 THEN 1 ELSE 0 END)
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
        """)
        st.metric("👥 Total Members", f"{stats[0][0]:,}")
        st.metric("⚠️ Members with Gaps", f"{stats[0][1]:,}")
        st.metric("⭐ Current Rating", "3.5 Stars")
        st.metric("🎯 Target Rating", "4.0 Stars")
        
        st.divider()
        st.markdown("### 💰 Revenue at Stake")
        st.markdown("**$91.4M** in Quality Bonus Payments unlocked at 4.0 stars")
    except:
        st.info("Connecting to database...")
    
    st.divider()
    st.caption("Built with Claude AI · Snowflake Cortex · Streamlit")


# ============================================================
# TABS
# ============================================================
tab_dashboard, tab_chat, tab_analytics, tab_equity, tab_providers = st.tabs([
    "📊 Dashboard", "💬 AI Chat", "📈 Analytics", "⚖️ Equity", "🏥 Providers"
])


# ============================================================
# TAB 1: DASHBOARD
# ============================================================
with tab_dashboard:
    try:
        stats = run_query("""
            SELECT COUNT(*),
                   SUM(CASE WHEN TOTAL_OPEN_GAPS > 0 THEN 1 ELSE 0 END),
                   ROUND(AVG(TOTAL_OPEN_GAPS), 1),
                   ROUND(AVG(HCC_RISK_SCORE), 2)
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
        """)
        
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            st.markdown(f'<div class="kpi-card"><div class="kpi-value">{stats[0][0]:,}</div><div class="kpi-label">Total Members</div></div>', unsafe_allow_html=True)
        with c2:
            st.markdown(f'<div class="kpi-card"><div class="kpi-value-warning">{stats[0][1]:,}</div><div class="kpi-label">Members with Gaps</div></div>', unsafe_allow_html=True)
        with c3:
            pct = round(stats[0][1] / stats[0][0] * 100, 1) if stats[0][0] else 0
            st.markdown(f'<div class="kpi-card"><div class="kpi-value-danger">{pct}%</div><div class="kpi-label">Gap Rate</div></div>', unsafe_allow_html=True)
        with c4:
            st.markdown(f'<div class="kpi-card"><div class="kpi-value">{stats[0][3]}</div><div class="kpi-label">Avg Risk Score</div></div>', unsafe_allow_html=True)
        with c5:
            st.markdown(f'<div class="kpi-card"><div class="kpi-value-warning">3.5⭐</div><div class="kpi-label">Current Rating</div></div>', unsafe_allow_html=True)

        st.markdown("")
        
        col_left, col_right = st.columns([3, 2])
        
        with col_left:
            st.markdown('<div class="section-header">📋 Measure Compliance Rates</div>', unsafe_allow_html=True)
            df_measures = run_query_df("""
                SELECT MEASURE, WEIGHT, ELIGIBLE, COMPLIANT, 
                       ROUND(RATE * 100, 1) AS RATE_PCT,
                       ELIGIBLE - COMPLIANT AS OPEN_GAPS
                FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_MEASURE_SUMMARY
                ORDER BY WEIGHT DESC, RATE ASC
            """)
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                y=df_measures["MEASURE"],
                x=df_measures["RATE_PCT"],
                orientation='h',
                marker=dict(
                    color=df_measures["RATE_PCT"],
                    colorscale=[[0, "#ef4444"], [0.5, "#f59e0b"], [1, "#10b981"]],
                    cmin=30, cmax=100
                ),
                text=df_measures["RATE_PCT"].apply(lambda x: f"{x}%"),
                textposition="outside",
                textfont=dict(size=11, family="Plus Jakarta Sans"),
            ))
            fig.update_layout(
                height=500,
                margin=dict(l=10, r=40, t=10, b=10),
                xaxis=dict(range=[0, 110], title="Compliance Rate (%)", gridcolor="#f1f5f9"),
                yaxis=dict(autorange="reversed"),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Plus Jakarta Sans"),
            )
            fig.add_vline(x=80, line_dash="dash", line_color="#f59e0b", 
                         annotation_text="4-Star Target Zone", annotation_position="top right",
                         annotation_font_color="#f59e0b")
            st.plotly_chart(fig, use_container_width=True)
        
        with col_right:
            st.markdown('<div class="section-header">🎯 Open Gaps by Measure</div>', unsafe_allow_html=True)
            df_gaps = run_query_df("""
                SELECT 'BCS' AS MEASURE, SUM(CASE WHEN BCS_ELIGIBLE=1 AND BCS_COMPLIANT=0 THEN 1 ELSE 0 END) AS GAPS FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
                UNION ALL SELECT 'COL', SUM(CASE WHEN COL_ELIGIBLE=1 AND COL_COMPLIANT=0 THEN 1 ELSE 0 END) FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
                UNION ALL SELECT 'A1C', SUM(CASE WHEN A1C_ELIGIBLE=1 AND A1C_COMPLIANT=0 THEN 1 ELSE 0 END) FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
                UNION ALL SELECT 'CBP', SUM(CASE WHEN CBP_ELIGIBLE=1 AND CBP_COMPLIANT=0 THEN 1 ELSE 0 END) FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
                UNION ALL SELECT 'KED', SUM(CASE WHEN KED_ELIGIBLE=1 AND KED_COMPLIANT=0 THEN 1 ELSE 0 END) FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
                UNION ALL SELECT 'SUPD', SUM(CASE WHEN SUPD_ELIGIBLE=1 AND SUPD_COMPLIANT=0 THEN 1 ELSE 0 END) FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
                UNION ALL SELECT 'ADH DM', SUM(CASE WHEN ADH_DM_ELIGIBLE=1 AND ADH_DM_COMPLIANT=0 THEN 1 ELSE 0 END) FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
                UNION ALL SELECT 'ADH RAS', SUM(CASE WHEN ADH_RAS_ELIGIBLE=1 AND ADH_RAS_COMPLIANT=0 THEN 1 ELSE 0 END) FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
                UNION ALL SELECT 'ADH STAT', SUM(CASE WHEN ADH_STATIN_ELIGIBLE=1 AND ADH_STATIN_COMPLIANT=0 THEN 1 ELSE 0 END) FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
                ORDER BY GAPS DESC
            """)
            fig2 = px.bar(df_gaps, x="MEASURE", y="GAPS", color="GAPS",
                         color_continuous_scale=["#10b981", "#f59e0b", "#ef4444"],
                         text="GAPS")
            fig2.update_layout(
                height=500, showlegend=False,
                margin=dict(l=10, r=10, t=10, b=10),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Plus Jakarta Sans"),
                xaxis=dict(title=""), yaxis=dict(title="Open Gaps", gridcolor="#f1f5f9"),
                coloraxis_showscale=False,
            )
            fig2.update_traces(textposition="outside", textfont_size=12)
            st.plotly_chart(fig2, use_container_width=True)

        st.markdown('<div class="section-header">🚨 Priority Outreach Members</div>', unsafe_allow_html=True)
        df_outreach = run_query_df("""
            SELECT MEMBER_ID, LAST_NAME, FIRST_NAME, AGE, PCP_NAME, PCP_GROUP,
                   TOTAL_OPEN_GAPS, OUTREACH_PRIORITY, ROUND(HCC_RISK_SCORE, 2) AS RISK_SCORE
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_OUTREACH_WORKLIST
            LIMIT 15
        """)
        st.dataframe(df_outreach, use_container_width=True, hide_index=True,
                     column_config={
                         "TOTAL_OPEN_GAPS": st.column_config.ProgressColumn("Open Gaps", min_value=0, max_value=9, format="%d"),
                         "RISK_SCORE": st.column_config.NumberColumn("Risk Score", format="%.2f"),
                     })
    except Exception as e:
        st.error(f"Error: {str(e)}")


# ============================================================
# TAB 2: AI CHAT
# ============================================================
with tab_chat:
    st.markdown('<div class="section-header">💬 Ask Claude About Your Care Gaps</div>', unsafe_allow_html=True)
    st.caption("Claude AI analyzes your plan data and CMS Star Ratings documentation to give you specific, actionable answers.")
    
    st.markdown("**Quick questions:**")
    qcols = st.columns(3)
    questions = [
        ("📋", "What are our compliance rates by measure?"),
        ("🎯", "Which measures to prioritize for 4 stars?"),
        ("👥", "Show members with the most open gaps"),
        ("⚖️", "Compare dual vs non-dual compliance"),
        ("🏥", "Which providers need improvement?"),
        ("📄", "What changed in 2026 Star Ratings?"),
    ]
    for i, (icon, q) in enumerate(questions):
        with qcols[i % 3]:
            if st.button(f"{icon} {q}", key=f"q_{i}", use_container_width=True):
                st.session_state["pending_question"] = q

    st.divider()

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"], avatar="🧑‍⚕️" if msg["role"] == "user" else "🤖"):
            st.write(msg["content"])
            if "dataframe" in msg:
                st.dataframe(msg["dataframe"], use_container_width=True, hide_index=True)

    if "pending_question" in st.session_state:
        prompt = st.session_state.pop("pending_question")
    else:
        prompt = st.chat_input("Ask about care gaps, CMS requirements, quality improvement...")

    if prompt:
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user", avatar="🧑‍⚕️"):
            st.write(prompt)

        with st.chat_message("assistant", avatar="🤖"):
            with st.spinner("🧠 Claude is analyzing your data and CMS documents..."):
                try:
                    measure_data = run_query("""
                        SELECT LISTAGG(MEASURE || ': Rate=' || RATE || ' Wt=' || WEIGHT || ' Elig=' || ELIGIBLE || ' Compl=' || COMPLIANT, '; ')
                        FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_MEASURE_SUMMARY
                    """)
                    measure_context = measure_data[0][0] if measure_data else "No data"
                    gap_count = run_query("SELECT COUNT(*) FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_OUTREACH_WORKLIST")[0][0]

                    cms_context = ""
                    try:
                        safe_q = prompt.replace("'", "").replace('"', '')
                        sr = run_query(f"""
                            SELECT PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW('CMS_STAR_RATINGS_SEARCH',
                                '{{"query":"{safe_q}","columns":["CHUNK_TEXT","DOCUMENT_TYPE"],"limit":3}}'
                            )):results[0]:CHUNK_TEXT::TEXT
                        """)
                        if sr and sr[0][0]: cms_context = sr[0][0][:3000]
                    except: pass

                    fp = f"""You are a Medicare Advantage Star Ratings quality improvement advisor.
Plan: Horizon Health Advantage (H1234), 1,500 members, IL/IN/WI, currently 3.5 stars.
Measure rates: {measure_context}
Open gap members: {gap_count}
CMS docs: {cms_context}
Question: {prompt}
Give specific numbers and actionable recommendations. Be concise."""

                    safe_fp = fp.replace("'", "''")
                    resp = run_query(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet','{safe_fp}')")
                    answer = resp[0][0] if resp else "Could not generate response."
                    st.write(answer)
                    msg = {"role": "assistant", "content": answer}

                    kws = ["show","list","member","who","which","top","compare","provider","rate"]
                    if any(k in prompt.lower() for k in kws):
                        try:
                            sq = f"""Write a Snowflake SQL query for: "{prompt}"
Views: HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY (MEMBER_ID,LAST_NAME,FIRST_NAME,AGE,GENDER,PCP_NAME,PCP_GROUP,DUAL_ELIGIBLE_FLAG,HCC_RISK_SCORE,TOTAL_OPEN_GAPS,BCS_ELIGIBLE,BCS_COMPLIANT,COL_ELIGIBLE,COL_COMPLIANT,A1C_ELIGIBLE,A1C_COMPLIANT,LAST_A1C_VALUE,CBP_ELIGIBLE,CBP_COMPLIANT,LAST_SYSTOLIC,LAST_DIASTOLIC,KED_ELIGIBLE,KED_COMPLIANT,SUPD_ELIGIBLE,SUPD_COMPLIANT,ADH_DM_ELIGIBLE,ADH_DM_COMPLIANT,PDC_DIABETES,ADH_RAS_ELIGIBLE,ADH_RAS_COMPLIANT,PDC_RAS,ADH_STATIN_ELIGIBLE,ADH_STATIN_COMPLIANT,PDC_STATIN)
V_MEASURE_SUMMARY (MEASURE,WEIGHT,ELIGIBLE,COMPLIANT,RATE)
V_OUTREACH_WORKLIST (MEMBER_ID,LAST_NAME,FIRST_NAME,AGE,GENDER,PCP_NAME,PCP_GROUP,TOTAL_OPEN_GAPS,OUTREACH_PRIORITY,HCC_RISK_SCORE)
Return ONLY SQL. LIMIT 20."""
                            safe_sq = sq.replace("'", "''")
                            sr2 = run_query(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet','{safe_sq}')")
                            sql = sr2[0][0].strip().replace("```sql","").replace("```","").strip()
                            if sql.upper().startswith("SELECT"):
                                df = run_query_df(sql)
                                if not df.empty:
                                    st.divider()
                                    st.caption("📋 Supporting Data")
                                    st.dataframe(df, use_container_width=True, hide_index=True)
                                    msg["dataframe"] = df
                        except: pass

                    st.session_state.messages.append(msg)
                except Exception as e:
                    st.error(f"Error: {str(e)}")


# ============================================================
# TAB 3: ANALYTICS
# ============================================================
with tab_analytics:
    st.markdown('<div class="section-header">📈 Quality Analytics</div>', unsafe_allow_html=True)
    
    try:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Gap Distribution Across Members**")
            df_dist = run_query_df("""
                SELECT 
                    CASE WHEN TOTAL_OPEN_GAPS = 0 THEN 'No Gaps'
                         WHEN TOTAL_OPEN_GAPS = 1 THEN '1 Gap'
                         WHEN TOTAL_OPEN_GAPS = 2 THEN '2 Gaps'
                         WHEN TOTAL_OPEN_GAPS >= 3 THEN '3+ Gaps' END AS CATEGORY,
                    COUNT(*) AS MEMBERS
                FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
                GROUP BY CATEGORY ORDER BY CATEGORY
            """)
            fig_donut = px.pie(df_dist, values="MEMBERS", names="CATEGORY", hole=0.55,
                              color_discrete_sequence=["#10b981", "#06b6d4", "#f59e0b", "#ef4444"])
            fig_donut.update_layout(height=350, margin=dict(l=10,r=10,t=30,b=10),
                                   font=dict(family="Plus Jakarta Sans"),
                                   paper_bgcolor="rgba(0,0,0,0)")
            fig_donut.update_traces(textposition='inside', textinfo='percent+label', textfont_size=12)
            st.plotly_chart(fig_donut, use_container_width=True)

        with col2:
            st.markdown("**Compliance by Age Group**")
            df_age = run_query_df("""
                SELECT 
                    CASE WHEN AGE < 65 THEN 'Under 65'
                         WHEN AGE BETWEEN 65 AND 74 THEN '65-74'
                         WHEN AGE BETWEEN 75 AND 84 THEN '75-84'
                         ELSE '85+' END AS AGE_GROUP,
                    ROUND(AVG(CASE WHEN A1C_ELIGIBLE=1 THEN A1C_COMPLIANT END)*100,1) AS A1C,
                    ROUND(AVG(CASE WHEN CBP_ELIGIBLE=1 THEN CBP_COMPLIANT END)*100,1) AS CBP,
                    ROUND(AVG(CASE WHEN ADH_DM_ELIGIBLE=1 THEN ADH_DM_COMPLIANT END)*100,1) AS ADH_DM
                FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
                GROUP BY AGE_GROUP ORDER BY AGE_GROUP
            """)
            fig_age = go.Figure()
            for i, col in enumerate(["A1C", "CBP", "ADH_DM"]):
                fig_age.add_trace(go.Bar(name=col, x=df_age["AGE_GROUP"], y=df_age[col],
                                        marker_color=CHART_COLORS[i], text=df_age[col],
                                        textposition="outside"))
            fig_age.update_layout(barmode="group", height=350, 
                                margin=dict(l=10,r=10,t=30,b=10),
                                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                                font=dict(family="Plus Jakarta Sans"),
                                yaxis=dict(title="Rate (%)", gridcolor="#f1f5f9"),
                                legend=dict(orientation="h", y=-0.15))
            st.plotly_chart(fig_age, use_container_width=True)

        st.markdown("**Risk Score vs Open Gaps (Members with Gaps)**")
        df_scatter = run_query_df("""
            SELECT HCC_RISK_SCORE, TOTAL_OPEN_GAPS, AGE,
                   CASE WHEN DUAL_ELIGIBLE_FLAG=1 THEN 'Dual' ELSE 'Non-Dual' END AS DUAL_STATUS
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
            WHERE TOTAL_OPEN_GAPS > 0
        """)
        fig_scatter = px.scatter(df_scatter, x="HCC_RISK_SCORE", y="TOTAL_OPEN_GAPS",
                                color="DUAL_STATUS", size="AGE",
                                color_discrete_map={"Dual": "#ef4444", "Non-Dual": "#0d9488"},
                                opacity=0.6, size_max=15)
        fig_scatter.update_layout(height=400, 
                                 margin=dict(l=10,r=10,t=10,b=10),
                                 plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                                 font=dict(family="Plus Jakarta Sans"),
                                 xaxis=dict(title="HCC Risk Score", gridcolor="#f1f5f9"),
                                 yaxis=dict(title="Open Gaps", gridcolor="#f1f5f9"))
        st.plotly_chart(fig_scatter, use_container_width=True)

    except Exception as e:
        st.error(f"Error: {str(e)}")


# ============================================================
# TAB 4: EQUITY
# ============================================================
with tab_equity:
    st.markdown('<div class="section-header">⚖️ Health Equity Analysis</div>', unsafe_allow_html=True)
    st.caption("Compare compliance rates across subpopulations to identify disparities")
    
    try:
        df_eq = run_query_df("""
            SELECT 
                CASE WHEN DUAL_ELIGIBLE_FLAG=1 THEN 'Dual Eligible' ELSE 'Non-Dual' END AS POPULATION,
                COUNT(*) AS MEMBERS,
                ROUND(AVG(CASE WHEN A1C_ELIGIBLE=1 THEN A1C_COMPLIANT END)*100,1) AS A1C,
                ROUND(AVG(CASE WHEN CBP_ELIGIBLE=1 THEN CBP_COMPLIANT END)*100,1) AS CBP,
                ROUND(AVG(CASE WHEN BCS_ELIGIBLE=1 THEN BCS_COMPLIANT END)*100,1) AS BCS,
                ROUND(AVG(CASE WHEN ADH_DM_ELIGIBLE=1 THEN ADH_DM_COMPLIANT END)*100,1) AS ADH_DM,
                ROUND(AVG(CASE WHEN ADH_RAS_ELIGIBLE=1 THEN ADH_RAS_COMPLIANT END)*100,1) AS ADH_RAS,
                ROUND(AVG(CASE WHEN ADH_STATIN_ELIGIBLE=1 THEN ADH_STATIN_COMPLIANT END)*100,1) AS ADH_STATIN
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
            GROUP BY DUAL_ELIGIBLE_FLAG ORDER BY POPULATION
        """)

        measures = ["A1C", "CBP", "BCS", "ADH_DM", "ADH_RAS", "ADH_STATIN"]
        fig_eq = go.Figure()
        colors = {"Dual Eligible": "#ef4444", "Non-Dual": "#0d9488"}
        for _, row in df_eq.iterrows():
            fig_eq.add_trace(go.Bar(
                name=row["POPULATION"],
                x=measures,
                y=[row[m] for m in measures],
                marker_color=colors.get(row["POPULATION"], "#64748b"),
                text=[f"{row[m]}%" for m in measures],
                textposition="outside"
            ))
        fig_eq.update_layout(barmode="group", height=450,
                            margin=dict(l=10,r=10,t=30,b=10),
                            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                            font=dict(family="Plus Jakarta Sans"),
                            yaxis=dict(title="Compliance Rate (%)", gridcolor="#f1f5f9"),
                            legend=dict(orientation="h", y=-0.1))
        st.plotly_chart(fig_eq, use_container_width=True)

        st.markdown("**Disparity Gaps (Dual - Non-Dual)**")
        if len(df_eq) >= 2:
            dual_row = df_eq[df_eq["POPULATION"] == "Dual Eligible"].iloc[0]
            nondual_row = df_eq[df_eq["POPULATION"] == "Non-Dual"].iloc[0]
            gap_data = []
            for m in measures:
                gap = round(float(dual_row[m] or 0) - float(nondual_row[m] or 0), 1)
                status = "🔴 Significant" if gap < -5 else ("🟡 Moderate" if gap < -2 else "🟢 Minimal")
                gap_data.append({"Measure": m, "Dual Rate": f"{dual_row[m]}%", "Non-Dual Rate": f"{nondual_row[m]}%", "Gap": f"{gap}%", "Status": status})
            st.dataframe(pd.DataFrame(gap_data), use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("**Disability Status Comparison**")
        df_dis = run_query_df("""
            SELECT 
                CASE WHEN DISABILITY_FLAG=1 THEN 'Disabled' ELSE 'Not Disabled' END AS POPULATION,
                COUNT(*) AS MEMBERS,
                ROUND(AVG(CASE WHEN A1C_ELIGIBLE=1 THEN A1C_COMPLIANT END)*100,1) AS A1C,
                ROUND(AVG(CASE WHEN CBP_ELIGIBLE=1 THEN CBP_COMPLIANT END)*100,1) AS CBP,
                ROUND(AVG(CASE WHEN ADH_DM_ELIGIBLE=1 THEN ADH_DM_COMPLIANT END)*100,1) AS ADH_DM
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
            GROUP BY DISABILITY_FLAG
        """)
        st.dataframe(df_dis, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Error: {str(e)}")


# ============================================================
# TAB 5: PROVIDERS
# ============================================================
with tab_providers:
    st.markdown('<div class="section-header">🏥 Provider Group Performance</div>', unsafe_allow_html=True)
    
    try:
        df_prov = run_query_df("""
            SELECT PCP_GROUP,
                   COUNT(DISTINCT MEMBER_ID) AS PANEL_SIZE,
                   ROUND(AVG(CASE WHEN A1C_ELIGIBLE=1 THEN A1C_COMPLIANT END)*100,1) AS A1C_RATE,
                   ROUND(AVG(CASE WHEN CBP_ELIGIBLE=1 THEN CBP_COMPLIANT END)*100,1) AS BP_RATE,
                   ROUND(AVG(CASE WHEN BCS_ELIGIBLE=1 THEN BCS_COMPLIANT END)*100,1) AS BCS_RATE,
                   ROUND(AVG(CASE WHEN ADH_DM_ELIGIBLE=1 THEN ADH_DM_COMPLIANT END)*100,1) AS ADH_DM_RATE,
                   ROUND(AVG(CASE WHEN ADH_RAS_ELIGIBLE=1 THEN ADH_RAS_COMPLIANT END)*100,1) AS ADH_RAS_RATE
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
            GROUP BY PCP_GROUP
            HAVING COUNT(DISTINCT MEMBER_ID) >= 10
            ORDER BY A1C_RATE ASC
        """)
        
        st.dataframe(df_prov, use_container_width=True, hide_index=True,
                    column_config={
                        "PANEL_SIZE": st.column_config.NumberColumn("Panel", format="%d"),
                        "A1C_RATE": st.column_config.ProgressColumn("A1C %", min_value=0, max_value=100, format="%.1f%%"),
                        "BP_RATE": st.column_config.ProgressColumn("BP %", min_value=0, max_value=100, format="%.1f%%"),
                        "BCS_RATE": st.column_config.ProgressColumn("BCS %", min_value=0, max_value=100, format="%.1f%%"),
                        "ADH_DM_RATE": st.column_config.ProgressColumn("Adh DM %", min_value=0, max_value=100, format="%.1f%%"),
                        "ADH_RAS_RATE": st.column_config.ProgressColumn("Adh RAS %", min_value=0, max_value=100, format="%.1f%%"),
                    })

        st.markdown("**Performance Heatmap**")
        measures_cols = ["A1C_RATE", "BP_RATE", "BCS_RATE", "ADH_DM_RATE", "ADH_RAS_RATE"]
        fig_heat = go.Figure(data=go.Heatmap(
            z=df_prov[measures_cols].values,
            x=["A1C", "BP", "BCS", "Adh DM", "Adh RAS"],
            y=df_prov["PCP_GROUP"],
            colorscale=[[0, "#ef4444"], [0.5, "#f59e0b"], [1, "#10b981"]],
            zmin=40, zmax=100,
            text=df_prov[measures_cols].values,
            texttemplate="%{text}%",
            textfont=dict(size=11),
        ))
        fig_heat.update_layout(
            height=max(300, len(df_prov) * 45),
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Plus Jakarta Sans"),
            yaxis=dict(autorange="reversed"),
        )
        st.plotly_chart(fig_heat, use_container_width=True)

    except Exception as e:
        st.error(f"Error: {str(e)}")


# ============================================================
# FOOTER
# ============================================================
st.markdown("""
<div class="footer">
    ⭐ Star Ratings AI Advisor · Powered by <a href="https://www.anthropic.com">Claude AI</a> on 
    <a href="https://www.snowflake.com">Snowflake Cortex</a> · 
    Built with <a href="https://streamlit.io">Streamlit</a> · 
    CMS Star Ratings 2026
</div>
""", unsafe_allow_html=True)
