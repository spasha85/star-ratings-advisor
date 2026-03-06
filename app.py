import streamlit as st
import snowflake.connector
import json

st.set_page_config(
    page_title="Star Ratings AI Advisor",
    page_icon="⭐",
    layout="wide"
)

# ============================================================
# CONNECTION — Uses Streamlit secrets for security
# Create a file: .streamlit/secrets.toml with your credentials
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

def run_query_with_columns(query):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return columns, rows


# ============================================================
# HEADER
# ============================================================
st.title("⭐ Medicare Advantage Star Ratings AI Advisor")
st.caption("Ask questions about care gaps, CMS requirements, and quality improvement strategies")

# ============================================================
# SIDEBAR — Plan stats and starter questions
# ============================================================
with st.sidebar:
    st.image("https://img.icons8.com/color/96/heart-health.png", width=80)
    st.header("Horizon Health Advantage")
    st.caption("Contract H1234 | IL, IN, WI")

    st.divider()
    st.header("📊 Plan Quick Stats")
    try:
        stats = run_query("""
            SELECT COUNT(*) AS TOTAL,
                   SUM(CASE WHEN TOTAL_OPEN_GAPS > 0 THEN 1 ELSE 0 END) AS WITH_GAPS
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY
        """)
        col1, col2 = st.columns(2)
        col1.metric("Total Members", f"{stats[0][0]:,}")
        col2.metric("With Open Gaps", f"{stats[0][1]:,}")
        st.metric("Current Star Rating", "3.5 ⭐")

        measures = run_query("""
            SELECT MEASURE, RATE FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_MEASURE_SUMMARY
            ORDER BY WEIGHT DESC LIMIT 5
        """)
        st.divider()
        st.header("📈 Top Measures")
        for m in measures:
            rate_pct = float(m[1]) * 100 if m[1] else 0
            st.write(f"**{m[0][:30]}**: {rate_pct:.1f}%")
    except Exception as e:
        st.error(f"Could not load stats: {e}")

    st.divider()
    st.header("🎯 Try These Questions")
    sample_questions = [
        "What are our compliance rates by measure?",
        "Which measures should we prioritize to reach 4 stars?",
        "Show me members with the most open gaps",
        "What changed in the 2026 Star Ratings methodology?",
        "Compare dual vs non-dual compliance rates",
        "Which provider groups need improvement?",
        "What does CMS require for the A1C measure?",
        "Show me members close to the adherence threshold",
    ]
    for q in sample_questions:
        if st.button(q, key=q, use_container_width=True):
            st.session_state["pending_question"] = q


# ============================================================
# CHAT INTERFACE
# ============================================================
if "messages" not in st.session_state:
    st.session_state.messages = []

# Show chat history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])
        if "table_data" in msg:
            cols, rows = msg["table_data"]
            import pandas as pd
            df = pd.DataFrame(rows, columns=cols)
            st.dataframe(df, use_container_width=True)

# Handle sidebar button clicks
if "pending_question" in st.session_state:
    prompt = st.session_state.pop("pending_question")
else:
    prompt = st.chat_input("Ask about your care gaps or CMS Star Ratings...")

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing your data and CMS documents..."):
            try:
                # Get plan measure data
                measure_data = run_query("""
                    SELECT LISTAGG(MEASURE || ': Rate=' || RATE || ' Weight=' || WEIGHT || ' Eligible=' || ELIGIBLE || ' Compliant=' || COMPLIANT, '; ')
                    FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_MEASURE_SUMMARY
                """)
                measure_context = measure_data[0][0] if measure_data else "No data available"

                # Get gap count
                gap_data = run_query("""
                    SELECT COUNT(*) FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_OUTREACH_WORKLIST
                """)
                gap_count = gap_data[0][0] if gap_data else 0

                # Search CMS documents for relevant context
                cms_context = ""
                try:
                    safe_prompt = prompt.replace("'", "").replace('"', '')
                    search_result = run_query(f"""
                        SELECT PARSE_JSON(
                            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                                'CMS_STAR_RATINGS_SEARCH',
                                '{{"query": "{safe_prompt}", "columns": ["CHUNK_TEXT", "DOCUMENT_TYPE"], "limit": 3}}'
                            )
                        ):results[0]:CHUNK_TEXT::TEXT AS CONTEXT
                    """)
                    if search_result and search_result[0][0]:
                        cms_context = search_result[0][0][:3000]
                except:
                    cms_context = ""

                # Build the full prompt
                full_prompt = f"""You are a Medicare Advantage Star Ratings quality improvement advisor for Horizon Health Advantage (H1234).
Plan details: 1,500 members in IL/IN/WI, currently at 3.5 stars, needs to reach 4.0 stars.

Current measure performance:
{measure_context}

Members with open care gaps: {gap_count}

Relevant CMS documentation context:
{cms_context}

User question: {prompt}

Instructions:
- Provide specific numbers from the data
- Give actionable recommendations
- If the question is about CMS rules, cite the document source
- If the question is about member data, reference specific measures and rates
- Be concise but thorough"""

                safe_full_prompt = full_prompt.replace("'", "''")

                # Call Cortex LLM
                response = run_query(f"""
                    SELECT SNOWFLAKE.CORTEX.COMPLETE(
                        'claude-3-5-sonnet',
                        '{safe_full_prompt}'
                    )
                """)
                answer = response[0][0] if response else "I couldn't generate a response. Please try again."

                st.write(answer)
                msg_data = {"role": "assistant", "content": answer}

                # If question seems data-related, also show a table
                data_keywords = ["show me", "list", "members", "who", "which", "top", "compare", "provider"]
                if any(kw in prompt.lower() for kw in data_keywords):
                    try:
                        # Ask LLM to generate a SQL query
                        sql_prompt = f"""Given this question: "{prompt}"
Write a single Snowflake SQL query to answer it using these views:
- HEDIS_QUALITY_DB.CLAIMS_DATA.V_CARE_GAP_REGISTRY (columns: MEMBER_ID, LAST_NAME, FIRST_NAME, AGE, GENDER, PCP_NAME, PCP_GROUP, DUAL_ELIGIBLE_FLAG, HCC_RISK_SCORE, TOTAL_OPEN_GAPS, BCS_ELIGIBLE, BCS_COMPLIANT, COL_ELIGIBLE, COL_COMPLIANT, A1C_ELIGIBLE, A1C_COMPLIANT, LAST_A1C_VALUE, CBP_ELIGIBLE, CBP_COMPLIANT, LAST_SYSTOLIC, LAST_DIASTOLIC, KED_ELIGIBLE, KED_COMPLIANT, SUPD_ELIGIBLE, SUPD_COMPLIANT, ADH_DM_ELIGIBLE, ADH_DM_COMPLIANT, PDC_DIABETES, ADH_RAS_ELIGIBLE, ADH_RAS_COMPLIANT, PDC_RAS, ADH_STATIN_ELIGIBLE, ADH_STATIN_COMPLIANT, PDC_STATIN, A1C_GAP_STATUS, CBP_GAP_STATUS, KED_GAP_STATUS, ADH_DM_GAP_STATUS, ADH_RAS_GAP_STATUS, ADH_STATIN_GAP_STATUS)
- HEDIS_QUALITY_DB.CLAIMS_DATA.V_MEASURE_SUMMARY (columns: MEASURE, WEIGHT, ELIGIBLE, COMPLIANT, RATE)
- HEDIS_QUALITY_DB.CLAIMS_DATA.V_OUTREACH_WORKLIST (columns: MEMBER_ID, LAST_NAME, FIRST_NAME, AGE, GENDER, PCP_NAME, PCP_GROUP, TOTAL_OPEN_GAPS, OUTREACH_PRIORITY, HCC_RISK_SCORE)
Return ONLY the SQL query, no explanation. LIMIT results to 20 rows."""

                        safe_sql_prompt = sql_prompt.replace("'", "''")
                        sql_response = run_query(f"""
                            SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', '{safe_sql_prompt}')
                        """)
                        generated_sql = sql_response[0][0].strip() if sql_response else ""

                        # Clean up the SQL
                        generated_sql = generated_sql.replace("```sql", "").replace("```", "").strip()

                        if generated_sql.upper().startswith("SELECT"):
                            cols, rows = run_query_with_columns(generated_sql)
                            if rows:
                                import pandas as pd
                                df = pd.DataFrame(rows, columns=cols)
                                st.divider()
                                st.caption("📋 Supporting Data")
                                st.dataframe(df, use_container_width=True)
                                msg_data["table_data"] = (cols, rows)
                    except:
                        pass

                st.session_state.messages.append(msg_data)

            except Exception as e:
                error_msg = f"Error: {str(e)}"
                st.error(error_msg)
                st.session_state.messages.append({"role": "assistant", "content": error_msg})

# Footer
st.divider()
st.caption("Powered by Snowflake Cortex AI | Data from HEDIS Quality DB | CMS Star Ratings 2026")
