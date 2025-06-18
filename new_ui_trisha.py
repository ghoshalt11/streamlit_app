# Import python packages
import streamlit as st

import pandas as pd
import json
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
import speech_recognition as sr

# from streamlit_audiorec import audio_recorder

connection_parameters = {
    "account": "LZAPLSF-WJB84947",
    "user": "ghoshalt11",
    "password": "Snowpals!@#123",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "CRM_SAMPLE_DB",
    "schema": "PUBLIC"
}
# session = Session.builder.configs(connection_parameters).create()
def show_sales_conversion_chart(session):
    chart_data = session.sql("""
        SELECT 
            TO_CHAR(DATE_TRUNC('MONTH', CLOSE_DATE), 'Mon') AS MONTH_LABEL,
            EXTRACT(MONTH FROM CLOSE_DATE) AS MONTH_NO,
            COUNT(*) AS DEALS_CLOSED,
            SUM(AMOUNT) AS REVENUE
        FROM DEALS
        WHERE LOWER(STAGE) ILIKE '%won%'
        AND CLOSE_DATE >= DATE_TRUNC('QUARTER', DATEADD(QUARTER, -1, CURRENT_DATE()))
        AND OWNER_ID = 'ghoshalt11'
        GROUP BY MONTH_LABEL, MONTH_NO
        ORDER BY MONTH_NO
    """)

    if chart_data.count() == 0:
        st.warning("No closed-won deals found in last quarter.")
        return

    # Convert to pandas and set MONTH_LABEL as index with proper order
    df = chart_data.to_pandas()
    df = df.sort_values("MONTH_NO")
    df.set_index("MONTH_LABEL", inplace=True)

    st.subheader("📊 Deals & Revenue (Last Quarter)")
    st.bar_chart(df[["DEALS_CLOSED"]])
    st.line_chart(df[["REVENUE"]])
#Task / reminder
def create_reminder_task(user_input, session):
    # st.subheader("\ud83d\udd14 Creating Reminder Task")
    with st.status("Parsing reminder intent...", expanded=True) as status:
        extract_prompt = f"""
        You are a CRM assistant. Extract the following from user input:
        - COMPANY
        - REMINDER_TYPE (e.g., Call, Email, Meeting, Task)
        - REMINDER_DATE (format: YYYY-MM-DD)
        - REMINDER_TIME (format: HH:MM)
        - DESCRIPTION

        Return strictly only valid JSON like:
        {{
            "COMPANY": "Adobe",
            "REMINDER_TYPE": "Follow-up Email",
            "REMINDER_DATE": "2024-06-20",
            "REMINDER_TIME": "15:00",
            "DESCRIPTION": "Send follow-up email regarding proposal."
        }}
        And for time always consider 24-hr format.
        Respond only with a valid JSON object. No pretext, no posttext, no explanation, no markdown, no extra formatting.

        Input: {user_input}
        """

        cortex_output = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3-8b', $$ {extract_prompt} $$) AS response
        """).collect()[0][0]
        # st.code(cortex_output, language="json")

        try:
            parsed_json = json.loads(cortex_output)
        except Exception:
            st.warning("Could not parse LLM response into JSON. Try rephrasing.")
            return

        company = parsed_json.get("COMPANY", "").strip()
        reminder_type = parsed_json.get("REMINDER_TYPE", "Follow-up")
        reminder_date = parsed_json.get("REMINDER_DATE", "")
        reminder_time = parsed_json.get("REMINDER_TIME", "09:00")
        description = parsed_json.get("DESCRIPTION", "")

        if not company or not reminder_date:
            st.warning("Missing company name or reminder date.")
            return

        lead_lookup = session.sql(f"""
            SELECT LEAD_ID FROM LEADS WHERE LOWER(COMPANY) LIKE '%{company.lower()}%'
            LIMIT 1
        """).collect()

        if not lead_lookup:
            st.warning("Could not find lead ID for the specified company.")
            return

        lead_id = lead_lookup[0]["LEAD_ID"]
        reminder_datetime = f"{reminder_date} {reminder_time}:00"

        insert_sql = f"""
            INSERT INTO TASKS (
                TASK_ID, SUBJECT, STATUS, PRIORITY, ACTIVITY_DATE, DESCRIPTION,
                OWNER_ID, WHO_ID, IS_REMINDER_SET, REMINDER_DATETIME, IS_TASK, CREATED_DATE
            )
            SELECT UUID_STRING(), '{reminder_type}', 'Pending', 'Normal', '{reminder_date}', '{description.replace("'", "''")}',
                   'ghoshalt11', '{lead_id}', TRUE, '{reminder_datetime}', TRUE, CURRENT_TIMESTAMP()
        """

        session.sql(insert_sql).collect()
        status.update(label=f"✅ Reminder for **{company}** set for {reminder_datetime}!", state="complete")
        st.success(f"Task created for {company} – {reminder_type} on {reminder_datetime}")
# recommeded folowup
def recommend_followup_tasks1(session):
    # st.subheader("📌 Top Follow-up Recommendations for Today")

    # status_container = st.status("🔍 Analyzing leads to suggest top follow-ups...", expanded=True)

    # Step 1: Pull top 5 leads needing urgent follow-up (based on RATING and CREATED_DATE or STATUS)
    leads_df = session.sql("""
        SELECT  COMPANY, STATUS, RATING,
               SNOWFLAKE.CORTEX.AI_CLASSIFY(RATING, ['Hot', 'Cold', 'Warm']) AS LEAD_QUALIFIED, 
               BUSINESS_PAIN_POINTS, CREATED_DATE
        FROM LEADS
        WHERE (
          LOWER(RATING) LIKE '%hot%' 
          OR LOWER(RATING) LIKE '%warm%'
        )
        AND LOWER(STATUS) NOT LIKE '%won%'
        AND LOWER(STATUS) NOT LIKE '%win%'
        AND BUSINESS_PAIN_POINTS IS NOT NULL
        AND STATUS IS NOT NULL
        AND CREATED_DATE >= DATEADD(DAY, -7, CURRENT_DATE())
        ORDER BY RATING DESC, CREATED_DATE ASC
        LIMIT 5
    """).collect()

    if not leads_df:
        # status_container.update(label="✅ No urgent follow-ups recommended for today.", state="complete")
        st.info("✅ No urgent follow-ups recommended for today.")
        return

    for lead in leads_df:
        company = lead['COMPANY']
        status = lead['STATUS'] or "Not updated"
        # rating = lead['LEAD_QUALIFIED'] if 'LEAD_QUALIFIED' in lead else "Unknown"
        raw_rating = lead['LEAD_QUALIFIED']
        try:
            parsed_rating = json.loads(raw_rating)
            rating = parsed_rating.get("labels", ["Unknown"])[0]
        except Exception:
            rating = "Unknown"

        pain_points = lead['BUSINESS_PAIN_POINTS'] or "(no pain points logged)"

        followup_prompt = f"""
        You are a CRM strategist assistant.

        Based on the following:
        - Company: {company}
        - Lead Status: {status}
        - Rating: {rating}
        - Pain Points: {pain_points}

        Suggest 2-3 short, actionable follow-up tasks or reminders a sales rep should act on **today or within next 3 days**.
        Output as crisp bullet points.
        """

        recommendation = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3-8b', $$ {followup_prompt} $$) AS advice
        """).collect()[0][0]

        st.markdown(f"### 🔔 **{company}** ({rating})")
        st.markdown(f"**Status**: `{status}`")
        st.markdown(recommendation)
        st.divider()

        # status.update(label="✅ Top follow-up leads and actions loaded.", state="complete")

def recommend_followup_tasks(session, user_input: str):
    with st.status("🔍 Analyzing your query for follow-up recommendations...", expanded=True) as status:

        extract_company_prompt = f"""
        You are a smart assistant. Extract only the company name from this input:

        Input: "{user_input}"

        Return only the company name, no extra explanation.
        """

        company_result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3-8b', $$ {extract_company_prompt} $$) AS company
        """).collect()[0][0].strip().strip('"')

        if not company_result:
            st.warning("⚠️ No company name detected. Please mention the client/lead name.")
            return

        st.info(f"📌 Checking CRM for lead: {company_result}")
        lead_info = session.table("LEADS").filter(f"COMPANY ILIKE '%{company_result}%'") \
                                       .select("BUSINESS_PAIN_POINTS", "STATUS", "RATING") \
                                       .limit(1).collect()

        if not lead_info:
            st.warning(f"⚠️ Lead for {company_result} not found in CRM.")
            return

        pain_points = lead_info[0]["BUSINESS_PAIN_POINTS"] or ""
        status = lead_info[0]["STATUS"] or ""
        rating = lead_info[0]["RATING"] or "Unknown"

        followup_prompt = f"""
        You are a CRM strategist assistant.

        Based on the following:
        - Company: {company_result}
        - Lead Status: {status}
        - Rating: {rating}
        - Pain Points: {pain_points}

        Suggest 2-3 actionable follow-up tasks:
        - Should a reminder or follow-up meeting be scheduled?
        - Is a sales deck required?
        - What is the urgency based on rating?

        Output in concise bullet points.
        """

        recommendation = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3-8b', $$ {followup_prompt} $$) AS advice
        """).collect()[0][0]

        status.update(label="✅ Recommendations ready!", state="complete")

        st.subheader(f"📌 Follow-up Suggestions for {company_result}")
        st.markdown(recommendation)

        # Optionally store follow-up in TASKS table or show a button to do so
# recommende followup
# sales insights
def show_last_quarter_insights(session):
    st.subheader("📊 Sales Insights – Last Quarter")

    # Define the SQL using Snowflake date functions to get last quarter range
    leads_df = session.sql("""
        SELECT 
            RATING,
            COUNT(*) AS TOTAL_LEADS
        FROM LEADS
        WHERE 
            CREATED_DATE >= DATE_TRUNC('QUARTER', DATEADD(QUARTER, -1, CURRENT_DATE()))
            AND CREATED_DATE < DATE_TRUNC('QUARTER', CURRENT_DATE())
        GROUP BY RATING
        ORDER BY TOTAL_LEADS DESC
    """)

    results = leads_df.collect()

    if not results:
        st.warning("No lead data found for last quarter.")
        return

    # Format for Streamlit bar_chart
    categories = [row["RATING"] or "Unknown" for row in results]
    values = [row["TOTAL_LEADS"] for row in results]

    st.bar_chart(data={"Leads": values}, x=categories)
# sales insights end
# fetch upcoming reminders
def fetch_upcoming_reminders(session):
    st.subheader("📆 Your Upcoming Reminders")

    reminders = session.sql("""
        SELECT SUBJECT, DESCRIPTION, REMINDER_DATETIME,
               SNOWFLAKE.CORTEX.AI_COMPLETE(
                   'llama3-8b',
                   CONCAT(
                       'You are a CRM assistant. Convert this task into a short friendly reminder:\n',
                       'Subject: ', SUBJECT, '\n',
                       'Description: ', DESCRIPTION, '\n',
                       'Time: ', REMINDER_DATETIME
                   )
               ) AS REMINDER_MESSAGE
        FROM TASKS t
        JOIN LEADS l ON l.LEAD_ID = t.WHO_ID
        WHERE IS_REMINDER_SET = TRUE
          AND REMINDER_DATETIME >= CURRENT_TIMESTAMP()
        ORDER BY REMINDER_DATETIME ASC
        LIMIT 5
    """).to_pandas()
    # reminders["REMINDER_MESSAGE"] = reminders["RAW_MESSAGE"].str.replace("\\n", " ", regex=False).str.replace('"', '')

    if reminders.empty:
        st.info("✅ No upcoming reminders.")
    else:
        for _, row in reminders.iterrows():
            st.markdown(f"- {row['REMINDER_MESSAGE']}")

# lead creation via chat
def lead_creation(mic_input: str, session, exception_occurred: bool = False) -> None:
    

    llm1_output_sql = None
    json_created = None
    lead_created = False

    user_intent = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
            '{mic_input}',
            ARRAY_CONSTRUCT('lead update', 'lead create','sales strategy','sales insights')
        ) AS intent
    """).collect()[0][0]

    if 'lead update' in user_intent:
        update_lead(mic_input, session)

    if exception_occurred is False and 'lead create' in user_intent:
        json_created = session.sql(f"""
            SELECT TRY_PARSE_JSON(
                SNOWFLAKE.CORTEX.COMPLETE(
                    'llama3-8b',
                    $$
You are a CRM assistant. Your job is to extract and return:
- FIRST_NAME, LAST_NAME, COMPANY, LEAD_SOURCE, STATUS, BUSINESS_PAIN_POINTS
Return valid JSON only. Use empty string if unknown. No explanation.
Now parse this input: "{mic_input}"
                    $$
                )
            ) AS json_data
        """).collect()[0][0]

        msg_prompt = None
        rating = ""
        pain_points = ""

        if json_created is None:
            msg_prompt = "❗ Your query or context regarding lead creation is unclear. Try again."

        else:
            json_created = json.loads(json_created)

            if "partial_lead" not in st.session_state:
                st.session_state.partial_lead = {}

            for field in ["FIRST_NAME", "LAST_NAME", "COMPANY", "LEAD_SOURCE", "STATUS", "BUSINESS_PAIN_POINTS"]:
                value = json_created.get(field, "")
                if value:
                    st.session_state.partial_lead[field] = value

            required_fields = ["FIRST_NAME", "COMPANY"]
            missing = [f for f in required_fields if not st.session_state.partial_lead.get(f)]

            if missing:
                msg_prompt = f"🛑 Still need: {', '.join(missing)} to create a lead. Please provide."

            else:
                lead = st.session_state.partial_lead
                pain_points = lead.get("BUSINESS_PAIN_POINTS", "").strip().lower()

                user_said_later = any(
                    phrase in mic_input.lower()
                    for phrase in ["not known", "pain points not known", "will update later",
                                   "no pain", "none for now", "later", "not now", "update later", "nothing for now"]
                )

                if not pain_points and not user_said_later:
                    msg_prompt = f"❓ You haven't mentioned any business pain points."

                else:
                    if pain_points:
                        rating = session.sql(f"""
                            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                                'llama3-8b',
                                $$ Act as CRM Sales analyst. Classify lead as Hot/Warm/Cold based on pain points and our services. {pain_points} $$
                            )
                        """).collect()[0][0].strip()

                    rating_clean = rating.replace("'", "''")
                    lead = st.session_state.partial_lead

                    insert_stmt = f"""
                        INSERT INTO LEADS (
                            LEAD_ID, FIRST_NAME, LAST_NAME, COMPANY, LEAD_SOURCE, STATUS, OWNER_ID, CREATED_DATE, BUSINESS_PAIN_POINTS, RATING
                        )
                        SELECT
                            UUID_STRING(),
                            '{lead.get("FIRST_NAME", "").replace("'", "''")}',
                            '{lead.get("LAST_NAME", "").replace("'", "''")}',
                            '{lead.get("COMPANY", "").replace("'", "''")}',
                            COALESCE(NULLIF('{lead.get("LEAD_SOURCE", "").replace("'", "''")}', ''), 'Manual Entry'),
                            '{lead.get("STATUS", "").replace("'", "''")}',
                            'ghoshalt11',
                            CURRENT_TIMESTAMP(),
                            '{lead.get("BUSINESS_PAIN_POINTS", "").replace("'", "''")}',
                            '{rating_clean}'
                    """
                    session.sql(insert_stmt).collect()
                    lead_created = True
                    msg_prompt = f"✅ Lead created with lead qualification {rating_clean}!"

        cortex_response = msg_prompt
        with st.chat_message("ai", avatar="❄️"):
            st.write(cortex_response)
            st.session_state.chat_history.append({"role": "ai", "message": cortex_response})

            if lead_created:
                recent_lead_df = session.sql("""
                    SELECT * 
                    FROM LEADS 
                    WHERE CREATED_DATE IS NOT NULL 
                    ORDER BY CREATED_DATE DESC 
                    LIMIT 1
                """).to_pandas()

                st.info("✅ Please Verify the Lead Just Created. Want to update any field?")
                st.dataframe(recent_lead_df)
                st.session_state.chat_history.append({
                    "role": "ai",
                    "message": "✅ Please Verify the Lead Just Created. If want to update any field, just type in the chat"
                })
                st.session_state.partial_lead = {}



# lead creation end via chat
def get_or_create_session():
    if "snowpark_session" not in st.session_state:
        st.session_state.snowpark_session = Session.builder.configs(connection_parameters).create()
    return st.session_state.snowpark_session
def sales_general(user_input, session):
     
     llm1_output_sql = session.sql(f"""
SELECT SNOWFLAKE.CORTEX.COMPLETE(
  'llama3-8b',
  $$
Act as a smart sales assistant cum strategist to user's. Dont be in passve tone.
User will be input something, you need to samrtly figure out what user wish to do.
If user want to create lead, before creating lead, take details of lead to identofy if its qualified lead or not.
 If qualified then give suggestions to nurture them further.
Suggest what business points, challengs clients/leads being user discussin we should look in for according to Services we are offering.

whatever you responds, wrap it smartly and short, so user no need to read long paragraphs what you reply back
Dont forget to check your memory  :"{context_str}" , which is recent last chat history so you understand context of user's input if its new request or continuation from past chat history.
Here is our (Nihilent's) Companies's Offerings /service :


                        1. Data engnieering 
                        2. AI driven customized cloud native app solutions. 
                        3. Data Analysis, Data Science and Business consulting support 
                        4. Cloud Infra Support
                       
                                          
Now User says: {user_input}
$$
)
""").collect()[0][0]
     cortex_response=llm1_output_sql
     # Chatbot relpying back to user response
     with st.chat_message("ai",avatar="❄️"):
             st.write(f"{cortex_response}")
        # st.line_chart(np.random.randn(30,3))
#         if generate_sql:
#             st.dataframe(result_df, use_container_width=True)
     st.session_state.chat_history.append({"role": "ai", "message": cortex_response})
     
def get_sales_deck(user_input, session):
     extract_prompt = f"""
You are a smart sales CRM assistant. Extract only the company name from the following input:

Input: "{user_input}"

Return just the company name string. No extra text.
"""
     company_df = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3-8b', $$ {extract_prompt} $$) AS company
        """)
     company_name = company_df.collect()[0]["COMPANY"].strip().strip('"')
     if not company_name or len(company_name) < 2:
          st.markdown("Looks like you not making enquiry on any lead /client associated with us. You want to create it or want general sales pitch ideas?")
    #  st.code(company_name)
     leads_df = session.table("LEADS").filter(f"COMPANY ILIKE '%{company_name}%'").select("BUSINESS_PAIN_POINTS", "STATUS").limit(1)
     results = leads_df.collect()
     if not results:
            # Company not found in CRM
            # st.warning(f"⚠️ No lead found in CRM for **{company_name}**.")
#             st.markdown("""
# If you’ve had any discussion or meeting with them, just tell me their business pain points or summary — and I’ll generate a pitch deck or follow-up strategy for you.
# """)
            sales_general(user_input,session)
            return
     # Step 4: Company found — give strategic advice
     pain_points = results[0]["BUSINESS_PAIN_POINTS"]
     status = results[0]["STATUS"]

     strategy_prompt = f"""
You are a CRM sales strategist.

Client: {company_name}
Lead Status: {status}
Pain Points: {pain_points}

Advise the sales rep on what to do next:
- What type of follow-up is ideal?
- Should a pitch deck be prepared?
- Can our offerings solve this?
- Is there any blocker or risk?

Output in 1-2 bullet points.
"""
     strategy_df = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3-8b', $$ {strategy_prompt} $$) AS advice
        """)
     advice = strategy_df.collect()[0]["ADVICE"]

     st.success(f"🎯 Strategic Guidance for **{company_name}**")
     st.markdown(advice)
     

#lead update logic defined
def update_lead(mic_input, session):
    with st.status("🧠 Cortex AI analyzing your update request...", expanded=True) as status:

        parsed_result = session.sql(f"""
            SELECT TRY_PARSE_JSON(
                SNOWFLAKE.CORTEX.COMPLETE(
                    'llama3-8b',
                    $$
You are a smart CRM sales work-flow assistant.

Your task is to extract exactly:
- The COMPANY (whose lead needs update or converted). Lead/company conversion means when lead's status need to update to 'Closed-won'
- The FIELDS_TO_UPDATE (a dictionary of field names and new values)
- Optional field: DEAL_VALUE if user specifies it (extract as a number only)
- When user saying a lead is getting converted then create another node in output json object

This is an example, how always return a JSON in this structure:
when deal value is present in input 
{{
  "COMPANY": "Company Name",
  "FIELDS_TO_UPDATE": {{
    "STATUS": "New Value",
    "PHONE": "Updated phone",
    "NUMBER_OF_EMPLOEES": 100
  }}
    "DEAL_VALUE": $200 
}}
                                    
when deal value is NOT present in input then output json object
  {{
  "COMPANY": "Company Name",
  "FIELDS_TO_UPDATE": {{
    "STATUS": "New Value",
    "PHONE": "Updated phone",
    "NUMBER_OF_EMPLOEES": 100
  }}
}}

⚠️ Rules:
- Always include the "COMPANY"
- Only include fields the user wants to update inside "FIELDS_TO_UPDATE"
- Do NOT add any prefix or explanation like "Here is the output, I think, I recommend etc."
- Valid field names: STATUS, INDUSTRY, PHONE, BUSINESS_PAIN_POINTS, NUMBER_OF_EMPLOYEES, RATING
- Return only pure JSON — no markdown or extra text

Input: "{mic_input}"$$
                )
            ) AS parsed
        """).collect()[0][0]

        if not parsed_result:
            st.warning("⚠️ Could not parse update request. Please try rephrasing.")
            return

        parsed_json = json.loads(parsed_result)
        company = parsed_json.get("COMPANY", "").strip()
        company =company.replace("'", "''")
        updates = parsed_json.get("FIELDS_TO_UPDATE", {})
        deal_value = parsed_json.get("DEAL_VALUE")  # optional field
        # st.code(parsed_json)

        if not company or not updates:
            st.warning("⚠️ Missing company or update fields.")
            return

        # Clean values and construct SET clause
        set_clauses = []
        for field, value in updates.items():
            if isinstance(value, str):
                value = value.replace("'", "''")  # escape single quotes
                set_clauses.append(f"{field.upper()} = '{value}'")
            else:
                set_clauses.append(f"{field.upper()} = {value}")

        update_sql = f"""
            UPDATE LEADS
            SET {', '.join(set_clauses)}
            WHERE LOWER(COMPANY) LIKE '%{company.lower()}%'
        """

        status.update(label="✅ Parsed input. Updating CRM record...", state="running")
        # st.code(update_sql)

        # Execute the update
        session.sql(update_sql).collect()
        status.update(label="✅ CRM record updated.", state="complete")
        # new added
        # Check if BUSINESS_PAIN_POINTS was updated
    if "BUSINESS_PAIN_POINTS" in [key.upper() for key in updates.keys()]:
        status.update(label="🔍 Running real-time qualification scoring...", state="running")
        pain_points = updates.get("BUSINESS_PAIN_POINTS", "")
        if pain_points:
            pain_points = pain_points.replace("'", "''")
            rating = session.sql(f"""
                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                    'llama3-8b',
                    $$ Act as CRM Sales analyst on behalf of a company Nihilent.
                    Classify lead as Hot/Warm/Cold based on comparing these pain points with 
                    Nihilent's offering services. Just return rating in one word only.Also can add why chosen that classification?

                    Pain Points: {pain_points}

                    Nihilent offerings:
                    1. Data engineering service
                    2. Cloud infrastructure support
                    3. AI-based solutions
                    4. Data analysis and BI consulting
                    $$
                )
            """).collect()[0][0].strip()
            rating = rating.replace("'", "''")
            session.sql(f"""
                UPDATE LEADS
                SET RATING = '{rating}'
                WHERE LOWER(COMPANY) LIKE '%{company.lower()}%'
            """).collect()
        # new added ends
        status.update(label="Lead qualification score updated based on business pain points shared.", state="complete")

    # Auto deal creation logic
    if "STATUS" in [key.upper() for key in updates.keys()] and 'won' in updates.get("STATUS", "").lower():
        status.update(label="🏁 Status marked as converted. Creating deal...", state="running")

        lead_id_result = session.sql(f"""
            SELECT LEAD_ID, CREATED_DATE FROM LEADS
            WHERE LOWER(COMPANY) LIKE '%{company.lower()}%'
            LIMIT 1
        """).collect()

        if lead_id_result:
            lead_id = lead_id_result[0]['LEAD_ID']
            close_date = lead_id_result[0]['CREATED_DATE']
            deal_amount = deal_value if deal_value is not None else 50000.0

            insert_deal_sql = f"""
                INSERT INTO DEALS (
                    DEAL_ID, LEAD_ID, OPPORTUNITY_NAME, AMOUNT, STAGE, PROBABILITY, TYPE,
                    CLOSE_DATE, FORECAST_CATEGORY, OWNER_ID, CREATED_DATE
                )
                SELECT UUID_STRING(), '{lead_id}', 'Opportunity - {company}', {deal_amount}, 'Closed-Won', 0.4,
                       'New Business', '{close_date}', 'Pipeline', 'ghoshalt11', CURRENT_TIMESTAMP()
            """
            session.sql(insert_deal_sql).collect()
            status.update(label="✅ New deal record created for converted lead.", state="complete")
    st.success(f"✅ Lead **{company}** has been updated.")
    

    # Show updated record to verify
    df = session.sql(f"""
        SELECT * FROM LEADS
        WHERE LOWER(COMPANY) LIKE '%{company.lower()}%'
        ORDER BY CREATED_DATE DESC
        LIMIT 1
    """).to_pandas()
    st.dataframe(df)

# end of lead update logic

session = get_or_create_session()


# python UDF function for retrieving recent user-bot conversations to have past context linked to current user prompt
def get_recent_context(chat_history, n):
    return chat_history[-n:] if len(chat_history) >= 1 else chat_history

st.markdown("""
    <style>
    div.stButton > button {
        background-color: #ffffff;
        color: #1a1a1a;
        font-weight: 600;
        font-size: 15px;
        padding: 0.75rem 1.5rem;
        border-radius: 18px;
        border: none;
        box-shadow: 
            0 4px 6px rgba(0, 0, 0, 0.1),
            0 1px 3px rgba(0, 0, 0, 0.08); /* ← Elevated button look */
        transition: all 0.2s ease-in-out;
    }

    div.stButton > button:hover {
        box-shadow: 
            0 6px 16px rgba(41, 181, 232, 0.25),
            0 3px 6px rgba(0, 0, 0, 0.1);
        transform: translateY(-2px);
        color: #007ACE;
    }

    div.stButton > button:active {
        transform: scale(0.98);
        box-shadow: 
            0 3px 6px rgba(0, 0, 0, 0.08),
            0 1px 2px rgba(0, 0, 0, 0.05);
    }
    </style>
""", unsafe_allow_html=True)



st.markdown(
    """
    <h2 style="
        background-image: linear-gradient(90deg, #007ace, #29B5E8, #5AC8FA);
        background-clip: text;
        -webkit-background-clip: text;
        color: transparent;
        -webkit-text-fill-color: transparent;
        font-weight: 700;
        text-align: center;
        margin-top: 10px;
    ">
    ❄️ How May I Assist You?
    </h2>
    """,
    unsafe_allow_html=True
)


st.markdown("<div style='margin-top: 12px;'></div>", unsafe_allow_html=True)

# col1, col2 = st.columns([1, 1])
# # col_center = st.container()

# with col1:
#     st.button("➕ Create / Update Lead Info", key="lead_tile")

# with col2:
#     st.button("📞 Log Call / Meetings notes", key="log_tile")


# st.markdown("<div style='text-align:center;'>", unsafe_allow_html=True)
# st.button("📈 Update Sales Pipeline", key="pipeline_tile")
# st.markdown("</div>", unsafe_allow_html=True)
# st.divider()

# To keep a sidebar menu chats
db_schema=session.sql(f"""SELECT LISTAGG($1, '\n') FROM @CRM_SAMPLE_DB.PUBLIC.CRM_SCHEMA_STAGE/crm_schema_ss.sql
      (FILE_FORMAT => 'crm_sql_format')""")
with st.sidebar:    
    if st.button("🗑 clear chat", help="Clear chat session history"):
        st.session_state.chat_history = []
    if st.button("💾 save chat"):
        chat_lines = []
        
        for entry in st.session_state.chat_history:
            role = "👤 User" if entry["role"] == "user" else "🤖 Bot"
            chat_lines.append(f"**{role}:** {entry['message']}")
        chat_md = "\n\n".join(chat_lines)
        
        # Write to file and trigger download
        st.download_button(
            label="Download",
            data=chat_md,
            file_name="sales_chat_history.txt",
            mime="text/plain"
        )
        
# Get the current active snowpark session/credentials
# session = get_active_session()

# keeping chat session history logic
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []  

# Display existing chat history 
for entry in st.session_state.chat_history:
    with st.chat_message(entry["role"], avatar="❄️" if entry["role"] == "ai" else None):
        st.markdown(entry["message"])

# Horizontal mini toolbar right above chat input

uploaded_file=None
# with st.container():
#     col1, col2   = st.columns([0.1, 0.1])

#     with col1:
#         if st.button("📎upload", key="upload_btn"):
#             st.session_state.show_upload = True
            
#             if st.session_state.show_upload:
                
st.markdown("""
<div style='font-weight:600; font-size:16px; margin-bottom:4px; margin-top:10px;'>
📎 Upload File (e.g., Notes, Contact Cards, PDFs)
</div>
""", unsafe_allow_html=True)

uploaded_file = st.file_uploader(
    label="",  # hide label now that we styled it above
    type=["pdf", "png", "jpg", "jpeg", "wav","xlsx", "xls","csv"],
    label_visibility="collapsed",
    accept_multiple_files=False
)                

                    
if uploaded_file:

    file_name = uploaded_file.name
    file_ext = file_name.split('.')[-1].lower()

    if file_ext in ["pdf", "png", "jpg", "jpeg"]:
         PutResult = session.file.put_stream(
    uploaded_file,
    f"@CRM_SCHEMA_STAGE/{uploaded_file.name}",
    auto_compress=False,
    overwrite=True
)
    if file_ext in ["pdf", "png", "jpg", "jpeg"]:
         if PutResult and PutResult.status in ["UPLOADED", "OVERWRITTEN"]:


        
        # st.info(f"✅ File uploaded : {PutResult.target}..parsing started..")
            with st.status("🧠 AI Image Parsing ...", expanded=True) as status:
               
             
            #  if file_ext in ["pdf", "png", "jpg", "jpeg"]:
               parsed = session.sql(f"""
                SELECT SNOWFLAKE.CORTEX.PARSE_DOCUMENT('@CRM_SCHEMA_STAGE',
                    '{file_name}', 
                    PARSE_JSON('{{"mode": "OCR"}}')
                )
            """).collect()
               extracted_text = parsed[0][0] if parsed else "No text extracted."
               extracted_text=session.sql(f"""SELECT SNOWFLAKE.CORTEX.COMPLETE(
                                       
                    'snowflake-arctic',
                            $$
                    You are a JSON parser.

                    Task: Extract only the value of the "content" field 
                                       from the given JSON input. Return only the raw value without any explanation or extra text.

                    Input JSON:
                    {extracted_text}

                    Output:
                    $$
                )""").collect()
                  
               extracted_text=extracted_text[0][0]
               status.update(label="✅ Analysis done!", state="complete", expanded=False)
            st.text_area("📄 Extracted Text from Image uploaded", value=extracted_text, height=400)
        
    
    if file_ext in ["xlsx", "xls"]: 
            
            # df = pd.read_excel(uploaded_file)
            excel_file = pd.ExcelFile(uploaded_file)
            sheet_names = excel_file.sheet_names
            selected_sheet = st.selectbox("Choose a sheet to load", sheet_names)
            df = excel_file.parse(selected_sheet)
            st.info("✅ Excel sheet loaded.")
            st.dataframe(df.head(20))
            with st.status("🧠 AI Analyzing ...", expanded=True) as status:
                        csv_text = df.head(100).to_csv(index=False)
                        llm_prompt=prompt = f"""
You are a sales analyst AI and chatting with a user currently you are speaking. 

Below is a dataset of leads in CSV format. Analyze it and provide in such a way that you speaking to the user directl and give insights analysis in below format:
- What recommendation / suggestion you would like to give as a sales rep assistant.
- Based Top regions can suggest some idea for increasing business there.
- Try  natural way give deep insights in short crisp (not too high paragraphs style) so user can quickly get it.


### Data:
{csv_text}

### Insights:
"""
                        exl_insight_response = session.sql(f"""
    SELECT snowflake.cortex.complete('llama3-8b', $$ {llm_prompt} $$)
""").collect()[0][0]
                        status.update(label="✅ Analysis complete!", state="complete", expanded=False)
            with st.chat_message("ai",avatar="❄️"):
                            
                            st.write(f"{exl_insight_response}")
            

        # csv files
    if file_ext in ["csv"]: 
            # df = pd.read_excel(uploaded_file)
            df = pd.read_csv(uploaded_file)
            # sheet_names = excel_file.sheet_names
            # selected_sheet = st.selectbox("Choose a sheet to load", sheet_names)
            # df = excel_file.parse(selected_sheet)
            st.info("✅ CSV loaded.")
            st.dataframe(df.head(7))
            with st.status("🧠 AI Analyzing ...", expanded=True) as status:
                        csv_text = df.head(20).to_csv(index=False)
                        llm_prompt=prompt = f"""
Act as CRM sales analyst chatting with a user currently and not speaking in passive tone.
Your tone should appear like you already analysed the data given here and ready to provide key insights. 

Analyze the provided data below in such a way that you speaking to the user directly and give insights analysis in below format:
- What recommendation / suggestion you would like to give as a sales rep assistant.
- if business pain points of leads missing any suggest immediate actions accordingly

### Data:
{csv_text}

### Insights:
"""
                        
                        exl_insight_response = session.sql(f"""
    SELECT snowflake.cortex.complete('llama3-8b', $$ {llm_prompt} $$)
""").collect()[0][0]
                        status.update(label="✅ CSV Analysis complete!", state="complete", expanded=False)
            with st.chat_message("ai",avatar="❄️"):
                            
                            st.write(f"{exl_insight_response}")

# logic to handle user enters chat
# Microphone input
mic_input=None
exception_occurred=False
if st.button("🎙️Speak"):
    recognizer = sr.Recognizer()
    # recognizer.energy_threshold = 300
    try:
        recent_context = get_recent_context(st.session_state.chat_history, n=25)
        context_str = "\n".join([f"{entry['role']}: {entry['message']}" for entry in recent_context])
        with sr.Microphone() as source:
            
            st.info("preparing mic...get ready in 5 seconds")
            recognizer.adjust_for_ambient_noise(source)
            st.info("🎤 listening...speak now (~ max 12-15 secs).")
            audio = recognizer.listen(source, timeout=5, phrase_time_limit=27)
            # audio = recognizer.listen(source)
            mic_input = recognizer.recognize_google(audio)

    

            st.info(f"🗣️ You said: {mic_input}")
        
            st.session_state.chat_history.append({"role": "user", "message": mic_input})
            
            


    except sr.UnknownValueError:
        exception_occurred = True
        # st.error("❌ Could not understand audio.")
        st.info("🎙️ Tip: Voice input not clear. Please speak clearly and ensure minimal background noise.")
    except sr.RequestError as e:
        exception_occurred = True
        st.error(f"❌ Could not request results; {e}")
    except Exception as e:
        exception_occurred = True
        # st.error(f"❌ Microphone error: {e}")
        st.info("🎙️ Tip: Possible microphone error..speak again")
    except sr.WaitTimeoutError:
        exception_occurred = True
        st.warning("⌛ You didn't say anything. Try again.")

    llm1_output_sql=None
    json_created=None
    lead_created=None
    user_intent=session.sql(f"""
SELECT SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
  '{mic_input}',
  ARRAY_CONSTRUCT('lead update', 'lead create','sales strategy','sales insights')
) AS intent
""").collect()[0][0]
    
    if 'lead update' in user_intent:
         update_lead(mic_input, session)
    
    if exception_occurred==False and 'lead create' in user_intent:
                  
        #  with st.status("🧠 Cortex analyzing ...", expanded=True) as status:
          
          
          
          
          json_created = session.sql(f"""
SELECT TRY_PARSE_JSON(
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3-8b',
        $$
You are a CRM assistant.

Your job is to extract from user input and return the following keys **strictly as a JSON object**:
- FIRST_NAME
- LAST_NAME
- COMPANY
- LEAD_SOURCE
- STATUS
- BUSINESS_PAIN_POINTS

Rules:
- If the contact name/person of the lead is like "Mr X" or "Ms X" or "Mrs X", ignore the title ("Mr") and set "X" as FIRST_NAME.
- If any field is missing in the input, still include the key but with an empty string ("").
- If LEAD_SOURCE is not mentioned, set it to "Manual Entry".
- Do **not** include explanations or extra text.
- Return **only** the JSON, nothing else.
- Do **not** wrap the JSON in triple backticks or markdown formatting.

Example:
{{ 
  "FIRST_NAME": "Jason", 
  "LAST_NAME": "Kaur", 
  "COMPANY": "XYZ Corporation", 
  "LEAD_SOURCE": "Manual Entry", 
  "STATUS": "",
  "BUSINESS_PAIN_POINTS": "Data silos and low reporting accuracy, problem in migration, need scaling solutions etc."
}}

Now parse this input and return JSON:
"{mic_input}"
        $$
    )
) AS json_data
""").collect()[0][0]
        #   print('json_created',json_created)
          msg_prompt=None
          rating = ""
          pain_points=""
          if json_created==None:
               msg_prompt=f"Your query or context regarding any lead creation not clear!. Try Again."
               
               
          else: 
               
               json_created = json.loads(json_created)
               if "partial_lead" not in st.session_state:
                     st.session_state.partial_lead = {}

               for field in ["FIRST_NAME", "LAST_NAME", "COMPANY", "LEAD_SOURCE", "STATUS", "BUSINESS_PAIN_POINTS"]:
                     value = json_created.get(field, "")
                     if value:  # Only update non-empty values
                           st.session_state.partial_lead[field] = value
               required_fields = ["FIRST_NAME", "COMPANY"]
               missing = [f for f in required_fields if not st.session_state.partial_lead.get(f)]
               if missing:
                     msg_prompt = f"🛑 Still need: {', '.join(missing)} to create a lead. Please provide."
               else:
                    lead = st.session_state.partial_lead
                    pain_points = lead.get("BUSINESS_PAIN_POINTS", "").strip().lower()

                    user_said_later = any(
                        phrase in mic_input.lower()
                        for phrase in ["not known","pain points not known","will update later","no pain", "none for now", "later", "not now", "update later","nothing for now"]
                    )

                    if pain_points == None or pain_points=="": #and not user_said_later:
                        msg_prompt = f"❓ You haven't mentioned any business pain points. "
                        #     "Would you like to add them now, or say 'I'll update later' to continue?"
                        # )
                    else:
                        # Optional: rate lead based on pain points
                        # rating = "Cold"
                        if pain_points:
                            rating = session.sql(f"""
                                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                                    'llama3-8b',
                                    $$ Act as CRM Sales analyst on behalf of a company Nihilent and be in active tone not passive. DO not give any title headling .
                                    Just in quick short - Classify lead (company) as Hot/Warm/Cold based on comparing these pain points with 
                                    Nihilent's offering services/products and suggest - if lead classified hot then need to quick follow up in next 2 days and setup reminder
                                    else if warm then follow-up in next 5 days else follow up in 1 week. 
                                    {pain_points}
                                    Nihilent offerings : 1.Data engineering service (non-streaming) - e.g extracting various multi sources data into a unified plarform.
                                                         2.Cloud infrastructure management support - which only manages access to Client's (lead's own hosted cloud).
                                    Just give output : lead classification , why classified so. No other details and quick action plan with the lead.
                                    $$
                                )
                            """).collect()[0][0].strip()
                    rating_clean = rating.replace("'", "''")
                    llm1_output_sql = session.sql(f"""
INSERT INTO LEADS (
    LEAD_ID, FIRST_NAME, LAST_NAME, COMPANY, LEAD_SOURCE, STATUS, OWNER_ID, CREATED_DATE,BUSINESS_PAIN_POINTS,RATING
)
SELECT
    UUID_STRING(),
    '{lead.get("FIRST_NAME", "")}',
    '{lead.get("LAST_NAME", "")}',
    '{lead.get("COMPANY", "")}',
    COALESCE(NULLIF('{lead.get("LEAD_SOURCE", "")}', ''), 'Manual Entry'),
    '{lead.get("STATUS", "")}',
    'ghoshalt11',
    CURRENT_TIMESTAMP(),    
    '{lead.get("BUSINESS_PAIN_POINTS", "").replace("'", "''")}',
    '{rating_clean}'
    
""").collect()
                    lead_created = True
                    # st.session_state.partial_lead = {}  # ✅ clear once inserted
                    msg_prompt=f"Lead created with lead qualification {rating_clean}!"



          if msg_prompt:
               cortex_response=msg_prompt
        #   else:
               
               
               
        #        cortex_response="Lead Created!" #llm1_output_sql
          with st.chat_message("ai", avatar="❄️"):
            
            #  cortex_response="Lead Created!"
             st.write(cortex_response)
             st.session_state.chat_history.append({"role": "ai", "message": cortex_response})
            #  st.session_state.chat_history.append({"role":"ai","message":st.session_state.partial_lead})
            #  st.session_state.chat_history.append({"role":"ai","message":json_created})
            #  st.session_state.chat_history.append({"role":"ai","message":pain_points})
             
             if lead_created:
                    
                  
            #       st.session_state.chat_history.append({"role": "ai", "message": json_created["COMPANY"]})
                    recent_lead_df = session.sql("""
    SELECT * 
    FROM LEADS 
    WHERE CREATED_DATE IS NOT NULL 
    ORDER BY CREATED_DATE DESC 
    LIMIT 1
""").to_pandas()
                    
           

# Display it to the user in Streamlit
                    st.info("✅ Please Verify the Lead Just Created. Want to update any field ?")
                    post_lead_creation_msg="✅ Please Verify the Lead Just Created. Want to update any field ?"
                    st.dataframe(recent_lead_df)             
                    st.session_state.chat_history.append({"role": "ai", "message": post_lead_creation_msg})
                    st.session_state.partial_lead = {}

     
# st.markdown("## 🔔 Daily Follow-Ups")
if st.button("## 🔔 Daily Follow-Ups"):
    recommend_followup_tasks1(session)
if st.button("📆 Upcoming Reminders"):
     fetch_upcoming_reminders(session)    

prompt = st.chat_input("Ask anything...")
if prompt:
        
    with st.chat_message("user"):
        st.write(f"{prompt}")
        safe_prompt = prompt.replace("'", "''") # to remove any single quote issue entered by user
        st.session_state.chat_history.append({"role": "user", "message": prompt})

        recent_context = get_recent_context(st.session_state.chat_history, n=20)
        context_str = "\n".join([f"{entry['role']}: {entry['message']}" for entry in recent_context])
        
        user_intent=session.sql(f"""
SELECT SNOWFLAKE.CORTEX.COMPLETE(
  'llama3-8b',$$  
Act as smart CRM sales intent classifier assistant using past discussion context with user.If context present and you found strong immediate link then classify accordingly.
past context : {context_str}
Classify the user's intent behind the message into one of these categories:
- lead create → when the user wants to create a new lead or new contact
- lead update → when the user wants to change or update lead info or convert a lead to close or win
- fetch record -> when the user wants to see /view just a particular client's info on display.
- sales strategy → when the user is asking for help,preparing sale pitch deck for clients, asking advice, or suggestions about how to sell, qualify, or plan
- sales_insights → when the user wants to understand past descriptive sales performance or past data
- reminder_setup -> 
- task_creation

Message: "{safe_prompt}"

Return only the matching label from the above list. Do not explain.$$
)
""").collect()[0][0]
#         st.code(user_intent)


#         user_intent=session.sql(f"""
# SELECT SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
#   '{safe_prompt}',
#   ARRAY_CONSTRUCT('lead update', 'lead create','sales strategy','fetch record','sales insights')
# ) AS intent
# """).collect()[0][0]
        st.code(user_intent)
    
    if 'lead update' in user_intent or 'lead_update' in user_intent:
         
         update_lead(safe_prompt, session)
    elif 'sales strategy' in user_intent or 'strategy' in user_intent:
         get_sales_deck(safe_prompt, session)
    elif 'sales_insights' in user_intent or 'insights' in user_intent:
         show_sales_conversion_chart(session)
    elif 'lead create' in user_intent:
         lead_creation(safe_prompt, session)
    elif 'recommendation' in user_intent:
         recommend_followup_tasks(session,safe_prompt)
    elif 'task' in user_intent or 'reminder' in user_intent:
         create_reminder_task(safe_prompt,session)
        # st.info("this feature coming soon...")
         
    else:


    # if 'sales strategy' in user_intent:
        
        llm1_output_sql = session.sql(f"""
SELECT SNOWFLAKE.CORTEX.COMPLETE(
  'llama3-8b',
  $$
Act as a smart sales assistant cum strategist to user's. Dont be in passve tone.
User will be input something, you need to samrtly figure out what user wish to do.
If user want to create lead, before creating lead, take details of lead to identofy if its qualified lead or not.
 If qualified then give suggestions to nurture them further.
Suggest what business points, challengs clients/leads being user discussin we should look in for according to Services we are offering.

whatever you responds, wrap it smartly and short, so user no need to read long paragraphs what you reply back
Dont forget to check your memory  :"{context_str}" , which is recent last chat history so you understand context of user's input if its new request or continuation from past chat history.
Here is our (Nihilent's) Companies's Offerings /service :


                        1. Data engnieering 
                        2. AI driven customized cloud native app solutions. 
                        3. Data Analysis, Data Science and Business consulting support 
                        4. Cloud Infra Support
                       
                                          
Now User says: {safe_prompt}
$$
)
""").collect()[0][0]
        
      

        cortex_response=llm1_output_sql
    # Chatbot relpying back to user response
        with st.chat_message("ai",avatar="❄️"):
             st.write(f"{cortex_response}")
        # st.line_chart(np.random.randn(30,3))
#         if generate_sql:
#             st.dataframe(result_df, use_container_width=True)


        st.session_state.chat_history.append({"role": "ai", "message": cortex_response})

     




