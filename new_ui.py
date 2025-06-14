# Import python packages
import streamlit as st

import pandas as pd
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
def get_or_create_session():
    if "snowpark_session" not in st.session_state:
        st.session_state.snowpark_session = Session.builder.configs(connection_parameters).create()
    return st.session_state.snowpark_session

session = get_or_create_session()

# audio_queue = queue.Queue()

# def audio_frame_callback(frame: av.AudioFrame) -> av.AudioFrame:
#     audio = frame.to_ndarray().flatten().astype(np.int16).tobytes()
#     audio_queue.put(audio)
#     return frame


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
if st.button("🎙️Speak"):
    recognizer = sr.Recognizer()
    recognizer.energy_threshold = 300
    try:
        recent_context = get_recent_context(st.session_state.chat_history, n=20)
        context_str = "\n".join([f"{entry['role']}: {entry['message']}" for entry in recent_context])
        with sr.Microphone() as source:
            
            st.info("* Preparing the mic...get ready to speak in 6 seconds")
            recognizer.adjust_for_ambient_noise(source)
            st.info("🎤 here you go...speak now (~ max 21 secs).")
            audio = recognizer.listen(source, timeout=5, phrase_time_limit=29)
            # audio = recognizer.listen(source)
            mic_input = recognizer.recognize_google(audio)

        st.info(f"🗣️ You said: {mic_input}")
        # mic_input='speaking mic: '+mic_input
        st.session_state.chat_history.append({"role": "user", "message": mic_input})
        with st.status("🧠 AI analyzing ...", expanded=True) as status:
             
            #  safe_prompt = mic_input.replace("'", "''")
            #  safe_prompt = mic_input llama3-70b llama3-8b
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


                        1. Data engnieering, Analysis & Business analysis as consulting.
                        2. AI driven customized cloud native app solutions. 
                        3. Small size tech startup support. 
                        4. Cloud Infra Support
                       
                                          
Now User says: {mic_input}
$$
)
""").collect()[0][0]
             cortex_response=llm1_output_sql
#              llm2_result = session.sql(f"""
# SELECT SNOWFLAKE.CORTEX.COMPLETE(
#   'snowflake-arctic',
#   $$
#   You are a strict SQL validator.
#   You are given a user prompt and a generated SQL insert statement.
#   Check if all mandatory fields are extracted: FIRST_NAME, LAST_NAME, COMPANY.
#   If all three are non-empty in the SQL, reply only: Yes
#   If any of them are missing (empty string or blank), reply only: Ask mandatory fields

#   User input: {mic_input}
#   Generated SQL: {llm1_output_sql}
#   $$
# )
# """).collect()[0][0].strip()
#              if "Yes" in llm2_result:
#                   session.sql(llm1_output_sql).collect()
#                   cortex_response = f"✅ Lead created successfully!"
#              else:
#                   cortex_response=f"{llm1_output_sql}"
#     #               cortex_response = (
#     #     "⚠️ I need  few more details to create this lead.\n\n"
#     #     "**Please provide:** FIRST_NAME, LAST_NAME, and COMPANY."
#     # )
             
                          
#              mic_prompt = f"""
#              You Act as an AI assistant for sales CRM workflow tasks automation and 
#              currently act as you directly speaking to user. No need to give any prompt context you are being given here.

#              Your job is first understand  User's need and also check with recent past chats with same user if it is anyhow related then act accordingly
#              here is context of past recent chats with you : "{context_str}"

#              Undertstand , Analyse properly then decide which category user's need falling into :
#              1. lead status update / creation ?
#              2. Log a call or schedule setup with lead/clients, or schedule a meeting or add a reminder task
#              If Any lead/deals data asked to create update delete request made then consider given below DB schema, 
#              according to that you will generate SQL statament and signal SQL to be generated.
#              Respond in direct mode (tone of conversation), not as passive mode.

           
#              The user said via microphone: "{mic_input}" .
#              Consider below available given database schema which is 
#              : {db_schema}

#  1. understand from user''s prompt if user want to create ? If yes then check what user want to create lead? log call, update lead status, lead conversion etc. 
#  then make sure atleast - user has provided below fields (refering from given DB schema-
#     for lead creation only -Contact name, Company.
#     for log a call, or schedule a meeting or add a reminder task consider the DB schema given and ask relavant fields if missing
#   2. when need to udpate check if proper fields are given according to given schema.
#   3. when its creation of lead .. for lead_id field - use uuid
#   by default insert/update this column - owner_id as value 'ghoshalt11'
# """
#              llm_response=session.sql(f"""
#             SELECT snowflake.cortex.complete('llama3-8b', $$ {mic_prompt} $$)

#             """).collect()[0][0]
#              status.update(label="", state="complete", expanded=False)
#         with st.chat_message("ai",avatar="❄️"):
#                             st.write(f"{llm_response}")
#                             st.session_state.chat_history.append({"role": "ai", "message": llm_response})
        

#         # ✅ Update session prompt with mic input
#         # st.session_state.prompt = mic_input
             status.update(label="", state="complete", expanded=False)
        with st.chat_message("ai", avatar="❄️"):
             st.write(f"{cortex_response}")
             st.session_state.chat_history.append({"role": "ai", "message": cortex_response})

    except sr.UnknownValueError:
        # st.error("❌ Could not understand audio.")
        st.info("🎙️ Tip: Voice input not clear. Please speak clearly and ensure there's minimal background noise.")
    except sr.RequestError as e:
        st.error(f"❌ Could not request results; {e}")
    except Exception as e:
        # st.error(f"❌ Microphone error: {e}")
        st.info("🎙️ Tip: Possible microphone error..speak again")
    except sr.WaitTimeoutError:
        st.warning("⌛ You didn't say anything. Try again.")

prompt = st.chat_input("Ask anything..")
if prompt:
        
    with st.chat_message("user"):
        st.write(f"{prompt}")
        safe_prompt = prompt.replace("'", "''") # to remove any single quote issue entered by user
        st.session_state.chat_history.append({"role": "user", "message": prompt})

        recent_context = get_recent_context(st.session_state.chat_history, n=20)
        context_str = "\n".join([f"{entry['role']}: {entry['message']}" for entry in recent_context])

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
#         llm2_result = session.sql(f"""
# SELECT SNOWFLAKE.CORTEX.COMPLETE(
#   'snowflake-arctic',
#   $$
#   You are a strict SQL validator.
#   You are given a user prompt and a generated SQL insert statement.
#   Check if all mandatory fields are extracted: FIRST_NAME, LAST_NAME, COMPANY.
#   If all three are non-empty in the SQL, reply only: Yes
#   If any of them are missing (empty string or blank), reply only: Ask mandatory fields

#   User input: {safe_prompt}
#   Generated SQL: {llm1_output_sql}
#   $$
# )
# """).collect()[0][0].strip()
        
#         if llm2_result == "Yes":
#              # SQL is valid → execute insert
#              session.sql(llm1_output_sql).collect()
#              cortex_response = f"✅ Lead created successfully!"
#         else:
#              # Missing fields → ask back
#              cortex_response=f"{llm2_result} {llm2_result}"
#     #          cortex_response = (
#     #     "⚠️ need a few more details to create this lead {llm1_output_sql}.\n\n" 
#     #     "**Please provide:** FIRST_NAME, LAST_NAME, and COMPANY."
#     # )


        # prompt engineering the LLM model 'snowflake-arctic' to reply user's contextual questions.
        # chat_prompt=
#         model_response = session.sql(f"""
#             SELECT snowflake.cortex.complete(
#             'llama3-8b',$$
#             You act as **SalesSense**, an AI-powered CRM assistant bot for sales reps.  
#             Follow these rules strictly.
#             ### Past conversation with User''s CONTEXT
#             {context_str}
#             ### Current NEW INPUT
#             "{safe_prompt}"
#             ### RESPONSE RULES
#             1. Respond naturally, clearly, and briefly — don't repeat the user’s message.
#             2. Only greet if the input is a pure greeting ("hi", "hello").
#             3. If User's input is unclear or incomplete, politely ask for **specific missing info** only (not a generic explanation).
#             4. If User asks CRM-related data entry tasks/action (like update, insert, view), respond like:
             
#              - ✅ Ask only what’s missing (according to below mentioned schema context given) and do not End with `[ACTION: SQL_GENERATION_REQUIRED]` unless it is clear enough to geerate SQL
#              - ⛔ Don’t assume missing values or overexplain
#              - Only End with `[ACTION: SQL_GENERATION_REQUIRED]` if input is clear enough to generate a respective SQL statement.
           

#             Consider below available given database schema which is 
#              : {db_schema}

#             Respond now:
#             $$)""").collect()

#         model_response = model_response[0][0] # Bot model's responses captured in variable model_response
        
#         if '[ACTION: SQL_GENERATION_REQUIRED]' in model_response:
#             generate_sql=True
#         else:
#             generate_sql=False
            
#         model_response=model_response.replace("[ACTION: SQL_GENERATION_REQUIRED]", "").strip()
        
#         cortex_response=model_response
#         # generate_sql=True
#         result_df=None

#         if generate_sql: # if need to generate SQL then

#             # Prompting model to generate SQL and execute
#             sql_query_generated=session.sql(f"""
#             SELECT snowflake.cortex.complete(
#             'snowflake-arctic', 
#             $$Act as an Snowflake SQL expert, convert this User's natural input prompt into a SQL statement.
#             Note - When context is clear to you, then only give me the SQL statement to be executed as output.

#              User's natural input prompt:
#             "{safe_prompt}"
            
#             CRM database schema context given below for for generating SQL :
            
#             - leads(lead_id, name, region, score)
#             - deals(deal_id, lead_id, status, close_date)  
#             sample deal status data - 'Closed-Won', 'Closed-lost', 
#             'Proposal' etc., This is just an example  $$)""").collect()[0][0]

#             # st.session_state.chat_history.append({"role": "ai", "message": sql_query_generated})

#             result_df=session.sql(f""" SELECT     
#     concat(l.FIRST_NAME, ' ', 
#     l.LAST_NAME) Lead_contact_name, 
#     l.company,
#     l.country, l.phone,
#     d.OPPORTUNITY_NAME, 
#     d.AMOUNT, 
#     d.STAGE 
# FROM 
#     CRM_SAMPLE_DB.PUBLIC.LEADS l
# JOIN 
#     CRM_SAMPLE_DB.PUBLIC.DEALS d ON l.LEAD_ID = d.LEAD_ID
# WHERE 
#     l.STATUS = 'New' 
#     AND d.STAGE = 'Prospecting' 
#     AND d.PROBABILITY > 0.5 
# ORDER BY 
#     d.PROBABILITY DESC;""").to_pandas()
            
#             cortex_response=cortex_response+' '+'sql generated:'+sql_query_generated

            
            
            

    cortex_response=llm1_output_sql
    # Chatbot relpying back to user response
    with st.chat_message("ai",avatar="❄️"):
        
        st.write(f"{cortex_response}")
        # st.line_chart(np.random.randn(30,3))
#         if generate_sql:
#             st.dataframe(result_df, use_container_width=True)
#             st.markdown("""
# <style>
#     .element-container:has(.dataframe) {
#         border-radius: 12px;
#         overflow: hidden;
#         box-shadow: 0 1px 4px rgba(0,0,0,0.05);
#     }
# </style>
# """, unsafe_allow_html=True)

        st.session_state.chat_history.append({"role": "ai", "message": cortex_response})

     




