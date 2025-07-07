import streamlit as st
import pandas as pd
import requests
import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
import plotly.express as px
from streamlit_option_menu import option_menu

# Page configuration
st.set_page_config(
    page_title="NiAB Client Management",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load environment variables
load_dotenv()

# Database connection parameters
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")

# Custom CSS
st.markdown("""
<style>
    .main {
        padding: 0rem 1rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        height: 3rem;
    }
    .stButton>button {
        width: 100%;
    }
    .upload-btn {
        background-color: #4CAF50;
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 0.3rem;
    }
    div[data-testid="stToolbar"] {
        display: none;
    }
    .st-emotion-cache-1y4p8pa {
        max-width: 100rem;
    }
</style>
""", unsafe_allow_html=True)

# Database connection
def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# Load data functions
def load_publications():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        WITH latest_publications AS (
             SELECT ranked_data.client_name,
    ranked_data.publication_id,
    ranked_data.publication_name,
    ranked_data.last_modified
   FROM ( SELECT cp.publication_name,
            cp.publication_id,
            cp.client_name,
            cp.remove,
            cp.last_modified,
            row_number() OVER (PARTITION BY cp.publication_id ORDER BY cp.last_modified DESC) AS rn
           FROM niab.client_publication cp) ranked_data
  WHERE ranked_data.rn = 1 AND ranked_data.remove = false;
    """)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return pd.DataFrame(data)

def load_abm_data():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT client_name, company, to_be_tracked, company_added_date
        FROM niab.client_abm_tracking
        ORDER BY client_name, company_added_date DESC
    """)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return pd.DataFrame(data)

def load_links_data():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT client_name, link_to_track, still_tracking, submitted_timestamp
        FROM niab.client_engaged_leads_link
        ORDER BY client_name DESC
    """)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return pd.DataFrame(data)

# Initialize session state
if 'data' not in st.session_state:
    st.session_state.data = load_publications()
if 'beehiiv_data' not in st.session_state:
    st.session_state.beehiiv_data = None
if 'edited_rows' not in st.session_state:
    st.session_state.edited_rows = {}
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = datetime.datetime.now()

# Add auto-refresh functionality
def auto_refresh():
    current_time = datetime.datetime.now()
    if 'last_refresh' not in st.session_state or (current_time - st.session_state.last_refresh).seconds >= 30:
        st.session_state.data = load_publications()
        st.session_state.last_refresh = current_time

def fetch_beehiiv_publications():
    with st.spinner('Fetching publications from Beehiiv...'):
        querystring = {"limit": 100, "direction": "desc", "order_by": "created"}
        all_publications = []
        current_page = 1
        total_pages = 1

        while current_page <= total_pages:
            querystring["page"] = current_page
            try:
                response = requests.get(
                    "https://api.beehiiv.com/v2/publications",
                    headers={
                        "Authorization": f"Bearer {os.environ.get('BEEHIIV_API_KEY')}"
                    },
                    params=querystring
                )
                response.raise_for_status()
                data = response.json()
                total_pages = data.get('total_pages', 1)
                all_publications.extend(data.get('data', []))
                current_page += 1
            except Exception as e:
                st.error(f"Error fetching publications from Beehiiv: {str(e)}")
                return None

        if not all_publications:
            st.warning("No publications found in Beehiiv")
            return None

        return pd.DataFrame([
            {"publication_name": pub["name"], "publication_id": pub["id"]} 
            for pub in all_publications
        ])

def main():
    # Sidebar
    with st.sidebar:
        # Logo removed for now
        st.markdown("---")
        
        # Navigation menu
        selected = option_menu(
            menu_title="Navigation",
            options=["Dashboard", "Publications", "ABM Lists", "Sponsored Links"],
            icons=["house", "journal-text", "building", "link"],
            menu_icon="cast",
            default_index=0,
        )
    
    # Main content area
    if selected == "Dashboard":
        show_dashboard()
    elif selected == "Publications":
        show_publications()
    elif selected == "ABM Lists":
        show_abm_lists()
    elif selected == "Sponsored Links":
        show_sponsored_links()

def show_dashboard():
    auto_refresh()
    st.title("📊 Dashboard")
    
    # Load all data
    publications_df = load_publications()
    abm_df = load_abm_data()
    links_df = load_links_data()
    
    # Create dashboard layout
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Publications", len(publications_df))
    with col2:
        st.metric("Companies Tracked", len(abm_df))
    with col3:
        st.metric("Active Links", len(links_df))
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Publications by Client")
        fig = px.pie(publications_df, names='client_name', title='Publication Distribution')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Recent Activity")
        recent_pubs = publications_df.head(5)
        st.dataframe(recent_pubs, use_container_width=True)

def show_publications():
    auto_refresh()
    st.title("📚 Publications Management")
    
    tab1, tab2 = st.tabs(["View Publications", "Import from Beehiiv"])
    
    with tab1:
        publications_df = load_publications()
        
        # Search and filter
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            search_term = st.text_input("🔍 Search publications", placeholder="Search by name, client, or ID")
        with col2:
            if not publications_df.empty:
                clients = sorted(publications_df['client_name'].unique())
                selected_client = st.selectbox("Filter by Client", ["All Clients"] + list(clients))
        with col3:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("🔄 Refresh", use_container_width=True):
                st.rerun()
        
        # Filter the dataframe based on search and client filter
        filtered_df = publications_df.copy()
        if search_term:
            filtered_df = filtered_df[
                filtered_df['publication_name'].str.contains(search_term, case=False) |
                filtered_df['client_name'].str.contains(search_term, case=False) |
                filtered_df['publication_id'].str.contains(search_term, case=False)
            ]
        if selected_client != "All Clients":
            filtered_df = filtered_df[filtered_df['client_name'] == selected_client]
        
        # Display publications with interactive features
        if not filtered_df.empty:
            # Initialize selection column
            filtered_df['select'] = False
            
            # Add help text for editing
            st.info("📝 Click on any field to edit. Changes are saved automatically.")
            
            edited_df = st.data_editor(
                filtered_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "last_modified": st.column_config.DatetimeColumn(
                        "Last Modified",
                        format="DD/MM/YYYY HH:mm",
                        required=True,
                        disabled=True
                    ),
                    "client_name": st.column_config.TextColumn(
                        "Client Name ✏️",
                        width="medium",
                        required=True,
                        help="Click to edit client name"
                    ),
                    "publication_name": st.column_config.TextColumn(
                        "Publication Name ✏️",
                        width="large",
                        required=True,
                        help="Click to edit publication name"
                    ),
                    "publication_id": st.column_config.TextColumn(
                        "Publication ID ✏️",
                        width="medium",
                        required=True,
                        help="Click to edit publication ID"
                    ),
                    "select": st.column_config.CheckboxColumn(
                        "Select",
                        help="Select for deletion",
                        default=False
                    )
                },
                num_rows="dynamic",
                key="publication_editor",
                on_change=lambda: None  # Enable real-time editing
            )
            
            # Handle edited rows
            if st.session_state.edited_rows != st.session_state.publication_editor:
                edited_rows = st.session_state.publication_editor.get("edited_rows", {})
                for idx, new_data in edited_rows.items():
                    old_data = filtered_df.iloc[idx].to_dict()
                    if new_data and not set(new_data.keys()) == {'select'}:  # Only update if changes are not just selection
                        merged_data = {**old_data, **new_data}
                        # Validate the changes
                        if all(merged_data[field] for field in ['client_name', 'publication_name', 'publication_id']):
                            update_publication(old_data, merged_data)
                        else:
                            st.error("All fields are required. Changes not saved.")
                st.session_state.edited_rows = st.session_state.publication_editor
            
            # Add action buttons for selected rows
            selected_rows = edited_df[edited_df['select'] == True]
            if not selected_rows.empty:
                col1, col2 = st.columns([4, 1])
                with col2:
                    if st.button("🗑️ Delete Selected", use_container_width=True, type="primary"):
                        with st.spinner("Deleting selected publications..."):
                            for _, row in selected_rows.iterrows():
                                delete_publication(
                                    row['client_name'],
                                    row['publication_name'],
                                    row['publication_id']
                                )
                            st.rerun()
            
            # Show editing tips
            with st.expander("ℹ️ Editing Tips"):
                st.markdown("""
                - Click any field with ✏️ to edit
                - Changes are saved automatically when you click outside the field
                - All fields except 'Last Modified' are editable
                - Use the checkbox to select publications for deletion
                - Click 'Refresh' to see the latest changes
                """)
        else:
            st.info("No publications found matching your criteria")
    
    with tab2:
        st.subheader("Import Publications from Beehiiv")
        
        # Simple fetch button
        if st.button("🔄 Fetch Publications from Beehiiv", type="primary", use_container_width=True):
            with st.spinner("Fetching publications from Beehiiv..."):
                st.session_state.beehiiv_data = fetch_beehiiv_publications()
        
        # Display and import section
        if st.session_state.beehiiv_data is not None:
            st.markdown("### Available Publications")
            
            # Simple search
            search = st.text_input("🔍 Search publications", placeholder="Filter by name")
            
            # Filter data
            filtered_df = st.session_state.beehiiv_data.copy()
            if search:
                filtered_df = filtered_df[
                    filtered_df['publication_name'].str.contains(search, case=False)
                ]
            
            # Display publications with selection
            if not filtered_df.empty:
                # Add selection column
                filtered_df['select'] = False
                
                edited_df = st.data_editor(
                    filtered_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "publication_name": st.column_config.TextColumn(
                            "Publication Name",
                            width="large"
                        ),
                        "publication_id": st.column_config.TextColumn(
                            "Publication ID",
                            width="medium"
                        ),
                        "select": st.column_config.CheckboxColumn(
                            "Select",
                            help="Select to import"
                        )
                    }
                )
                
                # Handle selected publications
                selected_pubs = edited_df[edited_df['select'] == True]
                if not selected_pubs.empty:
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        client_name = st.text_input("Client name for selected publications:")
                    with col2:
                        st.markdown("<br>", unsafe_allow_html=True)
                        if st.button("Add Selected", type="primary", use_container_width=True) and client_name:
                            with st.spinner("Adding publications..."):
                                for _, pub in selected_pubs.iterrows():
                                    add_publication(
                                        client_name,
                                        pub['publication_name'],
                                        pub['publication_id']
                                    )
                                st.success(f"Added {len(selected_pubs)} publications to {client_name}")
                                st.session_state.beehiiv_data = None
                                st.rerun()
            else:
                st.info("No publications found matching your search")

def show_abm_lists():
    auto_refresh()
    st.title("🏢 ABM List Management")
    
    tab1, tab2, tab3 = st.tabs(["View Lists", "Bulk Upload", "Add Company"])
    
    with tab1:
        abm_df = load_abm_data()
        clients = sorted(abm_df['client_name'].unique()) if not abm_df.empty else []
        
        col1, col2 = st.columns([1, 3])
        with col1:
            selected_client = st.selectbox("Select Client", [""] + clients)
        
        if selected_client:
            client_abm = abm_df[abm_df['client_name'] == selected_client]
            st.dataframe(
                client_abm,
                use_container_width=True,
                column_config={
                    "to_be_tracked": st.column_config.CheckboxColumn("Active"),
                    "company_added_date": st.column_config.DateColumn("Added Date")
                }
            )
    
    with tab2:
        # Get clients from publications data
        publications_df = load_publications()
        available_clients = sorted(publications_df['client_name'].unique())
        
        upload_client = st.selectbox(
            "Select Client for Upload",
            [""] + available_clients,
            key="upload_client"
        )
        
        uploaded_file = st.file_uploader(
            "Upload CSV file",
            type="csv",
            help="CSV file should contain company names"
        )
        
        if uploaded_file and upload_client:
            process_csv_upload(uploaded_file, upload_client)
    
    with tab3:
        col1, col2 = st.columns(2)
        with col1:
            # Get clients from publications data
            publications_df = load_publications()
            available_clients = sorted(publications_df['client_name'].unique())
            
            new_company_client = st.selectbox(
                "Client",
                [""] + available_clients,
                key="new_company_client"
            )
            new_company_name = st.text_input("Company Name")
        with col2:
            track_company = st.checkbox("Track Company", value=True)
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Add Company", use_container_width=True):
                add_company(new_company_client, new_company_name, track_company)

def show_sponsored_links():
    auto_refresh()
    st.title("🔗 Sponsored Links Tracking")
    
    links_df = load_links_data()
    
    tab1, tab2 = st.tabs(["View Links", "Add New Link"])
    
    with tab1:
        clients = sorted(links_df['client_name'].unique()) if not links_df.empty else []
        selected_client = st.selectbox("Select Client", [""] + clients)
        
        if selected_client:
            client_links = links_df[links_df['client_name'] == selected_client]
            st.dataframe(
                client_links,
                use_container_width=True,
                column_config={
                    "still_tracking": st.column_config.CheckboxColumn("Active")
                }
            )
    
    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            new_link_client = st.selectbox(
                "Client",
                [""] + sorted(st.session_state.data['client_name'].unique())
            )
        with col2:
            new_link_url = st.text_input("Link to Track")
        
        if st.button("Add Link", use_container_width=True):
            add_link(new_link_client, new_link_url)

# Helper functions for data operations
def add_publication(client, publication, pub_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO niab.client_publication 
            (client_name, publication_name, publication_id, remove, last_modified)
            VALUES (%s, %s, %s, FALSE, %s)
        """, (client, publication, pub_id, datetime.datetime.now()))
        conn.commit()
        st.success("Publication added successfully!")
        st.rerun()
    except Exception as e:
        conn.rollback()
        st.error(f"Error: {str(e)}")
    finally:
        cursor.close()
        conn.close()

def process_csv_upload(file, client):
    try:
        df = pd.read_csv(file)
        st.write("Preview:")
        st.dataframe(df.head())
        
        columns = df.columns.tolist()
        selected_column = st.selectbox(
            "Select company names column:",
            columns
        )
        
        if st.button("Confirm Upload"):
            companies = df[selected_column].dropna().unique()
            success_count = 0
            error_count = 0
            
            conn = get_db_connection()
            cursor = conn.cursor()
            
            try:
                for company in companies:
                    try:
                        cursor.execute("""
                            INSERT INTO niab.client_abm_tracking 
                            (client_name, company, to_be_tracked, company_added_date)
                            VALUES (%s, %s, TRUE, CURRENT_DATE)
                        """, (client, company.strip()))
                        success_count += 1
                    except:
                        error_count += 1
                
                conn.commit()
                st.success(f"Added {success_count} companies successfully!")
                if error_count > 0:
                    st.warning(f"{error_count} companies could not be added")
                st.rerun()
            except Exception as e:
                conn.rollback()
                st.error(f"Error: {str(e)}")
            finally:
                cursor.close()
                conn.close()
    except Exception as e:
        st.error(f"Error reading file: {str(e)}")

def add_company(client, company, track):
    if client and company:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO niab.client_abm_tracking 
                (client_name, company, to_be_tracked, company_added_date)
                VALUES (%s, %s, %s, CURRENT_DATE)
            """, (client, company, track))
            conn.commit()
            st.success("Company added successfully!")
            st.rerun()
        except Exception as e:
            conn.rollback()
            st.error(f"Error: {str(e)}")
        finally:
            cursor.close()
            conn.close()
    else:
        st.warning("Please fill all fields!")

def add_link(client, url):
    if client and url:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO niab.client_engaged_leads_link 
                (client_name, link_to_track, still_tracking)
                VALUES (%s, %s, TRUE)
            """, (client, url))
            conn.commit()
            st.success("Link added successfully!")
            st.rerun()
        except Exception as e:
            conn.rollback()
            st.error(f"Error: {str(e)}")
        finally:
            cursor.close()
            conn.close()
    else:
        st.warning("Please fill all fields!")

def delete_publication(client_name, publication_name, publication_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO niab.client_publication 
            (client_name, publication_name, publication_id, remove, last_modified)
            VALUES (%s, %s, %s, TRUE, %s)
        """, (client_name, publication_name, publication_id, datetime.datetime.now()))
        conn.commit()
        st.success(f"Successfully deleted publication: {publication_name}")
    except Exception as e:
        conn.rollback()
        st.error(f"Error deleting publication: {str(e)}")
    finally:
        cursor.close()
        conn.close()

def update_publication(old_data, new_data):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Insert new record with updated values
        cursor.execute("""
            INSERT INTO niab.client_publication 
            (client_name, publication_name, publication_id, remove, last_modified)
            VALUES (%s, %s, %s, FALSE, %s)
        """, (
            new_data['client_name'],
            new_data['publication_name'],
            new_data['publication_id'],
            datetime.datetime.now()
        ))
        conn.commit()
        st.success(f"Successfully updated publication: {new_data['publication_name']}")
    except Exception as e:
        conn.rollback()
        st.error(f"Error updating publication: {str(e)}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main() 
