import pandas as pd
import streamlit as st
import requests
import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import os

# Database connection parameters
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")



# Connect to database
def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# Load data from database
def load_data():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        WITH latest_publications AS (
            SELECT DISTINCT ON (publication_id) 
                client_name,
                publication_name,
                publication_id,
                remove,
                last_modified
            FROM niab.client_publication
            WHERE remove = FALSE
            ORDER BY publication_id, last_modified DESC
        )
        SELECT 
            client_name,
            publication_name,
            publication_id,
            last_modified
        FROM latest_publications
        ORDER BY last_modified DESC
    """)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return pd.DataFrame(data)

# Initialize session states
if "data" not in st.session_state:
    st.session_state.data = load_data()
if "beehiiv_data" not in st.session_state:
    st.session_state.beehiiv_data = None
if "page" not in st.session_state:
    st.session_state.page = "publications"  # Set default page

# Add navigation in sidebar
st.sidebar.title("Navigation")
if st.sidebar.button("Publications"):
    st.session_state.page = "publications"
if st.sidebar.button("Submit Links"):
    st.session_state.page = "links"

# Create different pages based on navigation
if st.session_state.page == "publications":
    # Set the title of the app
    st.title('Live Publications')

    # Display only active and unique publications
    st.dataframe(st.session_state.data)

    # Fetch all publications
    if st.button("Fetch all publications"):
        querystring = {"limit": 100, "direction": "desc", "order_by": "created"}
        all_publications = []
        current_page = 1
        total_pages = 1

        while current_page <= total_pages:
            querystring["page"] = current_page
            response = requests.get(
                "https://api.beehiiv.com/v2/publications",
                headers={
                    "Authorization": "Bearer xGDFXF1itk0qyIwA1gPHNvyToSlXZSl14dKoRvVXjsOl8zwk35ehbYNrfguCYCaE"
                },
                params=querystring
            )
            if response.status_code != 200:
                st.error("Error fetching publications from Beehiiv")
                break
            
            data = response.json()
            total_pages = data.get('total_pages', 1)
            all_publications.extend(data.get('data', []))
            current_page += 1
        
        if not all_publications:
            st.error("No publications found")
        else:
            # Store the data in session state
            st.session_state.beehiiv_data = pd.DataFrame([
                {"publication_name": pub["name"], "publication_id": pub["id"]} 
                for pub in all_publications
            ])

    # Display Beehiiv data if it exists
    if st.session_state.beehiiv_data is not None:
        st.write("Beehiiv Publications:")
        st.dataframe(st.session_state.beehiiv_data)

    # CRUD Operations
    st.sidebar.header("Manage Publications")

    # Select a row uniquely based on both publication_name and publication_id
    selected_pub = st.sidebar.selectbox(
        "Select a publication", 
        st.session_state.data[['publication_name', 'publication_id']].apply(lambda x: f"{x[0]} ({x[1]})", axis=1), 
        index=None
    )

    if selected_pub:
        # Extract publication_name and publication_id separately
        pub_name, pub_id = selected_pub.rsplit(" (", 1)
        pub_id = pub_id.rstrip(")")

        selected_entry = st.session_state.data[
            (st.session_state.data['publication_name'] == pub_name) & 
            (st.session_state.data['publication_id'] == pub_id)
        ]
        
        if selected_entry.empty:
            st.sidebar.error("Selected publication not found in database")
        else:
            client_name = st.sidebar.text_input("Client Name", selected_entry['client_name'].values[0])
            publication_name = st.sidebar.text_input("Publication Name", selected_entry['publication_name'].values[0])
            publication_id = st.sidebar.text_input("Publication ID", selected_entry['publication_id'].values[0])

            # Update operation
            if st.sidebar.button("Update"):
                if selected_pub:
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    try:
                        # Insert new record instead of updating
                        cursor.execute("""
                            INSERT INTO niab.client_publication 
                            (client_name, publication_name, publication_id, remove, last_modified)
                            VALUES (%s, %s, %s, FALSE, %s)
                        """, (client_name, publication_name, publication_id, datetime.datetime.now()))
                        
                        conn.commit()
                        st.success("Publication record appended successfully!")
                        st.session_state.data = load_data()
                        st.rerun()
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Error: {str(e)}")
                    finally:
                        cursor.close()
                        conn.close()

            # Delete operation (soft delete)
            if st.sidebar.button("Delete"):
                if selected_pub:
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    try:
                        # Set remove=TRUE for all records with this publication_id
                        cursor.execute("""
                            UPDATE niab.client_publication 
                            SET remove = TRUE, 
                                last_modified = %s
                            WHERE publication_id = %s
                        """, (datetime.datetime.now(), pub_id))
                        
                        conn.commit()
                        st.success("Publication marked as removed successfully!")
                        st.session_state.data = load_data()
                        st.rerun()
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Error: {str(e)}")
                    finally:
                        cursor.close()
                        conn.close()

    st.sidebar.markdown("---")

    # Add new publication
    st.sidebar.subheader("Add New Publication")
    new_client = st.sidebar.text_input("Client Name")
    new_publication = st.sidebar.text_input("Publication Name")
    new_pub_id = st.sidebar.text_input("Publication ID")

    if st.sidebar.button("Add"):
        if new_client and new_publication and new_pub_id:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            try:
                # Check if publication_id exists and is not removed
                cursor.execute("""
                    SELECT client_name, publication_name, publication_id, remove
                    FROM niab.client_publication 
                    WHERE publication_id = %s AND remove = FALSE
                """, (new_pub_id,))
                
                existing_active_pub = cursor.fetchone()
                
                if existing_active_pub:
                    # Publication exists and is active - just show warning
                    st.warning(f"Publication ID already exists for '{existing_active_pub[1]}'. Please update the existing record instead.")
                else:
                    # Either publication doesn't exist or was previously removed
                    # In both cases, insert new record
                    cursor.execute("""
                        INSERT INTO niab.client_publication 
                        (client_name, publication_name, publication_id, remove, last_modified)
                        VALUES (%s, %s, %s, FALSE, %s)
                    """, (new_client, new_publication, new_pub_id, datetime.datetime.now()))
                    st.success("New publication added successfully!")
                    
                    conn.commit()
                    st.session_state.data = load_data()
                    st.rerun()
                
            except Exception as e:
                conn.rollback()
                st.error(f"Error: {str(e)}")
            finally:
                cursor.close()
                conn.close()
        else:
            st.sidebar.warning("Please fill all fields!")

elif st.session_state.page == "links":
    st.title('Link Tracking')
    
    # Load links data
    def load_links():
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT client_name, link_to_track, still_tracking
            FROM niab.client_engaged_leads_link
            ORDER BY client_name DESC
        """)
        links_data = cursor.fetchall()
        cursor.close()
        conn.close()
        return pd.DataFrame(links_data)
    
    # Get unique clients
    links_df = load_links()
    clients = sorted(links_df['client_name'].unique()) if not links_df.empty else []
    
    # Client selection
    selected_client = st.selectbox("Select Client", [""] + clients)
    
    if selected_client:
        # Filter links for selected client
        client_links = links_df[links_df['client_name'] == selected_client]
        st.write(f"Links being tracked for {selected_client}:")
        st.dataframe(client_links)
    
    # Add new link form
    st.subheader("Add New Link")
    new_link_client = st.selectbox("Client", [""] + sorted(st.session_state.data['client_name'].unique()))
    new_link_url = st.text_input("Link to Track")
    
    if st.button("Submit Link"):
        if new_link_client and new_link_url:
            conn = get_db_connection()
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO niab.client_engaged_leads_link 
                    (client_name, link_to_track, still_tracking)
                    VALUES (%s, %s, TRUE)
                """, (new_link_client, new_link_url))
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
