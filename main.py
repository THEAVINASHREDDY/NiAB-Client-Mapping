import pandas as pd
import streamlit as st
import requests
import datetime

# CSV file path
file_path = 'publicationClientMappingFinal.csv'

# Load data and filter out removed entries
def load_data():
    df = pd.read_csv(file_path)
    return df[df['Remove'] != 'Yes'].drop(columns=['Remove'], errors='ignore')  # Hide 'Remove' column in UI

# Initialize session state for data persistence
if "data" not in st.session_state:
    st.session_state.data = load_data()

# Set the title of the app
st.title('Live Publications')

# Display only active publications
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
        pub_df = pd.DataFrame([{"publication_name": pub["name"], "publication_id": pub["id"]} for pub in all_publications])
        st.write(pub_df)


# CRUD Operations
st.sidebar.header("Manage Publications")

# Select a row uniquely based on both publication_name and publication_id
selected_pub = st.sidebar.selectbox(
    "Select a publication", 
    st.session_state.data[['publication_name', 'publication_ids']].apply(lambda x: f"{x[0]} ({x[1]})", axis=1), 
    index=None
)

if selected_pub:
    # Extract publication_name and publication_id separately
    pub_name, pub_id = selected_pub.rsplit(" (", 1)
    pub_id = pub_id.rstrip(")")

    selected_entry = st.session_state.data[
        (st.session_state.data['publication_name'] == pub_name) & 
        (st.session_state.data['publication_ids'] == pub_id)
    ]
    
    if selected_entry.empty:
        st.sidebar.error("Selected publication not found in the CSV file")
    else:
        client_name = st.sidebar.text_input("Client Name", selected_entry['client_name'].values[0])
        publication_name = st.sidebar.text_input("Publication Name", selected_entry['publication_name'].values[0])
        publication_id = st.sidebar.text_input("Publication ID", selected_entry['publication_ids'].values[0])

        # Update entry
        if st.sidebar.button("Update"):
            df = pd.read_csv(file_path)
            df.loc[
                (df['publication_name'] == pub_name) & 
                (df['publication_ids'] == pub_id), 
                ['client_name', 'publication_name', 'publication_ids', 'last_modified']
            ] = [client_name, publication_name, publication_id, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
            df.to_csv(file_path, index=False)
            st.session_state.data = load_data()  # Reload data to reflect changes
            st.experimental_rerun()

        # Mark as removed instead of deleting
        if st.sidebar.button("Delete"):
            df = pd.read_csv(file_path)
            df.loc[
                (df['publication_name'] == pub_name) & 
                (df['publication_ids'] == pub_id), 
                ['Remove', 'last_modified']
            ] = ['Yes', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]  # Mark as removed
            df.to_csv(file_path, index=False)
            st.session_state.data = load_data()  # Reload data to reflect changes
            st.experimental_rerun()

st.sidebar.markdown("---")  # Separator

# Add new publication
st.sidebar.subheader("Add New Publication")
new_client = st.sidebar.text_input("Client Name")
new_publication = st.sidebar.text_input("Publication Name")
new_pub_id = st.sidebar.text_input("Publication ID")

if st.sidebar.button("Add"):
    if new_client and new_publication and new_pub_id:
        df = pd.read_csv(file_path)

        # Check if publication name already present
        existing_entry = df[df['publication_name'] == new_publication]
        if not existing_entry.empty:
            df.loc[df['publication_name'] == new_publication, 
                   ['client_name', 'publication_ids', 'Remove', 'last_modified']] = [new_client, new_pub_id, 'No', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
        else:
            new_entry = pd.DataFrame([[new_client, new_publication, new_pub_id, 'No', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]], 
                                     columns=['client_name', 'publication_name', 'publication_ids', 'Remove', 'last_modified'])
            df = pd.concat([df, new_entry], ignore_index=True)
        
        df.to_csv(file_path, index=False)
        st.session_state.data = load_data()  # Reload data to reflect changes
        st.experimental_rerun()
    else:
        st.sidebar.warning("Please fill all fields!")
