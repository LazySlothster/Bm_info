import streamlit as st
import pandas as pd
import requests
import asyncio
import disnake  # Using disnake, a modern fork of discord.py
import json
import os
from pathlib importPath

# --- Page Configuration ---
st.set_page_config(
    page_title="User Dashboard",
    page_icon="✨",
    layout="wide",
)

# --- File Paths ---
CSV_FILE_PATH = Path("users.csv")
DISCORD_DATA_PATH = Path("discord_data.json")
COMBINED_DATA_PATH = Path("combined_data.json")

# --- Roblox API Constants ---
ROBLOX_AVATAR_SIZE = "150x150"
ROBLOX_AVATAR_PLACEHOLDER = "https://placehold.co/150x150/5865F2/FFFFFF?text=N/A"

# --- Helper Functions for Roblox API ---

def get_roblox_ids(usernames):
    """Fetches Roblox User IDs from a list of usernames."""
    url = "https://users.roblox.com/v1/usernames/users"
    payload = {"usernames": usernames, "excludeBannedUsers": True}
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        data = response.json().get("data", [])
        # Create a map of {lowercase_username: id}
        return {user["requestedUsername"].lower(): user["id"] for user in data}
    except requests.RequestException as e:
        st.error(f"Error fetching Roblox IDs: {e}")
        return {}

def get_roblox_creation_dates(user_ids):
    """Fetches Roblox creation dates from a list of user IDs."""
    dates_map = {}
    for user_id in user_ids:
        if not user_id:
            continue
        url = f"https://users.roblox.com/v1/users/{user_id}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                dates_map[user_id] = response.json().get("created")
            else:
                dates_map[user_id] = None
        except requests.RequestException:
            dates_map[user_id] = None
    return dates_map

def get_roblox_avatar_urls(user_ids):
    """Fetches Roblox avatar headshots in a single batch."""
    if not user_ids:
        return {}
    
    url = "https://thumbnails.roblox.com/v1/users/avatar-headshot"
    params = {
        "userIds": ",".join(map(str, user_ids)),
        "size": ROBLOX_AVATAR_SIZE,
        "format": "Png",
        "isCircular": False
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json().get("data", [])
        # Create a map of {userId: imageUrl}
        return {avatar["targetId"]: avatar["imageUrl"] for avatar in data}
    except requests.RequestException as e:
        st.error(f"Error fetching Roblox avatars: {e}")
        return {}

# --- Helper Function for Discord Bot ---

async def fetch_discord_data(guild_id, bot_token, target_ids):
    """Connects to Discord and fetches data for specific user IDs."""
    
    # We need the 'members' intent to get 'joined_at'
    intents = disnake.Intents.default()
    intents.members = True 
    
    client = disnake.Client(intents=intents)
    discord_data = {}

    @client.event
    async def on_ready():
        st.write(f"Bot connected as {client.user}...")
        try:
            guild = client.get_guild(int(guild_id))
            if not guild:
                st.error(f"Cannot find guild with ID: {guild_id}")
                await client.close()
                return

            st.write(f"Found guild: {guild.name}. Fetching {len(target_ids)} members...")
            fetch_count = 0
            for user_id in target_ids:
                try:
                    member = await guild.fetch_member(int(user_id))
                    if member:
                        discord_data[user_id] = {
                            "username": member.user.name,
                            "displayName": member.display_name,
                            "createdAt": member.user.created_at.isoformat(),
                            "joinedAt": member.joined_at.isoformat()
                        }
                        fetch_count += 1
                except disnake.NotFound:
                    st.warning(f"Could not find member with ID: {user_id}. They may have left.")
                except disnake.HTTPException as e:
                    st.error(f"HTTP error fetching member {user_id}: {e}")
            
            st.write(f"Successfully fetched {fetch_count}/{len(target_ids)} members.")
            
            # Save data to file
            with open(DISCORD_DATA_PATH, "w") as f:
                json.dump(discord_data, f, indent=2)
            st.write(f"Saved Discord data to {DISCORD_DATA_PATH}")

        except Exception as e:
            st.error(f"An error occurred during bot operation: {e}")
        finally:
            await client.close()
            st.write("Bot has disconnected.")

    try:
        await client.start(bot_token)
    except disnake.LoginFailure:
        st.error("Failed to log in to Discord. Is your DISCORD_BOT_TOKEN secret correct?")
    except Exception as e:
        st.error(f"An error occurred while running the bot: {e}")
        
    return discord_data

# --- Main Data Refresh Function ---

def refresh_all_data():
    """
    This is the main function. It reads the CSV, runs the bot,
    fetches Roblox data, and saves a combined JSON file.
    """
    
    # 0. Get Secrets
    # On Streamlit Cloud, these come from st.secrets
    # Locally, they can come from environment variables
    bot_token = st.secrets.get("DISCORD_BOT_TOKEN", os.environ.get("DISCORD_BOT_TOKEN"))
    guild_id = st.secrets.get("GUILD_ID", os.environ.get("GUILD_ID"))

    if not bot_token or not guild_id:
        st.error("DISCORD_BOT_TOKEN or GUILD_ID is not set. Please add them to your Streamlit secrets.")
        return

    progress_bar = st.progress(0, "Starting data refresh...")
    
    try:
        # 1. Read base data from CSV
        progress_bar.progress(10, "Reading users.csv...")
        if not CSV_FILE_PATH.exists():
            st.error("users.csv not found. Please upload it to your GitHub repository.")
            return
            
        df_csv = pd.read_csv(CSV_FILE_PATH)
        df_csv["DiscordID"] = df_csv["DiscordID"].astype(str)
        target_discord_ids = df_csv["DiscordID"].unique().tolist()
        
        # 2. Run Discord Bot Logic
        progress_bar.progress(25, "Fetching Discord data... (This may take a minute)")
        # Run the async bot function
        asyncio.run(fetch_discord_data(guild_id, bot_token, target_discord_ids))
        
        # Load the data the bot just saved
        if DISCORD_DATA_PATH.exists():
            with open(DISCORD_DATA_PATH, "r") as f:
                discord_data_map = json.load(f)
        else:
            st.error("Discord data file was not created. Bot run may have failed.")
            discord_data_map = {}

        progress_bar.progress(50, "Fetching Roblox data...")
        
        # 3. Fetch Roblox IDs
        roblox_usernames = df_csv["RobloxUsername"].dropna().unique().tolist()
        roblox_id_map = get_roblox_ids(roblox_usernames)
        
        # Map IDs back to the dataframe
        df_csv["RobloxID"] = df_csv["RobloxUsername"].str.lower().map(roblox_id_map)
        
        # 4. Fetch Roblox Avatars & Creation Dates
        all_roblox_ids = df_csv["RobloxID"].dropna().unique().tolist()
        avatar_url_map = get_roblox_avatar_urls(all_roblox_ids)
        creation_date_map = get_roblox_creation_dates(all_roblox_ids)
        
        # 5. Combine all data
        progress_bar.progress(90, "Combining all data...")
        combined_data = []
        for _, row in df_csv.iterrows():
            discord_id = row["DiscordID"]
            roblox_id = row["RobloxID"]
            discord_info = discord_data_map.get(discord_id, {})
            
            combined_data.append({
                "discordUsername": discord_info.get("username", row["DiscordUsername"]),
                "discordDisplayName": discord_info.get("displayName"),
                "discordId": discord_id,
                "discordJoinDate": discord_info.get("joinedAt"),
                "discordCreationDate": discord_info.get("createdAt"),
                "robloxUsername": row["RobloxUsername"],
                "robloxId": roblox_id,
                "robloxCreationDate": creation_date_map.get(roblox_id),
                "robloxAvatarUrl": avatar_url_map.get(roblox_id, ROBLOX_AVATAR_PLACEHOLDER)
            })

        # 6. Save combined data to cache file
        with open(COMBINED_DATA_PATH, "w") as f:
            json.dump(combined_data, f, indent=2)
            
        progress_bar.progress(100, "Data refresh complete!")
        st.success("All user data has been refreshed and cached.")
        # Rerun to clear the progress bar and show the new data
        st.experimental_rerun()
        
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        progress_bar.empty()

# --- Helper to load cached data ---

def load_cached_data():
    if not COMBINED_DATA_PATH.exists():
        return None
    try:
        with open(COMBINED_DATA_PATH, "r") as f:
            return json.load(f)
    except json.JSONDecodeError:
        st.error("Could not read cached data file. It may be corrupted.")
        return None
    except Exception as e:
        st.error(f"Error loading cache: {e}")
        return None

# --- Main App UI ---

# Title
st.title("✨ Verified User Dashboard")
st.caption("Combined Discord & Roblox account data.")

# --- Admin Section (Password Protected) ---
# This is a simple way to hide the refresh button from normal users
admin_password = st.secrets.get("ADMIN_PASSWORD", "admin") # Default to 'admin' if no secret is set

with st.sidebar:
    st.header("Admin Panel")
    password_input = st.text_input("Enter Admin Password", type="password")
    if password_input == admin_password:
        st.success("Admin access granted.")
        if st.button("Refresh All User Data", type="primary"):
            with st.spinner("Running full data refresh..."):
                refresh_all_data()
    elif password_input:
        st.error("Incorrect password.")
    
    st.markdown("---")
    st.info("To refresh data, enter the admin password and click the button. This will run the bot and fetch all new data from Discord and Roblox.")

# --- Load Data ---
user_data = load_cached_data()

if not user_data:
    st.info("No cached data found. Please ask an admin to log in and refresh the data.")
else:
    # --- Search Bar ---
    search_query = st.text_input("Search by name...", "", placeholder="Search Discord or Roblox username...")
    
    # Filter data
    if search_query:
        query = search_query.lower()
        filtered_data = [
            user for user in user_data
            if query in str(user.get("discordUsername", "")).lower() or
               query in str(user.get("robloxUsername", "")).lower() or
               query in str(user.get("discordDisplayName", "")).lower()
        ]
    else:
        filtered_data = user_data

    if not filtered_data:
        st.warning(f"No users found matching '{search_query}'.")
    
    # --- User Grid ---
    # Define grid columns. Responsive layout.
    num_columns = 4 # 4 columns on large screens
    
    # Simple date formatter
    def format_date(date_str):
        if not date_str:
            return "N/A"
        try:
            return pd.to_datetime(date_str).strftime("%b %d, %Y")
        except:
            return "Invalid Date"

    # Create the grid
    for i in range(0, len(filtered_data), num_columns):
        cols = st.columns(num_columns)
        for j in range(num_columns):
            if i + j < len(filtered_data):
                user = filtered_data[i + j]
                
                # Draw the card in the column
                with cols[j].container():
                    # Use a custom div for the card background
                    st.markdown(
                        f"""
                        <div style="background-color: #1F2937; border-radius: 10px; padding: 20px; border: 1px solid #374151;">
                            <div style="display: flex; flex-direction: column; align-items: center; text-align: center;">
                                <img src="{user.get('robloxAvatarUrl', ROBLOX_AVATAR_PLACEHOLDER)}" 
                                     style="width: 100px; height: 100px; border-radius: 50%; object-fit: cover; border: 2px solid #5865F2;">
                                
                                <h3 style="color: white; margin-top: 15px; margin-bottom: 0px; font-weight: bold; font-size: 1.1em;">
                                    {user.get('robloxUsername', 'N/A')}
                                </h3>
                                <p style="color: #9CA3AF; margin-top: 5px; font-size: 0.9em;">
                                    {user.get('discordDisplayName', user.get('discordUsername', 'N/A'))}
                                </p>
                            </div>
                        </div>
                        """, 
                        unsafe_allow_html=True
                    )
                    
                    # Expander for "View Details"
                    with st.expander("View Details"):
                        st.markdown(f"**Discord Username:** `{user.get('discordUsername', 'N/A')}`")
                        st.markdown(f"**Discord ID:** `{user.get('discordId', 'N/A')}`")
                        st.markdown(f"**Roblox ID:** `{user.get('robloxId', 'N/A')}`")
                        st.markdown("---")
                        st.markdown(f"**Server Join Date:** {format_date(user.get('discordJoinDate'))}")
                        st.markdown(f"**Discord Acct. Creation:** {format_date(user.get('discordCreationDate'))}")
                        st.markdown(f"**Roblox Acct. Creation:** {format_date(user.get('robloxCreationDate'))}")
