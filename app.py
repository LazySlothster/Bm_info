import streamlit as st
import pandas as pd
import requests
import asyncio
import disnake  # Using disnake, a modern fork of discord.py
import json
import os
import time
from datetime import datetime
import html  # <-- CRITICAL FIX 1: Import HTML library for escaping

# --- Page Configuration ---
st.set_page_config(
    page_title="User Dashboard",
    page_icon="✨",
    layout="wide",
)

# --- File Paths ---
# Get the absolute path of the directory where this script is.
CWD = os.path.dirname(__file__) 
CSV_FILE_PATH = os.path.join(CWD, "users.csv")
DISCORD_DATA_PATH = os.path.join(CWD, "discord_data.json")
COMBINED_DATA_PATH = os.path.join(CWD, "combined_data.json")


# --- Roblox API Constants ---
ROBLOX_AVATAR_SIZE = "150x150"
ROBLOX_AVATAR_PLACEHOLDER = "https://placehold.co/150x150/5865F2/FFFFFF?text=N/A"

# --- Helper Functions for Roblox API ---

def get_roblox_ids(usernames):
    """Fetches Roblox User IDs from a list of usernames."""
    url = "https://users.roblox.com/v1/usernames/users"
    # Ensure usernames is a list of strings
    usernames = [str(u) for u in usernames if u and pd.notna(u)]
    if not usernames:
        return {}
        
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
    # Filter out any None, 0, or pd.NA IDs
    valid_ids = [str(uid) for uid in user_ids if uid and pd.notna(uid)]
    if not valid_ids:
        return {}
    
    st.write(f"Fetching creation dates for {len(valid_ids)} users...")
    for user_id in valid_ids:
        url = f"https://users.roblox.com/v1/users/{user_id}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                dates_map[int(user_id)] = response.json().get("created")
            else:
                dates_map[int(user_id)] = None
        except requests.RequestException:
            dates_map[int(user_id)] = None
        time.sleep(0.05) # Add a small delay to avoid rate limiting
    return dates_map

def get_roblox_avatar_urls(user_ids):
    """Fetches Roblox avatar headshots in batches of 100."""
    # Filter out any None, 0, or pd.NA IDs
    valid_ids = [str(uid) for uid in user_ids if uid and pd.notna(uid)]
    if not valid_ids:
        return {}
    
    st.write(f"Fetching avatars for {len(valid_ids)} users...")
    avatar_map = {}
    
    # --- FIX: Batch the requests in chunks of 100 ---
    for i in range(0, len(valid_ids), 100):
        batch_ids = valid_ids[i:i+100]
        st.write(f"  > Fetching avatar batch {i//100 + 1}...")
        
        url = "https://thumbnails.roblox.com/v1/users/avatar-headshot"
        params = {
            "userIds": ",".join(batch_ids),
            "size": ROBLOX_AVATAR_SIZE,
            "format": "Png",
            "isCircular": False
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json().get("data", [])
            # Create a map of {userId (int): imageUrl}
            for avatar in data:
                avatar_map[avatar["targetId"]] = avatar["imageUrl"]
        except requests.RequestException as e:
            st.error(f"Error fetching Roblox avatar batch: {e}")
        # Add a small delay between batches
        time.sleep(0.1)
            
    return avatar_map

# --- Helper Function for Discord Bot ---

async def fetch_discord_data(guild_id, bot_token, target_ids):
    """Connects to Discord and fetches data for specific user IDs."""
    
    intents = disnake.Intents.default()
    intents.members = True # MUST have this intent enabled
    
    client = disnake.Client(intents=intents)
    discord_data = {}

    @client.event
    async def on_ready():
        st.write(f"Bot connected as {client.user}...")
        try:
            guild = client.get_guild(int(guild_id))
            if not guild:
                st.error(f"Cannot find guild with ID: {guild_id}. Check your GUILD_ID secret.")
                await client.close()
                return

            st.write(f"Found guild: {guild.name}. Fetching {len(target_ids)} members...")
            fetch_count = 0
            for user_id in target_ids:
                if not user_id or not str(user_id).isdigit():
                    st.warning(f"Skipping invalid Discord ID in CSV: {user_id}")
                    continue
                try:
                    member = await guild.fetch_member(int(user_id))
                    if member:
                        # --- FIX: Get attributes directly from member object ---
                        # This fixes the "'Member' object has no attribute 'user'" bug
                        discord_data[user_id] = {
                            "username": member.name,
                            "displayName": member.display_name,
                            "createdAt": member.created_at.isoformat(),
                            "joinedAt": member.joined_at.isoformat()
                        }
                        fetch_count += 1
                except disnake.NotFound:
                    st.warning(f"Could not find member with ID: {user_id}. They may have left the server.")
                    discord_data[user_id] = {"error": "User not found"}
                except disnake.HTTPException as e:
                    st.error(f"HTTP error fetching member {user_id}: {e}")
                except Exception as e:
                    st.error(f"Unknown error fetching member {user_id}: {e}")
            
            st.write(f"Successfully fetched {fetch_count}/{len(target_ids)} members.")
            
            with open(DISCORD_DATA_PATH, "w") as f:
                json.dump(discord_data, f, indent=2)
            st.success(f"Saved Discord data to {DISCORD_DATA_PATH}")

        except Exception as e:
            st.error("An error occurred during bot operation:")
            st.exception(e) # Print the full error
        finally:
            await client.close()
            st.write("Bot has disconnected.")

    try:
        # This will block until the bot logs in, runs on_ready, and disconnects.
        await client.start(bot_token)
    except disnake.LoginFailure:
        st.error("Failed to log in to Discord. Is your DISCORD_BOT_TOKEN secret correct?")
    except Exception as e:
        st.error("An error occurred while starting the bot:")
        st.exception(e)
        
    return discord_data

# --- Main Data Refresh Function ---

def refresh_all_data():
    """
    This is the main function. It reads the CSV, runs the bot,
    fetches Roblox data, and saves a combined JSON file.
    """
    
    bot_token = st.secrets.get("DISCORD_BOT_TOKEN")
    guild_id = st.secrets.get("GUILD_ID")

    if not bot_token or not guild_id:
        st.error("DISCORD_BOT_TOKEN or GUILD_ID is not set. Please add them to your Streamlit secrets.")
        return

    progress_bar = st.progress(0, "Starting data refresh...")
    
    try:
        # 1. Read base data from CSV
        st.info("Step 1/6: Reading users.csv...")
        if not os.path.exists(CSV_FILE_PATH):
            st.error(f"users.csv not found at {CSV_FILE_PATH}. Please upload it to your GitHub repository.")
            return
            
        df_csv = pd.read_csv(CSV_FILE_PATH, dtype={"DiscordID": str})
        # Clean up IDs
        df_csv["DiscordID"] = df_csv["DiscordID"].str.strip()
        target_discord_ids = df_csv["DiscordID"].dropna().unique().tolist()
        
        # 2. Run Discord Bot Logic
        st.info("Step 2/6: Fetching Discord data... (This may take a minute)")
        progress_bar.progress(25, "Connecting to Discord...")
        # Run the async bot function
        asyncio.run(fetch_discord_data(guild_id, bot_token, target_discord_ids))
        
        if os.path.exists(DISCORD_DATA_PATH):
            with open(DISCORD_DATA_PATH, "r") as f:
                discord_data_map = json.load(f)
        else:
            st.error("Discord data file was not created. Bot run may have failed. Stopping refresh.")
            return

        # 3. Fetch Roblox IDs
        st.info("Step 3/6: Fetching Roblox data...")
        progress_bar.progress(50, "Fetching Roblox IDs...")
        
        roblox_usernames = df_csv["RobloxUsername"].dropna().unique().tolist()
        if not roblox_usernames:
            st.warning("No Roblox usernames found in users.csv.")
            roblox_id_map = {}
        else:
            st.info(f"Fetching Roblox IDs for {len(roblox_usernames)} unique usernames...")
            roblox_id_map = get_roblox_ids(roblox_usernames)
            st.write(f"Found {len(roblox_id_map)} matching Roblox IDs.")
            if len(roblox_id_map) == 0:
                st.warning("Could not find any Roblox IDs. Usernames may be incorrect or the Roblox API is down.")

        # Map the found IDs back to the DataFrame
        df_csv["RobloxID"] = df_csv["RobloxUsername"].str.lower().map(roblox_id_map).astype('Int64') # Use nullable int
        
        # 4. Fetch Roblox Avatars & Creation Dates
        all_roblox_ids = df_csv["RobloxID"].dropna().unique().tolist()
        st.info(f"Step 4/6: Fetching avatars and creation dates for {len(all_roblox_ids)} valid Roblox IDs...")
        progress_bar.progress(75, "Fetching Roblox profiles...")
        
        avatar_url_map = get_roblox_avatar_urls(all_roblox_ids)
        creation_date_map = get_roblox_creation_dates(all_roblox_ids)
        st.write(f"Found {len(avatar_url_map)} avatars and {len(creation_date_map)} creation dates.")
        
        # 5. Combine all data
        st.info("Step 5/6: Combining all data...")
        progress_bar.progress(90, "Combining all data...")
        combined_data = []
        for _, row in df_csv.iterrows():
            discord_id = str(row["DiscordID"])
            roblox_id = row["RobloxID"] if pd.notna(row["RobloxID"]) else None
            
            discord_info = discord_data_map.get(discord_id, {})
            
            # --- ROBUST FALLBACK LOGIC ---
            raw_display_name = discord_info.get("displayName")
            
            # --- NEW LOGIC: Parse the display name ---
            if raw_display_name and "・" in raw_display_name:
                parts = raw_display_name.split("・", 1) # Split only once
                if len(parts) > 1:
                    discord_display = parts[1].strip() # Get the part after '・' and remove whitespace
                else:
                    # This case handles if '・' is the last character
                    discord_display = raw_display_name
            else:
                # Fallback if '・' is not present or raw_display_name is None
                discord_display = raw_display_name or discord_info.get("username") or row["DiscordUsername"] or "N/A"
            # --- END NEW LOGIC ---

            discord_user = discord_info.get("username") or row["DiscordUsername"] or "N/A"
            roblox_user = row["RobloxUsername"] or "N/A"
            
            # Get avatar URL (int key)
            avatar_url = avatar_url_map.get(roblox_id) if roblox_id else ROBLOX_AVATAR_PLACEHOLDER
            
            # Get creation date (int key)
            roblox_date = creation_date_map.get(roblox_id) if roblox_id else None
            
            combined_data.append({
                "discordUsername": discord_user,
                "discordDisplayName": discord_display, # This is now the parsed name
                "discordId": discord_id,
                "discordJoinDate": discord_info.get("joinedAt"),
                "discordCreationDate": discord_info.get("createdAt"),
                "robloxUsername": roblox_user,
                "robloxId": roblox_id or "N/A",
                "robloxCreationDate": roblox_date,
                "robloxAvatarUrl": avatar_url or ROBLOX_AVATAR_PLACEHOLDER,
            })

        # 6. Save combined data to cache file
        st.info(f"Step 6/6: Saving {len(combined_data)} records to cache...")
        with open(COMBINED_DATA_PATH, "w") as f:
            json.dump(combined_data, f, indent=2)
            
        progress_bar.progress(100, "Data refresh complete!")
        st.success("All user data has been refreshed and cached.")
        # Wait 2 seconds and rerun to show the new data
        time.sleep(2)
        st.rerun()
        
    except Exception as e:
        st.error("A critical error occurred during the data refresh:")
        st.exception(e) # This will print the full error!
        progress_bar.empty()

# --- Helper to load cached data ---

def load_cached_data():
    """Loads the combined_data.json file."""
    if not os.path.exists(COMBINED_DATA_PATH):
        return None
    try:
        with open(COMBINED_DATA_PATH, "r") as f:
            return json.load(f)
    except json.JSONDecodeError:
        st.error(f"Could not read cached data file ({COMBINED_DATA_PATH}). It may be corrupted. Try refreshing.")
        return None
    except Exception as e:
        st.error(f"Error loading cache: {e}")
        return None

# --- Main App UI ---

st.title("✨ Verified User Dashboard")
st.caption("Combined Discord & Roblox account data.")

# --- Admin Sidebar ---
admin_password = st.secrets.get("ADMIN_PASSWORD", "admin") # Default to 'admin' if no secret is set

with st.sidebar:
    st.header("Admin Panel")
    password_input = st.text_input("Enter Admin Password", type="password")
    
    if not password_input:
        st.info("Enter the admin password to enable data refresh.")
    elif password_input == admin_password:
        st.success("Admin access granted.")
        if st.button("Refresh All User Data", type="primary", help="This will re-fetch all data from Discord and Roblox"):
            with st.spinner("Running full data refresh..."):
                refresh_all_data()
    else:
        st.error("Incorrect password.")
    
    st.markdown("---")
    st.caption("Data is cached in `combined_data.json`. Refreshing re-builds this file.")


# --- Main Page Content ---
user_data = load_cached_data()

if not user_data:
    st.info("No cached data found. Please ask an admin to log in and refresh the data.")
else:
    search_query = st.text_input("Search by name...", "", placeholder="Search Discord or Roblox username...")
    
    # Filter data based on search
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
    
    # --- Display User Cards ---
    num_columns = 4
    
    # --- CRITICAL FIX 2: "Invalid Date" Fix ---
    def format_date(date_str):
        """Helper to format ISO date strings to be pretty."""
        if not date_str:
            return "N/A"
        try:
            # Split at the 'T' or ' ' to get just the date part
            date_part = date_str.split('T')[0].split(' ')[0]
            # Parse just the YYYY-MM-DD part
            dt = datetime.strptime(date_part, "%Y-%m-%d")
            return dt.strftime("%b %d, %Y")
        except (ValueError, TypeError, AttributeError):
            return "Invalid Date" # Return this if parsing fails

    # Create a grid of cards
    for i in range(0, len(filtered_data), num_columns):
        cols = st.columns(num_columns)
        for j in range(num_columns):
            if i + j < len(filtered_data):
                user = filtered_data[i + j]
                
                with cols[j].container(border=True):
                    
                    # --- HTML CARD ---
                    avatar_url = user.get('robloxAvatarUrl')
                    
                    # --- CRITICAL FIX 3: HTML Escaping ---
                    # This sanitizes names and prevents the UI from breaking
                    # This fixes the "black bar" bug.
                    roblox_name = html.escape(str(user.get('robloxUsername', "N/A")))
                    discord_name = html.escape(str(user.get('discordDisplayName', "N/A"))) # This will be the parsed name

                    html_card = f"""
                    <div style="display: flex; flex-direction: column; align-items: center; text-align: center; padding: 10px; min-height: 200px;">
                        <img src="{avatar_url}" 
                             style="width: 100px; height: 100px; border-radius: 50%; object-fit: cover; border: 2px solid #5865F2;">
                        
                        <h3 style="color: white; margin-top: 15px; margin-bottom: 0px; font-weight: bold; font-size: 1.1em; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; width: 100%;"
                            title="{roblox_name}">
                            {roblox_name}
                        </h3>
                        <p style="color: #9CA3AF; margin-top: 5px; font-size: 0.9em; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; width: 100%;"
                           title="{discord_name}">
                            {discord_name}
                        </p>
                    </div>
                    """
                    st.markdown(html_card, unsafe_allow_html=True)
                    
                    with st.expander("View Details"):
                        st.markdown(f"**Discord Username:** `{html.escape(str(user.get('discordUsername', 'N/A')))}`")
                        st.markdown(f"**Discord ID:** `{user.get('discordId', 'N/A')}`")
                        st.markdown(f"**Roblox ID:** `{user.get('robloxId', 'N/A')}`")
                        st.markdown("---")
                        # Display the three key dates
                        st.markdown(f"**Server Join Date:** {format_date(user.get('discordJoinDate'))}")
                        st.markdown(f"**Discord Acct. Creation:** {format_date(user.get('discordCreationDate'))}")
                        st.markdown(f"**Roblox Acct. Creation:** {format_date(user.get('robloxCreationDate'))}")

