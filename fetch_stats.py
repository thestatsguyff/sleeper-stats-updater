import os
import json
import gspread
import pandas as pd
import requests
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# --- CONFIGURATION ---
# The names of your Google Sheet and the specific worksheet
GOOGLE_SHEET_NAME = 'NFL Player Stats'
WORKSHEET_NAME = 'Sheet1' 

# Your Google credentials JSON content stored as a secret.
GOOGLE_CREDENTIALS_JSON = os.environ.get('GOOGLE_CREDENTIALS_JSON')

# --- DYNAMIC WEEK AND YEAR ---
def get_current_nfl_week():
    """Calculates the current NFL week based on the current date."""
    now = datetime.utcnow()
    year = now.year
    
    # NFL season typically starts on the Thursday of the first full week of September.
    # We'll set our "week 1" cutoff as the first Tuesday of September.
    first_tuesday_sept = None
    for day in range(1, 8):
        d = datetime(year, 9, day)
        if d.weekday() == 1: # Tuesday (0=Monday, 1=Tuesday, ...)
            first_tuesday_sept = d
            break
    
    # If it's before the season start, we are in the offseason.
    # The script will fetch the last week of the previous season.
    if now < first_tuesday_sept:
        return str(year - 1), '18'

    # Calculate how many full weeks have passed since the season started.
    week = ((now - first_tuesday_sept).days // 7) + 1
    
    # Return the current year and the calculated week, capped at 18 for the regular season.
    return str(year), str(week if week <= 18 else 18)

YEAR, WEEK = get_current_nfl_week()
print(f"Detected current season: {YEAR}, Week: {WEEK}")


# --- 1. FETCH AND PROCESS DATA FROM SLEEPER API ---
def fetch_and_process_data(year, week):
    """Fetches and processes player stats from Sleeper's free API."""
    base_url = "https://api.sleeper.app/v1"
    
    print("Fetching master player list from Sleeper...")
    players_response = requests.get(f"{base_url}/players/nfl")
    if players_response.status_code != 200:
        print("Error fetching player list.")
        return None
    players_data = players_response.json()
    
    player_map = {}
    for player_id, player_info in players_data.items():
        player_map[player_id] = {
            'name': f"{player_info.get('first_name', '')} {player_info.get('last_name', '')}".strip(),
            'team': player_info.get('team', 'N/A'),
            'position': player_info.get('position', 'N/A')
        }

    print(f"Fetching weekly stats for Week {week}, Season {year}...")
    stats_response = requests.get(f"{base_url}/stats/nfl/regular/{year}/{week}")
    if stats_response.status_code != 200:
        print(f"Error fetching weekly stats. Status: {stats_response.status_code}")
        return None
    weekly_stats_data = stats_response.json()

    all_player_stats = []
    for player_id, stats in weekly_stats_data.items():
        player_info = player_map.get(player_id)
        if not player_info:
            continue

        record = {
            'Week': week,
            'PlayerName': player_info['name'],
            'Team': player_info['team'],
            'Position': player_info['position'],
            'Receptions': stats.get('rec', 0),
            'ReceivingYards': stats.get('rec_yd', 0),
            'ReceivingTDs': stats.get('rec_td', 0),
            'RushingYards': stats.get('rush_yd', 0),
            'RushingTDs': stats.get('rush_td', 0),
            'PassingAttempts': stats.get('pass_att', 0),
            'Targets': stats.get('rec_tgt', 0),
            'SnapCounts': stats.get('off_snp', 0)
        }
        all_player_stats.append(record)
        
    print(f"Processed {len(all_player_stats)} total player records.")
    return all_player_stats

# --- 2. WRITE DATA TO GOOGLE SHEETS ---
def update_google_sheet(data_df, week_to_update):
    """Writes the data to the Google Sheet, deleting old data for the week first."""
    print("Connecting to Google Sheets...")
    
    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, 
            ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    except:
        print("Could not load Google credentials from environment variable.")
        print("Falling back to local 'your-credentials-file.json' for testing.")
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name('your-credentials-file.json', 
                ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
        except FileNotFoundError:
            print("Local credentials file not found. Please set up the GitHub Secret or local file.")
            return

    client = gspread.authorize(creds)
    sheet = client.open(GOOGLE_SHEET_NAME).worksheet(WORKSHEET_NAME)
    
    # --- Delete Old Data for the Week ---
    print(f"Checking for and deleting any existing data for Week {week_to_update}...")
    cell_list = sheet.findall(week_to_update, in_column=1)
    if cell_list:
        rows_to_delete = [cell.row for cell in cell_list]
        # Reverse sort to delete from the bottom up, avoiding index shifts
        for row_index in sorted(rows_to_delete, reverse=True):
            sheet.delete_rows(row_index)
        print(f"Deleted {len(rows_to_delete)} old rows for Week {week_to_update}.")

    # --- Append New Data ---
    if data_df.empty:
        print("No new data to write to the sheet.")
        return

    print(f"Writing {len(data_df)} new rows for Week {week_to_update}...")
    sheet.append_rows(data_df.values.tolist(), value_input_option='USER_ENTERED')
    
    print("Google Sheet has been updated successfully!")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    processed_records = fetch_and_process_data(YEAR, WEEK)
    
    if processed_records:
        df = pd.DataFrame(processed_records)
        
        if not df.empty:
            # Filter out players with zero snaps to keep the sheet clean
            df = df[df['SnapCounts'] > 0]

            df = df[[
                'Week', 'PlayerName', 'Team', 'Position', 'Receptions', 
                'ReceivingYards', 'ReceivingTDs', 'RushingYards', 'RushingTDs',
                'PassingAttempts', 'Targets', 'SnapCounts'
            ]]
            
            print("\n--- Sample of Processed Data ---")
            print(df.head())
            update_google_sheet(df, WEEK)
        else:
            print("Processed data is empty, nothing to update.")

    print("\nScript finished.")
```

***

### **Part 3: Automating with GitHub (The "Scheduler")**

This final part will put your script on the internet and tell it to run automatically every week.

1.  **Create a GitHub Account & Repository:**
    * If you don't have one, sign up at [GitHub.com](https://github.com).
    * Create a new repository. Name it `nfl-stats-automation`. Make it **Private**.

2.  **Upload Your Script:**
    * In your new repository, click **"Add file"** -> **"Upload files"**.
    * Upload the Python script file you just saved.

3.  **Store Your Google Credential Securely:**
    * In your repository, go to **`Settings`** > **`Secrets and variables`** > **`Actions`**.
    * Click the **`New repository secret`** button.
    * For the **Name**, type exactly: `GOOGLE_CREDENTIALS_JSON`
    * For the **Secret**, open the `your-credentials-file.json` file on your computer, select and copy *everything* inside it, and paste it into this box.
    * Click **"Add secret"**.

4.  **Create the Automation Schedule:**
    * Go to the **`Actions`** tab in your repository.
    * Click the link that says **`set up a workflow yourself`**.
    * It will open an editor for a file named `main.yml`. Delete all the text inside it and paste the following:

    ```yaml
    name: Weekly NFL Stat Fetch

    on:
      workflow_dispatch: # Allows you to run it manually from the Actions tab
      schedule:
        # This is a "cron schedule" that runs at 8:00 AM UTC every Tuesday.
        # This is early morning in the US, after Monday Night Football is over.
        - cron: '0 8 * * 2'

    jobs:
      fetch-and-update:
        runs-on: ubuntu-latest
        steps:
          - name: Checkout repository code
            uses: actions/checkout@v4

          - name: Set up Python
            uses: actions/setup-python@v5
            with:
              python-version: '3.9'

          - name: Install necessary Python libraries
            run: |
              python -m pip install --upgrade pip
              pip install pandas requests gspread oauth2client

          - name: Run the stats fetcher script
            env:
              GOOGLE_CREDENTIALS_JSON: ${{ secrets.GOOGLE_CREDENTIALS_JSON }}
            run: python fetch_stats.py
    

