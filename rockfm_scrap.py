import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import json
import os

day_idx = 1

yesterday = datetime.today().date() - timedelta(days=day_idx)
#print(yesterday)
yesterday_weekday = yesterday.weekday()
#print(yesterday_weekday)

def scrape_rockfm_page(base_url='https://onlineradiobox.com/es/rockfm/playlist/'):
    # Set headers to mimic a real browser
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}

    songs = []
    today = datetime.today().date()
    
    for page_idx in [day_idx]:
        
        idx = '' if page_idx == 0 else str(page_idx)
        
        date = today - timedelta(days=page_idx)
        date = date.strftime("%Y-%m-%d")
     
        page_url = base_url + idx + '?cs=es.rockfm'
    
        # Fetch the content of the webpage
        response = requests.get(page_url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the table
            table = soup.find("table", class_="tablelist-schedule")

            # Extract rows from the table
            rows = table.find_all("tr")
 
            for row in rows:
                # Extract time
                time_cell = row.find("td", class_="tablelist-schedule__time")
                time_text = time_cell.find("span", class_="time--schedule").text.strip() if time_cell else "N/A"

                # Extract track info
                track_cell = row.find("td", class_="track_history_item")
                try:
                    track_text = track_cell.find("a").text.strip() if track_cell else ''
                except:
                    track_text = track_cell.text.strip() if track_cell else ''
                    
                artist = track_text.split(' - ')[0]
                song = track_text.split(' - ')[1]
                
                # List with one song's data
                song = [date, time_text, artist, song]
                songs.append(song)
                songs = songs[::-1]
        
        # Dataframe with all songs data
        songs_df = pd.DataFrame(songs, columns=["Date", "Time", "Artist", "Song"])
                      
    return songs_df # type: ignore


# URL to scrape
base_url = 'https://onlineradiobox.com/es/rockfm/playlist/'

# Scrape the songs data
songs_df = scrape_rockfm_page(base_url)

songs_df['Datetime'] = pd.to_datetime(songs_df['Date'] + ' ' + songs_df['Time'] + ':00')
songs_df = songs_df.sort_values(by='Datetime')

days_mapping = {
    0: 'monday',
    1: 'tuesday',
    2: 'wednesday',
    3: 'thursday',
    4: 'friday',
    5: 'saturday',
    6: 'sunday'
}

div_day_class = days_mapping[yesterday_weekday]

def scrape_rockfm_programs(page_url='https://www.rockfm.fm/programacion'):
    # Set headers to mimic a real browser
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
        
    # Fetch the content of the webpage
    response = requests.get(page_url, headers=headers)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
            
        # Find the div for yesterday
        day_div = soup.find("div", class_=div_day_class)
                  
    return songs_df

scrape_rockfm_programs('https://www.rockfm.fm/programacion')

# Make a GET request to the API endpoint you found in the Network tab
api_url = 'https://www.rockfm.fm/ply/prg/37' #?0.2481731647902638'
response = requests.get(api_url)

# The response is in JSON format
data = response.json()

# Prettyfy the JSON
pretty_json = json.dumps(data, indent=4)

weekday_formated = f'd{yesterday_weekday}'
yesterday_obj = datetime.strptime(str(yesterday), "%Y-%m-%d")
programs_lt = []

for item in data["prg"][weekday_formated]["es"]:

    from_time = int(item["from"])
    from_time_datetime = yesterday_obj  + timedelta(minutes=from_time)
    to_time = int(item["to"])
    to_time_datetime = yesterday_obj  + timedelta(minutes=to_time+1)
    title = item["title"]
    timetable = item["horario"]
    program = [from_time_datetime, to_time_datetime, title]
    programs_lt.append(program)

columns = ['From', 'To', 'Title']
programs_df = pd.DataFrame(programs_lt, columns=columns)
programs_df = programs_df.sort_values(by='From')

# Merge songs with programs based on closest prior start time
merged_df = pd.merge_asof(
    songs_df,
    programs_df,
    left_on='Datetime',
    right_on='From',
    direction='backward'
)

# Filter rows where song time is within the program's duration
merged_df = merged_df[merged_df['Datetime'] < merged_df['To']]

# Drop two columns
merged_df = merged_df.drop(columns=['From', 'To'])

file_path = 'songs.csv'

# Check if the file exists
if os.path.isfile(file_path):
    # Write only new rows to the CSV file
    merged_df.to_csv(file_path, mode='a', index=False, header=False)
    print("New rows appended to the CSV file.")
else:
    # If the file doesn't exist, write the new DataFrame with headers
    merged_df.to_csv(file_path, index=False)
    print("CSV file created with the new data.")