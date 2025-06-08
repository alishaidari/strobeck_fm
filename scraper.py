#courtesy goes to skatevideosite for coordinating all the skate data
import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import pandas as pd
import logging
import datetime
import pathlib
import re

_LAST_FM_BASE_URL = 'http://ws.audioscrobbler.com/2.0/'
_SKATEVIDEOSITE_URL = r'https://www.skatevideosite.com'
_STROBECK_FILMAKER_URL = _SKATEVIDEOSITE_URL + r'/filmmakers/william-strobeck-fat-bill'
_LOG_PATH = rf'logs/{datetime.date.today()}_run.log'
_TRACK_DAT_PATH = rf'data/{datetime.date.today()}_strobeck_track_data.csv'

class SkateScraper():
    def __init__(self):
        self.scrape_links = []
        self.track_dat_list = []
        self.prep_env()
        self.populate_scrape_links()
    
    def log_exceptions(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logging.exception(f"Exception in {func.__name__}")
                raise
        return wrapper

    @log_exceptions
    def prep_env(self):

        #load config with last_fm api key        
        try:
            with open('config.json') as f:
                self.config = json.load(f)
                self.api_key = self.config['LAST_FM_API_KEY']
        except Exception as e:
            print(f'Error: Could not load the config.json with api key\n{e}')
            self.config = None
            self.api_key = None

        #mkdir for logs and data
        try:
            log_parent = pathlib.Path('./logs')
            song_parent = pathlib.Path('./data')
            log_parent.mkdir(parents=True, exist_ok=True)
            song_parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f'Error: Coult not set up project directories\n{e}')

        logging.basicConfig(
            filemode='w',
            filename=_LOG_PATH,
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.INFO
        )

    @log_exceptions
    def populate_scrape_links(self):
        try:
            #request the main page
            main_page = requests.get(_STROBECK_FILMAKER_URL)
            main_page_soup = BeautifulSoup(main_page.text, features='html.parser')
            main_page_soup.prettify()

            vid_container = main_page_soup.find_all('div', class_='flex h-full flex-col')

            for vid in vid_container:
                vid_link_tag = vid.find('a',  class_='font-semibold text-primary underline')
                if not vid_link_tag:
                    logging.warning(f'Warning: Something went wrong in getting video link {vid.text}')
                    continue
                vid_name = vid_link_tag.text.strip()
                vid_link = _SKATEVIDEOSITE_URL + vid_link_tag['href']
                vid_year = re.sub(r'[()]', '', vid.find('span', class_='ml-2 text-base font-semibold').text.strip())
                self.scrape_links.append((vid_name, vid_year, vid_link))
                
        except Exception as e:
            print(f"Error: could not generate the links to scrape \n {e}")

    @log_exceptions
    def get_track_info(self, artist:str, track_name: str):
        try:
            params = {
                'method': 'track.getInfo',
                'autocorrect': 1,
                'api_key': self.api_key,
                'artist': artist,
                'track': track_name,
                'format': 'json'
            }

            response = requests.get(_LAST_FM_BASE_URL, params=params)

            if response.status_code == 200:
                data = response.json()
                if 'track' in data:
                    return data['track']
                else:
                    print('Error: track not found:', data.get('message', 'No info'))
                    return (artist, track_name)
            else:
                print('Error: request failed:', response.status_code)
                return (artist, track_name)
            
        except Exception as e:
            print(f"Error: unexpected error with getting track info \n {e}")
            return (artist, track_name)
        
    @log_exceptions
    def has_missing_track_info(self, track_info):
        required_keys = ['name', 'artist', 'album', 'img', 'duration', 'tags']
        for key in required_keys:
            value = track_info.get(key, None)
            if value in (None, '', [], {}, 0, '0'):
                print(f"Missing or empty: {key}")
                return True
        return False

    @log_exceptions
    def parse_track_info(self, track_info: dict):
        try:
            if isinstance(track_info, dict):
                track_name = track_info['name']
                track_artist = track_info['album']['artist']
                track_album = track_info['album']['title']
                track_img = track_info['album']['image'][0]['#text']
                track_tags = '||'.join([x['name'] for x in track_info['toptags']['tag']])
                track_duration = track_info['duration']
                track_dict = {'name':track_name, 'artist':track_artist, 'album':track_album, 'img':track_img, 'duration':track_duration, 'tags':track_tags}

                missing = self.has_missing_track_info(track_dict)
                track_dict.update({'missing':missing})
                return track_dict
            elif isinstance(track_info, tuple):
                return {'name':track_info[1], 'artist':track_info[0], 'missing':True} 
        except Exception as e:
            print(f'Error: could not parse the track info \n {e}')
            return {'name':track_info[1], 'artist':track_info[0], 'missing':True} 


    @log_exceptions
    def scrape_link(self, vid_dat:tuple):
        try:
            vid_name, vid_year, vid_link = vid_dat
            vid_dat_keys = ('video_name', 'video_year', 'video_link')
            vid_dict = dict(map(lambda kv: (str(kv[0]), kv[1]), zip(vid_dat_keys, vid_dat)))
            video_page = requests.get(vid_link)
            video_page_soup = BeautifulSoup(video_page.text, features='html.parser')
            video_page_soup.prettify()
            
            soundtrack_container = video_page_soup.find('div', class_='mb-2 w-full')
            
            track_divs = soundtrack_container.find_all('div', class_='italic')
            for label_div in track_divs:
                if label_div and label_div.text and label_div.text.strip():
                    label_div.text.strip().split(' - ')[0].strip()
                    artist_song = tuple([part.strip() for part in label_div.text.strip().split('-', 1)])
                    
                    if len(artist_song) != 2:
                        logging.warning(f"Warning: Something went wrong in unpacking artist_song: '{label_div.text.strip()}' at {vid_link}")
                        continue
                    fm_track_data = self.get_track_info(*artist_song)
                    
                    if fm_track_data:
                        parsed_data = self.parse_track_info(fm_track_data)
                        parsed_data.update(vid_dict)
                        self.track_dat_list.append(parsed_data)

            raw_dataframe = pd.DataFrame(self.track_dat_list)
            start_cols = ['video_name', 'video_year', 'video_link']
            remaining_cols = [col for col in raw_dataframe.columns if col not in start_cols]

            # reorder the df
            raw_dataframe = raw_dataframe[start_cols + remaining_cols]

            #sort df by year
            raw_dataframe = raw_dataframe.sort_values(by='video_year')
            
            #write the dataframe
            raw_dataframe.to_csv(_TRACK_DAT_PATH)

        except Exception as e:
            print(f'Error: something went wrong in scrape_link {vid_link} \n {e}')
            return None
    
    def scrape(self):
        self.populate_scrape_links()
        self.scrape_all_links()

    @log_exceptions
    def scrape_all_links(self):
        if self.scrape_links:
            [self.scrape_link(vid_dat) for vid_dat in self.scrape_links[:]]


def main():   
    scraper = SkateScraper()
    scraper.scrape()


if __name__ == '__main__':
    main()

    

    
