import requests
import pandas as pd
from datetime import datetime, timedelta
import os

class retrieveFeedData:
    
    """
    The class fetches data from the emoncms API and returns the CSV files for each date range and the final dataframe and CSV file.

    Parameters
    ----------
    id : dict
        Dictionary containing feed information. Example: {'id_number': 508154, 'id_name': 'heatmeter_power', 'id_unit': 'W'}
    start : str
        Start time of the data in the format 'YYYY-MM-DD HH:MM:SS'.
    end : str
        End time of the data in the format 'YYYY-MM-DD HH:MM:SS'.
    apikey : str
        API key for accessing the data.
    interval : int, optional
        Interval of the data in seconds.
    average : str, optional
        Average of the data, default is '0' for no average.
    timeformat : str, optional
        Time format of the data, default is 'excel'. Options: 'excel', 'unix', 'iso8601'.
    skipmissing : str, optional
        Skip missing data, default is 'no'.
    limitinterval : str, optional
        Limit interval of the data, default is 'no'.
    delta : str, optional
        Delta of the data, default is 'no'.
    base_url : str, optional
        Base URL for the API endpoint.

    Methods
    -------
    fetch_data(start_date, end_date)
        Fetches data from the API for the given date range.
    json_to_csv(json_data, csv_file_path)
        Converts JSON data to a CSV file and returns a dataframe.
    retrieve_data()
        Retrieves data for the entire date range, saves CSV files, and returns the final concatenated dataframe.

    Returns
    -------
    pandas.DataFrame
        The final concatenated dataframe of the feed.
    """
    
    def __init__(self, id, start, end, apikey, interval=10, average='0', timeformat='excel', skipmissing='0', limitinterval='0', delta='0', base_url='https://emoncms.org/feed/data.json?'):
        self.id = id['id_number']
        self.name = id['id_name']
        self.unit = id['id_unit']
        self.start = start # datetime.strptime(start, '%d-%m-%Y %H:%M:%S')
        self.end = end # datetime.strptime(end, '%d-%m-%Y %H:%M:%S')
        self.interval = interval
        self.average = average
        self.timeformat = timeformat
        self.skipmissing = skipmissing
        self.limitinterval = limitinterval
        self.delta = delta
        self.apikey = apikey
        self.base_url = base_url
        
    def fetch_data(self, start_date, end_date):
        
        url = f"{self.base_url}id={self.id}&start={start_date.strftime('%d-%m-%Y')}%20{start_date.strftime('%H:%M:%S')}&end={end_date.strftime('%d-%m-%Y')}%20{end_date.strftime('%H:%M:%S')}&interval={self.interval}&average={self.average}&timeformat={self.timeformat}&skipmissing={self.skipmissing}&limitinterval={self.limitinterval}&delta={self.delta}&apikey={self.apikey}"
        
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
            
    def json_to_csv(self, json_data, csv_file_path):
    
        """The function converts json data to csv file

        Args:
            json_data: json data of the feed
            csv_file_path: path of the csv file
        Returns:
            df: dataframe of the feed
        """
    
        df = pd.DataFrame(json_data)
        
        if self.unit == '':
            unit = ''
        else:
            unit = ' ' + '(' + self.unit + ')'
        
        df.columns = ['Date', self.name + unit]
        df.set_index('Date', inplace=True)
        # df.to_csv(csv_file_path, index=True)
        return df
    
    def retrieve_data(self):
        
        # Get the current working directory
        cwd = os.getcwd()

        # Define the folder name
        folder_name = 'dataset'

        # Check if the folder exists
        if not os.path.exists(os.path.join(cwd, folder_name)):
            # Create the folder
            os.makedirs(os.path.join(cwd, folder_name))
            print(f"Folder '{folder_name}' created.")
        else:
            pass
        
        step = timedelta(days=5)

        all_dataframes = []

        current_date = self.start
        while current_date <= self.end:

            next_date = current_date + step - timedelta(seconds=self.interval)

            if next_date > self.end:
                next_date = self.end

                json_data = self.fetch_data(current_date, next_date)
                df = self.json_to_csv(json_data, f'./dataset/{self.name}_{current_date.strftime("%d_%m_%Y")}_{next_date.strftime("%d_%m_%Y")}.csv')
                all_dataframes.append(df)
                
                break

            else:
                json_data = self.fetch_data(current_date, next_date)
                df = self.json_to_csv(json_data, f'./dataset/{self.name}_{current_date.strftime("%d_%m_%Y")}_{next_date.strftime("%d_%m_%Y")}.csv')
                all_dataframes.append(df)

                current_date = next_date + timedelta(seconds=self.interval)


        # Concatenate all dataframes vertically
        final_dataframe = pd.concat(all_dataframes, ignore_index=False)

        final_dataframe.to_csv(f'./dataset/{self.name}_{self.start.strftime("%d_%m_%Y")}_{self.end.strftime("%d_%m_%Y")}.csv', index=True)
        
        return final_dataframe
    

class allFeeds(retrieveFeedData):
    
    """
    The allFeeds class fetches all feeds data from the emoncms API.

    Parameters
    ----------
    id : dict
        Dictionary containing feed information, e.g., {'id_number': 508154, 'id_name': 'heatmeter_power', 'id_unit': 'W'}.
    start : str
        Start time of the data in the format 'YYYY-MM-DD HH:MM:SS'.
    end : str
        End time of the data in the format 'YYYY-MM-DD HH:MM:SS'.
    apikey : str
        API key for accessing the data.
    feeds_url : str, optional
        URL to fetch the list of feeds, by default 'https://emoncms.org/feed/list.json?meta=1'.

    Methods
    -------
    retrieve_all_feeds()
        Retrieves all feeds data, concatenates them into a single DataFrame, and saves it as a CSV file.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the concatenated data of all feeds.
    """
    
    def __init__(self, id, start, end, apikey, feeds_url='https://emoncms.org/feed/list.json?meta=1'):
        super().__init__(id, start, end, apikey)
        self.feeds_url = feeds_url
    
    
    def retrieve_all_feeds(self):
        
        response = requests.get(f'{self.feeds_url}&apikey={self.apikey}')
        feeds = response.json()

        all_feeds_df = []
        # id = {}
        
        for feed in feeds:

            # id['id_number'] = int(feed['id'])
            # id['id_name'] = feed['name']
            # id['id_unit'] = feed['unit']
            
            self.id = int(feed['id'])
            self.name = feed['name']
            self.unit = feed['unit']

            all_feeds_df.append(self.retrieve_data())

        # Concatenate DataFrames horizontally based on the date index
        result = pd.concat(all_feeds_df, axis=1)

        # Saving the final dataframe to csv
        result.to_csv(f'./dataset/all_feeds_{self.start.strftime("%d_%m_%Y")}_{self.end.strftime("%d_%m_%Y")}.csv', index=True)
        
        return result