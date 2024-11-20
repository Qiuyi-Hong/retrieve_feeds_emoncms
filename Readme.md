# Retrieve all the feeds data from [emoncms.org](https://github.com/emoncms/emoncms)

This repository is used to retrieve all the feeds data automatically from emoncms.

## How to use:

Download or clone the repository and open the `retrieveFeed.ipynb` notebook or click on [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/Qiuyi-Hong/retrieve_feeds_emoncms/blob/main/retrieveFeeds.ipynb) and uncomment the first block of code.

There are three neccessary parameters needed: **(start_date, end_date, your_apikey)**. 

Other parameters are optional and have default values:

interval: 10 (in seconds)

average: 0

timeformat: excel (other formats: unix, unixms, iso8601)

skipmissing: 0

limitinterval: 0

delta: 0

## The outputs:

The CSV file with the name starting with the name of the feed for each feed is created. In addition, the final CSV file, with the name starting with `all_feeds`, is created to store all the feeds data in your preferred time period and interval (resolution).