# Web Scraping and Predictions

## Table of Contents
* [General Info](#general-information)
* [Technologies Used](#technologies-used)
* [Setup](#setup)
* [Project Status](#project-status)


## General Information
- nba_odds_example.py
> This file is an example of how Prop Hunter scrapes its NBA data. It runs every 30 minutes throughout the day and upserts any price changes for each selection to my database meaning each selection gets their closing odds. In this script I'm scraping 3 bookmakers which are local to my side of the world so 2 are based in Australia and 1 in New Zealand. Not sure if you'll get Geo-blocked from their sites if you try to run the code so I've attached a sample Fanduel scraper for you as well.

- nba_results_example.py
> This script is run each evening after all the NBA games have finished for the day. It uses the NBA's internal API to get the results for each market. Once we have all the results, it grabs the odds data from our earlier scrape and merges that with the results. All today's data is saved to the database and this process just repeats automatically each day.

- fanduel_nba_example.py
> This will grab all NBA events from Fanduel that are currently open and return every market/selection and their odds. This is a very basic example of how most of my scrapers work, utilizing the bookmakers internal APIs and then iterating through their events and only returning the data I'm interested in. 


## Technologies Used
- Python


## Setup
1.) Create a virtual environment using the following command:

`python3 -m venv venv`

2.) Activate the new environment:

`source venv/bin/activate`
 

## Project Status
Project is: _in progress_ 


 
