import argparse
from bs4 import BeautifulSoup
import requests
import pandas as pd
import os

# Base URL for the web pages to scrape
BASE_URL = "https://nextspaceflight.com"

def scrape_details(details_url):
    response = requests.get(details_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    launch_title = soup.find("h6", class_="rcorners status").text.strip()
    data_from_child_web = soup.find("div", class_="mdl-cell mdl-cell--6-col-desktop mdl-cell--4-col-tablet mdl-cell--4-col-phone").text.strip()
    return launch_title, data_from_child_web

def scrape_space_flights(num_entries=None):
    details_urls = []
    space_flight_list = []
    num_pages = num_entries // 10 + (num_entries % 10 > 0) if num_entries else 225  # Estimate the number of pages

    # Scrape the main list of space flights
    for page_num in range(1, num_pages + 1):
        url = f"{BASE_URL}/launches/past/?page={page_num}&search="
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        space_flights = soup.find_all('div', class_="mdl-cell mdl-cell--6-col")
        
        for space_flight in space_flights:
            if num_entries and len(space_flight_list) >= num_entries:
                break  # Stop if we've collected the desired number of entries
            details = space_flight.find("a", class_="mdc-button")
            if details:
                details_url = BASE_URL + details["href"]
                details_urls.append(details_url)
                space_flight_list.append(space_flight)

        if num_entries and len(space_flight_list) >= num_entries:
            break  # Stop if we've collected the desired number of entries

    # Scrape the details pages up to the number of required entries
    results = []
    for details_url in details_urls[:num_entries]:
        result, data_from_child_web = scrape_details(details_url)
        results.append((result, data_from_child_web))

    # Return both the list of space flights and the results
    flight_data = {
        'Details URL': details_urls[:num_entries],
        'Space Flight': [str(flight) for flight in space_flight_list[:num_entries]]
    }
    flight_df = pd.DataFrame(flight_data)
    return flight_df


def print_dataset(df):
    # Print the DataFrame
    print(df.to_string())

def save_dataset(df, path_to_dataset):
    # Save the DataFrame to the specified CSV file
    df.to_csv(path_to_dataset, index=False)
    print(f"Dataset saved to {path_to_dataset}")

def next_space_fligths():
    parser = argparse.ArgumentParser(description="Spaceflight Data Scraper")
    parser.add_argument('--scrape', type=int, help="Scrape only the first N entries of the dataset.")
    parser.add_argument('--save', type=str, help="Save the scraped data to a specified path.")
    args = parser.parse_args()

    # Scrape data
    space_flight_df = scrape_space_flights(args.scrape)

    # Save the scraped space flights data into the raw folder
    raw_data_path = os.path.join('..', 'data', 'raw', 'space_flights_raw.csv')
    space_flight_df.to_csv(raw_data_path, index=False)
    print(f"Space flights raw data saved to {raw_data_path}")

def nasa_missing_files():
    url_1 = "https://en.wikipedia.org/wiki/Timeline_of_space_exploration#1900%E2%80%931956"
    response_1 = requests.get(url_1)
    soup = BeautifulSoup(response_1.text, "lxml")
    tables = soup.find_all("table", class_="wikitable")
    table = tables[8]
    raw_html = str(table)
    raw_data_dir = '../data/raw'
    os.makedirs(raw_data_dir, exist_ok=True)
    file_path = os.path.join(raw_data_dir, "nasa_missing_files.csv")
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(raw_html)
    print(f"Raw HTML table saved as 'nasa_missing_files.csv' at {file_path}")

def nasa_budget_files():
    url_2 = "https://en.wikipedia.org/wiki/Budget_of_NASA#:~:text=Since%20its%20inception%20the%20United,in%20nominal%20dollars)%20on%20NASA."
    response_2 = requests.get(url_2)
    soup = BeautifulSoup(response_2.text, "lxml")
    tables = soup.find_all("table", class_="wikitable")
    table = tables[2]
    raw_html = str(table)
    raw_data_dir = '../data/raw'
    os.makedirs(raw_data_dir, exist_ok=True)
    file_path = os.path.join(raw_data_dir, "nasa_budget_files.csv")
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(raw_html)
    print(f"Raw HTML table saved as 'nasa_budget_files.csv' at {file_path}")

def isro_budget_files():
    url_2 = "https://en.wikipedia.org/wiki/ISRO#Statistics"
    response_2 = requests.get(url_2)
    soup = BeautifulSoup(response_2.text, "lxml")
    tables = soup.find_all("table", class_="wikitable")
    table = tables[11]
    raw_html = str(table)
    raw_data_dir = '../data/raw'
    os.makedirs(raw_data_dir, exist_ok=True)
    file_path = os.path.join(raw_data_dir, "isro_budget_files.csv")
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(raw_html)
    print(f"Raw HTML table saved as 'isro_budget_files.csv' at {file_path}")


def main():
    next_space_fligths()
    nasa_missing_files()
    nasa_budget_files()
    isro_budget_files()

if __name__ == "__main__":
    main()

