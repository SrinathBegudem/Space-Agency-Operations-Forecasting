import argparse
from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
from io import StringIO

def load_and_process_space_flights(csv_path):
    df = pd.read_csv(csv_path)
    details_urls = df['Details URL'].tolist()
    space_flight_htmls = [BeautifulSoup(html, 'lxml') for html in df['Space Flight'].tolist()]
    return space_flight_htmls, details_urls

def clean_next_space_fligths():
    csv_path = '../data/raw/space_flights_raw.csv'
    space_flight_list, details_results = load_and_process_space_flights(csv_path)

    space_flight_name = []
    data_main_web = []
    status = []

    for i in space_flight_list:
        space_flight_name.append(i.find("h5").text.strip())
        data_main_web.append(i.find("div",class_="mdl-card__actions mdl-card--border").text.strip())

    cleaned_main_web_data = []

    for item in data_main_web:
        cleaned_item = ' '.join(item.split('\n')).strip()
        cleaned_item = item.replace('Details', '').replace('Watch', '').strip()
        cleaned_main_web_data.append(cleaned_item)

    date = []
    place = []

    for i in range(len(cleaned_main_web_data)):
        date.append(cleaned_main_web_data[i].split("\n")[0])
        place.append(cleaned_main_web_data[i].split(",",2)[2])


    # Initialize an empty DataFrame
    df = pd.DataFrame(columns=['date', 'place', 'company_name', 'budget', 'status', 'result', 'space_flight_name'])

    # Populate the DataFrame with data
    df['date'] = pd.Series(date)
    df['place'] = pd.Series(place)
    df['space_flight_name'] = pd.Series(space_flight_name)

    # Now, extract the other details from details_results and populate the DataFrame
    for index, detail in enumerate(details_results):
        result = detail[0]
        detail_text = detail[1]
        
        # Parse the details string to extract 'company_name', 'budget', and 'status'
        company_name = detail.split('\n')[0]  # Assuming the first line contains the company name
        budget_line = next((s for s in detail.split('\n') if 'Price:' in s), 'Price: $0.0 million')
        budget = budget_line.split('Price: ')[1].strip()
        status_line = next((s for s in detail.split('\n') if 'Status:' in s), 'Status: Unknown')
        status = status_line.split('Status: ')[1].strip()
        
        # Assign the extracted values to the DataFrame for the current index
        df.at[index, 'company_name'] = company_name
        df.at[index, 'budget'] = budget
        df.at[index, 'status'] = status
        df.at[index, 'result'] = result  # Assuming 'result' comes from 'details_results'

    # Save the DataFrame to the processed directory
    processed_data_path = '../data/processed/processed_space_flights_raw.csv'
    df.to_csv(processed_data_path, index=False)
    print(f"Cleaned data saved to {processed_data_path}")

def clean_nasa_missing_files():
    raw_data_dir = '../data/raw'
    file_path = os.path.join(raw_data_dir, "nasa_missing_files.csv")
    if not os.path.exists(file_path):
        print("File does not exist. Please check the path and filename.")
        return
    with open(file_path, 'r', encoding='utf-8') as file:
        raw_html = file.read()

    table = BeautifulSoup(raw_html, 'lxml')

    columns = [th.text.strip() for th in table.find_all("th")]
    data_rows = []

    rows = table.find_all("tr")[1:]  # Start from index 1 to skip the header row

    for row in rows:
        row_data = [td.text.strip() for td in row.find_all("td")]
        data_rows.append(row_data)

    missing_space_mission_df = pd.DataFrame(data_rows, columns=columns)

    missing_space_mission_df = missing_space_mission_df.drop(columns=['Mission success', 'Ref(s).'], errors='ignore')
    missing_space_mission_df['Year'] = missing_space_mission_df['Date'].str.extract(r'(\d{4})')
    missing_space_mission_df = missing_space_mission_df.drop(columns="Date", axis=1, errors='ignore')

    NASA_data_df = missing_space_mission_df[missing_space_mission_df['Country/organization'].str.contains('NASA', case=False, na=False)].copy()
    NASA_data_df['Country/organization'] = 'USA (NASA)'
    split_data = NASA_data_df['Country/organization'].str.split('(', n=1, expand=True)
    NASA_data_df['Country'] = split_data[0].str.strip()
    NASA_data_df['Company Name'] = split_data[1].str.rstrip(')').str.strip()
    NASA_data_df = NASA_data_df[['Year', 'Company Name', 'Country', 'Mission name']]

    processed_data_path = '../data/processed/processed_nasa_missing_files.csv'
    NASA_data_df.to_csv(processed_data_path, index=False)
    print(f"Cleaned data saved to {processed_data_path}")

def clean_nasa_budget_files():
    raw_data_dir = '../data/raw'
    file_path = os.path.join(raw_data_dir, "nasa_budget_files.csv")

    if not os.path.exists(file_path):
        print("File does not exist. Please check the path and filename.")
        return

    with open(file_path, 'r', encoding='utf-8') as file:
        raw_html = file.read()

    table = BeautifulSoup(raw_html, 'lxml')

    headers = table.find_all("th")
    titles = [i.text.strip() for i in headers]
    main_column = titles.pop(1)

    NASA_budget_df = pd.DataFrame(columns=pd.MultiIndex.from_tuples([(main_column, title) for title in titles]))

    rows = table.find_all("tr")
    for j in rows[1:]:  
        data = j.find_all("td")
        if len(data) == len(titles):  
            row = [tr.text.strip() for tr in data]
            l = len(NASA_budget_df)
            NASA_budget_df.loc[l] = row
            
    NASA_budget_df = NASA_budget_df[[('NASA budget', 'CalendarYear'), ('NASA budget', 'Nominal Dollars(Millions)')]]
    NASA_budget_df.columns = ['Year', 'NASA Nominal Budget (millions of dollars)']
    
    NASA_budget_df['NASA Nominal Budget (millions of dollars)'] = NASA_budget_df['NASA Nominal Budget (millions of dollars)'].str.replace(r'\[.*?\]', '', regex=True).str.replace(',', '').astype(float)
    
    NASA_budget_df = NASA_budget_df.drop(index=range(15)).reset_index(drop=True)

    processed_data_path = '../data/processed/processed_nasa_budget_files.csv'
    NASA_budget_df.to_csv(processed_data_path, index=False)
    print(f"Cleaned data saved to {processed_data_path}")

def clean_isro_budget_files():
    raw_data_dir = '../data/raw'
    file_path = os.path.join(raw_data_dir, "isro_budget_files.csv")

    if not os.path.exists(file_path):
        print("File does not exist. Please check the path and filename.")
        return

    with open(file_path, 'r', encoding='utf-8') as file:
        raw_html = file.read()

    # Convert HTML table to DataFrame using StringIO to address FutureWarning about literal HTML
    df = pd.read_html(StringIO(raw_html), header=0)[0]

    # Attempt to convert budget columns to numeric, handling non-numeric entries
    try:
        df['Budget of Department of Space[250]'] = df['Budget of Department of Space[250]'].replace(',', '', regex=True)
        df['Budget of Department of Space[250]'] = pd.to_numeric(df['Budget of Department of Space[250]'], errors='coerce')
    except Exception as e:
        print(f"Error converting to float: {e}")
        return

    # Drop rows where conversion to numeric failed
    df.dropna(subset=['Budget of Department of Space[250]'], inplace=True)

    # Convert the budget from crores to millions of dollars
    crores_to_rupees = 10000000  # 1 crore = 10,000,000 rupees
    rupees_to_dollars = 0.012    # 1 rupee = 0.012 dollars
    dollars_to_millions = 1e-6   # 1 dollar = 1e-6 million dollars

    df['ISRO Nominal Budget (millions of dollars)'] = df['Budget of Department of Space[250]'] * crores_to_rupees * rupees_to_dollars * dollars_to_millions

    # Rename the columns for clarity
    df.rename(columns={'Calendar Year': 'Year', 'Budget of Department of Space[250]': 'ISRO Nominal Budget (crores)'}, inplace=True)

    # Save the cleaned data
    processed_data_path = '../data/processed/processed_isro_budget_files.csv'
    df.to_csv(processed_data_path, index=False)
    print(f"Cleaned data saved to {processed_data_path}")


def main():
    clean_next_space_fligths()
    clean_nasa_missing_files()
    clean_nasa_budget_files()
    clean_isro_budget_files()

if __name__ == "__main__":
    main()