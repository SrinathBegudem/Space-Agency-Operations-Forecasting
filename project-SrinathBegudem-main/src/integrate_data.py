import pandas as pd

def next_space_fligths():
    # Load the Excel file
    file_path = '../data/processed/processed_space_flight_data.xlsx'
    next_space_flight_data = pd.read_excel(file_path)

    # Step 1: Clean 'budget' column
    next_space_flight_data['budget (million of dollars)'] = next_space_flight_data['budget']\
        .str.replace('[^\d.]', '', regex=True)\
        .astype(float)

    # Step 2: Extract the year from the 'date' column
    next_space_flight_data['Year'] = next_space_flight_data['date'].str.extract(r'(\d{4})').astype(int)

    # Drop the original 'budget' and 'date' columns
    next_space_flight_data.drop(columns=['budget', 'date'], inplace=True)

    # Step 3: Replace 0.0 values in the 'budget (million of dollars)' column with the mean of the non-zero values
    mean_budget = next_space_flight_data['budget (million of dollars)']\
        [next_space_flight_data['budget (million of dollars)'] != 0].mean()

    # Replace 0.0 values with the mean budget
    next_space_flight_data['budget (million of dollars)'] = next_space_flight_data['budget (million of dollars)']\
        .replace(0.0, mean_budget)

    # Step 4: Round the 'budget (million of dollars)' column to 2 decimal places
    next_space_flight_data['budget (million of dollars)'] = next_space_flight_data['budget (million of dollars)'].round(2)

    next_space_flight_data = next_space_flight_data[['Year', 'place', 'company_name', 'budget (million of dollars)', 'status', 'result', 'space_flight_name']]

    # We split the 'place' string by commas and take the last element (-1) which should be the country
    next_space_flight_data['Country'] = next_space_flight_data['place'].str.split(',').str[-1].str.strip()

    # Now, we can drop the 'place' column if it's no longer needed
    next_space_flight_data = next_space_flight_data.drop('place', axis=1)

    # Optionally, you can rename the 'country' column to 'Country' for consistency
    next_space_flight_data.rename(columns={'country': 'Country'}, inplace=True)

    next_space_flight_data = next_space_flight_data[['Year', 'company_name',  'Country', 'space_flight_name']]

    # Renaming the DataFrame columns
    next_space_flight_data.rename(columns={
        'company_name': 'Company Name',
        'space_flight_name': 'Mission name'
    }, inplace=True)

    return next_space_flight_data







def nasa_missing_data():

    # Load the Excel file
    file_path = '../data/processed/processed_NASA_missing_space_missions.csv'
    nasa_missing_data = pd.read_csv(file_path)

    return nasa_missing_data





def nasa_budget():
    nasa_budget = pd.read_csv('../data/processed/processed_NASA_budget_data.csv')
    nasa_budget['NASA Nominal Budget (millions of dollars)'] = nasa_budget['NASA Nominal Budget (millions of dollars)'].str.replace(r'\[.*?\]', '', regex=True).str.replace(',', '').astype(float)
    nasa_budget['2023 Constant Dollars (Millions)'] = nasa_budget['2023 Constant Dollars (Millions)'].str.replace(r'\[.*?\]', '', regex=True).str.replace(',', '').astype(float)
    nasa_budget['2023 Constant Dollars (Millions)'] = nasa_budget['2023 Constant Dollars (Millions)'].fillna(27481.0).round(2)
    return nasa_budget





def isro_budget():
    isro_budget = pd.read_excel('../data/processed/processed_indian_budget.xlsx')
    return isro_budget


def main():
    df_1 = next_space_fligths()
    df_2 = nasa_missing_data()
    df_3 = nasa_budget()
    df_4 = isro_budget()
    combined_space_flights = pd.concat([df_1, df_2], ignore_index=True)

    # Sort the combined DataFrame based on 'Year'
    combined_space_flights_sorted = combined_space_flights.sort_values(by='Year')


    mission_counts = combined_space_flights_sorted.groupby(['Year', 'Company Name']).size().reset_index(name='Total Missions')
    company_names = ['NASA', 'ISRO', 'Roscosmos']

    # Filter the DataFrame to include only rows where the 'Company Name' is in the list of company names
    filtered_df = mission_counts[mission_counts['Company Name'].isin(company_names)]

    filtered_df = filtered_df.query('2004 <= Year <= 2021')

    # Pivot the DataFrame
    pivot_df = filtered_df.pivot_table(index='Year', columns='Company Name', values='Total Missions', fill_value=0)

    # Rename columns for better clarity
    pivot_df.columns = [col + '_Total_Missions' for col in pivot_df.columns]

    # Reset the index to make 'Year' a column again
    pivot_df.reset_index(inplace=True)

    mission_count_df = pivot_df.drop('Roscosmos_Total_Missions', axis=1) 


    combined_df = pd.merge(mission_count_df, df_3, on='Year', how='outer')

    # Merge the combined_df with isro_budget on the 'Year' column
    integrated_df = pd.merge(combined_df, df_4, on='Year', how='outer')

    # Assuming your DataFrame is named final_combined_df
    integrated_df.rename(columns={
        '2023 Constant Dollars (Millions)': 'NASA 2023 Constant (millions of dollars)',
        '2020 Constant USD (millions)': 'ISRO 2020 Constant (millions of dollars)'
    }, inplace=True)



    print(integrated_df)

if __name__ == "__main__":
    main()