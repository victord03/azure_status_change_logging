import os
import pandas as pd
from datetime import datetime


def combine_csv_files(csv_folder):
    """Reads CSV files from a folder, adds a Date column, and combines them into one DataFrame."""

    all_data = []
    for file in os.listdir(csv_folder):
        if file.endswith('.csv'):
            file_path = os.path.join(csv_folder, file)
            df = pd.read_csv(file_path)

            # Try to extract the date from the filename (expects format: region_YYYY-MM-DD.csv)
            try:
                date_str = file.split('_')[-1].replace('.csv', '')
                file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            except Exception as e:
                # Fallback: use the file's modification time if date extraction fails
                mod_time = os.path.getmtime(file_path)
                file_date = datetime.fromtimestamp(mod_time).date()

            # Add the Date column to the DataFrame
            df['Date'] = pd.to_datetime(file_date)
            all_data.append(df)

    # Combine all DataFrames into one
    combined_df = pd.concat(all_data, ignore_index=True)
    return combined_df


def log_status_changes(combined_df, start_date, end_date):
    """
    For each ID in the specified date range, logs changes in status.
    Returns a DataFrame with columns: ID, State, Assignee, Date.
    """

    # Filter DataFrame based on the given date range and sort by ID and Date
    mask = (combined_df['Date'] >= pd.to_datetime(start_date)) & (combined_df['Date'] <= pd.to_datetime(end_date))
    df_range = combined_df.loc[mask].sort_values(by=['ID', 'Date'])

    logs = []
    # Group the DataFrame by ID
    for id_val, group in df_range.groupby('ID'):
        group = group.sort_values(by='Date')
        previous_status = None

        # Iterate through each record in the group
        for _, row in group.iterrows():
            current_status = row['Status']

            # Log the entry if the status changed (or if it's the first record for that ID)
            if current_status != previous_status:
                logs.append({
                    'ID': row['ID'],
                    'State': current_status,  # Renaming 'Status' to 'State' as per the output requirement
                    'Assignee': row['Assignee'],
                    'Date': row['Date']
                })
                previous_status = current_status

    log_df = pd.DataFrame(logs)
    return log_df


if __name__ == "__main__":
    # Folder where CSV files are stored
    csv_folder = "./csv_files"

    # Combine CSV files and add the Date column
    combined_df = combine_csv_files(csv_folder)

    # Save the combined data into a single CSV file
    combined_df.to_csv("combined_data.csv", index=False)
    print("Combined CSV file saved as 'combined_data.csv'.")

    # Define the date range for logging status changes
    start_date = '2023-01-01'
    end_date = '2023-12-31'

    # Get the log of status changes within the specified date range
    status_changes = log_status_changes(combined_df, start_date, end_date)

    # Save the log to a CSV file
    status_changes.to_csv("status_changes_log.csv", index=False)
    print("Status changes log saved as 'status_changes_log.csv'.")
