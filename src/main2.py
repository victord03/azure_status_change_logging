import pandas as pd
import os

# Define the folder containing CSV files
csv_folder = "../data"
output_folder = "../output"

# Define date range
start_date = '2025-02-25'
end_date = '2025-02-26'

def csv_to_df(path: str, file_name: str) -> pd.DataFrame:
    os.chdir(path)
    return pd.read_csv(file_name, encoding="utf-8")

def drop_columns_from_df(columns_to_keep: list, df1: pd.DataFrame) -> pd.DataFrame:
    df1.drop(columns=[col for col in df1.columns if col not in columns_to_keep], inplace=True)
    return df1

if __name__ == "__main__":

    df = csv_to_df(csv_folder, "abgr_2025-02-25.csv")
    columns = ["ID", "State", "Assigned To"]
    df = drop_columns_from_df(columns, df)



    print(df.head)



    """def combine_csv_files(folder_path):
        '''Reads and combines all CSV files from the given folder.'''
        all_data = []
        for file in os.listdir(folder_path):
            if file.endswith(".csv"):
                file_path = os.path.join(folder_path, file)
                df = pd.read_csv(file_path, usecols=["ID", "State", "Assigned To"])
                df.rename(columns={"Assigned To": "Assignee"}, inplace=True)
                df["Date"] = file.split('.')[0]  # Extract date from filename if applicable
                all_data.append(df)
    
        if not all_data:
            print("No CSV files found or files are empty.")
            return pd.DataFrame(columns=["ID", "State", "Assignee", "Date"])
    
        return pd.concat(all_data, ignore_index=True)
    
    # Load and combine CSV files in the output folder
    combined_df = combine_csv_files(output_folder)
    
    # Convert Date column to datetime format for proper sorting
    combined_df["Date"] = pd.to_datetime(combined_df["Date"], errors="coerce")
    
    # Sort by ID and Date to ensure correct state change tracking
    combined_df = combined_df.sort_values(by=["ID", "Date"])
    
    # Filter data within the given date range
    filtered_df = combined_df[(combined_df["Date"] >= start_date) & (combined_df["Date"] <= end_date)]
    
    # Identify state changes
    filtered_df["Prev_State"] = filtered_df.groupby("ID")["State"].shift(1)
    state_change_df = filtered_df[filtered_df["State"] != filtered_df["Prev_State"]].drop(columns=["Prev_State"])
    
    # Save final state change log to the output folder
    output_file = os.path.join(output_folder, "state_changes_log.csv")
    state_change_df.to_csv(output_file, index=False)
    
    print(f"State change log saved to {output_file}")"""
