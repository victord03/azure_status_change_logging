import os
import pandas as pd
from datetime import datetime


def aggregate_csv_files(csv_folder):
    """
    Reads all CSV files in csv_folder, extracts only the columns "ID", "State" and "Assigned To",
    adds a "Date" column (extracted from the filename or file's modification time),
    and returns the aggregated DataFrame.
    """
    data_frames = []
    for file in os.listdir(csv_folder):
        if file.endswith('.csv'):
            file_path = os.path.join(csv_folder, file)
            try:
                df = pd.read_csv(file_path)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
                continue

            # Ensure we work only with the required columns.
            if not set(["ID", "State", "Assigned To"]).issubset(df.columns):
                print(f"File {file_path} is missing required columns; skipping.")
                continue
            df = df[["ID", "State", "Assigned To"]].copy()

            # Attempt to extract the date from the filename (expects format: something_YYYY-MM-DD.csv)
            try:
                date_str = file.split('_')[-1].replace('.csv', '')
                file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            except Exception:
                # Fallback: use the file's modification time if date extraction fails
                file_date = datetime.fromtimestamp(os.path.getmtime(file_path)).date()

            # Add the Date column
            df["Date"] = pd.to_datetime(file_date)
            data_frames.append(df)

    if not data_frames:
        return pd.DataFrame(columns=["ID", "State", "Assigned To", "Date"])
    combined_df = pd.concat(data_frames, ignore_index=True)
    return combined_df


def log_state_changes(df, start_date, end_date):
    """
    For the given date range (start_date and end_date), this function logs state changes for each ID.

    For an ID, if the state has changed at least once during the period, then every record
    where a change occurred (i.e. the first record and any record whose state differs from the previous one)
    is logged.

    The resulting DataFrame will have 4 columns: "ID", "State", "Assignee", and "Date".
    IDs that did not experience any change in state within the date range are not logged.
    """
    # Filter the aggregated data for the specified date range
    df_filtered = df[(df["Date"] >= pd.to_datetime(start_date)) & (df["Date"] <= pd.to_datetime(end_date))]
    df_filtered = df_filtered.sort_values(["ID", "Date"])

    logs = []
    # Group by ID to check for state changes
    for id_val, group in df_filtered.groupby("ID"):
        group = group.sort_values("Date")
        group_rows = group.to_dict("records")
        if len(group_rows) < 2:
            # No chance for a change if only one record exists
            continue

        state_log = []
        previous_state = None
        for row in group_rows:
            current_state = row["State"]
            # Always record the first state; thereafter record only if it changes
            if previous_state is None or current_state != previous_state:
                state_log.append(row)
            previous_state = current_state

        # Log this ID only if there was at least one change (i.e. more than one unique state)
        if len({row["State"] for row in state_log}) > 1:
            logs.extend(state_log)

    # Create a DataFrame for the log, renaming "Assigned To" to "Assignee"
    if logs:
        log_df = pd.DataFrame(logs)
        log_df.rename(columns={"Assigned To": "Assignee"}, inplace=True)
        # Reorder columns to: "ID", "State", "Assignee", "Date"
        log_df = log_df[["ID", "State", "Assignee", "Date"]]
    else:
        log_df = pd.DataFrame(columns=["ID", "State", "Assignee", "Date"])
    return log_df


if __name__ == "__main__":


    # Step 1: Aggregate the data from CSV files located in "../data"
    csv_folder = "../data"
    combined_df = aggregate_csv_files(csv_folder)

    # Step 2: Save the aggregated data as a CSV file in the output folder
    combined_csv_path = os.path.join("../output", "combined_data.csv")
    combined_df.to_csv(combined_csv_path, index=False)
    print(f"Combined data saved to {combined_csv_path}")

    # Step 3: For a given date range, log the state changes.
    # Only log an ID if at least one state change occurred in the date range.
    start_date = '2025-02-25'
    end_date = '2025-02-26'
    state_change_df = log_state_changes(combined_df, start_date, end_date)

    # Save the state changes log to a CSV file in the output folder.
    state_change_csv_path = os.path.join("../output", "state_changes_log.csv")
    state_change_df.columns = ["ID", "State", "Assignee", "Date"]
    state_change_df.to_csv(state_change_csv_path, index=False, encoding="utf-8", sep=",")  # header=True
    print(f"State changes log saved to {state_change_csv_path}")

    print(state_change_df)