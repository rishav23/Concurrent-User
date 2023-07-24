import requests
import urllib3
import pandas as pd
import tarfile
import io
import time
import numpy as np

start_time = time.perf_counter()
urllib3.disable_warnings()

headers = {
    'accept': 'application/json',
    'AuthUsername': 'user',
    'AuthPassword': 'passwrd',
}

response = requests.get('your_server_url', headers=headers, stream=True, verify=False)

if response.status_code == 200:
    tar = tarfile.open(fileobj=io.BytesIO(response.content), mode='r:gz')
    csv_file = tar.extractfile("file_name.csv")
    df = pd.read_csv(csv_file)
    loaded_df = df[(df['start_at'] > 'your_start_date') & (df['start_at'] < 'your_start_date')]
    loaded_df = loaded_df.copy()
    loaded_df.drop(columns=['your_column_to_drop'], inplace=True)

    # convert to datetime
    loaded_df.loc[:, 'start_at'] = pd.to_datetime(loaded_df['start_at'])
    loaded_df.loc[:, 'end_at'] = pd.to_datetime(loaded_df['end_at'])

    #duration
    loaded_df['duration'] = (loaded_df['end_at'] - loaded_df['start_at']).dt.total_seconds()
    loaded_df['duration_minutes'] = loaded_df['duration'].div(60)

    # convert to date
    loaded_df['date'] = loaded_df['start_at'].dt.date
 
    loaded_df['start_minute_bucket'] = loaded_df['start_at'].dt.strftime("%H:%M")
    loaded_df['client'] = loaded_df['client'].astype(str)
    loaded_df['client'] = loaded_df['client'].replace(np.setdiff1d(loaded_df.client.unique(), ['category1', 'categry2']), 'category3')
    loaded_df = loaded_df[loaded_df['client'] != 'category3']
    loaded_df = loaded_df[loaded_df['status'] != 'error2']
    loaded_df['launch_date'] = loaded_df['start_at'].dt.date
    loaded_df['end_date'] = loaded_df['end_at'].dt.date
    loaded_df["minutes"] = pd.to_timedelta(loaded_df["start_minute_bucket"] + ":00").dt.total_seconds() // 60
    loaded_df["minute"] = (loaded_df["minutes"] // 60) % 24 * 60 + loaded_df["minutes"] % 60
    loaded_df.loc[loaded_df['status'] == 'error3', 'duration_minutes'] = loaded_df['duration_minutes'].sub(15).clip(lower=0)
    loaded_df.dropna(subset=['minute'], inplace=True)
    loaded_df.dropna(subset=['duration_minutes'], inplace=True)
    loaded_df.drop(loaded_df[loaded_df['minute'] > 1440].index, inplace=True)
    loaded_df.drop(loaded_df[loaded_df['duration_minutes'] > 1440].index, inplace=True)

    def add_duration_to_minute_cols(df):
        ranges = 1440
        minute_cols = {}

        for i in range(ranges + 1):
            minute_cols[f'col_{i}.0'] = []

        for row in df.itertuples():
            formatted_value = int(row.minute)
            duration_minutes = row.duration_minutes
            whole_minutes = int(duration_minutes)
            fraction_minutes = duration_minutes - whole_minutes

            if formatted_value <= ranges and formatted_value + whole_minutes > 0 and formatted_value + whole_minutes <= ranges:
                for i in range(formatted_value, formatted_value + whole_minutes):
                    minute_cols[f'col_{i}.0'].append(1)

            if formatted_value + whole_minutes < ranges and formatted_value + whole_minutes > 0:
                minute_cols[f'col_{formatted_value + whole_minutes}.0'].append(fraction_minutes)

        max_minutes = []
        max_duration = []
        for date in df.date.unique():
            date_minute_cols = []
            for i in range(ranges + 1):
                date_minute_cols.append(sum(minute_cols[f'col_{i}.0'][j] for j in range(len(minute_cols[f'col_{i}.0']))) if len(minute_cols[f'col_{i}.0']) > 0 else 0)
            max_cols = max(range(len(date_minute_cols)), key=date_minute_cols.__getitem__)
            max_minutes.append(max_cols)
            max_duration.append(date_minute_cols[max_cols])

        max_time = []
        for minute in max_minutes:
            max_time.append(time.strftime('%H:%M', time.gmtime(minute * 60)))

        result = {'Date': df.date.unique(), 'Time': max_time, 'CCU': max_duration}
        return pd.DataFrame(result)

    def process_daily_data(df):
        if len(df['date'].unique()) == 1:
            return add_duration_to_minute_cols(df)
        else:
            return None

    dates = loaded_df['date'].unique()
    results = []

    for date in dates:
        daily_df = loaded_df[loaded_df['date'] == date]
        daily_result = process_daily_data(daily_df)
        if daily_result is not None:
            results.append(daily_result)

    final_df = pd.concat(results).reset_index(drop=True)
    print(final_df)

else:
    print(f'Request failed with status code {response.status_code}')

end_time = time.perf_counter()
elapsed_time = end_time - start_time
print(f'Elapsed time: {elapsed_time:.2f} seconds')
