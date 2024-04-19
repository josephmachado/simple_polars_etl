from datetime import datetime

import requests 
import polars as pl

# Extract data
url="https://data.cityofnewyork.us/resource/833y-fsy8.json"
offset = 1000
iteration_size = 1000

all_data = []
while True and offset <= 30000: # we know from docs that there is only 27k records
    print(f"Pulling records from {offset - iteration_size} to {offset}")
    response = requests.get(url, params={'$offset': offset})
    data = response.json()

    # Check if there is data returned
    if not data:
        break  # Exit loop if no data is returned

    # Append fetched data to all_data
    print(f"Number of rows pulled in this iteration {len(data)}")
    all_data.extend(data)

    # Move the offset forward
    offset += iteration_size

print(f"Total records fetched: {len(all_data)}")

df = pl.DataFrame(all_data)

# Transform data

# Naming and data types
df = df.with_columns(pl.col("occur_date").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f").cast(pl.Date).alias("date"))
df = df.with_columns(pl.col("date").dt.year().alias("year"))

# Creating metrics
result_df = df.group_by(['year', 'boro']).agg([
    pl.col('incident_key').n_unique().alias('incident_count')
    ]).sort(["year", "boro"], descending=True)

# todo: Validate data

# Store data
current_datetime = datetime.now()
formatted_datetime = current_datetime.strftime("%Y-%m-%d-%H-%M-%S")
result_df.write_csv(f"./nyc_year_boro_incidents_{formatted_datetime}.csv")

# Visualize data
print(result_df)