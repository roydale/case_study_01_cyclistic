import os
import sqlite3
import pandas as pd
from pandasgui import show
from pathlib import Path

# === CONFIGURATION ===
csv_dir = '../data'  # Directory containing the CSV files
db_name = 'cyclistic.db'  # SQLite database name

# === TABLE SCHEMA ===
standard_columns = [
    'ref_id',
    'source',
    'rideable_type',
    'bike_id',
    'start_time',
    'end_time',
    'trip_duration',
    'start_station_id',
    'start_station_name',
    'end_station_id',
    'end_station_name',
    'start_latitude',
    'start_longitude',
    'end_latitude',
    'end_longitude',
    'user_type',
    'user_gender',
    'user_birth_year'
]

# === TABLE & CSV COLUMN MAPPINGS ===
data_2019_column_mappings = {
    'trip_id': 'ref_id',
    'start_time': 'start_time',
    'end_time': 'end_time',
    'bikeid': 'bike_id',
    'tripduration': 'trip_duration',
    'from_station_id': 'start_station_id',
    'from_station_name': 'start_station_name',
    'to_station_id': 'end_station_id',
    'to_station_name': 'end_station_name',
    'usertype': 'user_type',
    'gender': 'user_gender',
    'birthyear': 'user_birth_year'
}

data_2019_Q2_column_mappings = {
    '01 - Rental Details Rental ID': 'ref_id',
    '01 - Rental Details Local Start Time': 'start_time',
    '01 - Rental Details Local End Time': 'end_time',
    '01 - Rental Details Bike ID': 'bike_id',
    '01 - Rental Details Duration In Seconds Uncapped': 'trip_duration',
    '03 - Rental Start Station ID': 'start_station_id',
    '03 - Rental Start Station Name': 'start_station_name',
    '02 - Rental End Station ID': 'end_station_id',
    '02 - Rental End Station Name': 'end_station_name',
    'User Type': 'user_type',
    'Member Gender': 'user_gender',
    '05 - Member Details Member Birthday Year': 'user_birth_year'
}

data_2020_column_mappings = {
    'ride_id': 'ref_id',
    'rideable_type': 'rideable_type',
    'started_at': 'start_time',
    'ended_at': 'end_time',
    'start_station_name': 'start_station_name',
    'start_station_id': 'start_station_id',
    'end_station_name': 'end_station_name',
    'end_station_id': 'end_station_id',
    'start_lat': 'start_latitude',
    'start_lng': 'start_longitude',
    'end_lat': 'end_latitude',
    'end_lng': 'end_longitude',
    'member_casual': 'user_type'
}

# === FILE MAPPINGS ===
# List of tuples containing file names and their corresponding column mappings
file_map_mappings = [
    ('Divvy_Trips_2019_Q1.csv', data_2019_column_mappings),
    ('Divvy_Trips_2019_Q2.csv', data_2019_Q2_column_mappings),
    ('Divvy_Trips_2019_Q3.csv', data_2019_column_mappings),
    ('Divvy_Trips_2019_Q4.csv', data_2019_column_mappings),
    ('Divvy_Trips_2020_Q1.csv', data_2020_column_mappings)
]

# === FUNCTIONS ===
def get_year_quarter_from_filename(file_name):
    file_path = Path(file_name)
    file_path_parts = file_path.stem.split('_')
    yr_qtr = file_path_parts[-2] + '_' + file_path_parts[-1]
    return yr_qtr

def load_data_and_standardize_column(file_name, column_mappings, columns):
    file_path = os.path.join(csv_dir, 'raw', file_name)
    csv_df = pd.read_csv(filepath_or_buffer=file_path)
    csv_df = csv_df.rename(columns=column_mappings)    
    csv_df['source'] = get_year_quarter_from_filename(file_name)
    
    for col in columns:
        if col not in csv_df.columns:
            csv_df[col] = pd.NA
    csv_df = csv_df[columns]
    
    return csv_df

def create_and_populate_table(db_connection, table_name, data_frame, columns):
    column_definitions = [
        'id INTEGER PRIMARY KEY AUTOINCREMENT'
    ]
    
    table_type_mappings = {
        'TEXT': [
            'ref_id',
            'source',
            'rideable_type',        
            'start_time',
            'end_time',        
            'start_station_name',        
            'end_station_name',
            'user_type',
            'user_gender'        
        ],
        'REAL': [      
            'trip_duration',  
            'start_latitude',
            'start_longitude',
            'end_latitude',
            'end_longitude'
        ],
        'INTEGER': [
            'bike_id',
            'start_station_id',
            'end_station_id',            
            'user_birth_year'
        ]
    }
    
    for column in columns:
        column_type = 'TEXT'
        
        for data_type, column_list in table_type_mappings.items():
            if column in column_list:
                column_type = data_type
                break
                
        column_definitions.append(f"{column} {column_type}")
    
    create_table_script = f'''
    DROP TABLE IF EXISTS {table_name};

    CREATE TABLE {table_name} (
        {', '.join(column_definitions)}
    );
    '''    
    cursor = db_connection.cursor()
    cursor.executescript(create_table_script)
    
    data_frame.to_sql(table_name, db_connection, if_exists='append', index=False)

# === START PROCESS ===
db_connection = sqlite3.connect(os.path.join(csv_dir, db_name))

stg_df_list = []
excluded_files = []

for file_name, column_mappings in file_map_mappings:
    file_path = os.path.join(csv_dir, 'raw', file_name)
    stg_table_name = f'py_stg_{get_year_quarter_from_filename(file_name)}'
    
    print(f'📥 Loading {file_name} into staging table {stg_table_name}...')

    try:
        stg_df = load_data_and_standardize_column(
            file_name,
            column_mappings,
            standard_columns
        )
        
        print(f'✅ Loaded {file_name}: {len(stg_df)} rows, {stg_df.shape[1]} columns')
        
        if stg_df.empty or stg_df.isna().all(axis=None):
            excluded_files.append(file_name)
            continue
        
        stg_df_list.append(stg_df)
        
        create_and_populate_table(
            db_connection,
            stg_table_name,
            stg_df,
            standard_columns
        )
        
    except Exception as e:
        print(f'⚠️ Skipping {file_name} due to error: {e}')
        excluded_files.append(file_name)
    
# === MERGE DATA ===
merged_table_name = 'py_stg_MERGED_2019_2020'

print(f'🛠️ Merging all data into one staging table {merged_table_name} for further processing')

merged_df = pd.concat(stg_df_list, ignore_index=True)

create_and_populate_table(
    db_connection,
    merged_table_name,
    merged_df,
    standard_columns
)

print(f'✅ Done. All data inserted into {merged_table_name}.')

if excluded_files:
    print('🗂️ Skipped the following files (empty or all-NA):')
    for file_name in excluded_files:
        print(f' - {file_name}')
else:
    print('✅ All files included in merge.')

db_connection.close()

# show(stg_df)