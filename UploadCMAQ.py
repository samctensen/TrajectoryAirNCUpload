import numpy as np
import boto3
import json
import os
import pwinput
import requests
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

base_url = 'https://home.chpc.utah.edu/~u0703457/people_share/CREATE_AQI/forecast_output/'
folder_path = 'npyfiles/'
PM25_path = 'aq_PM25_array.npy'
time_path = 'forecast_time.npy'
lat_path = 'forecast_lat.npy'
lon_path = 'forecast_lon.npy'

# Function to clear old files in the ncfiles directory
def clear_directory():
    for file in os.listdir(folder_path):
        delete_file_path = os.path.join(folder_path, file)
        os.remove(delete_file_path)

# Function very mapbox username and token before starting
def verify_credentials(mapbox_username, mapbox_access_token):
    mapbox_credentials_url = f'https://api.mapbox.com/uploads/v1/{mapbox_username}/credentials?access_token={mapbox_access_token}'
    valid_mapbox_credentials = requests.post(mapbox_credentials_url).ok
    if valid_mapbox_credentials:
        print("Mapbox token authenticated.")
        return True
    else:
        print("Invalid Mapbox username or token. Please try again.")
        return False

def download_files():
    futures = []
    completed_jobs = 0
    total_jobs = 4
    print(f"\r({completed_jobs}/{total_jobs}) .npy forecast files downloaded.", end="")
    
    with ThreadPoolExecutor(max_workers=4) as executor:  # Adjust max_workers as needed
        futures.append(executor.submit(download_file, PM25_path))
        futures.append(executor.submit(download_file, time_path))
        futures.append(executor.submit(download_file, lat_path))
        futures.append(executor.submit(download_file, lon_path))

        for future in as_completed(futures):
            completed_jobs += 1
            print(f"\r({completed_jobs}/{total_jobs}) .npy files downloaded.", end="")
            sys.stdout.flush()
        print(np.load(folder_path + time_path))

# Child function, downloads a single forecast file.
def download_file(numpy_filename: str) -> (str | None):
    file_url = base_url + numpy_filename
    response = requests.get(file_url)
    numpy_file_path = os.path.join(folder_path, numpy_filename)
    if response.status_code == 200:
        with open(numpy_file_path, 'wb') as f:
            f.write(response.content)
        return numpy_filename
    else:
        print(f'Failed to download: {numpy_filename}')
        return None

# Parent function, schedules individial nc to geojson jobs.
def numpy_to_geojsons() -> list:
    geojson_filenames = []
    futures = []
    completed_jobs = 0
    total_jobs =  np.load(folder_path + time_path).size
    print(f"\r({completed_jobs}/{total_jobs}) .geojson files generated from .npy forecast file.", end="")
    
    with ThreadPoolExecutor(max_workers=8) as executor:  # Adjust max_workers as needed
        for i in range(np.load(folder_path + time_path).size):
            futures.append(executor.submit(array_to_geojson, i))

        for future in as_completed(futures):
            try:
                geojson_filename = future.result()  # Retrieve the result from the future
                if geojson_filename:
                    geojson_filenames.append(geojson_filename)
            except Exception as e:
                print(f"Exception occurred: {e}")
            completed_jobs += 1
            print(f"\r({completed_jobs}/{total_jobs}) .geojson files generated from .npy forecast file.", end="")
            sys.stdout.flush()
    os.remove(folder_path + PM25_path)
    os.remove(folder_path + time_path)
    os.remove(folder_path + lat_path)
    os.remove(folder_path + lon_path)
    return geojson_filenames
            
# Child function, converts individual nc files to geojson.
def array_to_geojson(time_index: int) -> str:
    PM25 = np.load(folder_path + PM25_path)
    time = np.load(folder_path + time_path)
    lat = np.load(folder_path + lat_path)
    lon = np.load(folder_path + lon_path)
    formatted_date = datetime.strptime(time[time_index], '%Y-%m-%d %H:%M:%S %Z')
    geojson_filename = formatted_date.strftime("%Y-%m-%d_%H_CMAQ.geojson")
    geojson_file_path = os.path.join(folder_path, geojson_filename)

    features = []
    feature_id = 1
    # Iterate over the lon, lat, and PM25 arrays and combine them into features
    for j in range(lat.size):
        for k in range(lon.size):
            pm25_value = float(PM25[time_index][j][k])
            # Filter PM25 values between 8 and 200
            if 5 <= pm25_value:
                feature = {
                    "type": "Feature",
                    "properties": {
                        "id": str(feature_id),
                        "PM25": pm25_value
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [float(lon[k]), float(lat[j]), 0]
                    }
                }
                features.append(feature)
                feature_id += 1

    # Start writing the JSON file
    with open(geojson_file_path, 'w') as f:
        f.write('{\n')
        f.write('"type": "FeatureCollection",\n')
        f.write('"crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } },\n')
        f.write('"features": [\n')

        # Write each feature in one line
        for idx, feature in enumerate(features):
            feature_str = json.dumps(feature)
            if idx < len(features) - 1:
                feature_str += ','
            f.write(feature_str + '\n')

        # End the JSON file
        f.write(']\n')
        f.write('}\n')

    return geojson_filename

# Parent function, schedules individial geojson to mbtile jobs.
def geojsons_to_mbtiles(geojson_filenames: list) -> list:
    mbtile_filenames = []
    futures = []
    completed_jobs = 0
    total_jobs = len(geojson_filenames)
    print(f"\r({completed_jobs}/{total_jobs}) .geojson files converted to .mbtiles.", end="")

    with ThreadPoolExecutor(max_workers=8) as executor:  # Adjust max_workers as needed
        for geojson_filename in geojson_filenames:
            futures.append(executor.submit(geojson_to_mbtiles, geojson_filename))

        for future in as_completed(futures):
            try:
                mbtile_filename = future.result()  # Retrieve the result from the future
                if mbtile_filename:
                    mbtile_filenames.append(mbtile_filename)
            except Exception as e:
                print(f"Exception occurred: {e}")
            completed_jobs += 1
            print(f"\r({completed_jobs}/{total_jobs}) .geojson files converted to .mbtiles.", end="")
            sys.stdout.flush()

    return mbtile_filenames

# Child function, converts individual geojson files to mbtiles.
def geojson_to_mbtiles(geojson_filename: str) -> str:
    geojson_file_path = os.path.join(folder_path, geojson_filename)
    mbtiles_filename = geojson_filename.replace('.geojson', '.mbtiles')
    mbtiles_file_path = os.path.join(folder_path, mbtiles_filename)
    tippe_canoe_command = f"tippecanoe -o {mbtiles_file_path} -l PM25 -zg --drop-fraction-as-needed {geojson_file_path}"
    subprocess.run(tippe_canoe_command, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.remove(geojson_file_path)
    return mbtiles_filename

# Parent function, schedules individial mbtile upload jobs.
def upload_mbtiles_to_mapbox(mbtiles_filenames: list, mapbox_username: str, mapbox_access_token: str):
    futures = []
    completed_jobs = 0
    total_jobs = len(mbtiles_filenames)
    print(f"\r({completed_jobs}/{total_jobs}) .mbtiles uploaded to MapBox.", end="")

    with ThreadPoolExecutor(max_workers=8) as executor:  # Adjust max_workers as needed
        for mbtiles_filename in mbtiles_filenames:
            futures.append(executor.submit(upload_mbtile_file_to_mapbox, mbtiles_filename, mapbox_username, mapbox_access_token))

        for future in as_completed(futures):
            try:
                future.result()  # Retrieve the result from the future
            except Exception as e:
                print(f"Exception occurred: {e}")
            completed_jobs += 1
            print(f"\r({completed_jobs}/{total_jobs}) .mbtiles uploaded to MapBox.", end="")
            sys.stdout.flush()

# Child function, uploads individual mbtile files to Mapbox.
def upload_mbtile_file_to_mapbox(mbtiles_filename: str, mapbox_username: str, mapbox_access_token: str):
    max_retries = 3
    retry_delay = 5  # seconds

    for attempt in range(max_retries):
        try:
            mbtiles_file_path = os.path.join(folder_path, mbtiles_filename)
            # Delete existing tileset if it exists
            tileset_id = f"{mapbox_username}.{mbtiles_filename.removesuffix('.mbtiles')}"
            mapbox_delete_tileset_url = f'https://api.mapbox.com/tilesets/v1/{tileset_id}?access_token={mapbox_access_token}'
            requests.delete(mapbox_delete_tileset_url)

            # Get Mapbox credentials
            mapbox_credentials_url = f'https://api.mapbox.com/uploads/v1/{mapbox_username}/credentials?access_token={mapbox_access_token}'
            mapbox_credentials_response = requests.post(mapbox_credentials_url)
            mapbox_credentials = mapbox_credentials_response.json()

            # Upload the tileset to AWS S3
            s3 = boto3.client(
                's3',
                aws_access_key_id=mapbox_credentials['accessKeyId'],
                aws_secret_access_key=mapbox_credentials['secretAccessKey'],
                aws_session_token=mapbox_credentials['sessionToken'],
            )
            bucket = mapbox_credentials['bucket']
            key = mapbox_credentials['key']
            s3.upload_file(mbtiles_file_path, bucket, key)

            # Upload S3 tileset to Mapbox Studio
            upload_url = f'https://api.mapbox.com/uploads/v1/{mapbox_username}?access_token={mapbox_access_token}'
            upload_payload = {
                "url": f"https://{bucket}.s3.amazonaws.com/{key}",
                "tileset": tileset_id,
                "name": f"{mbtiles_filename.removesuffix('.mbtiles')}"
            }
            response = requests.post(upload_url, json=upload_payload)
            upload_response = response.json()
            if upload_response.get('error'):
                print("Error uploading tileset to Mapbox.")
            else:
                os.remove(mbtiles_file_path)
                break
        except requests.exceptions.SSLError as e:
            if attempt < max_retries - 1:
                print(f"SSL error occurred, retrying in {retry_delay} seconds... (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                print(f"SSL error occurred, all retries failed. Error: {e}")
        except Exception as e:
            print(f"Exception occurred: {e}")
            break

# Deletes tilesets that are older than 5 hours
def clear_depreciated_tilesets(mapbox_username: str, mapbox_access_token: str):
    print("Removing depriciated tilesets.")
    all_tilesets_url = f'https://api.mapbox.com/tilesets/v1/{mapbox_username}?access_token={mapbox_access_token}&limit=500&sortby=modified'
    response = requests.get(all_tilesets_url)
    tilesets = json.loads(response.text)
    current_time = datetime.now(timezone.utc)
    for i in range(len(tilesets) - 1, -1, -1):
        index = int("{:03}".format(i))
        tileset = tilesets[index]
        date_modified = datetime.strptime(tileset['modified'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
        time_threshold = current_time - timedelta(hours=5)
        # Tileset is older than 5 hours, delete
        if date_modified < time_threshold:
            tileset_id = tilesets[index]['id']
            mapbox_delete_tileset_url = f'https://api.mapbox.com/tilesets/v1/{tileset_id}?access_token={mapbox_access_token}'
            requests.delete(mapbox_delete_tileset_url)
        # Tileset is too new to delete
        else:
            break
    print("All files processed and uploaded.")

if __name__ == '__main__':
    mapbox_username = input('Input Mapbox username: ')
    mapbox_access_token = pwinput.pwinput('Input Mapbox access token: ')
    clear_directory()
    valid_auth = verify_credentials(mapbox_username, mapbox_access_token)
    if valid_auth:
        download_files()
        geojson_filenames = numpy_to_geojsons()
        mbtile_filenames = geojsons_to_mbtiles(geojson_filenames)
        upload_mbtiles_to_mapbox(mbtile_filenames, mapbox_username, mapbox_access_token)
        clear_depreciated_tilesets(mapbox_username, mapbox_access_token)
        
