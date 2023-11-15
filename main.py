"""
This code will download all the preview videos single threaded.
The filename format is Category - Name - url end part.mov
"""
import json
import pandas
from pprint import pprint
import requests
import os
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Disable SSL warnings
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# read the entries.json from system
ORIGINAL_ENTRIES_PATH = './data/entries.json'

with open(ORIGINAL_ENTRIES_PATH, 'r') as f:
    ENTRIES = json.load(f)

DISPLAY_NAMES =  pandas.read_csv('data/display_names.csv').reset_index(names='orderId').set_index('assetId')

ASSET_URLS = pandas.DataFrame.from_dict(
        dict(
            assetId=asset['id'], 
            previewImage=asset['previewImage'], 
            fullVideo=asset['url-4K-SDR-240FPS']
        )
        for asset in ENTRIES['assets']
    )\
    .set_index('assetId')

NAMES_AND_URLS = DISPLAY_NAMES.join(ASSET_URLS)

if len(ASSET_URLS) > len(DISPLAY_NAMES):
    warnings.warn(f'There are {len(ASSET_URLS)} assets but only {len(DISPLAY_NAMES)} display names. Some assets will be missing from output.')


def download_file(url, destination):
    if os.path.exists(destination):
        print(f"File '{destination}' already exists. Skipping download.")
        return

    response = requests.get(url, stream=True, verify=False)
    with open(destination, 'wb') as file:
        for chunk in response.iter_content(chunk_size=128):
            file.write(chunk)

def download_single_thread():
    print(NAMES_AND_URLS.columns)

    for index, row in NAMES_AND_URLS.iterrows():
        # asset_id = row['assetId']
        category = row['category']
        name = row['name']
        url = row['fullVideo']

        url_id = row['fullVideo'].split('/')[-1][:-4]
        print(url_id)

        filename = "%s - %s [%s].mov" % (category,name,url_id)
        print(filename)

        download_file(url, "./videos/" + filename)
        # quit()

if __name__ == "__main__":
    download_single_thread()
