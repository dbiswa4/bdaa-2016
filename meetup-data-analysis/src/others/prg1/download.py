import pandas as pd
import requests
import zipfile

if __name__ == '__main__':
    print 'Download Dataset'
    # The direct link to the Kaggle data set
    data_url = 'https://www.kaggle.com/saurograndi/airplane-crashes-since-1908/downloads/Airplane_Crashes_and_Fatalities_Since_1908.csv.zip'

    local_file_path = "/Users/Documents/source_code/Python/ms-big-data-app/src/prog1/Airplane_Crashes_and_Fatalities_Since_1908.csv.zip"

    local_filename = "Airplane_Crashes_and_Fatalities_Since_1908.csv.zip"


    # Kaggle Username and Password
    kaggle_info = {'UserName': "userid", 'Password': "password"}

    # Attempts to download the CSV file. Gets rejected because we are not logged in.
    r = requests.get(data_url)

    # Login to Kaggle and retrieve the data.
    r = requests.post(r.url, data=kaggle_info)


    # Writes the data to a local file one chunk at a time.
    f = open(local_filename, 'w')
    for chunk in r.iter_content(chunk_size=512 * 1024):  # Reads 512KB at a time into memory
        if chunk:  # filter out keep-alive new chunks
            f.write(chunk)
    f.close()

    # Unzip dowloaded dataset file
    zip = zipfile.ZipFile(local_file_path)
    zip.extractall()