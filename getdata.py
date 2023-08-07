#!/usr/bin/env python3
# @Author: Tanel Treuberg
# @Github: https://github.com/The-Magicians-Code
# @Description: Automate the process of getting data from a database API 
# by getting an updated CSV file from GitLab using GitLab API, processing it, 
# creating URLs with requested points and concatenating the results into a CSV file

from configparser import ConfigParser
from pathlib import Path
from io import StringIO
import pandas as pd
import numpy as np
import urllib.parse
import argparse
import datetime
import requests
import base64

parser = argparse.ArgumentParser(description="Get data from a database using a file with request data from GitLab")
parser.add_argument('--conf', default="settings.cfg", help="Configuration file, defaults to settings.cfg from the script directory")
args = parser.parse_args()

project_directory = Path(__file__).parent.resolve()

conf = ConfigParser()
if args.conf:
    config_path = Path(args.conf).__str__()
else:
    config_path = (project_directory / args.conf).__str__()

conf.read(config_path)
configuration = conf["global"]

# Create the URL for requesting the file
# https://docs.gitlab.com/ee/api/repository_files.html#get-file-from-repository
url = f'{configuration["url"]}projects/{configuration["project_id"]}/repository/files/{urllib.parse.quote_plus(configuration["source_path"])}?ref={configuration["branch"]}'

# Disable unverified request warning, since it will be printed every time a request is being made
ssl_verify = configuration.getboolean("ssl_verify")
if not ssl_verify:
    # InsecureRequestWarning: Unverified HTTPS request is being made to host 'www.mydb.local'. Adding certificate verification is strongly advised.
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

source = requests.get(
    url, 
    verify=ssl_verify, 
    headers={
        "PRIVATE-TOKEN": configuration["private_token"]
    }
).json()

# File is encoded in base64, decode it and use utf-8 for file content formatting, 
# StringIO for converting the resulting bytestring into a raw CSV string which is readable by pandas
df = pd.read_csv(StringIO(base64.b64decode(source["content"]).decode("utf-8")))
# Get the requested points from the file and format them correctly
requested_points = df["FromMwAnalogPointRef"].dropna().map('{:.0f}'.format).to_numpy()
# Split the list into chunks with a defined size to avoid URL length errors
sliced_requests = np.array_split(requested_points, np.arange(configuration.getint("chunk_size"), len(requested_points), configuration.getint("chunk_size")))
print(f"Number of requests: {len(sliced_requests)}")

# Prepare the query URL parameters
params = { 
    'start_time': pd.Timestamp(
        datetime.datetime.utcnow() - datetime.timedelta(minutes=configuration.getint("timedelta"))
    ).round("5T").isoformat(timespec="seconds"),
    # 'end_time': pd.Timestamp(
    #     datetime.datetime.now().astimezone(datetime.timezone.utc) - datetime.timedelta(minutes=5) # Endtime is specified when timedelta > 5T   
    # ).round("5T").tz_convert(None).isoformat(timespec="seconds").__str__(),
}

results = []
for index, points in enumerate(sliced_requests):
    # Assign the requested points to parameters
    params["scada_point"] = points

    if "end_time" not in params:
        params["end_time"] = params["start_time"]

    # Create URL for requests from the ISR API
    isr_query = f'{configuration["db_api"]}?' + urllib.parse.urlencode(params, doseq=True)
    print(f"QUERY {index + 1}:\n{isr_query}\n")

    # Request data
    isr_response = requests.get(isr_query, verify=False)
    try:
        data = isr_response.json()["data"]
    except requests.exceptions.JSONDecodeError as e:
        raise ValueError(
            f'Could not receive data from the server at current time: {params["start_time"], params["end_time"]}\n{isr_response.content}\nChange timedelta in {config_path}'
        )

    # Insert data to dataframe and add it to the list
    results.append(pd.DataFrame(data))

# Concatenate all results by column
endresult = pd.concat(results, axis=1)
# Convert the time index to local time and remove the difference with UTC time from the end of the timestamp
endresult.index = pd.to_datetime(endresult.index).tz_convert("Europe/Tallinn").tz_localize(None)
# Convert all timestamps to ISO8604 format
endresult.index = endresult.index.map(lambda x: x.isoformat(timespec='seconds'))
# Rename the index with the local time
endresult.index.name = "Europe/Tallinn"
print(f"RESULT:\n{endresult}\n\nUploading updated data to GitLab")

# Upload the data to GitLab
upload = requests.put(
    url=f'{configuration["url"]}projects/{configuration["project_id"]}/repository/files/{urllib.parse.quote_plus(configuration["upload_path"])}', 
    verify=ssl_verify, 
    headers={
        "PRIVATE-TOKEN": configuration["private_token"]
    },
    data={
        "branch": {configuration["branch"]},
        "commit_message": f"Automatic upload at: {pd.to_datetime(datetime.datetime.utcnow().isoformat(timespec='seconds'))} UTC",
        "content": endresult.to_csv(),
    }
).json()
print(f"Response:\n{upload}")
print("Done!")
