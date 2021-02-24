from contextlib import closing
import csv
from datetime import datetime
import os
import boto3
import requests


CSV_URL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
S3_CLIENT = boto3.client('s3')
S3_BUCKET_NAME = os.getenv('PROJECT_NAME')


class Pipe:
    value = ""
    def write(self, text):
        self.value = self.value + text


def write_to_s3(key, content):
    params = {
        'Bucket': S3_BUCKET_NAME,
        'ACL': 'public-read',
        'Key': key,
        'Body': content,
    }
    return S3_CLIENT.put_object(**params)


def slice_columns(row):
    return {
        'date': row['date'],
        'cases': row['cases'],
        'deaths': row['deaths'],
    }


def to_csv(data):
    pipe = Pipe()
    headers = data[0].keys()
    writer = csv.DictWriter(pipe, headers)
    writer.writeheader()
    for row in data:
        writer.writerow(row)

    return pipe.value


def handle_state(mo_data):

    state_csv = to_csv(mo_data)
    file_name = f"missouri-counties.csv"

    return write_to_s3(file_name, state_csv)


def handle_county(county_name, mo_data):

    county_data = [slice_columns(r) for r in mo_data if r['county'] == county_name]

    county_csv = to_csv(county_data)

    file_name = f"{county_name.lower()}.csv"

    return write_to_s3(file_name, county_csv)


def main():
    with closing(requests.get(CSV_URL, stream=True)) as r:
        r.raise_for_status()
        f = (line.decode('utf-8') for line in r.iter_lines())
        reader = csv.DictReader(f)

        mo_data = []

        for row in reader:
            if row['state'] == "Missouri":
                mo_data.append(row)

    handle_state(mo_data)
    handle_county('Boone', mo_data)
    handle_county('Cole', mo_data)
    handle_county('Callaway', mo_data)
    handle_county('Audrain', mo_data)
    handle_county('Randolph', mo_data)
    handle_county('Cooper', mo_data)
    handle_county('Moniteau', mo_data)


def lambda_handler(event, context):
    main()


if __name__ == '__main__':
    main()
