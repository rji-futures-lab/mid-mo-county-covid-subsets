from csv import DictReader, DictWriter
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


def parse_text(text):
    return DictReader(
        [l for l in text.split('\n')],
    )


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
    writer = DictWriter(pipe, headers)
    writer.writeheader()
    for row in data:
        writer.writerow(row)

    return pipe.value


def handle_county(county_name, mo_data):

    county_data = [slice_columns(r) for r in mo_data if r['county'] == county_name]

    county_csv = to_csv(county_data)

    file_name = f"{county_name.lower()}.csv"

    return write_to_s3(file_name, county_csv)


def main():
    r = requests.get(CSV_URL)
    r.raise_for_status()

    data = parse_text(r.text)

    mo_data = [
        r for r in data if r['state'] == 'Missouri'
        ]

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
