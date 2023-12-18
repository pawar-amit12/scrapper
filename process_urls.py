import argparse
import logging
import os
from warcio.capture_http import capture_http
from warcio import WARCWriter
import boto3
from io import BytesIO
from urllib.parse import urlparse
import requests

# Initialize logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#input_urls /Users/amitpawar/code/warc/urls.txt
#output_location s3://bq-dev-ss-data/daily_data/warc_poc/
# Argument parsing setup

parser = argparse.ArgumentParser(description='Process URLs and store them in WARC format in a specified location.')
parser.add_argument('--input_urls', required=True, help='File containing URLs or a comma-separated string of URLs')
parser.add_argument('--output_location', required=True, help='Output location for the WARC files, either a local path or an S3 bucket (e.g., file://path/to/dir or s3://bucket-name)')

# Parse arguments
args = parser.parse_args()


def process_urls(input_urls, output_location):
    """
    Process URLs and store them in a single WARC file at the specified output location.
    """
    # Determine if input is a file or a string of comma-separated URLs
    if os.path.isfile(input_urls):
        with open(input_urls, 'r') as file:
            urls = [line.strip() for line in file]
    else:
        urls = input_urls.split(',')

    # Parse the output location    
    parsed_output_location = urlparse(output_location)
    logger.info(f"parsed_output_location: {parsed_output_location}")
    if parsed_output_location.scheme == 'file':
        output_dir = parsed_output_location.path
        s3_bucket = None
    elif parsed_output_location.scheme == 's3':
        s3_bucket = parsed_output_location.netloc
        logger.info(f"s3_bucket: {s3_bucket}")
        output_dir = None
    else:
        logger.error('Invalid output location scheme. Use file:// for local paths or s3:// for S3 buckets.')
        return

    # Generate a filename for the WARC file
    warc_filename = "crawled_urls.warc.gz"

    # Initialize WARCWriter
    if output_dir:
        warc_path = os.path.join(output_dir, warc_filename)
        warc_file = open(warc_path, 'wb')
    else:
        warc_file = BytesIO()

    writer = WARCWriter(warc_file, gzip=True)

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; Storebot-Google/1.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36'
    }

    for url in urls:
        try:
            logger.info(f"Capturing URL: {url}")
            with capture_http(writer, warc_version='1.1'):
                response = requests.get(url, headers=headers, verify=False)
                logging.info(f"Response: {response}")
                if response.status_code == 200:
                    logger.info(f"Captured response for URL: {url}")
                else:
                    logger.info(f"Request failed with response code: {response.status_code}")
        except (requests.RequestException, IOError) as e:
            logger.error(f"Error processing URL {url}: {e}")

    # Close the WARC file if writing locally
    if output_dir:
        warc_file.close()
        logger.info(f"Stored WARC file locally: {warc_path}")
    # If an S3 bucket is specified, upload the WARC file to S3
    elif s3_bucket:
        s3_client = boto3.client('s3')
        warc_file.seek(0)
        s3_client.upload_fileobj(warc_file, s3_bucket, warc_filename)
        logger.info(f"Stored WARC file to S3: s3://{s3_bucket}{warc_filename}")


if __name__ == '__main__':
    # Perform the processing of URLs
    process_urls(args.input_urls, args.output_location)