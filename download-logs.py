import urllib.request
import os


def download_log_file(url, dest_folder, filename):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
    dest_path = os.path.join(dest_folder, filename)
    urllib.request.urlretrieve(url, dest_path)
    print(f"Downloaded {filename} to {dest_folder}")


# URLs of sample log files
log_files = [
    {
        "url": "https://raw.githubusercontent.com/elastic/examples/master/Common%20Data%20Formats/apache_logs/apache_logs",
        "filename": "apache_logs.log"
    },
    {
        "url": "https://raw.githubusercontent.com/cloudera/hue/master/apps/log_analytics/examples/sample-hue-logs.log",
        "filename": "hue_logs.log"
    },
    {
        "url": "https://raw.githubusercontent.com/cloudera/hue/master/apps/log_analytics/examples/sample-oozie-logs.log",
        "filename": "oozie_logs.log"
    },
    {
        "url": "https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/corpora/webtext.zip",
        "filename": "webtext.zip"
    },
    {
        "url": "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-01.csv",
        "filename": "yellow_tripdata_2020-01.csv"
    }
]

dest_folder = "./tmp/sample_logs"

for log in log_files:
    try:
        download_log_file(log["url"], dest_folder, log["filename"])
    except urllib.error.HTTPError as e:
        print(f"Failed to download {log['filename']} from {log['url']}: {e}")
