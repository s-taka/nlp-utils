import csv
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import time
import os
import argparse

def main():
    parser = argparse.ArgumentParser(description='get gz files from common crawl')
    parser.add_argument('in_file', help='uniq url [from common_crawl_uniq_url.py]')
    parser.add_argument('out_dir', help='out_pur dir')
    parser.add_argument('--wait_100', help='wait seconds per 100 itr', default=3.0, dest='sleep100', type=float)
    parser.add_argument('--wait', help='wait seconds per 1 itr', default=0.1, dest='sleep1', type=float)
    parser.add_argument('--files_per_dir', help='limit files in a dir', default=10000, dest='fpd', type=int)

    args = parser.parse_args()

    in_file = args.in_file
    out_dir = args.out_dir
    sleep_by_100 = args.sleep100
    sleep_by_1 = args.sleep1
    files_per_dir = args.fpd

    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    f = open(in_file, 'r')
    csv_in = csv.reader(f, dialect='excel')

    start = time.time()
    dir_name = ""
    for i, h in enumerate(csv_in):

        if i == 0:
            continue
        dir_name = "{}/{}".format(out_dir, int(i / files_per_dir))
        if not os.path.exists(dir_name):
            os.mkdir(dir_name)
        try:
            idx = i
            warc_filename = h[3]
            warc_record_offset = int(h[4])
            warc_record_length = int(h[5])
            url_host_name = h[6]
        except:
            print("error i={}".format(i))
            continue

        print("{}, {}, {}, {}, {}".format(idx, warc_filename, warc_record_offset, warc_record_length, url_host_name))

        offset_end = warc_record_offset + warc_record_length - 1
        byte_range = 'bytes={offset}-{end}'.format(offset=warc_record_offset, end=offset_end)
        gzipped_text = s3.get_object(Bucket='commoncrawl', Key=warc_filename, Range=byte_range)['Body'].read()

        # The requested file in GZIP
        gz_filename = "{}/{}_{}.gz".format(dir_name, idx, url_host_name)
        with open(gz_filename, 'wb') as f:
            f.write(gzipped_text)

        time.sleep(sleep_by_1)
        if i % 100 == 0:
            print("count={} / elapsed time={} sec".format(i, time.time() - start))
            time.sleep(sleep_by_100)
            print("count={} / elapsed time={} sec".format(i, time.time() - start))

    return


if __name__ == "__main__":
    main()
