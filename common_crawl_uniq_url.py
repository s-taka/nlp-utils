import csv
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import time
import os
import re
import argparse

# url file from Athena
# Ex:
#  SELECT url, url_host_name, url_host_tld, content_mime_type, content_mime_detected, content_charset, content_languages, warc_filename, warc_record_offset, warc_record_length, warc_segment, crawl, subset
#  FROM "ccindex"."ccindex"
#  WHERE
#    subset = 'warc'
#    AND (url_host_tld = 'jp')
#    AND (url_host_registered_domain = 'staka.jp')
#  LIMIT 30000000


def main():
    parser = argparse.ArgumentParser(description='uniq url for Athena output')
    parser.add_argument('in_file', help='athena output')
    parser.add_argument('out_file', help='uniq url [for common_crawl_getgz.py]')

    args = parser.parse_args()

    in_file = args.in_file
    out_file = args.out_file

    f = open(in_file, 'r')
    csv_in = csv.reader(f, dialect='excel')

    start = time.time()
    url_info = {}
    for i, h in enumerate(csv_in):
        if i == 0:
            continue
        if i % 1000000 == 0:
            print("now proc = {} time={} sec".format(i, time.time() - start))
        idx = i
        warc_filename = h[7]
        warc_record_offset = int(h[8])
        warc_record_length = int(h[9])
        url_host_name = h[1]
        url = h[0]
        crawl = h[11]
        if url not in url_info:
            url_info[url] = (crawl,
                             {"idx": idx, "warc_filename": warc_filename, "warc_record_offset": warc_record_offset,
                              "warc_record_length": warc_record_length, "url_host_name": url_host_name})
        else:
            if crawl > url_info[url][0]:
                url_info[url] = (crawl,
                                 {"idx": idx, "warc_filename": warc_filename, "warc_record_offset": warc_record_offset,
                                  "warc_record_length": warc_record_length, "url_host_name": url_host_name})

    with open(out_file, "w") as csv_out:
        csv_out.write('"{}","{}","{}","{}","{}","{}","{}"\n'.format("url", "crawl", "idx", "warc_filename", "warc_record_offset",
                                                      "warc_record_length", "url_host_name"))
        for k in url_info:
            csv_out.write('"{}","{}","{}","{}","{}","{}","{}"\n'.format(k, url_info[k][0], url_info[k][1]["idx"],
                                                          url_info[k][1]["warc_filename"],
                                                          url_info[k][1]["warc_record_offset"],
                                                          url_info[k][1]["warc_record_length"],
                                                          url_info[k][1]["url_host_name"]))
    return


if __name__ == "__main__":
    main()
