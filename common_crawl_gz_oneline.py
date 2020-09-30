from zipfile import ZipFile
import re
import gzip
import os
import csv
import time
import sys

import argparse

def to_one_line(txt):
    return txt.replace("\r", "<CR>").replace("\n", "<LF>").replace("\t", "<TAB>")

def read_metadata(file_name):
    f = open(file_name, 'r')
    csv_in = csv.reader(f, dialect='excel')
    ret = {}
    for i, h in enumerate(csv_in):
        if i == 0:
            continue
        idx = i
        url_host_name = h[6]
        url = h[0]
        gz_filename = "{}_{}.gz".format(idx, url_host_name)
        ret[gz_filename] = url
    return ret


def main():
    parser = argparse.ArgumentParser(description='output one line per one gz file ')
    parser.add_argument('data_dir', help='data dir')
    parser.add_argument('metadata', help='metadata [from common_crawl_uniq_url.py]')

    args = parser.parse_args()

    data_dir = args.data_dir
    metadata_csv = args.metadata

    filename_url = read_metadata(metadata_csv)

    for idx, filename in enumerate(os.listdir(data_dir)):
        match = re.search(r"([^/]+\.gz)$", filename)
        if match:
            jagz_id = match.group(1)
            url = jagz_id
            if jagz_id in filename_url:
                url = filename_url[jagz_id]
            with gzip.open(os.path.join(data_dir, filename)) as gz_file:
                try:
                    gzdata = gz_file.read()
                    ja_html = gzdata.decode(encoding='utf-8')
                except:
                    try:
                        ja_html = gzdata.decode(encoding='euc-jp')
                    except:
                        try:
                            ja_html = gzdata.decode(encoding='cp932')
                        except:
                            print("{}\t{}\t{}\t{}".format(url, "encode_err", "not_zip", filename))
                            continue
                print("{}\t{}\t{}\t{}".format(url, to_one_line(ja_html), "not_zip", filename))

    return


if __name__ == "__main__":
    main()
