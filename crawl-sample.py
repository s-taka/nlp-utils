import pprint as pp
import re
import os, sys
import time
import urllib
import urllib.request
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import sqlite3
import random
from selenium import webdriver

# export PATH="$PATH:/home/ml/PycharmProjects/nlp/crawl/bin/"
# create table crawl_data(url text, reg_url text, status text, content text, priority int);
# create index url_idx on crawl_data(url);
# create index reg_url on crawl_data(reg_url);

driver = webdriver.Firefox()

def get_reg_url(url):
    ret_url = url
    ret_url = re.sub("^https?://(www.|web)?", "", ret_url)
    if not re.search("(entry_id|year|page)", ret_url):
        ret_url = re.sub("\?.*$", "", ret_url)
    ret_url = re.sub("\/\/", "/", ret_url)
    ret_url = re.sub("\/$", "", ret_url)
    ret_url = re.sub(".php$", "", ret_url)
    ret_url = re.sub("#[^#^/]+$", "", ret_url)
    return "REG:" + ret_url

def get_html_a_list(url):
    driver.get(url)
    driver.implicitly_wait(30)
    html = driver.page_source.encode('utf-8')
    soup = BeautifulSoup(html, 'html.parser')
    a_list = []
    for a_tag in soup.findAll('a'):
        try:
            a_tgt = urljoin(url, a_tag.attrs['href'])
            if re.search("^http", a_tgt):
                a_list.append(a_tgt)
        except:
            print("cannot get href [{}]".format(a_tag))
            continue
    return html, a_list

def _filter_conf(a, filter_conf):
    for fl in filter_conf:
        if a.find(fl) != -1:
            return True
    return False

def filter_a_list(a_list, filter_conf, db_con):
    # filter with filter_conf
    # uniq by reg_url and find_status
    processed = {}
    ret = []
    for a in filter(lambda x: _filter_conf(x, filter_conf), a_list):
        if re.search("(pdf|PDF|jpg|jpeg|JPG|gif|GIF|png|PNG|svg|SVG|zip|ZIP)$", a):
            continue
        reg_a = get_reg_url(a)
        if reg_a in processed:
            continue
        processed[reg_a] = True
        status = db_con.execute('SELECT status FROM crawl_data WHERE reg_url=?;', (reg_a,)).fetchone()
        if status:
            continue
        ret.append((a, reg_a))
    return ret

def main():
    if len(sys.argv) != 3:
        print("Usage: $ python %s db_file urlfilter_file" % sys.argv[0])
        quit()

    db_file = sys.argv[1]
    urlfilter_file = sys.argv[2]

    db_con = sqlite3.connect(db_file)

    filter_list = []
    for idx, line in enumerate(open(urlfilter_file)):
        if idx == 0:
            continue
        filter_list.append(line.rstrip())
    start = time.time()
    for i in range(100000):
        time.sleep(2 + random.randint(0, 10))
        ret = db_con.execute('SELECT url, reg_url,status FROM crawl_data WHERE status=? order by priority, random() limit 2;',
                             ('-',)).fetchone()
        if ret:
            url = ret[0]
            reg_url = ret[1]
            status = ret[2]
            print("now process [{}] / {} sec".format(url, time.time() - start))
            if not re.search("^https?\:\/\/", url) or re.search("^https?\:\/\/(twitt|social|www\.face)", url):
                print("err")
                ret = db_con.execute('UPDATE crawl_data set status=?, content=? WHERE url=?;',
                                 ('err', '', url)).fetchone()
                db_con.commit()
                continue
            try:
                str_html, a_list = get_html_a_list(url)
                add_a_list = filter_a_list(a_list, filter_list, db_con)
                ret = db_con.execute('UPDATE crawl_data set status=?, content=? WHERE url=?;',
                                 ('ok',str_html, url)).fetchone()
                db_con.commit()
                for a in add_a_list:
                    add_url = a[0]
                    add_reg_url = a[1]
                    ret = db_con.execute('insert into crawl_data(url, reg_url, status, priority) values (?, ?, ?, 2);',
                                     (add_url, add_reg_url, '-')).fetchone()
                    db_con.commit()
            except urllib.error.URLError as e:
                print(e.reason)
            except:
                ret = db_con.execute('UPDATE crawl_data set status=?, content=? WHERE url=?;',
                                 ('err', '', url)).fetchone()
                db_con.commit()
                print("Unexpected error:", sys.exc_info())

        else:
            print("end process  / {} sec".format( time.time() - start))
            break


    return


if __name__ == "__main__":
    main()
