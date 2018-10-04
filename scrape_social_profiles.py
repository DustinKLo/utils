import requests
import MySQLdb
from bs4 import BeautifulSoup
import csv
import re
from pprint import pprint


PRAYPROD_CONFIG = {
    'host': "", 
    'passwd': "", 
    'db': "", 
    'user': ""
}
prayprod = MySQLdb.connect(**PRAYPROD_CONFIG)

query = 'SELECT * FROM table'
cur = prayprod.cursor()
cur.execute(query)
headers = [desc[0] for desc in cur.description]
orgs = cur.fetchall()
prayprod.close()


csv_headers = []


CSV_FILE_NAME = 'social_profile.csv'
ERROR_FILE_NAME = 'errors.csv'


f1 = open(CSV_FILE_NAME, 'w')
csv_1 = csv.writer(f1)
csv_1.writerow(csv_headers)

f2 = open(ERROR_FILE_NAME, 'w')
csv_2 = csv.writer(f2)


counter = 0
for row in orgs:
    existing_links = []
    url = row[4]
    org_id = row[2]
    if 'https://' not in url and 'http://' not in url:
        url = 'https://' + url

    try:
        res = requests.get(url, timeout=30)
        
        body = res.text
        soup = BeautifulSoup(body, features="lxml")
        for a in soup.find_all('a', href=True):
            href = a['href'].strip()
            if 'facebook' in href or 'instagram' in href or 'twitter' in href:
                href_match = re.match(r'(http|https):\/\/(www\.)?(twitter|instagram|facebook).com\/[^?\/]+', href).group()
                slug = href_match.split('/')[-1]

                if ('instagram' in href and slug[0] == 'B') or \
                   ('twitter' in href and slug == 'home') or \
                   ('facebook' in href and slug == 'events') or \
                   href == 'https://www.instagram.com/p':
                    continue

                csv_row = list(row) + [href_match]
                if href_match not in existing_links:
                    csv_1.writerow(csv_row)
                    existing_links.append(href_match)
                    f1.flush()
    
    except Exception as e:
        print("org id: {} website: {} social_profile: {} did not work".format(org_id, url, href))
        csv_2.writerow([org_id, url, href, e])
        f2.flush()

    counter += 1
    if counter % 100 == 0:
        print('{} orgs processed'.format(counter))
