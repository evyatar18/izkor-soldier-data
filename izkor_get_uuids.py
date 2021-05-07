import requests
import codecs

import izkor_date_utils as dates

def write_iterable(i, into):
    for elem in i:
        into.write(elem)
        into.write('\n')


# https://www.izkor.gov.il/search/date/1-1-1800/27-4-2020/7999/100/
# https://izkorblobcdn.azureedge.net/fallenjson/en_914b97e34e6a752fead4eced44469457.json
# https://www.izkor.gov.il/search/LifeStoriesPagination/en_dd4e71786acc7f4e797eb44e9bdc338b/person_az_sortedlist

def crawl_date(from_date, last_date):
    # https://www.izkor.gov.il/search/date/{0}/28-04-2020/0/10000/
    crawled_url = "https://www.izkor.gov.il/search/date/{0}/{1}/0/10000/".format(dates.tostring(from_date), dates.to_string(last_date))
    print("crawling {0}".format(crawled_url))
    r = requests.get(crawled_url)
    dct = r.json()
    data = dct['data']

    last_date = from_date
    crawled = set()
    not_added = set()

    for elem in data:
        uuid = elem['uuid']
        death_date = dates.from_string(elem['death_date'])

        if death_date > last_date:
            last_date = death_date
            crawled = crawled.union(not_added)
            not_added = set()

        not_added.add(uuid)

    return last_date, crawled

def crawl_loop(file):
    total_crawled = 0
    current_date = dates.from_string('1-1-1800')
    last_date = dates.from_string('28-04-2020')

    while current_date < last_date:
        next_date, _crawled = crawl_date(current_date, last_date)
        write_iterable(_crawled, file)
        file.flush()

        total_crawled += len(_crawled)
        print("crawled {0} now. crawled {1} in total.".format(len(_crawled), total_crawled))

        if current_date == next_date:
            next_date = dates.add_one_day(next_date)

        current_date = next_date

    file.write(str(total_crawled))

file = open("izkor.txt", "w")
crawl_loop(file)
file.close()

