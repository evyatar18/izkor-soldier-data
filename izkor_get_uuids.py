import requests
import codecs

def write_iterable(i, into):
    for elem in i:
        into.write(elem)
        into.write('\n')


# https://www.izkor.gov.il/search/date/1-1-1800/27-4-2020/7999/100/
# https://izkorblobcdn.azureedge.net/fallenjson/en_914b97e34e6a752fead4eced44469457.json
# https://www.izkor.gov.il/search/LifeStoriesPagination/en_dd4e71786acc7f4e797eb44e9bdc338b/person_az_sortedlist
def get_uuids(uuid):
    req = requests.get("https://www.izkor.gov.il/search/LifeStoriesPagination/{0}/person_az_sortedlist".format(uuid))
    data = req.json()['data']

    return data['prev']['uuid'], data['next']['uuid']

def loop(file):
    crawled = set()
    queued = {"en_c6c7695e7ef139550123929209272d0e"}
    unsaved = []

    def queue_up(uuid):
        if not (uuid in crawled):
            queued.add(uuid)

    def print_crawled():
        print("crawled in total: {0}".format(len(crawled)))
    
    n = 23817
    while n > 0 and len(queued) > 0:
        if n % 50 == 0:
            print_crawled()
            writeIterable(unsaved, file)
            file.flush()
            unsaved = []
        
        uuid = queued.pop()

        prv, nxt = get_uuids(uuid)

        queue_up(nxt)
        queue_up(prv)

        crawled.add(uuid)
        unsaved.append(uuid)
        n = n - 1

    print_crawled()


def add_one_day(dt):
    from datetime import timedelta
    return dt + timedelta(days=1)

def from_string(st):
    from datetime import datetime
    
    return datetime.strptime(st, "%d-%m-%Y")

def to_string(dt):
    return dt.strftime("%d-%m-%Y")


def crawl_date(from_date, last_date):
    # https://www.izkor.gov.il/search/date/{0}/28-04-2020/0/10000/
    crawled_url = "https://www.izkor.gov.il/search/date/{0}/{1}/0/10000/".format(to_string(from_date), to_string(last_date))
    print("crawling {0}".format(crawled_url))
    r = requests.get(crawled_url)
    dct = r.json()
    data = dct['data']

    last_date = from_date
    crawled = set()
    not_added = set()
    
    for elem in data:
        uuid = elem['uuid']
        death_date = from_string(elem['death_date'])

        if death_date > last_date:
            last_date = death_date
            crawled = crawled.union(not_added)
            not_added = set()

        not_added.add(uuid)
    
    return last_date, crawled

def crawl_loop(file):
    total_crawled = 0
    current_date = from_string('1-1-1800')
    last_date = from_string('28-04-2020')

    while current_date < last_date:
        next_date, _crawled = crawl_date(current_date, last_date)
        write_iterable(_crawled, file)
        file.flush()

        total_crawled += len(_crawled)
        print("crawled {0} now. crawled {1} in total.".format(len(_crawled), total_crawled))

        if current_date == next_date:
            next_date = add_one_day(next_date)
        
        current_date = next_date

    file.write(str(total_crawled))

file = open("izkor.txt", "w")
crawl_loop(file)
file.close()
    
