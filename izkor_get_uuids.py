import datetime as dt
import functools

import requests
import codecs

import izkor_date_utils as dates


# the program assumes that MAX_IZKORS_A_DAY < MAX_RECORDS_ALLOWED_PER_ONE_REQUEST
# also assumes that the memory is able to store a response of size MAX_RECORDS_ALLOWED_PER_ONE_REQUEST
MAX_IZKORS_A_DAY = 1000
MAX_RECORDS_ALLOWED_PER_ONE_REQUEST = 10000
RECORDS_PER_REQUEST = max(MAX_IZKORS_A_DAY, MAX_RECORDS_ALLOWED_PER_ONE_REQUEST)

def make_uuids_url(from_date: dt.date, to_date: dt.date) -> str:
    uuid_url = "https://www.izkor.gov.il/search/date/{0}/{1}/0/{2}/".format(
        dates.to_string(from_date),
        dates.to_string(last_date),
        MAX_RECORDS_ALLOWED_PER_ONE_REQUEST
    )

    return uuid_url

def make_uuids_request(from_date: dt.date, to_date: dt.date) -> requests.Response:
    uuid_url = make_uuids_url(from_date, to_date)

    try:
        response = requests.get(crawled_url)
        return response
    except requests.RequestException as e:
        raise IzkorRequestException() from e

def parse_response(response: requests.Response) -> List[Dict]:
    try:
        json = response.json()
        soldiers = json["data"]

        return soldiers
    except ValueError as e:
        raise IzkorRequestException("Response body is not a json") from e

def extract_uuids(soldiers: List[Dict]) -> List[str]:
    return list(
        map(
            lambda soldier: soldier["uuid"],
            soldiers
        )
    )

def find_last_fallen_soldier_date(soliders: List[Dict]) -> dt.date:
    dates = map(
        lambda soldier: dates.from_string(soldier["death_date"]),
        soldiers
    )

    last_date = functools.reduce(
        min,
        dates
    )

    return last_date

def request_uuids_between(from_date: dt.date, to_date: dt.date) -> Tuple[Iterable[str], dt.date]:
    response = make_uuids_request(from_date, to_date)
    soldiers = parse_response(response)

    soldier_uuids = extract_uuids(soldiers)
    last_fallen_date = find_last_fallen_soldier_date(soldiers)

    return soldier_uuids, last_fallen_date

def write_fallen_uuids(from_date: dt.date, to_date: dt.date, into: file)
def write_iterable(i, into):
    for elem in i:
        into.write(elem)
        into.write('\n')


# https://www.izkor.gov.il/search/date/1-1-1800/27-4-2020/7999/100/
# https://izkorblobcdn.azureedge.net/fallenjson/en_914b97e34e6a752fead4eced44469457.json
# https://www.izkor.gov.il/search/LifeStoriesPagination/en_dd4e71786acc7f4e797eb44e9bdc338b/person_az_sortedlist

def crawl_date(from_date, last_date):
    # https://www.izkor.gov.il/search/date/{0}/28-04-2020/0/10000/
    crawled_url = "https://www.izkor.gov.il/search/date/{0}/{1}/0/10000/".format(dates.to_string(from_date), dates.to_string(last_date))
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
    last_date = dates.from_string('12-12-2021')

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

file = open("lol.txt", "w")
crawl_loop(file)
file.close()

