import datetime as dt

DATE_FORMAT = "%d-%m-%Y"
ONE_DAY = dt.timedelta(days=1)

def from_string(text: str) -> dt.date:
    return dt.datetime.strptime(text, DATE_FORMAT)

def to_string(date: dt.date) -> str:
    return date.strftime(DATE_FORMAT)

def add_one_day(date: dt.date) -> dt.date:
    return date + ONE_DAY