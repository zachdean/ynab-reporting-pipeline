import datetime


def add_month(date: datetime.datetime, delta: int) -> datetime.datetime:
    """Adds a month to a datetime object

    :param datetime.datetime date:
      A datetime object to add a month to.
    """

    new_year = date.year
    new_month = date.month + delta

    if new_month > 12:
        new_year += 1
        new_month -= 12

    if new_month < 1:
        new_year -= 1
        new_month += 12

    # Handle cases where the resulting month doesn't have the same number of days as the original month
    if date.day > 28 and new_month != date.month:
        last_day_of_month = (date.replace(
            day=1, month=new_month + 1) - datetime.timedelta(days=1)).day
        new_day = min(date.day, last_day_of_month)
    else:
        new_day = date.day

    return date.replace(year=new_year, month=new_month, day=new_day)
