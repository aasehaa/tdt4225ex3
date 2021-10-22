from datetime import time


def single_val(cursor, key : str):
    """Helper to easier get a single value out of a query result.
    Parses cursor to list and then to dictionary, which gets out the desired value 
    :param cursor: MongoDB result cursor
    :param key: Dictionary key of desired value
    :type key: str
    :return: Desired value
    """
    as_list = list(cursor)
    as_dict = dict(as_list[0])
    return as_dict[key]

def excel_to_posix(excel_timestamp : float) -> float:
    """Converts timestamps from Excel-style to the normal Unix/POSIX style
    We use the formula UnixTS = 86400(ExcelTS - 25569) - 7200, since there's 25569 between Dec 30, 1899
    and Jan 1, 1970, and there's 86400 seconds in a day. We also adjust time zone by adding 7200secs(2hr)

    :param excel_timestamp: Number of days passed since December 30th 1899
    :type excel_timestamp: float
    :param to_datetime: Determines wether or not the result should be float (False) or DateTime (True)
    :type to_datetime: bool
    :return: Datetime object, or time in seconds since January 1st 1970
    :rtype: float
    """
    return round(86400 * (float(excel_timestamp) - 25569) - 7200,0)

def posix_to_excel(posix_timestamp : float) -> float:
    return round(1/86400 * (float(posix_timestamp)+7200)+ 25569, 10)

def testing():
    from datetime import datetime
    time_dt = datetime.strptime('2009-10-11 14:04:30', '%Y-%m-%d %H:%M:%S')
    time_stamp = datetime.timestamp(time_dt)
    print("Using datetime, date in timestamp is", time_stamp, sep="\n")
    excel_time = 40097.5864583333
    time_stamp_fromexcel = excel_to_posix(excel_time)
    print("With function we get", time_stamp_fromexcel, sep="\n")
    print("Diff", abs(time_stamp_fromexcel - time_stamp))

    ## Inverse testing
    inv = posix_to_excel(time_stamp)
    print(excel_time-inv)
    # print(inv)
    print("new test")
    excel_time_inf = 39684.6513888889
    print("conv infected dude")
    posix_now = excel_to_posix(excel_time_inf) 
    datetime_now = datetime.fromtimestamp(posix_now)
    print(posix_now, datetime_now)

if __name__ == '__main__':
    testing()