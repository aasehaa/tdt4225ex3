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
