import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

logger = logging.getLogger("airflow.task")


def get_process_dates(
    date_format: str = "%Y%m%d", **kwargs: Dict[str, Any]
) -> List[str]:
    """
    Helper function that gets the list of dates to process
    and transform raw twitch data.

    When this function is called within a dag, provide_context
    must be set to True so that the task context gets passed
    to this function.

    This function will then look for start_date and end_date
    params. If None, it will return a list containing the
    date from 2 days ago. If these params are provided,
    a list will be returned containing all the dates between.

    Parameters
    ----------
    date_format: str
        The format to process inputs. This format will be used to
        save date outputs as well. Default is "%Y%m%d"
    **kwargs:
        This function needs to be called within a task with
        provide_context = True.

    Returns
    -------
    date_list: List[str]
        This list will return the list of dates to to process
        the raw twitch data. Dates will be represented as
        "YYYYMMDD"
    """

    # get start_date and end_date params
    # will be None if not there
    params = kwargs.get("params", {})
    start_date = params.get("start_date")
    end_date = params.get("end_date")

    logger.info(f"Date params: start_date: {start_date} end_date: {end_date}")

    # if either start_date or end_date is None, use 2 days ago
    # this will give us a 1 day buffer
    if start_date is None or end_date is None:
        logger.info("Date params are None. Setting to 2 days ago")
        two_days_ago_date = datetime.utcnow() - timedelta(days=2)
        return [two_days_ago_date.strftime(date_format)]

    # if date_params were provided, create dates list
    date_list = get_date_range(start_date, end_date, date_format)

    return date_list


def get_date_range(start_date, end_date, date_format):
    """
    Generates a list of dates between the specified start_date and end_date, inclusive.

    This function takes two date strings and returns a list of date strings between them in the
    specified format. The start_date and end_date must be in the same format, and the function
    ensures that the end_date is after the start_date. If the end_date is before the start_date,
    a ValueError will be raised.

    Parameters
    ----------
    start_date : str
        The start date in the format specified by date_format (e.g., "YYYYMMDD").

    end_date : str
        The end date in the format specified by date_format (e.g., "YYYYMMDD").

    date_format : str
        The date format used to parse the start_date and end_date, and also to return the date strings
        in the final list (e.g., "%Y%m%d").

    Returns
    -------
    List[str]
        A list of date strings in the specified date_format, representing each date between
        start_date and end_date, inclusive.

    Raises
    ------
    ValueError
        If the end_date is earlier than the start_date, a ValueError will be raised.
        If the start_date or end_date cannot be parsed using the specified date_format, a ValueError
        will also be raised.

    Examples
    --------
    >>> get_date_range("20230101", "20230105", "%Y%m%d")
    ['20230101', '20230102', '20230103', '20230104', '20230105']
    """

    # parse our dates
    start_date = datetime.strptime(start_date, date_format)
    end_date = datetime.strptime(end_date, date_format)

    if end_date < start_date:
        raise ValueError("End date must be after start date")

    # create our list of dates - there's prolly a better way to do this ha
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime(date_format))
        current_date += timedelta(days=1)

    return date_list
