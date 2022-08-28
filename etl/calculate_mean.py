"""Calculate mean ratings"""

import pandas as pd


def calculate_mean_by_col(data: pd.DataFrame, groupby_cols: list[str], groupby_on: str):
    """_summary_

    :param data: Dataframe to calculate mean from
    :type data: pd.DataFrame
    :param groupby_cols: Columns to group by
    :type groupby_cols: list[str]
    :param groupby_on: Column to calculate mean of
    :type groupby_on: str
    :return: Dataframe with mean values
    :rtype: _type_
    """

    grouped = data.groupby(by=groupby_cols, as_index=False)[groupby_on].mean()

    return grouped
