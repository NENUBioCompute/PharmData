"""
  -*- encoding: utf-8 -*-
  @Author: Deepwind
  @Time  : 4/28/2024 5:35 PM
  @Email: deepwind32@163.com
"""
import math


def split_list(lst, num_splits):
    """
    Divide the list into sublists
    :param lst: target list
    :param num_splits: number of sublist
    :return: the list of sublist
    """
    n = len(lst)
    step = math.ceil(n / num_splits)
    result = []
    for i in range(0, n + 1, step):
        result.append(lst[i:i + step])
    return result
