import pandas as pd
import numpy as np


def check_element_wise(df, column1, column2, combine):
    combine_len = len(df[combine].unique())
    first_len = len(df[column1].unique())
    second_len = len(df[column2].unique())
    print "first_unique: %s, second_unique: %s, combine_unique: %s" % (first_len, second_len, combine_len)
    if combine_len == second_len:
        return True
    else:
        # column2count = combine_count.groupby([column2]).size()
        # print 'max number of [%s] corresponding to the same [%s] is %s' % (column1, column2, column2count.max())
        # print 'number of [%s] which corresponds to multiple [%s] is %s' % (column2, column1, len(column2count[column2count > 1]))
        return False


def overlap_frac(df1, df2, column_name):
    print "first df %s num: %s" % (column_name, len(df1[column_name].unique()))
    print "second df %s num: %s" % (column_name, len(df2[column_name].unique()))
    print "num of overlap: %s" % len(np.intersect1d(df1[column_name].unique(), df2[column_name].unique()))
