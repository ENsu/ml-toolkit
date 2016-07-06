import pandas as pd


def transform_to_dummies(df, columns_list):
    for i in columns_list:
        df = pd.concat([df.drop([i], axis=1), pd.get_dummies(df[i], prefix=str(i))], axis=1)
    return df


def make_same_columns(df1, df2):  # make valid_ouput same columns with input_feature
    columns = df2.columns
    df1['df'] = 'df1'
    df2['df'] = 'df2'
    tmp = pd.concat([df1, df2.iloc[:1, :]])
    df1.drop(['df'], axis=1, inplace=True)
    df2.drop(['df'], axis=1, inplace=True)
    return tmp[tmp['df'] == 'df1'][columns]