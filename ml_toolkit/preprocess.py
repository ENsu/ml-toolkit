import pandas as pd
import re


def transform_to_dummies(df, columns_list):
    for i in columns_list:
        df = pd.concat([df.drop([i], axis=1), pd.get_dummies(df[i], prefix=str(i)).astype(bool)], axis=1)
    return df


def make_same_columns(df1, df2):  # make valid_ouput same columns with input_feature
    columns = df2.columns
    df1['df'] = 'df1'
    df2['df'] = 'df2'
    tmp = pd.concat([df1, df2.iloc[:1, :]])
    df1.drop(['df'], axis=1, inplace=True)
    df2.drop(['df'], axis=1, inplace=True)
    return tmp[tmp['df'] == 'df1'][columns]


def _to_vw_format_float_or_boolean(input, input_v):
    if isinstance(input_v, bool):
        if input_v:
            return " %s" % input
        else:
            return ""
    else:
        return " %s:%s" % (input, input_v)


def _trans_row_to_vw_format(row, label_col=None, namespace_dict=None):
    row_dict = row.to_dict()
    if label_col:
        label_v = row_dict[label_col]
        del row_dict[label_col]
    namespace_str_list = []
    for namespace in namespace_dict:
        namespace_str = "".join(_to_vw_format_float_or_boolean(i, row_dict[i]) for i in namespace_dict[namespace])
        namespace_str = "|%s%s" % (namespace, namespace_str)
        namespace_str_list.append(namespace_str)
    namespace_str = " ".join(namespace_str_list)
    if label_col:
        namespace_str = "%s %s" % (label_v, namespace_str)
    return namespace_str


def to_vw_file(df, file_name, label_col=None, namespace_dict=None):
    '''
    namespace_dict_example = {"name_space1": ["feature1", "feature2"],
                              "name_space2": ["feature3", "feature4"]}
    only features in the namespace_dict are used
    '''
    tmp = df.apply(_trans_row_to_vw_format, axis=1, label_col=label_col, namespace_dict=namespace_dict) 
    with open(file_name, 'w') as f:
        for i in tmp.values:
            f.write(i)
            f.write("\n")
    return None


def read_vw_prob(file_name):
    df = pd.read_csv("probs.txt", header=None, delim_whitespace=True)
    df = df.applymap(lambda x: x.split(":")[1])
    return df


def read_vw_invert_hash(file_name, class_size):
    df = pd.DataFrame(columns=["var"]+range(int(class_size)))
    with open(file_name) as f:
        current_var = ""
        current_dict = {}
        for l in f.readlines()[11:]:  # skip the first 10 lines
            (new_var, hash_var, weight) = l.replace("\n", "").split(":")
            m = re.search(r".*(\[\d+\])", new_var)
            if m:
                new_var = new_var[:-1*len(m.group(1))]
                class_num = int(m.group(1)[1:-1])
            else:
                class_num = 0
            if new_var != current_var:  # init a new row of var-weight
                df = df.append(current_dict, ignore_index=True)
                current_dict = {"var":new_var}
                current_var = new_var
            current_dict[class_num] = weight
    df = df.drop(0)
    return df
