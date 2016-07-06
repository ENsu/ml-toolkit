import tqdm
import json


def apply_with_log(series, apply_funciton):
    for i, r in tqdm(series.iterrows(), total=len(series.index)):
        series.at[i] = apply_funciton(series.iloc[i])


def get_BQschema(df):
    type_map_dict = {"int64":"INTEGER", "float64":"FLOAT", "object":"STRING"}
    field_list = []
    for i in range(len(df.columns)):
        df_type = type_map_dict[str(df.dtypes[i])]
        field_list.append({"name":df.columns[i], "type":df_type})
    return json.dumps(field_list)