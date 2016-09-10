import tqdm
import json


def apply_with_log(series, apply_funciton):
    for i, r in tqdm(series.iterrows(), total=len(series.index)):
        series.at[i] = apply_funciton(series.iloc[i])


def get_BQschema(df):
    type_map_dict = {"int64":"INTEGER", "float64":"FLOAT", "object":"STRING", "bool":"BOOLEAN"}
    null_count_list = df.isnull().sum().tolist()
    field_list = []
    for i in range(len(df.columns)):
        df_type = type_map_dict[str(df.dtypes[i])]
        mode = "NULLABLE" if null_count_list[i] else "REQUIRED"
        field_list.append({"name":df.columns[i], "type":df_type, "mode": mode})
    return json.dumps(field_list)
