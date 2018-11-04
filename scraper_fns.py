import pandas as pd
import requests
import geopandas
import json

def result_to_table_matrix(result, source, dest, source_field, source_name):

    points_source = [s[source_field] for s in source]
    points_dest = ([s[source_field] for s in dest])

    points = points_source +  points_dest

    df1 = pd.DataFrame(result["durations"])
    df1.columns = points
    df1.index = points
    df1[source_name] = points
    df1 = df1.melt(id_vars=[source_name])
    df1.rename(columns={"value": "duration_seconds", source_name: f"from_{source_name}", "variable": f"to_{source_name}"}, inplace=True)

    df2 = pd.DataFrame(result["distances"])
    df2.columns = points
    df2.index = points
    df2[source_name] = points
    df2 = df2.melt(id_vars=source_name)
    df2.rename(columns={"value": "distance_meters"}, inplace=True)
    df2.drop([source_name, "variable"], inplace=True, axis=1)
    df = pd.concat([df1, df2], axis=1)
    df = df.drop_duplicates([f"from_{source_name}", f"to_{source_name}"])

    return df


def make_request(source_list, dest_list):
    sources = ";".join([f"{c['lng']:.4f},{c['lat']:.4f}" for c in source_list])
    dests = ";".join([f"{c['lng']:.4f},{c['lat']:.4f}" for c in dest_list])
    url = f"https://osrm-great-britain.apps.alpha.mojanalytics.xyz/table/v1/driving/{sources};{dests}?annotations=distance,duration"
    r = requests.get(url)
    result = json.loads(r.text)
    return result


def scrape(BLOCK_SIZE, csv_path, out_base_path, source_field='lsoa11cd', source_name='lsoa'):

    df_to_scrape = pd.read_csv(csv_path)
    # df_to_scrape = df_to_scrape[:55]
    jsons = df_to_scrape.to_dict(orient="records")

    for row in range(10000):
        dfs = []
        start_row = row * BLOCK_SIZE
        end_row = (row+1) * BLOCK_SIZE
        source_list = jsons[start_row:end_row]
        if len(source_list) == 0:
            break

        for col in range(row+1, 10000):
            start_col = col * BLOCK_SIZE
            end_col = (col+1) * BLOCK_SIZE
            print(f"Start row: {start_row}, End row: {end_row}, Start col: {start_col}, End col: {end_col}")
            source_list = jsons[start_row:end_row]
            dest_list = jsons[start_col:end_col]
            if len(dest_list) == 0:
                break

            result = make_request(source_list, dest_list)
            df = result_to_table_matrix(result, source_list, dest_list, source_field, source_name)
            dfs.append(df)
        if len(dfs) > 0:
            df_all_row = pd.concat(dfs)
            df_all_row.to_parquet(f"{out_base_path}/sr{start_row}-er{end_row}-sc{start_col}-ec{end_col}.parquet")
            del df_all_row
            del dfs



