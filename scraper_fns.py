import pandas as pd
import requests
import geopandas
import json

def result_to_table_matrix(result, source, dest):
    """Take the result of make_request and turn it into a pandas dataframe

    Args:
        result: The json data structure returned from the OSRM API:
        source:  The list of source points (lat lng) with their geography_id (e.g. 'E0000010') and geography_type (e.g. 'lsoa')
        dest:  The list of destination points (lat lng) with their geography_id (e.g. 'E0000010') and geography_type (e.g. 'lsoa')

    Returns:
        A pandas dataframe of the drivetimes
    """

    points = source +  dest
    source_geography_id =  [p["geography_id"] for p in points]
    source_geography_type = [p["geography_type"] for p in points]

    def get_df_of_values(value_variable):

        if value_variable == "durations":
            value_col = "duration_sections"

        if value_variable == "distances":
            value_col = "distance_meters"

        df = pd.DataFrame(result[value_variable])

        tuples = list(zip(source_geography_id, source_geography_type))

        df.index = tuples
        df.columns = tuples

        df["source_geography_id"] = source_geography_id
        df["source_geography_type"] = source_geography_type
        df = df.melt(id_vars=["source_geography_id", "source_geography_type"])

        # Unpack index of tuples
        df[['destination_geography_id', 'destination_geography_type']] = df['variable'].apply(pd.Series)
        df.drop("variable", axis=1, inplace=True)
        df.rename(columns={"value": value_col}, inplace=True)
        col_order = ["source_geography_id","source_geography_type","destination_geography_id","destination_geography_type",value_col]
        df = df[col_order]

        return df



    df1 = get_df_of_values("durations")

    df2 = get_df_of_values("distances")
    df2.drop(["source_geography_id","source_geography_type","destination_geography_id","destination_geography_type"], axis=1, inplace=True)
    df = pd.concat([df1, df2], axis=1)
    df = df.drop_duplicates([f"source_geography_id", f"destination_geography_id"])

    return df


def make_request(source_list, dest_list):
    sources = ";".join([f"{c['lng']:.4f},{c['lat']:.4f}" for c in source_list])
    dests = ";".join([f"{c['lng']:.4f},{c['lat']:.4f}" for c in dest_list])
    url = f"https://osrm-great-britain.apps.alpha.mojanalytics.xyz/table/v1/driving/{sources};{dests}?annotations=distance,duration"
    r = requests.get(url)
    result = json.loads(r.text)
    return result


def scrape(BLOCK_SIZE, df_to_scrape, out_base_path):
    """Gather drivetime information for all (lat,lng) combinations in df_to_scrape and output to dataframe

    Args:
        BLOCK_SIZE: Sets the size of each request to the osrm table endpoint -ie how big the table is
        df_to_scrape: A datafarme with columns 'lat' 'lng' 'geography_id' (e.g. E01008657), and 'geography_type' (e.g. lsoa)
        out_base_path: A string pointing to the base path of the output (e.g. on disk or in s3)

    Returns:
        None.  This function has a side effect of writing outputs to disk
    """

    # Split records into all combinations of source and destination geography type - this will be our file system partitioning scheme

    df_to_scrape["geography_type"] = df_to_scrape["geography_type"].astype(str)
    df_to_scrape["geography_id"] = df_to_scrape["geography_id"].astype(str)

    records_to_scrape = df_to_scrape.to_dict(orient="records")

    for row in range(10000):
        dfs = []
        start_row = row * BLOCK_SIZE
        end_row = (row+1) * BLOCK_SIZE
        source_list = records_to_scrape[start_row:end_row]
        if len(source_list) == 0:
            break

        # This logic accounts for the possibility that the block size < num records, in which case nothing would happen
        if len(records_to_scrape) <= BLOCK_SIZE:
            col_adjustment = 0
        else:
            col_adjustment = 1

        print(f"Len records to scrape {len(records_to_scrape)}")
        print(f"Block size {BLOCK_SIZE}")
        print(f"Col adjustment {col_adjustment}")

        for col in range(row+col_adjustment, 10000):
            start_col = col * BLOCK_SIZE
            end_col = (col+1) * BLOCK_SIZE

            dest_list = records_to_scrape[start_col:end_col]
            if len(dest_list) == 0:
                break

            result = make_request(source_list, dest_list)
            df = result_to_table_matrix(result, source_list, dest_list)
            dfs.append(df)
        if len(dfs) > 0:
            df_all_row = pd.concat(dfs)
            out_path = f"{out_base_path}/sr{start_row}-er{end_row}-sc{0}-ec{(col)*BLOCK_SIZE}.parquet"
            df_all_row.to_parquet(out_path)
            print(f"Written file: {out_path}")
            del df_all_row
            del dfs



