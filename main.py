from get_list_of_msoa_lsoa import get_lsoas, get_msoas
from scraper_fns import scrape
import pandas as pd

BLOCK_SIZE = 200

if __name__ == "__main__":
    lsoas = get_lsoas()
    msoas = get_msoas()
    courts = pd.read_csv("courts_list.csv")
    points = pd.concat([lsoas,msoas, courts])
    scrape(BLOCK_SIZE, points, "s3://mojap-raw-hist/open_data/osrm/combinations=courts_lsoas_msoas")