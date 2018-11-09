from get_list_of_msoa_lsoa import get_lsoas, get_msoas
from scraper_fns import scrape

BLOCK_SIZE = 200

if __name__ == "__main__":
    get_lsoas()
    get_msoas()
    scrape(BLOCK_SIZE, "all_lsoas_with_coords.csv", "s3://mojap-raw-hist/open_data/osrm/geographytype=lsoa", "lsoa11cd", "lsoa")
    scrape(BLOCK_SIZE, "all_msoas_with_coords.csv", "s3://mojap-raw-hist/open_data/osrm/geographytype=msoa", "msoa11cd", "msoa")