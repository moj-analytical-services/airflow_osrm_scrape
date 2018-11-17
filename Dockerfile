FROM python:3.6

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY main.py main.py
COPY utils.py utils.py
COPY get_list_of_msoa_lsoa.py get_list_of_msoa_lsoa.py
COPY scraper_fns.py scraper_fns.py
COPY courts_list.csv courts_list.csv