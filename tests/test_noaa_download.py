from datetime import date

from ingest.noaa_download import file_url
from pipeline.config import load_config


def test_file_url_matches_published_name():
    pattern = load_config()["noaa"]["file_url"]
    assert file_url(pattern, date(2024, 7, 1)) == (
        "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/AIS_2024_07_01.zip"
    )
