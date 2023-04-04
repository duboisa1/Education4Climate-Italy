from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://www.polimi.it/corsi"
CYCLE_DICT = {
    "Laurea": "bac",
    "Laurea magistrale": "master",
}


# TODO
#  - Different types of programs seem to have very different structures
#  - Should the different 'orientations' as different programs or combine them

class PoliMiSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Politecnico di Milano
    """

    name = 'polimi-programs'
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}polimi_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        yield scrapy.Request(
            url=BASE_URL,
            callback=self.parse_main
        )

    @staticmethod
    def parse_main(response):
        yield {
            "id": '',
            "name": '',
            "cycle": '',
            "faculties": [],
            "campuses": [],
            "url": "",
            "courses": "",
            "ects": ""
        }
