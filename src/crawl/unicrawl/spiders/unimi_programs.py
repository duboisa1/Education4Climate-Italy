from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://www.unimi.it/it/corsi/facolta-e-scuole"
CYCLE_DICT = {
    "Laurea": "bac",
    "Laurea magistrale": "master",
}


# TODO
#  - Problem of code 403

class UniMiSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Università degli studi di Milano
    """

    name = 'unimi-programs'
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unimi_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):

            yield scrapy.Request(
                url=BASE_URL,
                callback=self.parse_faculty
            )

    def parse_faculty(self, response):

        faculty_links = response.xpath("//div[@class='tronchetto']/a/@href")
        for link in faculty_links:
            yield response.follow(link, self.parse_main)

    @staticmethod
    def parse_main(response):

        faculty = response.xpath("//h1/span/text()").get()

        program_links = response.xpath("//div[@class='bp-title']/a/@href").getall()
        for link in program_links:
            print(link)

        return

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
