from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URLS = ["https://www.unifi.it/p12189.html", "https://www.unifi.it/p11992.html"]
CYCLE_DICT = {
    "Laurea": "bac",
    "Laurea magistrale": "master",
}


# TODO
#  -

class UniFiSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Università degli studi di Firenze
    """

    name = 'unifi-programs'
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unifi_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        for url in BASE_URLS:
            yield scrapy.Request(
                url=url,
                callback=self.parse_main
            )

    def parse_main(self, response):

        program_links = response.xpath("//a[text()='info insegnamenti']/@href").getall()
        for link in program_links:
            yield response.follow(link, self.parse_program)


    @staticmethod
    def parse_program(response):

        print(response.xpath(""))

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
