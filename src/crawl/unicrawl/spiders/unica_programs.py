from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://www.unica.it/unica/it/studenti_s01.page"
CYCLE_DICT = {
    "Laurea": "bac",
    "Laurea magistrale": "master",
}


# TODO
#  - Liste des cours plus dispo sur le site

class UniCaSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Università di Cagliari
    """

    name = 'unica-programs'
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unica_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        yield scrapy.Request(
            url=BASE_URL,
            callback=self.parse_main
        )

    def parse_main(self, response):

        cycle_links = response.xpath("//ul[@class='article-links-list']//a[contains(@title, 'Lauree')]/@href").getall()
        for link in cycle_links:
            yield response.follow(link, self.parse_cycle)

    def parse_cycle(self, response):

        program_links = response.xpath("//ul[@class='article-links-list']//a/@href()").getall()
        for link in program_links:
            yield response.follow(link, self.parse_program)

    def parse_program(self, response):

        program_id = "-".join(response.url.split("crs_")[1].split(".")[0].split("_"))
        program_name = response.xpath("//h1/a/text()").get()
        cycle = response.xpath("//h1/span/text()").get()
        faculty_url = response.xpath("//li/a[span/b[contains(text(), 'La Facolt')]]/@href").get()
        faculty = faculty_url

        base_dict = {
            "id": program_id,
            "name": program_name,
            "cycle": cycle,
            "faculties": [faculty],
            "campuses": ["Cagliari"],  # Check if there are several campuses
            "url": response.url
        }

        courses_url = ""

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
