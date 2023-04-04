from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = f"https://www.unict.it/it/didattica/lauree-e-lauree-magistrali/offerta-formativa-{YEAR}-{YEAR+1}"
CYCLE_DICT = {
    "Laurea": "bac",
    "Laurea magistrale": "master",
}


# TODO
#  - This one seems easy
# TODO: could not find number of credits


class UniCTSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Università di Catania
    """

    name = 'unict-programs'
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unict_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        yield scrapy.Request(
            url=BASE_URL,
            callback=self.parse_main
        )

    def parse_main(self, response):

        program_links = response.xpath("//a[text()='INFORMAZIONI SUL CORSO']/@href").getall()
        for link in program_links:
            yield response.follow(link, self.parse_program)

    def parse_program(self, response):

        program_name = response.xpath("//h1/text()").get()
        cycle = response.xpath("//div[div[contains(text(), 'Tipo di corso')]]//div[@id='first']/text()").get()
        faculty = response.xpath("//div[div[contains(text(), 'Struttura didattica')]]//div[@id='first']/text()").get()
        campus = response.xpath("//div[div[contains(text(), 'Sede')]]//div[@id='first']/text()").get()

        # TODO: no working for : Giurisprudenza, Medecina e Chirurgia, Tecniche della prevenzione nell'ambiente e nei luoghi di lavoro
        courses_link = response.xpath("//a[text()='VAI AL SITO DEL CORSO']/@href").get()
        if 'presentazione-del-corso' in courses_link:
            courses_link = courses_link.split("presentazione-del-corso")[0] + 'programmi'
        elif 'course-overview' in courses_link:
            courses_link = courses_link.split("course-overview")[0] + 'study-plan'
        else:
            courses_link += '/programmi'
        # Try to create a unique id
        program_id = courses_link.split(".")[1][0:3].upper() + '-' + courses_link.split("/")[-2].upper()

        base_dict = {
            "id": program_id,
            "name": program_name,
            "cycle": cycle,
            "faculties": [faculty],
            "campuses": [campus],
            "url": response.url
        }

        yield response.follow(courses_link, self.parse_courses, cb_kwargs={"base_dict": base_dict})

    @staticmethod
    def parse_courses(response, base_dict):

        courses_ids = response.xpath("//tr/td[1]/a/text()").getall()
        courses_ids = [idx.split(" -")[0] for idx in courses_ids]
        courses_urls = response.xpath("//tr/td[1]/a/@href").getall()
        if 'corsi' in response.url:
            courses_urls = [response.url.split('/corsi')[0] + url for url in courses_urls]
        elif 'courses' in response.url:
            courses_urls = [response.url.split('/courses')[0] + url for url in courses_urls]

        base_dict["courses"] = courses_ids
        base_dict["ects"] = [0]*len(courses_ids)
        base_dict["courses_urls"] = courses_urls

        yield base_dict
