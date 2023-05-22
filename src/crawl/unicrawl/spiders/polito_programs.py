from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://www.polito.it/didattica/{}"

CYCLE_DICT = {
    "Laurea": 'bac',
    "Bachelor's degree": 'bac',
    "Laurea professionalizzante": 'bac',
    "Professional Bachelor's degree": 'bac',
    "Laurea magistrale": 'master',
    "Master's degree": 'master'
}


class PoliToSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Politecnico di Torino
    """

    name = 'polito-programs'
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}polito_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        for cycle in ['corsi-di-laurea', 'corsi-di-laurea-magistrale']:
            yield scrapy.Request(
                url=BASE_URL.format(cycle),
                callback=self.parse_programs
            )

    def parse_programs(self, response):

        programs_urls = response.xpath("//td[@class='pol-courses--item-name']/a/@href").getall()
        for link in programs_urls:
            yield response.follow(link, self.parse_program)

    def parse_program(self, response):

        # Did not find better than this as id
        program_id = response.url.split("/")[-1]

        program_name = response.xpath("//h1/text()[2]").get()
        if program_name is None:
            print(f"Did not find a program name at url {response.url}")
            return
        program_name = program_name.strip(" \n").title()

        cycle = response.xpath("//div[contains(@class, 'accordion')]//h3[contains(text(), 'Tipo di corso') "
                               "or contains(text(), 'Degree:')]//following::div[1]/text()").get()
        cycle = CYCLE_DICT[cycle.strip(' \n')]

        # Need to do this to differentiate between programs of different cycles but similar names
        program_id = f"{program_id}-{cycle}"

        faculty = response.xpath("//div[contains(@class, 'accordion')]//h3[contains(text(), 'Department') "
                                 "or contains(text(), 'Dipartimento')]//following::a[1]/text()").get()
        faculty = faculty.strip(' \n').replace('"', "'")

        base_dict = {
            "id": program_id,
            "name": program_name,
            "cycle": cycle,
            "faculties": [faculty],
            "campuses": ["Torino"],
            "url": response.url
        }

        list_of_courses_url = '/programme-curriculum' \
            if 'https://www.polito.it/en' in response.url else '/piano-di-studi'

        yield response.follow(response.url + list_of_courses_url, self.parse_courses,
                              cb_kwargs={'base_dict': base_dict})

    @staticmethod
    def parse_courses(response, base_dict):

        ects = response.xpath("//table//td[@class='pol-course-programme--item-credits'][2]/text()").getall()
        ects = [e.strip("\n ") for e in ects if '\xa0' not in e]
        ects = [int(float(e)) if e != '' else 0 for e in ects]  # Have to do this for one course in Computer Eng.
        courses_ids = response.xpath("//table//td[@class='pol-course-programme--item-code']/text()").getall()
        courses_ids = [c.strip("\n ") for c in courses_ids if '\xa0' not in c]
        courses_names = response.xpath("//table//td[@class='pol-course-programme--item-name']/a/text()").getall()

        # Remove the ones corresponding to 'free courses' in the full curriculum
        ects, courses_ids, _ = zip(*[(e, idx, name) for (e, idx, name) in zip(ects, courses_ids, courses_names)
                                     if 'Full curriculum' not in name])

        base_dict['courses'] = courses_ids
        base_dict['ects'] = ects

        yield base_dict
