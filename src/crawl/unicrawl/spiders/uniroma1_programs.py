from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://corsidilaurea.uniroma1.it/"
CYCLE_DICT = {
    "Laurea": "bac",
    "Laurea magistrale": "master",
    "Laurea magistrale a ciclo unico": "master",
    "Laurea magistrale a percorso unitario": "master"
}
LANGUAGE_DICT = {
    "Italiano": 'it',
    "Inglese": 'en',
    "Francese": 'fr',
    "Spagnolo": 'es',
    "deu": 'de'
}


class UniRoma1Spider(scrapy.Spider, ABC):
    """
    Programs crawler for Università degli studi di Roma “La Sapienza”
    """

    name = 'uniroma1-programs'
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}uniroma1_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        yield scrapy.Request(
            url=BASE_URL,
            callback=self.parse_main
        )

    def parse_main(self, response):

        program_links = response.xpath("//table//tr/td[2]/a/@href").getall()
        program_faculties = response.xpath("//table//td[contains(@class,'facolta')]/text()").getall()
        for link, faculties in zip(program_links, program_faculties):
            faculties = faculties.strip("\r\n ").split("; ")
            yield response.follow(link, self.parse_program, cb_kwargs={"faculties": faculties})

    def parse_program(self, response, faculties):

        program_name = response.xpath("//h2/text()").get().strip("\n\t\xa0\b ")
        program_id = response.url.split("/")[-2]
        cycle = response.xpath("//div[@class='tipologia-corso-title']/text()").get()
        if cycle in CYCLE_DICT.keys():
            cycle = CYCLE_DICT[cycle]
        else:
            cycle = 'other'

        # Problem with "Tecniche per l'edilizia e il territorio per
        # la professione del geometra - corso professionalizzante"
        if program_id == "30386":
            cycle = 'bac'
            faculties = ["Ingegneria civile e industriale", "Architettura"]

        base_dict = {
            "id": program_id,
            "name": program_name,
            "cycle": cycle,
            "faculties": faculties,
            "campuses": ["Roma"],  # FIXME: are there several campuses?
        }

        courses_detail_link = f"{response.url[:-4]}programmazione"
        yield response.follow(courses_detail_link, self.parse_courses, cb_kwargs={"base_dict": base_dict})

    @staticmethod
    def parse_courses(response, base_dict):

        xpath_tr_condition = "//tr[@data-row-type='insegnamento']"

        courses_ects = response.xpath(f"{xpath_tr_condition}/td[4]/text()[1]").getall()
        courses_ects = [int(float(ects.strip("\r\n\t "))) for ects in courses_ects]
        courses_languages = response.xpath(f"{xpath_tr_condition}/td[last()]/img/@title").getall()
        courses_languages = [LANGUAGE_DICT[l] for l in courses_languages]
        courses_class = response.xpath(f"{xpath_tr_condition}/@class").getall()
        courses_data_id = response.xpath(f"{xpath_tr_condition}/@data-id").getall()
        courses_parent_data_id = response.xpath(f"{xpath_tr_condition}/@data-parent-id").getall()
        courses_ids = []
        for data_id, course_class in zip(courses_data_id, courses_class):
            if 'figlio' in course_class:
                xpath = f"//tr[@data-id=\'{data_id}\']/preceding::tr[contains(@class, 'gruppo')][1]/td[2]/text()[1]"
                courses_ids += [response.xpath(xpath).get().strip("\r\n\t ") + "-" + data_id[0:3]]
            else:
                xpath = f'//tr[@data-id=\'{data_id}\']/td[2]/text()[1]'
                courses_ids += [response.xpath(xpath).get().strip("\r\n\t ")]

        courses_urls = [f"{parent_id}{data_id}" for parent_id, data_id
                        in zip(courses_parent_data_id, courses_data_id)]

        # Remove all the courses with no id
        courses = []
        for course_id, course_ects, course_url, course_language \
                in zip(courses_ids, courses_ects, courses_urls, courses_languages):
            if len(course_id) > 0:
                courses += [(course_id, course_ects, course_url, course_language)]
        courses_ids_trim, courses_ects_trim, courses_urls_trim, courses_languages_trim = zip(*courses)

        new_dict = {
            "url": response.url,
            "courses": courses_ids_trim,
            "ects": courses_ects_trim,
            "courses_languages": courses_languages_trim,
            "courses_url_codes": courses_urls_trim
        }

        yield {**base_dict, **new_dict}
