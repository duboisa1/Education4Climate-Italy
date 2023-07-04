from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://www.unito.it/didattica/offerta-formativa/corsi-studio"

CYCLE_DICT = {
    "Laurea": "bac",
    "Laurea Magistrale": "master",
    "Laurea Magistrale a Ciclo Unico": "master"
}


# FIXME: Error 403, not allowed to access the data

class UniToSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Università di Torino
    """

    name = 'unito-programs'
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unito_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        yield scrapy.Request(
            url=BASE_URL,
            callback=self.parse_main
        )

    def parse_main(self, response):

        program_links = response.xpath("//div[div/h4/a[text()='PER DIPARTIMENTO']]"
                                       "//div[@class='field-content']/a/@href").getall()
        print(program_links)
        for link in program_links:
            yield response.follow(link, self.parse_programs)

    def parse_programs(self, response):

        program_name = response.xpath("//h1[contains(@class, 'page-header')]/text()").get()
        program_id = response.xpath("//div[contains(text(), 'Codice del corso di studio')]"
                                    "/following::div[1]/div/text()").get()
        cycle = response.xpath("//div[contains(text(), 'Tipo di corso')]"
                               "/following::div[1]/div/text()").get()
        cycle = CYCLE_DICT[cycle]
        faculty = response.xpath("//div[contains(text(), 'Dipartimento di afferenza')]"
                                 "/following::div[1]/div/text()").get()
        campus = response.xpath("//div[contains(text(), 'Sede didattica')]"
                                "/following::div[1]/div/text()").get()

        base_dict = {
            "id": program_id,
            "name": program_name,
            "cycle": cycle,
            "faculties": [faculty],
            "campuses": [campus],
            "url": response.url,
            "courses": [],
            "ects": []
        }

        courses_links = response.xpath("//div[h3/a[text()='Insegnamenti']]"
                                       "//span[@class='field-content']/a/@href").getall()

        # if len(courses_links) != 0:
        if 0:
            yield response.follow(courses_links[0], self.parse_courses,
                                  cb_kwargs={"base_dict": base_dict, "courses_links": courses_links})
        else:
            yield base_dict

    def parse_courses(self, response, base_dict, courses_links):

        course_id = response.xpath("//div[contains(text(), 'Codice')]"
                                   "/following::div[1]/div/text()").get()
        course_ects = int(float(response.xpath("//div[contains(text(), 'Crediti')]"
                                               "/following::div[1]/div/text()").get()))

        base_dict['courses'] += [course_id]
        base_dict['ects'] += [course_ects]

        if len(courses_links) != 0:
            yield response.follow(courses_links[0], self.parse_courses,
                                  cb_kwargs={"base_dict": base_dict, "courses_links": courses_links})
        else:
            yield base_dict


