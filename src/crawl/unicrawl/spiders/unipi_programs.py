from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://www.unipi.it/index.php/{}"
CYCLE_DICT = {
    "Corso di laurea": "bac",
    "Corso di laurea magistrale": "master",
    "Laurea magistrale ciclo unico 5 anni": "master",
    "Corso di laurea magistrale ciclo unico 6 anni": "master"
}

# TODO:
#  - Divide by sub-programs ?


class UniPiSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Università degli studi di Pisa
    """

    name = 'unipi-programs'
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unipi_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):

        for cycle_url in ["lauree", "master"]:
            yield scrapy.Request(
                url=BASE_URL.format(cycle_url),
                callback=self.parse_main
            )

    def parse_main(self, response):

        programs_links = response.xpath("//a[contains(@href, 'corso')]/@href").getall()
        for link in programs_links:
            yield response.follow(link, self.parse_program)

    def parse_program(self, response):

        program_id = response.url.split("/")[-1]
        program_name = response.xpath("//h1[@id='maincontent']/text()").get().strip(" \n").title()
        cycle = response.xpath("//p[@class='sottotitolocontent']/text()").get().strip(" ")
        cycle = CYCLE_DICT[cycle]
        faculty = response.xpath("//ul[@id='uldidatticadipartimenti']/li/text()").get().title()

        base_dict = {
            "id": program_id,
            "name": program_name,
            "cycle": cycle,
            "faculties": [faculty],
            "campuses": ["Pisa"],  # TODO: check if there are other campuses
            "url": response.url,
            "courses": [],
            "ects": [],
            "courses_names": []
        }

        courses_url = response.xpath("//a[contains(@href, 'regolamento')]/@href").get()

        yield response.follow(courses_url, self.parse_courses, cb_kwargs={"base_dict": base_dict})

    def parse_courses(self, response, base_dict):

        courses_name_ects = response.xpath("//li[ul/li/div/a[contains(text(), 'Programma')]]/p/text()").getall()
        courses_ects = [int(float(e.split(" (")[-1].split(" cfu")[0])) for e in courses_name_ects]
        courses_names = [" (".join(e.split(" cfu)")[0].split(" (")[:-1]) for e in courses_name_ects]
        courses_urls = response.xpath("//a[contains(text(), 'Programma')]/@href").getall()
        # courses_ids = [url.split("ad=")[1].split("&")[0] for url in courses_urls]

        # Remove duplicates
        courses_urls, courses_ects, courses_names = zip(*list(set(zip(courses_urls, courses_ects, courses_names))))

        # Call each course iteratively to get ids
        if len(courses_urls) != 0:
            yield response.follow(courses_urls[0], self.parse_course,
                                  cb_kwargs={"base_dict": base_dict, "courses_ects": courses_ects,
                                             "courses_names": courses_names, "courses_urls": courses_urls[1:]},
                                  dont_filter=True)
        else:
            yield base_dict

    def parse_course(self, response, base_dict, courses_ects, courses_names, courses_urls):

        course_id = response.xpath("//span[text()='Codice']/following::span[1]/text()").get()
        if course_id is None:
            course_id = response.url.split("cod=")[1].split("&")[0]
        if course_id != '-':
            base_dict["courses"] += [course_id]
            base_dict["ects"] += [courses_ects[0]]
            base_dict["courses_names"] += [courses_names[0]]
            print(base_dict["id"], f"{len(base_dict['courses'])}/{len(courses_ects)}")

        if len(courses_urls) != 0:
            yield response.follow(courses_urls[0], self.parse_course,
                                  cb_kwargs={"base_dict": base_dict, "courses_ects": courses_ects[1:],
                                             "courses_names": courses_names[1:], "courses_urls": courses_urls[1:]},
                                  dont_filter=True)
        else:
            yield base_dict


