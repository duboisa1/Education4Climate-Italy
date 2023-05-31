from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://elearning.unimib.it/?lang=it"
CYCLE_DICT = {
    "Corso di Laurea Triennale": "bac",
    "Corso di Laurea Magistrale": "master",
    "Corso di Laurea Magistrale a Ciclo Unico (5 anni)": "master",
    "Corso di Laurea Magistrale a Ciclo Unico (6 anni)": "master"
}


class UniMiBSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Milano Bicocca
    """

    name = 'unimib-programs'
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unimib_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        yield scrapy.Request(
            url=BASE_URL,
            callback=self.parse_faculties
        )

    def parse_faculties(self, response):

        faculties_urls = response.xpath("//a[contains(@href, 'area')]/@href").getall()
        for link in faculties_urls:
            yield response.follow(link, self.parse_cycles)

    def parse_cycles(self, response):

        cycles_urls = response.xpath("//a[contains(@title, 'Corso')]/@href").getall()
        cycles = response.xpath("//a[contains(@title, 'Corso')]//h3//span//following::text()[1]").getall()
        faculty = response.xpath("//h1/text()").get()
        for link, cycle in zip(cycles_urls, cycles):
            yield response.follow(link, self.parse_programs,
                                  cb_kwargs={'cycle': CYCLE_DICT[cycle.strip(" \n")],
                                             'faculty': faculty.replace("Area ", '').replace('di ', '')})

    def parse_programs(self, response, cycle, faculty):

        programs_urls = response.xpath("//div[@id='page-content']//div[@class='subcategories']"
                                       "//a[contains(@href, 'course')]/@href").getall()
        for link in programs_urls:
            yield response.follow(link, self.parse_program, cb_kwargs={'cycle': cycle, 'faculty': faculty})

    def parse_program(self, response, cycle, faculty):

        program_name_id = response.xpath("//h1/text()").get()
        program_name = program_name_id.split("[")[0].strip(" ")
        program_id = program_name_id.split("[")[1].strip("] ")

        base_dict = {
            "id": program_id,
            "name": program_name,
            "cycle": cycle,
            "faculties": [faculty],
            "campuses": ["Milan"],
            "url": response.url,
            "courses": [],
            "ects": [],
            "courses_names": [],
            "courses_urls": []
        }

        courses_link = response.xpath("//a[@title='Insegnamenti' or @title='Courses']/@href").get()
        if courses_link is None:
            print(f"Program {program_name} with url {response.url} has no courses description.")
            return

        yield response.follow(courses_link, self.parse_acad,
                              cb_kwargs={'base_dict': base_dict})

    def parse_acad(self, response, base_dict):

        acad_url = response.xpath(f"//a[contains(@title, '{YEAR}-{YEAR+1}')]/@href").get()
        # Analyse the course only if a description is available for the year of interest
        if acad_url:
            yield response.follow(acad_url, self.parse_years,
                                  cb_kwargs={'base_dict': base_dict})

    def parse_years(self, response, base_dict):

        year_urls = response.xpath("//a[contains(@title, 'anno') or contains(@title, 'year')]/@href").getall()
        yield response.follow(year_urls[0], self.parse_courses,
                              cb_kwargs={'base_dict': base_dict, 'links': year_urls[1:]})

    def parse_courses(self, response, base_dict, links):

        ects = response.xpath("//div[contains(@class, 'course-metadata')]/text()").getall()
        ects = [int(float(e.replace("CFU: ", '').replace("ECTS: ", ''))) for e in ects]

        line_with_ects = "//div[contains(@class, 'card-header') and " \
                         "descendant::div[contains(@class, 'course-metadata')]]"
        courses_urls = response.xpath(f"{line_with_ects}//div[contains(@class, 'courseinfo')]/a/@href").getall()
        courses = response.xpath(f"{line_with_ects}//div[contains(@class, 'course-shortname')]/text()").getall()
        courses_names = response.xpath(f"{line_with_ects}//div[contains(@class, 'course-fullname')]/text()").getall()

        if (len(courses) != len(ects)) or (len(courses_urls) != len(courses)):
            print(response.url)
            print(len(courses))
            print(len(ects))

        base_dict['courses'] += courses
        base_dict['ects'] += ects
        base_dict['courses_names'] += courses_names
        base_dict['courses_urls'] += courses_urls

        if len(links) == 0:
            yield base_dict
        else:
            if len(base_dict['courses']) != len(set(base_dict['courses'])):
                print(base_dict['url'])
                print(len(base_dict['courses']))
                print(len(set(base_dict['courses'])))
                print(len(base_dict['courses_urls']))
            yield response.follow(links[0], self.parse_courses,
                                  cb_kwargs={'base_dict': base_dict, 'links': links[1:]})
