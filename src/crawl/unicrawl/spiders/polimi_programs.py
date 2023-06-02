from abc import ABC
from pathlib import Path

import pandas as pd
import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://www4.ceda.polimi.it/manifesti/manifesti/controller/MostraIndirizziPublic.do"
PROGRAM_URL = "https://www4.ceda.polimi.it"
CYCLE_DICT = {
    "Laurea": "bac",
    "Laurea magistrale": "master",
}
BASE_DATA = {
    "lang": "IT",
    "aa": f"{YEAR}",
    "sede": "ALL_SEDI",
    "k_cf": "",
    "k_corso_la": "",
    "evn_default": "Aggiorna"
}


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

        yield scrapy.http.FormRequest(
            BASE_URL,
            callback=self.parse_faculties,
            formdata=BASE_DATA
        )

    def parse_faculties(self, response):

        faculties_option_values = response.xpath("//select[@name='k_cf']//option/@value").getall()
        print(faculties_option_values)
        for k_cf in faculties_option_values:
            BASE_DATA['k_cf'] = k_cf
            yield scrapy.http.FormRequest(
                BASE_URL,
                callback=self.parse_programs,
                formdata=BASE_DATA,
                cb_kwargs={'faculty_id': k_cf}
            )

    def parse_programs(self, response, faculty_id):

        program_option_values = response.xpath("//select[@name='k_corso_la']//option/@value").getall()
        print(faculty_id, program_option_values, len(program_option_values))
        # Little trick to avoid duplicates
        if faculty_id == "225":
            program_option_values = list(set(program_option_values) - {'1091', '1096', '495'})
        for k_corso_la in program_option_values:
            BASE_DATA['k_cf'] = faculty_id
            BASE_DATA['k_corso_la'] = k_corso_la
            yield scrapy.http.FormRequest(
                BASE_URL,
                callback=self.parse_program,
                formdata=BASE_DATA
            )

    def parse_program(self, response):

        name_and_id = response.xpath("//td[contains(text(), 'Corso di Studio')]//following::td[1]/text()").get()
        program_name_and_id = name_and_id.replace("\r\n", '').replace('\t', '')
        program_name = program_name_and_id.split(" (")[0]
        program_id = program_name_and_id.split("(")[1].strip(")")

        campus = response.xpath("//td[contains(text(), 'Sede')]//following::td[1]/text()").get()
        campus = campus.replace("\r\n", '').replace('\t', '')

        faculty = response.xpath("//td[contains(text(), 'Scuola')]//following::td[1]/text()").get()
        faculty = faculty.replace("\r\n", '').replace('\t', '').strip(" ")

        cycle = response.xpath("//td[contains(text(), 'Livello')]//following::td[1]/text()").get()
        cycle = cycle.replace("\r\n", '').replace('\t', '')
        if 'Laurea Di Primo Livello' in cycle:
            cycle = 'bac'
        elif 'Laurea Magistrale' in cycle:
            cycle = 'master'
        else:
            print(f"Unidentified cycle {cycle}")

        sub_programs_links = response.xpath("//td[contains(text(), 'Struttura Corso di Studi')]"
                                            "//following::tr/td[1]/a/@href").getall()
        base_dict = {
            "id": program_id,
            "name": program_name,
            "cycle": cycle,
            "faculties": [faculty],
            "campuses": [campus],
            "url": PROGRAM_URL + sub_programs_links[0],
            "courses": [],
            "ects": [],
            "courses_urls": []
        }
        yield response.follow(sub_programs_links[0], self.parse_courses,
                              cb_kwargs={'sub_programs_links': sub_programs_links[1:], 'base_dict': base_dict})

    def parse_courses(self, response, sub_programs_links, base_dict):

        table_path = "//table[contains(@class,'TableDati')]"
        courses = response.xpath(f"{table_path}//tr//td[contains(@class, 'ElementInfoCard')"
                                 f" and @width='5%'][1]/text()").getall()
        ects = response.xpath(f"{table_path}//tr//td[contains(@class, 'ElementInfoCard')"
                              f" and contains(text(), '.')][1]/text()").getall()
        courses_urls = response.xpath(f"{table_path}//tr//td[contains(@class, 'ElementInfoCard')"
                                      f" and @width='44%'][1]/a[1]/@href").getall()

        # Remove '--'
        courses, ects, courses_urls = zip(*[(c, e, url) for c, e, url in zip(courses, ects, courses_urls) if c != '--'])
        ects = [int(float(e)) for e in ects]

        base_dict['courses'] += courses
        base_dict['ects'] += ects
        base_dict['courses_urls'] += courses_urls

        if len(sub_programs_links) == 0:
            # Remove duplicates
            df = pd.DataFrame({'courses': base_dict['courses'], 'ects': base_dict['ects'],
                               'courses_urls': base_dict['courses_urls']})
            df = df.drop_duplicates(subset=['courses'])
            base_dict['courses'] = df['courses'].tolist()
            base_dict['ects'] = df['ects'].tolist()
            base_dict['courses_urls'] = df['courses_urls'].tolist()
            yield base_dict
        else:
            yield response.follow(sub_programs_links[0], self.parse_courses,
                                  cb_kwargs={'sub_programs_links': sub_programs_links[1:], 'base_dict': base_dict})
