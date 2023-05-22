from abc import ABC
from pathlib import Path

import pandas as pd
import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://didattica.polito.it/pls/portal30/gap.pkg_guide.viewGap?p_cod_ins={}" + f"&p_a_acc={YEAR+1}"
PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}polito_programs_{YEAR}.json')

LANGUAGE_DICT = {
    "Italiano": 'it',
    "Inglese": 'en',
    "Spagnolo": 'es',
    "Francese": 'fr'
}


class PoliToCourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for Politecnico di Torino
    """

    name = "polito-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}polito_courses_{YEAR}.json').as_uri()
    }

    def start_requests(self):

        courses_ids = sorted(list(set(pd.read_json(open(PROG_DATA_PATH, "r"))["courses"].sum())))

        for course_id in courses_ids:
            yield scrapy.Request(BASE_URL.format(course_id), self.parse_course, cb_kwargs={'course_id': course_id})

    @staticmethod
    def parse_course(response, course_id):

        course_name = response.xpath("//h3/text()").get().strip("\n")

        language = response.xpath("//div[contains(text(), 'Course Language') or contains(text(), 'Lingua dell')]"
                                  "//following::div[1]/p/text()").get()
        language = LANGUAGE_DICT[language]

        teachers = response.xpath("//div[contains(text(), 'Lecturers') or contains(text(), 'Docenti')]"
                                  "//following::div[1]//a[@class='policorpolink']/text()").getall()
        teachers = list(set(teachers) - {"Docente Da Nominera"})

        section_txt = "//div[contains(@class, 'row') and div/label[@for='{}']]//div[contains(@class, 'col-sm-10')]"
        content = "\n".join(cleanup(response.xpath(section_txt.format("idPresentazione")).getall()))
        content += "\n" + "\n".join(cleanup(response.xpath(section_txt.format("idProgramma")).getall()))
        goal = "\n".join(cleanup(response.xpath(section_txt.format("idRisAttesi")).getall()))

        yield {
            'id': course_id,
            'name': course_name,
            'year': f"{YEAR}-{YEAR + 1}",
            'languages': [language],
            'teachers': teachers,
            'url': response.url,
            'content': content.strip("\n "),
            'goal': goal.strip("\n "),
            'activity': '',
            'other': ''
        }
