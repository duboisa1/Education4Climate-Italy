from abc import ABC
from pathlib import Path

import pandas as pd
import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://corsi.unisa.it/{}" + f"/didattica/insegnamenti?anno={YEAR}" + "&id={}"
PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}unisa_programs_{YEAR}.json')


class UniSaCourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for Università di Salerno
    """

    name = "unisa-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unisa_courses_{YEAR}.json').as_uri()
    }

    def start_requests(self):

        programs_df = pd.read_json(open(PROG_DATA_PATH, "r")).set_index("id")[["url", "courses"]]

        for _, (url, courses) in programs_df.iterrows():
            program_code = url.split("/")[3]
            for course_idx in courses:
                yield scrapy.Request(BASE_URL.format(program_code, course_idx), self.parse_course,
                                     cb_kwargs={'course_id': course_idx})

    @staticmethod
    def parse_course(response, course_id):

        course_name = response.xpath("//h1[@id='rescue-title']/span[2]/text()").get().title()
        teachers = response.xpath("//div[div[h4[a[text()='DOCENTI']]]]//td/a[1]/text()").getall()
        teachers = [" ".join(t.split(" ")[1:] + [t.split(" ")[0]]).title() for t in teachers]
        ects = response.xpath("//div[div[h4[a[text()='MODULI']]]]//tr/td[3]/text()").getall()
        ects = sum([int(float(e)) for e in ects])

        content = cleanup(response.xpath("//table[tr/th[text()='Contenuti']]//tr[2]").get())
        goal = cleanup(response.xpath("//table[tr/th[text()='Obiettivi']]//tr[2]").get())

        yield {
            'id': course_id,
            'name': course_name,
            'year': f"{YEAR}-{YEAR + 1}",
            'languages': ["it"],  # TODO: check if there are other languages
            'teachers': teachers,
            'url': response.url,
            'ects': [ects],
            'content': content,
            'goal': goal,
            'activity': '',
            'other': ''
        }
