from abc import ABC
from pathlib import Path

import pandas as pd
import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://esami.unipi.it/esami2/programma.php?cod={}" + f"&aa={YEAR}"
PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}unipi_programs_{YEAR}.json')


LANGUAGE_DICT = {
    "Italiano": 'it',
    "Inglese": 'en'
}


# TODO:
#  - Probably rerun later in year because lots of missing descriptions


class UniPiCourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for Università degli studi di Pisa
    """

    name = "unipi-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unipi_courses_{YEAR}.json').as_uri()
    }

    def start_requests(self):

        programs_df = pd.read_json(open(PROG_DATA_PATH, "r")).set_index("id")[["courses", "courses_names"]]

        for _, (courses_ids, courses_names) in programs_df.iterrows():
            for course_idx, course_name in zip(courses_ids, courses_names):
                yield scrapy.Request(BASE_URL.format(course_idx), self.parse_course,
                                     cb_kwargs={'course_id': course_idx, 'course_name': course_name})

    @staticmethod
    def parse_course(response, course_id, course_name):

        course_name_new = response.xpath("//div[contains(@class, 'div-table-cell titolatura')]/text()[1]").get()
        if course_name_new is None:
            yield {
                'id': course_id,
                'name': course_name,
                'year': f"{YEAR}-{YEAR + 1}",
                'languages': ["it"],
                'teachers': [],
                'url': response.url,
                'content': '',
                'goal': '',
                'activity': '',
                'other': ''
            }
            return

        course_name = course_name_new.title()

        language = response.xpath("//span[text()='Lingua']//following::span[1]/text()").get()
        language = LANGUAGE_DICT[language]

        teacher = response.xpath("///div[contains(@class, 'titolare')]/text()").get()
        teacher = " ".join(teacher.split(" ")[1:] + [teacher.split(" ")[0]]).title()
        teachers = [teacher] if not teacher.startswith("0000") else []

        def get_section_text(section_name):
            xpath = f"//div[contains(text(), '{section_name}') and @class='titolo-elemento-programma']" \
                    "//following::div[@class='prog_it' and div[@class='titolo-elemento-programma']][1]/div/text()"
            fol_section = response.xpath(xpath).get()
            xpath = f"//div[preceding::div[contains(text(), '{section_name}') and @class='titolo-elemento-programma']" \
                    f" and following::div[contains(text(), \"{fol_section}\") and @class='titolo-elemento-programma']" \
                    " and contains(@class, 'prog_it')]"
            return "\n".join(cleanup(response.xpath(xpath).getall())).strip("\n")
        goal = get_section_text('Obiettivi')

        # Different structure for content and activity than goal
        content = cleanup(response.xpath("//div[div[contains(text(), 'Programma')"
                                         " and @class='titolo-elemento-programma']]").get())
        activity = cleanup(response.xpath("//div[div[contains(text(), 'Indicazioni metodologiche')"
                                          " and @class='titolo-elemento-programma']]").get())

        yield {
            'id': course_id,
            'name': course_name,
            'year': f"{YEAR}-{YEAR + 1}",
            'languages': [language],
            'teachers': teachers,
            'url': response.url,
            'content': content,
            'goal': goal,
            'activity': activity,
            'other': ''
        }
