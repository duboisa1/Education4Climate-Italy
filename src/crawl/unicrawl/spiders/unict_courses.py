from abc import ABC
from pathlib import Path

import pandas as pd
import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}unict_programs_{YEAR}.json')

# TODO:
#  - There are several structures for courses


class UniCTCourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for U Università di Catania
    """

    name = "unict-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unict_courses_{YEAR}.json').as_uri()
    }

    def start_requests(self):

        programs_df = pd.read_json(open(PROG_DATA_PATH, "r")).set_index("id")[["courses", "courses_urls"]]

        for _, (courses_ids, courses_urls) in programs_df.iterrows():
            for course_idx, course_url in zip(courses_ids, courses_urls):
                yield scrapy.Request(course_url, self.parse_course,
                                     cb_kwargs={'course_id': course_idx})

    @staticmethod
    def parse_course(response, course_id):

        course_name = cleanup(response.xpath("//h1").get()).replace(" \n", '').replace("  ", ' ')
        print(course_name)

        teacher = response.xpath("//a[contains(@href, 'docenti_open')]/b/text()").get()
        teacher = " ".join(teacher.split(" ")[1:] + [teacher.split(" ")[0]]).title()

        language = "it" if 'corsi' in response.url else 'en'

        def get_section_text(section_name):
            fol_sec_name = response.xpath(f"//h2[preceding::h2[contains(text(), \"{section_name}\")]][1]/text()").get()
            return "\n".join(cleanup(response.xpath(f"//p[preceding::h2[contains(text(), \"{section_name}\")] "
                                          f"and following::h2[text()=\"{fol_sec_name}\"]]").getall())).strip("\n")

        content = ''
        goal = ''
        if 'corsi' in response.url:
            content = get_section_text("Contenuti")
            goal = get_section_text("apprendimento attesi")
        elif 'courses' in response.url:
            content = get_section_text('Course Content')
            goal = get_section_text('Learning Outcomes')

        yield {
            'id': course_id,
            'name': course_name,
            'year': f"{YEAR}-{YEAR + 1}",
            'languages': [language],
            'teachers': [teacher],
            'url': response.url,
            'content': content,
            'goal': goal,
            'activity': '',
            'other': ''
        }
