from abc import ABC
from pathlib import Path

import pandas as pd
import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}unica_programs_{YEAR}.json')


# TODO: harmonize with unisa

class UniCaCourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for Università degli Studi di Calgiari
    """

    name = "unica-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unica_courses_{YEAR}.json').as_uri()
    }

    def start_requests(self):

        programs_df = pd.read_json(open(PROG_DATA_PATH, "r")).set_index("id")[["courses", "courses_urls",
                                                                               "courses_urls_xhr"]]

        already_seen_ids = []
        for _, (courses_ids, courses_urls, courses_xhr) in programs_df.iterrows():
            for course_idx, course_url, course_xhr in zip(courses_ids, courses_urls, courses_xhr):
                print(course_idx, course_url, course_xhr)
                # TODO: not sur this is a correct way to do, might need to change the ids to differentiate between
                #  courses with same ids but different web pages
                if course_idx in already_seen_ids:
                    continue
                already_seen_ids += [course_idx]
                yield scrapy.Request(course_xhr, self.parse_course,
                                     cb_kwargs={'course_id': course_idx, 'course_url': course_url})

    @staticmethod
    def parse_course(response, course_id, course_url):

        response_json = response.json()
        course_name = response_json['des_it'].title()

        teachers = []
        for teacher_json in response_json['docenti']:
            if 'des' in teacher_json:
                teachers += [teacher_json['des'].title()]
        teachers = list(set(teachers))

        def get_text(section, section_name):
            if f'{section_name}_it' in contents_json and contents_json[f'{section_name}_it']:
                section += '\n' + contents_json[f'{section_name}_it']
            elif f'{section_name}_en' in contents_json and contents_json[f'{section_name}_en']:
                section += '\n' + contents_json[f'{section_name}_en']
            return section

        content = ''
        goal = ''
        activity = ''
        for contents_json in response_json['testiTotali']:
            content = get_text(content, 'contenuti')
            goal = get_text(goal, 'obiettivi_formativi')
            activity = get_text(activity, 'metodi_didattici_est')

        yield {
            'id': course_id,
            'name': course_name,
            'year': f"{YEAR}-{YEAR + 1}",
            'languages': ['it'],  # could not find a way to get the language, tried to identify it from name of the course
            'teachers': teachers,
            'url': course_url,
            'content': content.strip("\n"),
            'goal': goal.strip("\n"),
            'activity': activity.strip("\n"),
            'other': ''
        }
