from abc import ABC
from pathlib import Path

import pandas as pd
import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = ("https://unisa.coursecatalogue.cineca.it/api/v1/insegnamento?af_percorso={}&anno={}&corso_aa={}"
            "&corso_cod={}&insegnamento={}&ordinamento_aa={}")
BASE_SHOW_URL = "https://unisa.coursecatalogue.cineca.it/insegnamenti/{}/{}/{}/{}/{}?coorte={}"
PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}unisa_programs_{YEAR}.json')


LANGUAGE_DICT = {
    "Italiano": ['it'],
    "Italiano E Inglese": ['en', 'it'],
    "Inglese": ['en'],
    "Tedesco": ['de'],
    "Francese": ['fr'],
    "Russo": ['ru'],
    "Spagnolo": ['es']
}


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

        programs_df = pd.read_json(open(PROG_DATA_PATH, "r")).set_index("id")[["courses", "courses_urls_values"]]

        fn = "/home/duboisa1/shifters/Education4Climate-Italy/data/crawling-output/unisa_courses_2023_first.json"
        seen_ids = pd.read_json(fn, orient='records')['id'].to_list()

        already_seen_ids = [] + seen_ids
        for _, (courses, courses_urls_values) in programs_df.iterrows():
            for course_id, course_url_values in zip(courses, courses_urls_values):
                # TODO: not sur this is a correct way to do, might need to change the ids to differentiate between
                #  courses with same ids but different web pages
                if course_id in already_seen_ids:
                    continue
                already_seen_ids += [course_id]
                af_percorso_id, anno, aa, corso_cod, cod, aaOrdId, schemaId = course_url_values.split('-')
                course_url = BASE_URL.format(af_percorso_id, anno, aa, corso_cod, cod, aaOrdId, schemaId)
                course_url_show = BASE_SHOW_URL.format(anno, cod, aaOrdId, af_percorso_id, corso_cod, aa, schemaId)
                print(course_id)
                print(course_url)
                print(course_url_show)
                yield scrapy.Request(course_url, self.parse_course,
                                     cb_kwargs={'course_id': course_id, 'course_url_show': course_url_show})

    @staticmethod
    def parse_course(response, course_id, course_url_show):

        response_json = response.json()

        course_name = (response_json['des_it'].title().strip(" \n").replace('Iii', 'III')
                       .replace('Ii', 'II').replace('Iv', 'IV'))
        if 'lingua_des_it' in response_json:
            language = response_json['lingua_des_it']
            languages = LANGUAGE_DICT[language.title()]
        else:
            languages = ['it']

        teachers = []
        for teacher_json in response_json['docenti']:
            if 'des' in teacher_json:
                teachers += [teacher_json['des'].title()]
        teachers = list(set(teachers))

        def get_text(cur_content, section_name):
            new_content = ''
            if f'{section_name}_it' in contents_json and contents_json[f'{section_name}_it']:
                new_content = contents_json[f'{section_name}_it']
            elif f'{section_name}_en' in contents_json and contents_json[f'{section_name}_en']:
                new_content = contents_json[f'{section_name}_en']
            # Allows to decrease the size of text when it is repeated several times
            if new_content != cur_content:
                cur_content += '\n' + new_content
            return cur_content.strip("\n")

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
            'languages': languages,
            'teachers': teachers,
            'url': response.url,
            'content': content,
            'goal': goal,
            'activity': activity,
            'other': ''
        }
