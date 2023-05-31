from abc import ABC
from pathlib import Path

import pandas as pd
import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}unimib_programs_{YEAR}.json')


LANGUAGE_DICT = {
    "Italiano": 'it',
    "Italian": 'it',
    "ita": 'it',
    "italiano": 'it',
    "eng": 'en',
    "Inglese": 'en',
    "English": 'en',
    "fra": 'fr',
    "spa": 'es',
    "deu": 'de'
}


class UniMiBCourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for Milano Bicocca
    """

    name = "unimib-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unimib_courses_{YEAR}.json').as_uri()
    }

    def start_requests(self):

        programs_df = pd.read_json(open(PROG_DATA_PATH, "r")).set_index("id")[["courses", "courses_names",
                                                                               "courses_urls"]]

        fn = '/home/duboisa1/shifters/Education4Climate-Italy/data/crawling-output/unimib_courses_2022_old.json'
        old_courses = pd.read_json(fn, orient='records')['id'].tolist()
        print(old_courses)
        print(len(set(programs_df['courses'].sum())))

        for _, (courses_ids, courses_names, courses_urls) in programs_df.iterrows():
            for course_id, course_name, course_url in zip(courses_ids, courses_names, courses_urls):

                if course_id in old_courses:
                    continue
                print(course_id)

                base_dict = {
                    'id': course_id,
                    'name': course_name,
                    'year': f"{YEAR}-{YEAR + 1}",
                    'languages': [],
                    'teachers': [],
                    'url': course_url,
                    'content': '',
                    'goal': '',
                    'activity': '',
                    'other': ''
                }
                yield scrapy.Request(course_url, self.parse_course,
                                     cb_kwargs={'base_dict': base_dict, 'sub_courses_links': []})

    def parse_course(self, response, base_dict, sub_courses_links):

        # Check if there are sub-pages
        sub_courses_links_new = response.xpath("//a[@class='coursename-linked']//following::div[1]/a/@href").getall()
        if len(sub_courses_links_new) > 0:
            yield response.follow(sub_courses_links_new[0], self.parse_course,
                                  cb_kwargs={'base_dict': base_dict, 'sub_courses_links': sub_courses_links_new[1:]})
            return

        # If we are on a final page, collect the content
        languages = response.xpath("//dt[text()='Lingua' or text()='Language']//following::dd/text()").get()
        if languages:
            languages = [LANGUAGE_DICT[l.strip(" ")] for l in languages.split(",")]
            base_dict['languages'] = list(set(base_dict['languages']).union(set(languages)))
        else:
            base_dict['languages'] = ['it']

        teachers = response.xpath("//div[contains(@class, 'course-contacts')]"
                                  "//div[@class='contact-name']/text()").getall()
        teachers = [" ".join(t.split(" ")[1:] + [t.split(" ")[0]]) for t in teachers]
        base_dict['teachers'] = list(set(base_dict['teachers']).union(set(teachers)))

        def get_text(field, section_name):
            section_name = 'field ' + section_name
            text = cleanup(response.xpath(f"//div[contains(@class, \'{section_name}\')]//div").get())
            if len(text) > 0:
                if section_name == 'field-program' or section_name == 'field-objective' or section_name == 'field-goal':
                    print(response.url, section_name)
                base_dict[field] += "\n" + text

        # TODO Does it make sense to add field-Programma? Does field-program exist ?
        for sn in ['field-content', 'field-Contenuti', 'field-Programma', 'field-program']:
            get_text('content', sn)

        # TODO do field-objective or field-goal exist ?
        for sn in ['field-Obiettivi', 'field-objective', 'field-goal']:
            get_text('goal', sn)

        # If there are no other sub courses to explore, save the result, otherwise visit the other pages
        if len(sub_courses_links) == 0:
            base_dict['content'] = base_dict['content'].strip("\n ")
            base_dict['goal'] = base_dict['goal'].strip("\n ")
            yield base_dict
        else:
            yield response.follow(sub_courses_links[0], self.parse_course,
                                  cb_kwargs={'base_dict': base_dict, 'sub_courses_links': sub_courses_links[1:]})

