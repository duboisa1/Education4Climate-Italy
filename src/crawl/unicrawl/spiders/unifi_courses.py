from abc import ABC
from pathlib import Path

import pandas as pd
import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://www.unifi.it/{}"
PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}unifi_programs_{YEAR}.json')
LANGUAGE_DICT = {
    "Italian": 'it',
    "Inglese": 'en',
    "English": 'en',
    "Francese": 'fr',
    "Spagnolo": 'es',
    "Russo": 'ru',
    "Portoghese": 'pt',
    "Ungherese": 'hu',
    "Cinese": 'cn',
    "Giapponese": 'jp',
    "Tedesca": 'de',
    "Turco": 'tr'
}


class UniFiCourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for Università degli studi di Firenze
    """

    name = "unifi-courses"
    custom_settings = {
        # FIXME: might need to change it to 'pre.json' to deal with duplicates
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unifi_courses_{YEAR}.json').as_uri()
    }

    def start_requests(self):

        programs_df = pd.read_json(open(PROG_DATA_PATH, "r")).set_index("id")[["courses", "courses_urls"]]

        for _, (courses_ids, courses_urls) in programs_df.iterrows():
            for course_idx, course_url in zip(courses_ids, courses_urls):
                yield scrapy.Request(BASE_URL.format(course_url), self.parse_course,
                                     cb_kwargs={'course_id': course_idx})

    @staticmethod
    def get_languages(response):
        language = response.xpath("//h3[contains(text(), 'Lingua')]//following::div[1]/text()").get()
        print(language.title())
        if language:
            languages = [value for key, value in LANGUAGE_DICT.items() if key in language.title()]
            return ['it'] if len(languages) == 0 else languages
        else:
            return ['it']

    @staticmethod
    def get_teachers(response):
        teachers = response.xpath("//a[contains(@href, 'p-doc')]/text()").getall()
        if len(teachers) != 0:
            teachers = [t.strip(" \n").replace('  ', ' ').title() for t in teachers if len(t.strip(' \n')) != 0]
        return teachers

    @staticmethod
    def get_content(response):
        content = response.xpath("//h3[contains(text(), 'Contenuto')]//following::div[1]/text()").get()
        content = content if content else ''
        content2 = response.xpath("//h3[contains(text(), 'Programma')]//following::div[1]/text()").get()
        content = content + '\n' + content2 if content2 else content
        return content.strip("\n")

    @staticmethod
    def get_goal(response):
        goal = response.xpath("//h3[contains(text(), 'Obiettivi')]//following::div[1]/text()").get()
        return goal.strip("\n") if goal else ''

    def parse_course(self, response, course_id):

        course_name = response.xpath("//main//h1/text()").get()
        course_name = course_name.split("- ")[1].title().replace("Ii", "II")

        ects = response.xpath("//div[contains(text(), 'Crediti')]//following::div[1]/text()").get()
        ects = int(float(ects.strip(" \n"))) if ects else 0

        base_dict = {
            'id': course_id,
            'name': course_name,
            'year': f"{YEAR}-{YEAR + 1}",
            'languages': [],
            'teachers': [],
            'url': response.url,
            'ects': [ects],
            'content': '',
            'goal': '',
            'activity': '',
            'other': '',
        }

        # There are cases were the course in divided in sub-modules
        sub_courses_links = response.xpath("//a[contains(@href, 'p-ins')]/@href").getall()
        if len(sub_courses_links) > 1:
            yield response.follow(sub_courses_links[0], self.parse_sub_courses,
                                  cb_kwargs={'sub_courses_links': sub_courses_links[1:], 'base_dict': base_dict})

        base_dict['languages'] = self.get_languages(response)
        base_dict['teachers'] = self.get_teachers(response)
        base_dict['content'] = self.get_content(response)
        base_dict['goal'] = self.get_goal(response)

        yield base_dict

    def parse_sub_courses(self, response, sub_courses_links, base_dict):

        base_dict['languages'] += self.get_languages(response)
        base_dict['teachers'] += self.get_teachers(response)
        base_dict['content'] += '\n' + self.get_content(response)
        base_dict['goal'] += '\n' + self.get_goal(response)

        if len(sub_courses_links) == 0:
            base_dict['languages'] = list(set(base_dict['languages']))
            print(base_dict['teachers'])
            base_dict['teachers'] = list(set(base_dict['teachers']))
            base_dict['content'] = base_dict['content'].strip("\n")
            base_dict['goal'] = base_dict['goal'].strip("\n")
            yield base_dict
        else:
            yield response.follow(sub_courses_links[0], self.parse_sub_courses,
                                  cb_kwargs={'sub_courses_links': sub_courses_links[1:], 'base_dict': base_dict})