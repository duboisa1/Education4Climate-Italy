from abc import ABC
from pathlib import Path

import pandas as pd
import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://corsidilaurea.uniroma1.it/it/view-course-details{}"
PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}uniroma1_programs_{YEAR}.json')

LANGUAGE_DICT = {
    "Italiano": 'it',
    "Inglese": 'en',
    "Francese": 'fr',
    "Spagnolo": 'es',
    "Tedesco": 'de',
    "Russo": 'ru',
    "Portoghese": 'pt',
    "Cinese": 'cn',
    "Polacco": 'pl',
    "Olandese": 'nl',
    "Giapponese": 'jp',
    "Catalano": 'ca',
    "Slovacco": 'sk',
    "Arabo": 'ar',
    "Bulgaro": 'bg',
    "Ucraino": 'ua'
}


class UniRoma1CourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for Università degli studi di Roma “La Sapienza”
    """

    name = "uniroma1-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}uniroma1_courses_{YEAR}.json').as_uri()
    }

    def start_requests(self):

        courses_df = pd.read_json(open(PROG_DATA_PATH, "r")).set_index("id")[["courses", "courses_url_codes",
                                                                              "courses_languages"]]
        courses_ids_list = courses_df["courses"].sum()
        courses_urls_list = courses_df["courses_url_codes"].sum()
        courses_lang_list = courses_df["courses_languages"].sum()

        courses_df = pd.DataFrame({'id': courses_ids_list, 'url': courses_urls_list, 'language': courses_lang_list})
        courses_df = courses_df.drop_duplicates(subset='id')

        for _, (course_id, course_url_code, course_lang) in courses_df.iterrows():
            yield scrapy.Request(BASE_URL.format(course_url_code), self.parse_course,
                                 cb_kwargs={'course_id': course_id, 'language': course_lang})

    @staticmethod
    def parse_course(response, course_id, language):

        course_name = response.xpath("//li[contains(text(), 'Insegnamento')]/text()[2]").get()
        sub_course_name = response.xpath("//section[@id='breadcrumb']//li[last()]//text()").get()
        if course_name is None:
            course_name = sub_course_name.split("- ")[1].title().strip(" ")
        else:
            course_name = course_name.split("- ")[1].title().strip(" ") + ' - '
            sub_course_name = sub_course_name.title().strip(" ") if sub_course_name != 'UNIT II' else 'Unit II'
            course_name += sub_course_name

        teachers = response.xpath("//div[contains(@class, 'teacher')]/h3/img/following::text()[1]").getall()
        teachers = [t.strip("\r\b\t\n \xa0") for t in teachers]
        teachers = [" ".join(t.split(" ")[1:] + [t.split(" ")[0]]).title() for t in teachers]
        goal = cleanup(response.xpath("//h3[div[contains(text(), 'Obiettivi')]]"
                                      "//following::div[contains(@class,'teacher-text')][1]").get())

        yield {
            'id': course_id,
            'name': course_name,
            'year': f"{YEAR}-{YEAR + 1}",
            'languages': [language],
            'teachers': teachers,
            'url': response.url,
            'content': '',
            'goal': goal,
            'activity': '',
            'other': ''
        }
