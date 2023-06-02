from abc import ABC
from pathlib import Path

import pandas as pd
import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://www4.ceda.polimi.it"
PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}polimi_programs_{YEAR}.json')


LANGUAGE_DICT = {
    "Italian": 'it',
    "English": 'en',
    "Inglese": 'en'
}


class PoliMiCourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for Politecnico di Milano
    """

    name = "unict-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}polimi_courses_{YEAR}_pre.json').as_uri()
    }

    def start_requests(self):

        programs_df = pd.read_json(open(PROG_DATA_PATH, "r")).set_index("id")[["courses", "courses_urls"]]

        for _, (courses_ids, courses_urls) in programs_df.iterrows():
            for course_id, course_url in zip(courses_ids, courses_urls):
                yield scrapy.Request(BASE_URL + course_url, self.parse_main,
                                     cb_kwargs={'course_id': course_id})

    def parse_main(self, response, course_id):

        course_name = response.xpath("//td[contains(text(), 'Denominazione Insegnamento')]"
                                     "//following::td[1]/text()").get()
        course_name = course_name.strip(" \t\r\n").title().replace('Iii', 'III').replace('Ii', 'II')
        print(course_name)

        # Just get the first description link, can have several for different names
        desc_link = response.xpath("//a[contains(@href, 'aunicalogin') and img][1]/@href").get()
        if desc_link:
            yield scrapy.Request(desc_link, self.parse_course,
                                 cb_kwargs={'course_id': course_id, "course_name": course_name, 'url': response.url})
        else:
            yield {
                'id': course_id, 'name': course_name, 'year': f"{YEAR}-{YEAR + 1}",
                'languages': ['it'], 'teachers': [], 'url': response.url,
                'content': '', 'goal': '', 'activity': '', 'other': ''
            }

    @staticmethod
    def parse_course(response, course_id, course_name, url):

        teachers = response.xpath("//td[contains(text(), 'Docente')]//following::td[1]/a/text()").getall()
        teachers = [t.strip(" \t\r\n") for t in teachers]

        language = " ".join(response.xpath("//div[contains(text(), 'erogato in lingua')]//text()").getall())
        languages = [value for key, value in LANGUAGE_DICT.items() if key in language.title()]
        languages = ['it'] if len(languages) == 0 else languages

        content = cleanup(response.xpath("//td[@class='TitleInfoCard' and contains(text(), 'Argomenti')]"
                                         "//following::table[1]").get())
        goal = cleanup(response.xpath("//td[@class='TitleInfoCard' and contains(text(), 'Obiettivi')]"
                                      "//following::table[1]").get())
        goal += "\n" + cleanup(response.xpath("//td[@class='TitleInfoCard' and contains(text(), 'Risultati')]"
                                              "//following::table[1]").get())
        goal = goal.strip("\n")

        yield {
            'id': course_id,
            'name': course_name,
            'year': f"{YEAR}-{YEAR + 1}",
            'languages': languages,
            'teachers': teachers,
            'url': url,
            'content': content,
            'goal': goal,
            'activity': '',
            'other': ''
        }
