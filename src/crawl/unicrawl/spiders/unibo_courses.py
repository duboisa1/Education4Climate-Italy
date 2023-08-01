from abc import ABC
from pathlib import Path

import pandas as pd
import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://www.unibo.it/it/didattica/insegnamenti?codiceMateria={}" + f"&annoAccademico={YEAR}" \
           + "&codiceCorso={}&single=True&search=True"
PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}unibo_programs_{YEAR}.json')

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


class UniBoCourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for Universita di Bologna
    """

    name = "unibo-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unibo_courses_{YEAR}_pre.json').as_uri()
    }

    def start_requests(self):

        courses_ids = pd.read_json(open(PROG_DATA_PATH, "r")).set_index("id")[["courses",
                                                                               "courses_names", "courses_url_codes"]]

        for program_id, (courses_ids, courses_names, courses_urls_codes) in courses_ids.iterrows():
            for course_id, course_name, course_url_code in zip(courses_ids, courses_names, courses_urls_codes):

                base_dict = {
                    'id': course_id,
                    'name': course_name.strip(" "),
                    'year': f"{YEAR}-{YEAR+1}",
                    'languages': [],
                    'teachers': [],
                    'url': None,
                    'content': '',
                    'goal': '',
                    'activity': '',
                    'other': ''
                }
                yield scrapy.Request(BASE_URL.format(course_id.split("-")[-1], course_url_code), self.parse_course,
                                     cb_kwargs={"base_dict": base_dict, "sub_links": []})

    def parse_course(self, response, base_dict, sub_links):
        """ Call this function in a recursive way to get all subcourses"""

        # If we are on the first call, check that there are sublinks
        if len(sub_links) == 0:
            sub_courses_links = response.xpath(f"//span[@class='teachingname']"
                                               f"/a[contains(text(), '{base_dict['id']}')]/@href").getall()

            # Launch the first recursive call
            if len(sub_courses_links) != 0:
                base_dict['url'] = response.url
                yield scrapy.Request(sub_courses_links[0], self.parse_course,
                                     cb_kwargs={"base_dict": base_dict, "sub_links": sub_courses_links[1:-1]})
                return

        # If there is no sublink or if it is the first recursive call, get name and year
        if base_dict["url"] is None:
            base_dict["url"] = response.url

        # For all calls, complete the list of teachers and languages
        teachers = response.xpath("//div[@id='u-content-main']//span[@class='title' "
                                  "and text()='Docente']/following::text()[2]").getall()
        # Put surname first
        teachers = [f"{' '.join(t.split(' ')[1:])} {t.split(' ')[0]}" for t in teachers]
        base_dict["teachers"] += teachers

        languages = response.xpath("//div[@id='u-content-main']//span[@class='title' "
                                   "and contains(text(), 'Lingua')]/following::span[1]/text()").getall()
        languages = [LANGUAGE_DICT[l] for l in languages]
        base_dict["languages"] += languages

        # For all calls, append the course description
        def get_sections_text(section_name):
            following_section_name = response.xpath(f'//h2[contains(text(), "{section_name}")]/following::h2[1]/text()').get()
            if following_section_name:
                xpath = f'//h2[contains(text(), "{section_name}")]' \
                        f'/following-sibling::*[following::h2[contains(text(), "{following_section_name}")]]'
            else:
                xpath = f'//h2[contains(text(), "{section_name}")]/following-sibling::*'
            texts = cleanup(response.xpath(xpath).getall())
            return "\n".join(texts).strip("\n")
        base_dict["content"] += "\n" + get_sections_text("Contenuti")
        base_dict["goal"] += get_sections_text("Conoscenze e abilità da conseguire")

        if len(sub_links) != 0:
            yield scrapy.Request(sub_links[0], self.parse_course,
                                 cb_kwargs={"base_dict": base_dict, "sub_links": sub_links[1:-1]})
        else:
            # On the last call trim content, remove duplicates from lists
            base_dict["teachers"] = sorted(list(set(base_dict["teachers"])))
            base_dict["languages"] = sorted(list(set(base_dict["languages"])))
            if len(base_dict["languages"]) == 0:
                base_dict["languages"] = ["it"]
            base_dict["content"] = base_dict["content"].strip("\n")
            base_dict["goal"] = base_dict["goal"].strip("\n")

            yield base_dict
