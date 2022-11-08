from abc import ABC
from pathlib import Path

import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://www.unibo.it/it/didattica/corsi-di-studio"


class UniBoSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Universita di Bologna
    """

    name = 'unibo-programs'
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unibo_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        yield scrapy.Request(
            url=BASE_URL,
            callback=self.parse_main
        )

    def parse_main(self, response):

        # TODO: get faculties name
        # faculties_ids = [idx.split('=')[-1] for idx in response.xpath("//h2/button/@data-params").getall()]
        faculties_ids = response.xpath("//h2/button/@data-params").getall()
        for idx in faculties_ids:
            yield response.follow(f"elenco?{idx}", self.parse_faculty)

    def parse_faculty(self, response):

        program_names = response.xpath("//h3/text()").getall()
        program_ids = response.xpath("//h3/following::span[1]/text()").getall()
        program_ids = [idx.split(' ')[-1] for idx in program_ids]
        cycles = response.xpath("//h3/preceding::p[1]/text()").getall()
        cycles = ['master' if 'Magistrale' in c else 'bac' for c in cycles]
        campuses = response.xpath("//h3/following::p[1]/text()").getall()
        campuses = [c.split(" ")[-1] for c in campuses]
        languages = response.xpath("//h3/following::p[contains(text(), 'Lingua')][1]").getall()

        program_links = response.xpath("//h3/following::p[@class='goto'][1]/a/@href").getall()

        for i in range(len(program_ids)):
            base_dict = {'cycle': cycles[i], 'campuses': [campuses[i]]}
            complete_url = 'insegnamenti' if 'Italian' in languages[i] else 'course-structure-diagram'
            yield scrapy.Request(f"{program_links[i]}/{complete_url}",
                                 self.parse_structure_diagram, cb_kwargs={'base_dict': base_dict})

    def parse_structure_diagram(self, response, base_dict):

        sub_programs_links = response.xpath("//main//a[contains(text(), '2022/2023')"
                                            " or contains(text(), '2022-2023')"
                                            " or contains(text(), '2022-23')]/@href").getall()
        if len(sub_programs_links) > 1:
            for i, sub_program_link in enumerate(sub_programs_links):
                program_id = '-'.join(sub_program_link.split('/')[-4:-2])
                # TODO: not working
                program_name = response.xpath(f"//h3[following::a/@href='{sub_program_link}']/text()").get()
                program_name = program_name.replace("CURRICULUM ", '').title()
                new_dict = {'program_id': program_id, 'program_name': program_name}
                yield scrapy.Request(sub_program_link,
                                     self.parse_program, cb_kwargs={**new_dict, **base_dict})

        # TODO: add case with single program

    @staticmethod
    def parse_program(response, program_id, program_name, cycle, campuses):

        courses_links = response.xpath("//tr/td/a/@href").getall()
        courses_ids = [c.split('codiceMateria=')[1].split("&")[0] for c in courses_links]
        ects = response.xpath("//tr[td/a]//td[@class='info'][last()]/text()").getall()
        ects = [int(e) for e in ects]
        if len(courses_ids) != len(ects):
            print(response.url)
            print(len(ects), len(courses_ids))

        yield {
            "id": program_id,
            "name": program_name,
            "cycle": cycle,
            "faculties": [],
            "campuses": campuses,
            "url": response.url,
            "courses": courses_ids,
            "ects": ects
        }
