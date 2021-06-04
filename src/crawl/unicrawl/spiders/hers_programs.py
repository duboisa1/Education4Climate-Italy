# -*- coding: utf-8 -*-
from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "http://progcours.hers.be/cocoon/fac/fac{}"
DEPARTMENTS_CODES = {"M": "Département Paramédicale",
                     "P": "Département Pédagogique",
                     "E": "Département Economique",
                     "T": "Département Technique",
                     "S": "Département Social"}


class HERSProgramSpider(scrapy.Spider, ABC):
    """
    Program crawler for Haute Ecole Robert Schuman
    """

    name = "hers-programs"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}hers_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        for code in DEPARTMENTS_CODES.keys():
            yield scrapy.Request(BASE_URL.format(code), self.parse_main,
                                 cb_kwargs={'faculty': DEPARTMENTS_CODES[code]})

    def parse_main(self, response, faculty):
        # Get list of programs
        programs_names = response.xpath(f"//a[@class='LienProg']/text()").getall()
        programs_links = response.xpath(f"//a[@class='LienProg']/@href").getall()
        programs_codes = [link.split("/")[-1].split("_")[0] for link in programs_links]
        programs_cycles = [name.split(" ")[0].lower() for name in programs_names]

        for name, code, link, cycle in zip(programs_names, programs_codes, programs_links, programs_cycles):

            if 'bachelier' in cycle:
                cycle = 'bac'
            elif 'master' in cycle:
                cycle = 'master'
            else:
                cycle = 'other'

            base_dict = {'id': code,
                         'name': name,
                         'cycle': cycle,
                         'faculty': faculty,
                         'campus': ''}
            yield response.follow(link, self.parse_program, cb_kwargs={'base_dict': base_dict})

    @staticmethod
    def parse_program(reponse, base_dict):

        ects = reponse.xpath("//td[contains(@class, 'ContColG')]/text()").getall()
        ects = [e for e in ects if e != '\xa0']
        # TODO: check if there are UEs
        courses_ids = reponse.xpath("//nobr/text()").getall()

        cur_dict = {"ects": ects,
                    "courses": courses_ids
                    }

        yield {**base_dict, **cur_dict}
