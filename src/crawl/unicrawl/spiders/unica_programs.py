from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = f"https://unica.coursecatalogue.cineca.it/api/v1/corsi?anno={YEAR}" + "&area={}&minimal=true"
PROGRAM_URL = f"https://unica.coursecatalogue.cineca.it/corsi/{YEAR}" + "/{}"
PROGRAM_URL_XHR = f"https://unica.coursecatalogue.cineca.it/api/v1/corso/{YEAR}" + "/{}"
COURSE_URL = "https://unica.coursecatalogue.cineca.it/insegnamenti/{}/{}/{}/{}/{}?coorte={}&schemaid={}"
COURSE_URL_XHR = "https://unica.coursecatalogue.cineca.it/api/v1/insegnamento?af_percorso={}&anno={}&corso_aa={}" \
                 "&corso_cod={}&insegnamento={}&ordinamento_aa={}&schema_id={}"
CYCLE_DICT = {
    "Corsi di Laurea": "bac",
    "Corsi di Laurea Magistrale": "master",
    "Corsi di Laurea  Magistrale Ciclo Unico 5 anni": "master",
    "Corsi di Laurea  Magistrale Ciclo Unico 6 anni": "master"
}


class UniCaSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Università di Cagliari
    """

    name = 'unica-programs'
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unica_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        for area in range(1, 7):
            yield scrapy.Request(
                url=BASE_URL.format(area),
                callback=self.parse_programs
            )

    def parse_programs(self, response):

        subgroups = response.json()[0]['subgroups']
        for subgroup in subgroups:
            cds = subgroup['cds']
            for cd in cds:
                program_url = cd['cod']
                yield response.follow(PROGRAM_URL_XHR.format(program_url), self.parse_program)

    @staticmethod
    def parse_program(response):

        main_json = response.json()

        cycle = main_json['gruppo_des_it']
        cycle = CYCLE_DICT[cycle]
        faculty = main_json['area_des_it'].title()

        main_name = main_json['ordinamento_it'].title()
        main_id = main_json['cdsCod']

        programs_json = response.json()['percorsi']
        for i, program_json in enumerate(programs_json):

            if len(programs_json) > 1:
                program_id = f"{main_id}-{i+1}"
                sub_program_name = program_json['des_it'].replace('Curriculum ', '').replace("CURRICULUM: ", '')
                program_name = f"{main_name} - {sub_program_name.title()}"
            else:
                program_id = main_id
                program_name = main_name

            courses_ects = []
            courses_ids = []
            courses_urls = []
            courses_urls_xhr = []
            aaOrdId = program_json['aaOrdId']
            for anno_json in program_json['anni']:
                if 'insegnamenti' in anno_json:
                    for inseganemto in anno_json['insegnamenti']:
                        if 'attivita' in inseganemto:
                            for at_json in inseganemto['attivita']:
                                ects = at_json['crediti']
                                # Courses with ects = 0 are not taken in (generally corresponds to groups of courses)
                                if ects > 0:
                                    courses_ects += [ects]
                                    courses_ids += [at_json['codiciUd'][0]]
                                    courses_urls += [COURSE_URL.format(at_json['aa'], at_json['cod'], aaOrdId,
                                                                       at_json['af_percorso_id'],
                                                                       at_json['corso_cod'],
                                                                       YEAR, at_json['schemaId'])]
                                    courses_urls_xhr += [COURSE_URL_XHR.format(at_json['af_percorso_id'], at_json['aa'],
                                                                               YEAR, at_json['corso_cod'],
                                                                               at_json['cod'],
                                                                               at_json['ordinamento_aa'],
                                                                               at_json['schemaId'])]

            yield {
                "id": program_id,
                "name": program_name,
                "cycle": cycle,
                "faculties": [faculty],
                "campuses": ['Cagliari'],  # Only campus
                "url": PROGRAM_URL.format(response.url.split("/")[-1]),
                "courses": courses_ids,
                "ects": courses_ects,
                "courses_urls": courses_urls,
                "courses_urls_xhr": courses_urls_xhr
            }
