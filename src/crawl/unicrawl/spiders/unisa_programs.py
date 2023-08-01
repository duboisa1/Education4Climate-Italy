from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = f"https://unisa.coursecatalogue.cineca.it/api/v1/gruppi/{YEAR}"
CYCLE_URL = f"https://unisa.coursecatalogue.cineca.it/api/v1/corsi?anno={YEAR}&gruppo=" + "{}"
PROGRAM_URL = f"https://unisa.coursecatalogue.cineca.it/api/v1/corso/{YEAR}/" + "{}"
PROGRAM_SHOW_URL = f"https://unisa.coursecatalogue.cineca.it/corsi/{YEAR}/" + "{}"
# https://unisa.coursecatalogue.cineca.it/api/v1/insegnamento?
COURSES_URL = "af_percorso={}&anno={}&corso_aa={}&corso_cod={}&insegnamento={}&ordinamento_aa={}"
# https://unisa.coursecatalogue.cineca.it/insegnamenti/
COURSES_SHOW_URL = "{}/{}/{}/{}/{}?coorte={}"

FACULTY_DICT = {
    "dcb": "Dipartimento di Chimica e Biologia 'Adolfo Zambelli'",
    "dispac": 'Dipartimento di Scienze del Patrimonio Culturale',
    "dises": 'Dipartimento di Scienze Economiche e Statistiche',
    "disa": 'Dipartimento di Scienze Aziendali - Management & Innovation Systems',
    "difarma": 'Dipartimento di Farmacia',
    "df": "Dipartimento di Fisica 'E.R. Caianiello'",
    "dipmed": "Dipartimento di Medicina, Chirurgia a Odontoiatria 'Scuola Medica Salernitana'",
    "dsg": 'Dipartimento di Scienze Giuridiche',
    "di": 'Dipartimento di Informatica',
    "diin": 'Dipartimento di Ingegneria Industriale',
    "diciv": 'Dipartimento di Ingegneria Civile',
    "diem": "Dipartimento di Ingegneria dell'Informazione ed Elettrica e Matematica Applicata",
    "dipmat": 'Dipartimento di Matematica',
    "dispc": 'Dipartimento di Scienze Politiche e della Comunicazione',
    "disuff": 'Dipartimento di Scienze Umane, Filosofiche e della Formazione',
    "disps": 'Dipartimento di Studi Politici e Sociali',
    "dipsum": 'Dipartimento di Studi Umanistici'

}


class UniSaiSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Università di Salerno
    """

    name = 'unisa-programs'
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unisa_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        yield scrapy.Request(
            url=BASE_URL,
            callback=self.parse_main
        )

    def parse_main(self, response):

        print(response.json())
        for cycle_json in response.json():
            cycle = cycle_json['des_it']
            if 'Triennali' in cycle:
                cycle = 'bac'
            elif 'Magistrali' in cycle:
                cycle = 'master'
            else:
                # Do not consider programs that are not bachelor or master for now
                continue
            yield response.follow(CYCLE_URL.format(cycle_json['cod']), self.parse_programs,
                                  cb_kwargs={'cycle': cycle})

    def parse_programs(self, response, cycle):

        for subgroup_json in response.json()[0]['subgroups']:
            for cd_json in subgroup_json['cds']:
                program_id = cd_json['cdsCod']
                program_name = cd_json['des_it'].title().strip("\n ")
                url_id = cd_json['cdsId']
                base_dict = {
                    "id": program_id,
                    "name": program_name,
                    "cycle": cycle,
                    "faculties": [],
                    "campuses": ["Salerno"],  # FIXME: not sur there is another campus
                    "url": PROGRAM_SHOW_URL.format(url_id)
                }
                yield response.follow(PROGRAM_URL.format(url_id), self.parse_program,
                                      cb_kwargs={'base_dict': base_dict})

    @staticmethod
    def parse_program(response, base_dict):

        base_dict['faculties'] = [response.json()['dip_des_it'].title().replace("'", '"')]

        courses = []
        ects = []
        courses_urls_values = []
        # aa = str(response.json()['aa'])
        for percorso_json in response.json()['percorsi']:
            # aaOrdId = str(percorso_json['aaOrdId'])
            for anno_json in percorso_json['anni']:
                anno = str(anno_json['annoOfferta'])
                for ins_json in anno_json['insegnamenti']:
                    for att in ins_json['attivita']:
                        courses += [att['adCod']]
                        ects += [att['crediti']]
                        # TODO: still not sure these are the right inputs
                        course_url_code = att['corsoOfferta']['cdsId'] if 'corsoOfferta' in att else att['corso_cod']
                        courses_urls_values += ["-".join([att['af_percorso_id'], str(att['aa']), str(YEAR),
                                                          str(course_url_code), str(att['cod']), str(att['ordinamento_aa']),
                                                          str(att['schemaId'])])]

        base_dict['courses'] = courses
        base_dict['ects'] = ects
        base_dict['courses_urls_values'] = courses_urls_values
        yield base_dict
