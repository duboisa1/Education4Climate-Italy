from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = "https://web.unisa.it/didattica/corsi-laurea"
CYCLE_DICT = {
    "CORSO DI LAUREA": "bac",
    "CORSO DI LAUREA MAGISTRALE": "master",
    "CORSO DI LAUREA MAGISTRALE A CICLO UNICO DI 5 ANNI": "master",
    "CORSO DI LAUREA MAGISTRALE A CICLO UNICO DI 6 ANNI": "master"
}

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

        program_links = response.xpath("//table//tr/td[2]/a/@href").getall()
        program_ids = response.xpath("//table//tr/td[2]/span[1]/text()").getall()
        for link, idx in zip(program_links, program_ids):
            yield response.follow(link.strip(" ") + "/didattica/insegnamenti", self.parse_program,
                                  cb_kwargs={"program_id": idx})

    @staticmethod
    def parse_program(response, program_id):

        program_name = response.xpath("//a/h1/text()").get()
        cycle = response.xpath("//a/h4/text()").get().strip(' ')
        cycle = CYCLE_DICT[cycle]

        faculty_link = response.xpath("//a[@aria-label='Sezione Dipartimento']/@href").get()
        faculty_id = faculty_link.split("www.")[1].split(".unisa.it")[0]
        faculty = FACULTY_DICT[faculty_id]

        courses_urls = response.xpath("//table//tr//a/@href").getall()
        courses_ids = [url.split("id=")[1] for url in courses_urls]

        yield {
            "id": program_id,
            "name": program_name,
            "cycle": cycle,
            "faculties": [faculty],
            "campuses": ["Salerno"],  # FIXME: not sur there is another campus
            "url": response.url.split("/didattica")[0],
            "courses": courses_ids,
            "ects": []
        }
