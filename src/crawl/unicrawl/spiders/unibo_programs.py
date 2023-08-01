from abc import ABC
from pathlib import Path

import scrapy

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

        faculties_ids = response.xpath("//h2/button/@data-params").getall()
        faculties_names = response.xpath("//h2/button/span[1]/text()").getall()
        for idx, faculty in zip(faculties_ids, faculties_names):
            yield response.follow(f"elenco?{idx}", self.parse_faculty, cb_kwargs={'faculties': [faculty]})

    def parse_faculty(self, response, faculties):

        program_ids = response.xpath("//h3/following::span[1]/text()").getall()
        program_ids = [idx.split(' ')[-1] for idx in program_ids]
        cycles = response.xpath("//h3/preceding::p[1]/text()").getall()
        cycles = ['master' if 'Magistrale' in c else 'bac' for c in cycles]
        campuses = response.xpath("//h3/following::p[1]/text()").getall()
        campuses = [c.split(" ")[-1] for c in campuses]
        languages = response.xpath("//h3/following::p[contains(text(), 'Lingua')][1]/text()").getall()

        program_links = response.xpath("//h3/following::p[@class='goto'][1]/a/@href").getall()

        for i in range(len(program_ids)):
            base_dict = {'cycle': cycles[i], 'faculties': faculties, 'campuses': [campuses[i]]}
            if program_ids[i] == "9245":
                # Special case with Physics, given partly in Italian but described in English
                complete_url = f"{program_links[i]}/course-structure-diagram"
            elif 'Italian' in languages[i]:
                complete_url = f"{program_links[i]}/insegnamenti"
            else:  # only Inglese
                complete_url = f"{program_links[i]}/course-structure-diagram"
                # Translate certain parts to english
                complete_url = complete_url.replace('magistralecu', 'singlecycle').replace('magistrale', '2cycle')
                # Special case with some programs
                complete_url = complete_url.replace('BiologiaMolecolareCellulare', 'MolecularCellBiology')
                complete_url = complete_url.replace('IngegneriaMeccanica', 'MechanicalEngineering')

            yield scrapy.Request(complete_url, self.parse_multi_year_pages, cb_kwargs={'base_dict': base_dict})

    def parse_multi_year_pages(self, response, base_dict):
        # This function takes care of the cases where programs are listed on two different pages
        # Hoping that there are not programs of the same year on two different pages
        sub_pages_urls = response.xpath("//a[contains(@href, 'code=')]/@href").getall()
        if len(sub_pages_urls) == 0:
            yield scrapy.Request(response.url + '/',  # add this bar to no be a duplicated request
                                 self.parse_structure_diagram, cb_kwargs={'base_dict': base_dict})
        else:
            for sub_page_url in sub_pages_urls:
                yield scrapy.Request(sub_page_url,
                                     self.parse_structure_diagram, cb_kwargs={'base_dict': base_dict})

    def parse_structure_diagram(self, response, base_dict):

        main_col_txt = "//div[h2[contains(text(), 'Piani disponibili') or contains(text(), 'Plans available')]]"
        contains_txt = f"contains(text(), '{YEAR}/{YEAR+1}') or contains(text(), '{YEAR}-{YEAR+1}')" \
                       f" or contains(text(), '{YEAR}/{YEAR+1-2000}') or contains(text(), '{YEAR}-{YEAR+1-2000}')"
        sub_programs_links = response.xpath(f"{main_col_txt}//a[{contains_txt}]/@href").getall()
        if len(sub_programs_links) > 1:
            sub_program_names = response.xpath(f"{main_col_txt}//a[{contains_txt}]/preceding::h3[1]/text()").getall()
            # Allows to take care of the case where there are no names above the links
            # For instance here: https://corsi.unibo.it/magistralecu/Giurisprudenza-Bologna/insegnamenti
            if len(sub_program_names) == 0:
                sub_program_names = ["".join(url.split("/")[-2].split("-")) for url in sub_programs_links]
            # used when multiple links for the same year
            already_seen_ids = []
            i = 0
            for sub_program_link in sub_programs_links:
                program_id = '-'.join(sub_program_link.split('/')[-4:-2])
                if program_id in already_seen_ids:
                    continue
                already_seen_ids += [program_id]
                program_name = sub_program_names[i].title().strip(" \n")
                program_name = program_name.replace('Curriculum', '').strip(": ")
                new_dict = {'id': program_id, 'name': program_name}
                yield scrapy.Request(sub_program_link,
                                     self.parse_program, cb_kwargs={'base_dict': {**new_dict, **base_dict}})
                i += 1
        elif len(sub_programs_links) == 1:
            program_id = sub_programs_links[0].split('/')[-4]
            program_name = response.xpath("//span[contains(text(), 'Laurea') or "
                                          "contains(text(), 'Degree')]/following::text()[1]").get()
            program_name = program_name.strip(" \n").title()
            new_dict = {'id': program_id, 'name': program_name}
            yield scrapy.Request(sub_programs_links[0],
                                 self.parse_program, cb_kwargs={'base_dict': {**new_dict, **base_dict}})
        else:
            print(f'No program description for {response.url}')

    @staticmethod
    def parse_program(response, base_dict):

        courses_links = response.xpath("//tr/td/a/@href").getall()
        courses_names = response.xpath("//tr/td/a/text()").getall()
        courses_names = [name.title() for name in courses_names]
        courses_url_codes = [c.split('codiceCorso=')[1].split("&")[0] for c in courses_links]
        courses_ids = [c.split('codiceMateria=')[1].split("&")[0] for c in courses_links]
        # Add program id because there are identic ids across different programs
        courses_ids = [f"{courses_url_codes[i]}-{idx}" for i, idx in enumerate(courses_ids)]
        ects = response.xpath("//tr[td/a]//td[@class='info'][last()]/text()").getall()
        ects = [int(e) for e in ects]

        new_dict = {
            "url": response.url,
            "courses": courses_ids,
            "ects": ects,
            "courses_names": courses_names,
            "courses_url_codes": courses_url_codes
        }

        yield {**base_dict, **new_dict}
