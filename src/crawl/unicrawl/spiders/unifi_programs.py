from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URLS = ["https://www.unifi.it/p12189.html", "https://www.unifi.it/p11992.html"]


# TODO
#  -

class UniFiSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Università degli studi di Firenze
    """

    name = 'unifi-programs'
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}unifi_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        for url in BASE_URLS:
            yield scrapy.Request(
                url=url,
                callback=self.parse_main
            )

    def parse_main(self, response):

        program_links = response.xpath("//a[text()='info insegnamenti']/@href").getall()
        for link in program_links:
            # Special case with studi umanistici
            if 'p-lis' in link:
                yield response.follow(link, self.parse_studi_umanistici)
                return
            yield response.follow(link, self.parse_program)

    def parse_studi_umanistici(self, response):

        program_links = response.xpath("//div[@id='C_id_1']//a[contains(@href, 'p-cor2')]/@href").getall()
        for link in program_links:
            yield response.follow(link, self.parse_program)

    @staticmethod
    def parse_program(response):

        program_id = "-".join(response.url.split('-')[4:6])

        cycle_name = response.xpath("//main//h1/text()").get()

        program_name = "  in ".join(cycle_name.split("  in ")[1:]).title()
        # Add curriculum name if there is one
        curriculum_name = response.xpath("//div[@id='curriculum']/text()").get()
        if curriculum_name:
            program_name += ' - ' + curriculum_name.replace("Curriculum ", '').title()

        cycle = cycle_name.split("  in")[0]
        if 'Laurea Triennale' in cycle:
            cycle = 'bac'
        elif 'Laurea Magistrale Ciclo unico 5 anni' in cycle:
            cycle = 'master'
        elif 'Laurea Magistrale' in cycle:
            cycle = 'master'
        else:
            print(f"Cycle {cycle} is unidentified.")

        faculty = response.xpath("//div[@id='scuoladiafferenza']//a//text()").get().replace("Scuola di ", '')
        faculty = faculty.strip(" ").replace('"', '"')

        courses_urls = response.xpath("//a[contains(@href, 'p-ins')]/@href").getall()
        # FIXME: not the id given by the university but faster to crawl
        courses_ids = [c.split("-")[-2] for c in courses_urls]

        yield {
            "id": program_id,
            "name": program_name,
            "cycle": cycle,
            "faculties": [faculty],
            "campuses": ["Firenze"],
            "url": response.url,
            "courses": courses_ids,
            "ects": [],  # FIXME: could gather ects if we visit the pages of the courses to get the right ids
            "courses_urls": courses_urls
        }
