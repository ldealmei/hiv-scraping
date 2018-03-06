import sys
import os
import pandas as pd
import numpy as np
sys.path.insert(0,'hiv_scraping/spiders')

import scrapy
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from hiv_org2 import HIVBootstraper, HIVChecker, HIVSatellite
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, defer
from scrapy.utils.log import configure_logging

import logging


NUMBER_OF_SATELLITES = 25
DEPTH = 20
URL_SAMPLING_SIZE=4

possible_domains = ['www.nacosa.org.za', 'www.aids.org.za','hivsa.com','www.caprisa.org']
possible_urls = ['http://www.nacosa.org.za/', 'https://www.aids.org.za' ,'http://hivsa.com/','http://www.caprisa.org/Default']


init_domains = [str(item) for item in np.random.choice(possible_domains,URL_SAMPLING_SIZE)]
idxs = [possible_domains.index(item) for item in init_domains]
init_urls = [possible_urls[i] for i in idxs]  #



if __name__ == "__main__" :
# --------------- PHASE 1 : Crawl initial list --------------------
# Attention, qd on crawle via un CrawlerProcess les parametres dans settings.py ne sont pas automatiquement loades
# utiliser : = > get_project_settings()

    configure_logging() #when using a crawlerrunner
    runner = CrawlerRunner(get_project_settings())

    logging.basicConfig(filename='myapp.log', level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S')


    @defer.inlineCallbacks
    def crawl():
        yield runner.crawl(HIVBootstraper,
                           allowed_domains = init_domains,
                           start_urls = init_urls)

        for l in range(DEPTH):
            yield runner.crawl(HIVChecker)
            for i in range(NUMBER_OF_SATELLITES):
                yield runner.crawl(HIVSatellite)

        reactor.stop()

    crawl()
    reactor.run()

""" SHORT-TERM / PRECISE"""
# TODO : Make sure everything behaves as expected
# TODO : Write debug and monitoring functions
# TODO :  Catch AttributeErrors when parsing
# TODO : Do not leave the spiderclass after 30 seconds

"""LONG_TERM / VAGUE"""

# TODO : Test and integrate neo4j uploading (do it after the HIVChecker spider and change the orgs.csv filename with an uploading time timestamp
# TODO : desactivate TELNET console
# TODO : improve detection of relevance
