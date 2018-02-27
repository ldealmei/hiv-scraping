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
#
# def prepare_launch():
#     doms = pd.read_csv('domains.csv')
#     doms = doms[np.logical_and(doms['to_crawl'] == 1, doms['crawled'] == 0)]['domain'].tolist()
#
#     return doms

if __name__ == "__main__" :
# --------------- PHASE 1 : Crawl initial list --------------------
# Attention, qd on crawle via un CrawlerProcess les parametres dans settings.py ne sont pas automatiquement loades
# utiliser : = > get_project_settings()

    configure_logging() #when using a crawlerrunner
    runner = CrawlerRunner(get_project_settings())

    @defer.inlineCallbacks
    def crawl():
        yield runner.crawl(HIVBootstraper,
                           allowed_domains = ['www.nacosa.org.za'],
                           start_urls = ['http://www.nacosa.org.za/'])

        # ['www.nacosa.org.za' 'www.aids.org.za','hivsa.com','www.caprisa.org']
        # start_urls = ['http://www.nacosa.org.za/', 'https://www.aids.org.za' ,'http://hivsa.com/','http://www.caprisa.org/Default']

        yield runner.crawl(HIVChecker)
        #TODO : Make this value adaptable (or not, maybe we could limit the exploration to the top x sites)
        for i in range(5):
            yield runner.crawl(HIVSatellite)
        reactor.stop()

    crawl()
    reactor.run()

""" FIRST"""
# TODO : HIV Satellites export to orgs.csv logic is not correct (multiple links to same site)
# TODO : Why is pepfar.org, a site that should be accepted to be scraped not in the domains.csv ?
# TODO : test a second loop starting at transform_list()

"""AFTER"""
# TODO : Test and integrate neo4j uploading (do it after the HIVChecker spider and change the orgs.csv filename with an uploading time timestamp
# TODO : desactivate TELNET console
# TODO : set-up broad crawling options
# TODO : improve detection of relevance