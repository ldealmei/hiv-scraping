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
    #TODO  :keep the http (url scheme) when saving the domain
# --------------- PHASE 1 : Crawl initial list --------------------
# Attention, qd on crawle via un CrawlerProcess les parametres dans settings.py ne sont pas automatiquement loades
# utiliser : = > get_project_settings()

    configure_logging() #when using a crawlerrunner
    runner = CrawlerRunner(get_project_settings())

    @defer.inlineCallbacks
    def crawl():
        yield runner.crawl(HIVBootstraper)
        yield runner.crawl(HIVChecker)
        #TODO : Make this value adaptable (or not, maybe we could limit the exploration to the top x sites)
        for i in range(5):
            yield runner.crawl(HIVSatellite)
        reactor.stop()

    crawl()
    reactor.run()


""" FIRST"""

    # TODO : the hiv checker should also indicate whether the domain is valid or not and delete it from orgs
    # This means catching errors and parsing them

# TODO : test a second loop starting at transform_list()
"""AFTER"""
# TODO : Test and integrate neo4j uploading (do it after the HIVChecker spider and change the orgs.csv filename with an uploading time timestamp
# TODO : desactivate TELNET console
# TODO : set-up broad crawling options
# TODO : improve detection of relevance