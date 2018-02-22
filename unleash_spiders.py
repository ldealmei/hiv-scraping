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

def prepare_launch():
    doms = pd.read_csv('domains.csv')
    doms = doms[np.logical_and(doms['to_crawl'] == 1, doms['crawled'] == 0)]['domain'].tolist()

    return doms

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
        reactor.stop()


    crawl()
    reactor.run()

    # make_domain_selection()
    #
    # count =0 # for testing purposes
    # for dom in prepare_launch():
    #     runner.crawl('hiv_satellite', domain = [dom])
    #
    #     count +=1
    #     if count >= 5 :
    #         break
    #
    # sat.addBoth(lambda _: reactor.stop())
    # reactor.run()  # the script will block here until the crawling is finished
    #

#  TODO : when multiple spiders they must write to different files otherwise conflict
# TODO : test a second loop starting at transform_list()
# TODO : the hiv checker should also indicate whether the domain is valid or not and delete it from orgs
# TODO : Test and integrate neo4j uploading
# TODO : desactivate TELNET console
# TODO : set-up broad crawling options
# TODO : improve detection of relevance