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
from neo4juploader import Neo4jUploader
import logging
from logging.config import fileConfig
from clf import *
# in clf, the custom classes of the sklearn pipeline are defined. Needed to import this so that the trained object can be loaded



fileConfig('logging_config.ini')
logger = logging.getLogger()
#
# logger = logging.getLogger(__name__)
# log_format= '%(asctime)s :: %(levelname)s :: %(name)s :: %(message)s'
# log_datefmt ='%m/%d/%Y %I:%M:%S'
# logging.basicConfig(filename ='hiv_scraping.log', level=logging.INFO,
#                     format=log_format,datefmt=log_datefmt)
#
# std_handler = logging.StreamHandler()
# formatter = logging.Formatter(fmt = log_format, datefmt=log_datefmt)
# std_handler.setFormatter(formatter)
# logger.addHandler(std_handler)


NUMBER_OF_SATELLITES = 1
DEPTH = 1
URL_SAMPLING_SIZE=1

possible_domains = ['www.nacosa.org.za', 'www.aids.org.za','hivsa.com','www.caprisa.org']
possible_urls = ['http://www.nacosa.org.za/', 'https://www.aids.org.za' ,'http://hivsa.com/','http://www.caprisa.org/Default']


init_domains = [str(item) for item in np.random.choice(possible_domains,URL_SAMPLING_SIZE)]
idxs = [possible_domains.index(item) for item in init_domains]
init_urls = [possible_urls[i] for i in idxs]  #


def transform_list():
    # TODO : make sure loading the whole file doesnt become a bottleneck
    # TODO : Clean this function (the huge try-except) is bad bad bad
    # logger = logging.getLogger(__name__)
    # Load files
    try:
        orgs_df = pd.read_csv('tmp.csv')

    except pd.errors.EmptyDataError:

        logger.info("Returning with no new links")
        return

    try:  # if file already exists
        domains_df = pd.read_csv('domains.csv')

        # Update
        update_doms = pd.merge(orgs_df[['domain']], domains_df, how='inner', on='domain')
        update_doms = update_doms.drop_duplicates()

        logger.info("%s domains to be updated" % len(update_doms))


        if len(update_doms) > 0:
            update_doms['references'] = update_doms.apply(_get_domain_count, args=(orgs_df,), axis=1)

        # Add
        new_doms = pd.merge(orgs_df[['domain']], domains_df, how='left', on='domain')
        new_doms = new_doms[new_doms['references'].isnull()].drop_duplicates()

        logger.info("%s domains to be added" % len(new_doms))

        if len(new_doms) > 0:
            new_doms[['crawled', 'references']] = new_doms[['crawled', 'references']].fillna(
                0)  # we keep 'to_crawl' as NaN to allow the CheckerSpider to decide over that
            new_doms['references'] = new_doms.apply(_get_domain_count, args=(orgs_df,), axis=1)
        # Leave as is

        unchanged_doms = pd.merge(orgs_df[['domain', 'referer']], domains_df, how='right', on='domain')
        unchanged_doms = unchanged_doms[unchanged_doms['referer'].isnull()].drop('referer', axis=1)

        logger.info("%s domains left alone" % len(unchanged_doms))

        # Combine everything together and save to disk
        domains_df = pd.concat([unchanged_doms, update_doms, new_doms])
        domains_df.sort_values(by='references',ascending=False)\
                    .to_csv('domains.csv', index=False)

    except:  # if file does not yet exist
        domains_df = pd.DataFrame(columns=['domain', 'to_crawl', 'crawled', 'references'])

        # Create
        new_doms = pd.merge(orgs_df[['domain']], domains_df, how='left', on='domain')
        new_doms = new_doms[new_doms['references'].isnull()].drop_duplicates()

        logger.info("%s domains to be added" % len(new_doms))

        if len(new_doms) > 0:
            new_doms[['crawled', 'references']] = new_doms[['crawled', 'references']].fillna(
                0)  # we keep 'to_crawl' as NaN to allow the CheckerSpider to decide over that
            new_doms['references'] = new_doms.apply(_get_domain_count, args=(orgs_df,), axis=1)

        # Save to disk
        new_doms.sort_values(by='references', ascending=False)\
                .to_csv('domains.csv', index=False)

def _get_domain_count( df_row, orgs_df):
    new_refs = sum(orgs_df['domain'] == df_row['domain'])
    return df_row['references'] + new_refs

if __name__ == "__main__" :
# --------------- PHASE 1 : Crawl initial list --------------------
# Attention, qd on crawle via un CrawlerProcess les parametres dans settings.py ne sont pas automatiquement loades
# utiliser : = > get_project_settings()

    # configure_logging() #when using a crawlerrunner
    runner = CrawlerRunner(get_project_settings())

    @defer.inlineCallbacks
    def crawl():
        logger.info('Launch HIVBootstrapper')
        yield runner.crawl(HIVBootstraper,
                           allowed_domains = init_domains,
                           start_urls = init_urls)

        # raw_input('Check files - After Bootscrap')
        transform_list()
        # raw_input('Check files - After transform_list')

        yield runner.crawl(HIVChecker)
        # raw_input('Check files - After HIV Checker')

        for l in range(DEPTH):
            for i in range(NUMBER_OF_SATELLITES):
                yield runner.crawl(HIVSatellite)
                # raw_input('Check files - After one Satellite Spider')

            transform_list()
            # raw_input('Check files - After transform_list')

            yield runner.crawl(HIVChecker)
            # raw_input('Check files - After HIV Checker')

        reactor.stop()

    crawl()
    reactor.run()

    # uploader = Neo4jUploader()
    # uploader.set_graph('orgs.csv')
    # uploader.push_graph()

""" EN COURS"""


""" SHORT-TERM / PRECISE"""
# TODO : Instead of multiple satelite spiders only open one?
# TODO : include the use of SiteMaps to improve website craling (https://www.sitemaps.org/index.html)

"""LONG_TERM / VAGUE"""
