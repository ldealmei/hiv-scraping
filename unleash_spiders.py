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


def transform_list():
    # TODO : make sure loading the whole file doesnt become a bottleneck
    # TODO : Clean this function (the huge try-except) is bad bad bad

    # Load files
    try:
        orgs_df = pd.read_csv('tmp.csv')
    except pd.errors.EmptyDataError:
        print "Returning with no new links"
        return

    try:  # if file already exists
        domains_df = pd.read_csv('domains.csv')

        # Update
        update_doms = pd.merge(orgs_df[['domain']], domains_df, how='inner', on='domain')
        update_doms = update_doms.drop_duplicates()

        print "%s domains to be updated" % len(update_doms)

        if len(update_doms) > 0:
            update_doms['references'] = update_doms.apply(_get_domain_count, args=(orgs_df,), axis=1)

        # Add
        new_doms = pd.merge(orgs_df[['domain']], domains_df, how='left', on='domain')
        new_doms = new_doms[new_doms['references'].isnull()].drop_duplicates()

        print "%s domains to be added" % len(new_doms)

        if len(new_doms) > 0:
            new_doms[['crawled', 'references']] = new_doms[['crawled', 'references']].fillna(
                0)  # we keep 'to_crawl' as NaN to allow the CheckerSpider to decide over that
            new_doms['references'] = new_doms.apply(_get_domain_count, args=(orgs_df,), axis=1)
        # Leave as is

        unchanged_doms = pd.merge(orgs_df[['domain', 'referer']], domains_df, how='right', on='domain')
        unchanged_doms = unchanged_doms[unchanged_doms['referer'].isnull()].drop('referer', axis=1)

        print "%s domains left alone" % len(unchanged_doms)

        # Combine everything together and save to disk
        domains_df = pd.concat([unchanged_doms, update_doms, new_doms]).sort_values(by='references',
                                                                                    ascending=False)
        domains_df.to_csv('domains.csv', index=False)

    except:  # if file does not yet exist
        domains_df = pd.DataFrame(columns=['domain', 'to_crawl', 'crawled', 'references'])

        # Create
        new_doms = pd.merge(orgs_df[['domain']], domains_df, how='left', on='domain')
        new_doms = new_doms[new_doms['references'].isnull()].drop_duplicates()

        print "%s domains to be added" % len(new_doms)

        if len(new_doms) > 0:
            new_doms[['crawled', 'references']] = new_doms[['crawled', 'references']].fillna(
                0)  # we keep 'to_crawl' as NaN to allow the CheckerSpider to decide over that
            new_doms['references'] = new_doms.apply(_get_domain_count, args=(orgs_df,), axis=1)

        # Save to disk
        new_doms = new_doms.sort_values(by='references', ascending=False)
        new_doms.to_csv('domains.csv', index=False)


def _get_domain_count( df_row, orgs_df):
    new_refs = sum(orgs_df['domain'] == df_row['domain'])
    return df_row['references'] + new_refs

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
            transform_list()
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
