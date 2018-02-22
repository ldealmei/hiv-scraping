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

def update_domain_count(df_row,orgs_df):
    new_refs = sum(orgs_df['domain']==df_row['domain'])
    return df_row['references'] + new_refs

def check_hiv_relevance(row):
    #TODO : improve these conditions to make it more relevant
    if row['hiv'] > 0 and row['aids'] >0 :
        return 1
    elif row['hiv'] > 0 and row['health'] >0 :
        return 1
    else :
        return 0

def make_domain_selection():
    dom_check = pd.read_csv('homepage_check.csv')
    doms = pd.read_csv('domains.csv')
    doms_to_set = doms[doms['to_crawl'].isnull()]  # select only those that were not fixed yet

    dom_join = pd.merge(doms_to_set, dom_check, on='domain')

    dom_join['to_crawl'] = dom_join.apply(check_hiv_relevance, axis=1)

    dom_join = dom_join.sort_values(by=['references', 'to_crawl'], ascending=False) \
                        .drop(['hiv', 'ngo', 'health', 'aids', 'gov', 'data'], axis=1)
    dom_join.to_csv('domains.csv')

def transform_list():
    #TODO : make sure loading the whole file doesnt become a bottleneck
    # Load files
    orgs_df = pd.read_csv('orgs.csv') # might be dangerous if this file gets too big

    try :
        domains_df = pd.read_csv('domains.csv')
    except :
        with open('domains.csv', 'w') as f:
            f.write('domain,to_crawl,crawled,references')
        domains_df = pd.read_csv('domains.csv')

    # Split domains dataset into : Domains to add, Domains to update, domains to leave as is

    # Update
    update_doms = pd.merge(orgs_df[['domain']], domains_df, how='inner', on='domain')
    update_doms = update_doms.drop_duplicates() #A verifier
    if len(update_doms) > 0 :
        update_doms['references'] = update_doms.apply(update_domain_count,args=(orgs_df,),axis=1)
    # Add
    new_doms = pd.merge(orgs_df[['domain']], domains_df, how='left', on='domain')
    new_doms = new_doms[new_doms['references'].isnull()].drop_duplicates()

    if len(new_doms) > 0 :

        new_doms[['crawled','references']] = new_doms[['crawled','references']].fillna(0) # we keep 'to_crawl' as NaN to allow the CheckerSpider to decide over that
        new_doms['references'] = new_doms.apply(update_domain_count,args=(orgs_df,),axis=1)
    # Leave as is
    unchanged_doms = pd.merge(orgs_df[['domain','referer']], domains_df, how='right', on='domain')
    unchanged_doms = unchanged_doms[unchanged_doms['referer'].isnull()].drop('referer', axis=1)


    # Combine everything together and save to disk
    domains_df = pd.concat([unchanged_doms, update_doms, new_doms]).sort_values(by='references',ascending=False)
    domains_df.to_csv('domains.csv',index=False)


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