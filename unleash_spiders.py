import sys
import os
import pandas as pd
sys.path.insert(0,'hiv_scraping/spiders')

import scrapy
from scrapy.crawler import CrawlerProcess
from hiv_org2 import HivOrg2Spider, HIVChecker
from scrapy.utils.project import get_project_settings

def update_domain_count(df_row,orgs_df):
    new_refs = sum(orgs_df['domain']==df_row['domain'])
    return df_row['references'] + new_refs


def transform_list():
    #TODO : make sure this doesnt become a bottleneck

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
    if len(update_doms) > 0 :
        update_doms['references'] = update_doms.apply(update_domain_count,args=(orgs_df,),axis=1)

    # Add
    new_doms = pd.merge(orgs_df[['domain']], domains_df, how='left', on='domain')
    new_doms = new_doms[new_doms['references'].isnull()]

    if len(new_doms) > 0 :

        new_doms[['crawled','references']] = new_doms[['crawled','references']].fillna(0) # we keep 'to_crawl' as NaN to allow the CheckerSpider to decide over that
        new_doms['references'] = new_doms.apply(update_domain_count,args=(orgs_df,),axis=1)

    # Leave as is
    unchanged_doms = pd.merge(orgs_df, domains_df, how='right', on='domain')
    print unchanged_doms.head()
    unchanged_doms = unchanged_doms[unchanged_doms['referer'].isnull()]

    # Combine everything together and save to disk

    domains_df = pd.concat([unchanged_doms, update_doms, new_doms])
    domains_df.to_csv('domains.csv')


# --------------- PHASE 1 : Crawl initial list --------------------
# Attention, qd on crawle via un CrawlerProcess les parametres dans settings.py ne sont pas automatiquement loades
# utiliser : = > get_project_settings()

process = CrawlerProcess(get_project_settings())

# process.crawl(HivOrg2Spider)
# process.start()

# --------------- PHASE 2 : Select New Domains & Unleash Spiders in newly found domains --------------------
transform_list()
process.crawl(HIVChecker)
process.start()

#TODO : debug the transform_list function (the merge result dataframe especially) and the execution of the HIVCheck spiders
# TODO : debug the domains.csv file creation : if a row has multiple references it appears as many times as it is referenced... we want it only once