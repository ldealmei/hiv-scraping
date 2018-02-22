# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exporters import CsvItemExporter
import re
import pandas as pd

class HivBootstrapScrapingPipeline(object):
    def open_spider(self, spider):
        self.file = open("orgs.csv", 'ab')
        self.exporter = CsvItemExporter(self.file, unicode)
        self.exporter.start_exporting()

    def process_item(self, item, spider):

        self.exporter.export_item(item)
        return item

    def close_spider(self, spider):
        # populate with logic to log the result of the scraping of that domain
        self.exporter.finish_exporting()
        self.file.close()
        self.transform_list()

    def transform_list(self):
        # TODO : make sure loading the whole file doesnt become a bottleneck
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
            update_doms['references'] = update_doms.apply(self._update_domain_count,args=(orgs_df,),axis=1)
        # Add
        new_doms = pd.merge(orgs_df[['domain']], domains_df, how='left', on='domain')
        new_doms = new_doms[new_doms['references'].isnull()].drop_duplicates()

        if len(new_doms) > 0 :

            new_doms[['crawled','references']] = new_doms[['crawled','references']].fillna(0) # we keep 'to_crawl' as NaN to allow the CheckerSpider to decide over that
            new_doms['references'] = new_doms.apply(self._update_domain_count,args=(orgs_df,),axis=1)
        # Leave as is
        unchanged_doms = pd.merge(orgs_df[['domain','referer']], domains_df, how='right', on='domain')
        unchanged_doms = unchanged_doms[unchanged_doms['referer'].isnull()].drop('referer', axis=1)


        # Combine everything together and save to disk
        domains_df = pd.concat([unchanged_doms, update_doms, new_doms]).sort_values(by='references',ascending=False)
        domains_df.to_csv('domains.csv',index=False)

    def _update_domain_count(self,df_row, orgs_df):
        new_refs = sum(orgs_df['domain'] == df_row['domain'])
        return df_row['references'] + new_refs

class CheckHIVPipeline(object):
    def open_spider(self, spider):
        self.file = open("homepage_check.csv", 'ab')
        self.exporter = CsvItemExporter(self.file, unicode)
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        #TODO : make a better, more advanced nlp processing of the text dump
        #TODO : create a processing that outputs a score for each category (ngo, university, blog, gov/ministry, private, info)
        words_of_interest = ['aids','hiv','health','gov','ngo','data']

        txt = item['text_dump']
        page_scan={word : len(re.findall(word,txt)) for word in words_of_interest}
        page_scan['domain'] = item['domain']

        self.exporter.export_item(page_scan)
        pass

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
        self.make_domain_selection()

    def make_domain_selection(self):
        dom_check = pd.read_csv('homepage_check.csv')
        doms = pd.read_csv('domains.csv')
        doms_to_set = doms[doms['to_crawl'].isnull()]  # select only those that were not fixed yet

        dom_join = pd.merge(doms_to_set, dom_check, on='domain')

        dom_join['to_crawl'] = dom_join.apply(self._check_hiv_relevance, axis=1)

        dom_join = dom_join.sort_values(by=['references', 'to_crawl'], ascending=False) \
                            .drop(['hiv', 'ngo', 'health', 'aids', 'gov', 'data'], axis=1)
        dom_join.to_csv('domains.csv')

    def _check_hiv_relevance(self,row):
        # TODO : improve these conditions to make it more relevant
        if row['hiv'] > 0 and row['aids'] > 0:
            return 1
        elif row['hiv'] > 0 and row['health'] > 0:
            return 1
        else:
            return 0

class HivSatScrapingPipeline(object):
    def open_spider(self, spider):
        self.file = open("orgs.csv", 'ab')
        self.exporter = CsvItemExporter(self.file, encoding = unicode, include_headers_line=False)
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def close_spider(self, spider):
        # populate with logic to log the result of the scraping of that domain
        self.exporter.finish_exporting()
        self.file.close()