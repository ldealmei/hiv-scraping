# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exporters import CsvItemExporter, JsonLinesItemExporter
import re
import pandas as pd
from sklearn.externals import joblib
import logging
import json


class HivBootstrapScrapingPipeline(object):

    def open_spider(self, spider):
        self.file = open("tmp.csv", 'wb')
        self.exporter = CsvItemExporter(self.file, unicode)
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def close_spider(self, spider):
        # populate with logic to log the result of the scraping of that domain
        self.exporter.finish_exporting()
        self.file.close()


class CheckHIVPipeline(object):
    """Pipeline for checking when using the business rules"""
    def open_spider(self, spider):

        self.file = open("homepage_check.csv", 'wb')
        self.exporter = CsvItemExporter(self.file, unicode)
        self.exporter.start_exporting()


    def process_item(self, item, spider):
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
        self.clean_orgs()
        self.empty_tmp()

    def make_domain_selection(self):
        """ Domain selection based on the raw business rules"""
        try :
            dom_check = pd.read_csv('homepage_check.csv')
        except :
            return

        doms = pd.read_csv('domains.csv')
        doms_to_set = doms[doms['to_crawl'].isnull()]  # select only those that were not fixed yet
        doms_past = doms[doms['to_crawl'].notnull()] # keep those already set

        doms_join = pd.merge(doms_to_set, dom_check, on='domain')

        doms_join['to_crawl'] = doms_join.apply(self._check_hiv_relevance, axis=1)

        doms_join = doms_join.drop(['hiv', 'ngo', 'health', 'aids', 'gov', 'data'], axis=1)

        doms = pd.concat([doms_past, doms_join])
        doms.sort_values(by=['to_crawl', 'references'], ascending=False)\
                .to_csv('domains.csv', index=False)
    def _check_hiv_relevance(self,row):
        if row['hiv'] > 0 and row['aids'] > 0:
            return 1
        elif row['hiv'] > 0 and row['health'] > 0:
            return 1
        else:
            return 0

    def clean_orgs(self):
        # TODO : We load the whole orgs.csv file, might become big and unmanageable
        raw_orgs = pd.read_csv('tmp.csv')
        domains = pd.read_csv('domains.csv')

        raw_orgs['to_keep'] = raw_orgs['domain'].apply(self._keep_or_not, args = (domains,))

        orgs_clean = raw_orgs[raw_orgs['to_keep']==1]\
                                .drop('to_keep', axis=1)

        # Load past orgs
        try :
            orgs_past = pd.read_csv('orgs.csv')
            orgs_clean = pd.concat([orgs_past, orgs_clean])
        except :
            pass

        orgs_clean.to_csv('orgs.csv', index = False)

    def _keep_or_not(self,s, domains):
        return s in domains['domain'].tolist() and bool(domains.loc[domains['domain']==s, 'to_crawl'].values[0])

    def empty_tmp(self):
        # This is necessary because now, multiple spiders upload to the tmp.csv file (when I replace it with 1 spider only, this function will become useless)
        empty_df = pd.DataFrame(columns=['referer','domain','link'])
        empty_df.to_csv('tmp.csv', index=False)

class ClfHIVPipeline(object):
    """Pipeline for checking when using a trained classifier"""
    def open_spider(self, spider):
        self.file = open("homepage_clf.jsonl", 'wb')
        self.exporter = JsonLinesItemExporter(self.file)
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
        self.domain_classification()
        self.clean_orgs()
        self.empty_tmp()

    def domain_classification(self):
        """Domain classification based on a trained classifier"""

        #Read file
        homepage_list = []
        with open("homepage_clf.jsonl") as json_file:
            for line in json_file:
                homepage_list.append(json.loads(line))
        homepages = pd.DataFrame(homepage_list)

        doms = pd.read_csv('domains.csv')
        doms_to_set = doms[doms['to_crawl'].isnull()]  # select only those that were not fixed yet
        doms_past = doms[doms['to_crawl'].notnull()] # keep those already set
        doms_join = pd.merge(doms_to_set, homepages, on='domain')

        logging.debug(doms_join)

        doms_join['to_crawl'] = doms_join.apply(self._predict_hiv, axis=1)
        doms_join = doms_join.drop(['text_dump'], axis=1)

        doms = pd.concat([doms_past, doms_join])
        doms.sort_values(by=['to_crawl', 'references'], ascending=False)\
                .to_csv('domains.csv', index=False)

    def _predict_hiv(self,txt):
        logging.info('Predicting...')
        try :
            pipeline = joblib.load('hiv_sites_py2.pipeline')
        except Exception as err :
            logging.critical("Could not load classification pipeline!")
            logging.info(err)


        logging.debug(pipeline)
        logging.debug(txt['text_dump'])

        res = pipeline.predict([txt['text_dump']])
        logging.info("This domain is HIV? {}".format(res[0]))

        return res[0]

    def clean_orgs(self):
        # TODO : We load the whole orgs.csv file, might become big and unmanageable
        raw_orgs = pd.read_csv('tmp.csv')
        domains = pd.read_csv('domains.csv')

        raw_orgs['to_keep'] = raw_orgs['domain'].apply(self._keep_or_not, args = (domains,))

        orgs_clean = raw_orgs[raw_orgs['to_keep']==1]\
                                .drop('to_keep', axis=1)
        # Load past orgs
        try :
            orgs_past = pd.read_csv('orgs.csv')
            orgs_clean = pd.concat([orgs_past, orgs_clean])
        except :
            pass

        orgs_clean.to_csv('orgs.csv', index = False)

    def _keep_or_not(self,s, domains):
        return s in domains['domain'].tolist() and bool(domains.loc[domains['domain']==s, 'to_crawl'].values[0])

    def empty_tmp(self):
        # This is necessary because now, multiple spiders upload to the tmp.csv file (when I replace it with 1 spider only, this function will become useless)
        empty_df = pd.DataFrame(columns=['referer', 'domain', 'link'])
        empty_df.to_csv('tmp.csv', index=False)

class HivSatScrapingPipeline(object):
    def open_spider(self, spider):
        self.file = open("tmp.csv", 'ab')
        self.exporter = CsvItemExporter(self.file, encoding = unicode, include_headers_line=False)
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def close_spider(self, spider):
        # populate with logic to log the result of the scraping of that domain
        self.exporter.finish_exporting()
        self.file.close()

class DataSetPipeline(object):
    def open_spider(self, spider):
        self.file = open("classifier_pos_data.jsonl", 'ab')
        self.exporter = JsonLinesItemExporter(self.file)
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()