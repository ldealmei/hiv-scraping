# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exporters import CsvItemExporter
import re
import pandas as pd
import os

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
        self.transform_list()

    def transform_list(self):
        # TODO : make sure loading the whole file doesnt become a bottleneck
        # Load files
        orgs_df = pd.read_csv('tmp.csv') # might be dangerous if this file gets too big

        domains_df = pd.DataFrame(columns=['domain','to_crawl','crawled','references'])

        # Create
        # TODO : Clean these lines, the merge is probably useless
        new_doms = pd.merge(orgs_df[['domain']], domains_df, how='left', on='domain')
        new_doms = new_doms[new_doms['references'].isnull()].drop_duplicates()

        print "%s domains to be added" % len(new_doms)

        if len(new_doms) > 0 :

            new_doms[['crawled','references']] = new_doms[['crawled','references']].fillna(0) # we keep 'to_crawl' as NaN to allow the CheckerSpider to decide over that
            new_doms['references'] = new_doms.apply(self._get_domain_count,args=(orgs_df,),axis=1)

        # Save to disk
        new_doms = new_doms.sort_values(by='references',ascending=False)
        new_doms.to_csv('domains.csv',index=False)

    def _get_domain_count(self,df_row, orgs_df):
        new_refs = sum(orgs_df['domain'] == df_row['domain'])
        return df_row['references'] + new_refs

class CheckHIVPipeline(object):
    def open_spider(self, spider):

        if os.path.exists('homepage_check.csv'):
            first_check = False
        else :
            first_check = True

        self.file = open("homepage_check.csv", 'ab')

        if first_check :
            print "FIRST HIV CHECK"
            self.exporter = CsvItemExporter(self.file, unicode)
        else :
            print "SUBSEQUENT HIV CHECK"
            self.exporter = CsvItemExporter(self.file, include_headers_line=False)

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
        self.clean_orgs()

    def make_domain_selection(self):
        dom_check = pd.read_csv('homepage_check.csv')
        doms = pd.read_csv('domains.csv')
        doms_to_set = doms[doms['to_crawl'].isnull()]  # select only those that were not fixed yet

        dom_join = pd.merge(doms_to_set, dom_check, on='domain')

        dom_join['to_crawl'] = dom_join.apply(self._check_hiv_relevance, axis=1)

        dom_join = dom_join.sort_values(by=['to_crawl', 'references'], ascending=False) \
                            .drop(['hiv', 'ngo', 'health', 'aids', 'gov', 'data'], axis=1)
        dom_join.to_csv('domains.csv',index=False)

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

        print "Keeping {} orgs from pool of {}".format(len(orgs_clean),len(raw_orgs))


    def _keep_or_not(self,s, domains):
        return s in domains['domain'].tolist() and bool(domains.loc[domains['domain']==s, 'to_crawl'].values[0])

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
        self.file = open("tmp.csv", 'wb')
        self.exporter = CsvItemExporter(self.file, encoding = unicode)
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
        try :
            orgs_df = pd.read_csv('tmp.csv')
        except pd.errors.EmptyDataError :
	    print "Returning with no new links"
            return

        domains_df = pd.read_csv('domains.csv')

        # Split domains dataset into : Domains to add, Domains to update, domains to leave as is

        # Update
        update_doms = pd.merge(orgs_df[['domain']], domains_df, how='inner', on='domain')
        update_doms = update_doms.drop_duplicates()

        print "%s domains to be updated" % len(update_doms)

        if len(update_doms) > 0:
            update_doms['references'] = update_doms.apply(self._update_domain_count, args=(orgs_df,), axis=1)

        # Add
        new_doms = pd.merge(orgs_df[['domain']], domains_df, how='left', on='domain')
        new_doms = new_doms[new_doms['references'].isnull()].drop_duplicates()

        print "%s domains to be added" % len(new_doms)

        if len(new_doms) > 0:
            new_doms[['crawled', 'references']] = new_doms[['crawled', 'references']].fillna(
                0)  # we keep 'to_crawl' as NaN to allow the CheckerSpider to decide over that
            new_doms['references'] = new_doms.apply(self._update_domain_count, args=(orgs_df,), axis=1)
        # Leave as is

        unchanged_doms = pd.merge(orgs_df[['domain', 'referer']], domains_df, how='right', on='domain')
        unchanged_doms = unchanged_doms[unchanged_doms['referer'].isnull()].drop('referer', axis=1)

        print "%s domains left alone" % len(unchanged_doms)

        # Combine everything together and save to disk
        domains_df = pd.concat([unchanged_doms, update_doms, new_doms]).sort_values(by='references', ascending=False)
        domains_df.to_csv('domains.csv', index=False)

    def _update_domain_count(self, df_row, orgs_df):
        new_refs = sum(orgs_df['domain'] == df_row['domain'])
        return df_row['references'] + new_refs
