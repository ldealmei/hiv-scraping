# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.selector import Selector
import re
import pandas as pd
import numpy as np
import logging

from w3lib.html import remove_tags, remove_tags_with_content

def get_domain(url):
    matched = re.match('^(?:http[s]?://)+[^/]*', url).group(0)

    return matched.split('://')[-1]

def trim_url(url):
    matched = re.match('^(?:http[s]?://)+[^/]*', url).group(0)

    return matched

class OrgWebsite(scrapy.Item):
    link = scrapy.Field()
    domain = scrapy.Field()
    referer = scrapy.Field()


class HIVBootstraper(scrapy.Spider):
    #TODO : Change custom setting when not debugging
    name = 'hiv_bootstraper'
    custom_settings = {
        'ITEM_PIPELINES': {'hiv_scraping.pipelines.HivBootstrapScrapingPipeline': 300},
        'CLOSESPIDER_ITEMCOUNT': 100
    }

    saved_domains = []
    dead_ends = {}
    restricted_sections = []

    def __init__(self, **kw):
        super(HIVBootstraper, self).__init__(**kw)
        # self.start_urls = self.__getattribute__()
        # self.allowed_domains = [get_domain(self.start_urls[0])]

        logging.info('Starting Bootstrap Spider with : %s', ', '.join(self.start_urls))

    def parse(self, response):
        links = LinkExtractor(allow=(), deny=self.allowed_domains + self.saved_domains).extract_links(response)

        for link in links:
            if get_domain(link.url) not in self.saved_domains:
                self.saved_domains.append(get_domain(link.url))
                orgwebsite = OrgWebsite(link=link.url, domain=trim_url(link.url),
                                        referer=trim_url(response.request.url))

                yield orgwebsite

        next_links = LinkExtractor(allow=self.allowed_domains, deny=self.restricted_sections).extract_links(response)
        if len(links) == 0:
            try:
                self.dead_ends[response.request.url] += 1
            except:
                self.dead_ends[response.request.url] = 1

            self._update_restrictions()
        else:
            for link in next_links:
                yield scrapy.Request(link.url, callback=self.parse)

    def _update_restrictions(self):
        self.restricted_sections = [k for k in self.dead_ends.keys() if self.dead_ends[k] > 3]

class HIVChecker(scrapy.Spider) :

    name = 'hiv_checker'
    start_urls= []
    custom_settings = {
        'ITEM_PIPELINES': {'hiv_scraping.pipelines.ClfHIVPipeline': 300} #CheckHIVPipeline
        }

    def start_requests(self):
        return [scrapy.Request(dom, callback=self.hiv_check) for dom in self._load_domains_to_check()]

    def hiv_check(self, response): #parse method
        sel = Selector(response = response)
        raw_dump = sel.xpath('//body/descendant-or-self::*[not(self::script)]/text()').extract()

        word_dump = ' '.join([txt for txt in raw_dump if self._has_content(txt)])

        yield {'domain' : trim_url(response.request.url),
               'text_dump' : word_dump}

    def _has_content(self, txt):
        for t in txt :
            if t not in ['\n', '\t', ' ', '\r'] :
                return True

    def _load_domains_to_check(self):
        doms = pd.read_csv('domains.csv')
        doms = doms[doms['to_crawl'].isnull()].sort_values(by='references')['domain'].tolist()

        logging.info("%s new domains to be check for HIV" % str(len(doms)))

        return doms

class HIVSatellite(scrapy.Spider):
    #TODO : update the dead-end mechanism to also check whether the new pages are relevant
    name = 'hiv_satellite'

    custom_settings = { 'ITEM_PIPELINES': {'hiv_scraping.pipelines.HivSatScrapingPipeline': 300},
                        'CLOSESPIDER_PAGECOUNT' : 500}

    saved_domains = []
    dead_ends = {}
    restricted_sections = []

    def __init__(self, **kw):
        super(HIVSatellite, self).__init__(**kw)
        self.start_urls, self.allowed_domains = self._get_starting_state()

        if len(self.start_urls)==1 :

            logging.info('New satellite spider : %s', self.start_urls[0])

    def parse(self, response):
        # TODO : Find a way to have the exact same logic as the HIVBootstrap spider (maybe just have the exact same type?)
        links = LinkExtractor(allow=(), deny=self.allowed_domains + self.saved_domains).extract_links(response)

        for link in links:
            if get_domain(link.url) not in self.saved_domains:
                self.saved_domains.append(get_domain(link.url))
                orgwebsite = OrgWebsite(link=link.url, domain=trim_url(link.url),
                                        referer=trim_url(response.request.url))

                yield orgwebsite

        next_links = LinkExtractor(allow=self.allowed_domains, deny=self.restricted_sections).extract_links(response)
        if len(links) == 0:
            try:
                self.dead_ends[response.request.url] += 1
            except:
                self.dead_ends[response.request.url] = 1

            self._update_restrictions()
        else:
            for link in next_links:
                yield scrapy.Request(link.url, callback=self.parse)

    def _update_restrictions(self):
        self.restricted_sections = [k for k in self.dead_ends.keys() if self.dead_ends[k] > 3]

    def _get_starting_state(self):

        doms = pd.read_csv('domains.csv')
        eligible_doms = doms[np.logical_and(doms['to_crawl'] == 1, doms['crawled'] == 0)]['domain'].tolist()

        if len(eligible_doms) > 0 :
        # take first result
            chosen_dom = eligible_doms[0]
            # update file
            doms.loc[doms['domain'] == chosen_dom, 'crawled'] = 1
            doms.to_csv('domains.csv', index=False)

            return [chosen_dom], [get_domain(chosen_dom)]
        else :
            return [],[]


class DataSetBuilder(scrapy.Spider):
    name = 'dataset_builder'
    start_urls= []
    custom_settings = { 'ITEM_PIPELINES': {'hiv_scraping.pipelines.DataSetPipeline': 300} }
    dom_lbl = pd.read_csv('dataset/dom_lbl.csv')

    def start_requests(self):
        return [scrapy.Request(dom, callback=self.parse) for dom in self._load_domains()]

    def parse(self, response):
        sel = Selector(response = response)
        raw_dump = sel.xpath('//body/descendant-or-self::*[not(self::script)]/text()').extract()
        word_dump = ' '.join([txt for txt in raw_dump if self._has_content(txt)])

        yield {'domain' : trim_url(response.request.url),
               'text_dump' : word_dump,
               'hiv' : self.dom_lbl[self.dom_lbl['domain']==trim_url(response.request.url)]['hiv'].values[0],
               'research': self.dom_lbl[self.dom_lbl['domain'] == trim_url(response.request.url)]['research'].values[0],
               'gov': self.dom_lbl[self.dom_lbl['domain'] == trim_url(response.request.url)]['gov'].values[0],
               'uni': self.dom_lbl[self.dom_lbl['domain'] == trim_url(response.request.url)]['uni'].values[0],
               'ngo': self.dom_lbl[self.dom_lbl['domain'] == trim_url(response.request.url)]['ngo'].values[0],
               'association': self.dom_lbl[self.dom_lbl['domain'] == trim_url(response.request.url)]['association'].values[0]}

    def _load_domains(self):
        doms = pd.read_csv('dataset/dom_lbl.csv')
        dom_list = doms[doms['hiv'].notnull()]['domain'].tolist()
        return dom_list

    def _has_content(self, txt):
        for t in txt :
            if t not in ['\n', '\t', ' ', '\r'] :
                return True

        return False

class DataSetEnricher(scrapy.Spider):
    # TODO : Change custom setting when not debugging
    name = 'dataset_enricher'

    start_urls = []

    custom_settings = {'ITEM_PIPELINES': {'hiv_scraping.pipelines.EnrichPipeline': 300}}

    dom_lbl = pd.read_csv('dataset/dom_lbl.csv')

    def start_requests(self):
        return [scrapy.Request(dom, callback=self.parse) for dom in self._load_domains()]

    def parse(self, response):
        sel = Selector(response=response)
        raw_dump = sel.xpath('//body/descendant-or-self::*[not(self::script)]/text()').extract()

        word_dump = ' '.join([txt for txt in raw_dump if self._has_content(txt)])

        yield {'domain': trim_url(response.request.url),
               'about_dump': word_dump}

    def _load_domains(self):

        doms = pd.read_csv('dataset/dom_lbl.csv')

        dom_list = doms[doms['hiv'].notnull()]['domain'].tolist()
        about_list = [d + "/about" for d in dom_list] + [d + "/about-us" for d in dom_list]

        return about_list

    def _has_content(self, txt):
        for t in txt:
            if t not in ['\n', '\t', ' ', '\r']:
                return True

        return False
