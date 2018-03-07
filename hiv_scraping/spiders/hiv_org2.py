# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
import re
import pandas as pd
import numpy as np
import logging

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

        print
        print "----------------------- UNLEASHING BOOTSTRAP SPIDER -----------------------"
        print "start_urls : %s" % self.start_urls[0]
        print "allowed domains : %s" % self.allowed_domains[0]
        print "----------------------- --- ------ -----------------------"
        print

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
        'ITEM_PIPELINES': {'hiv_scraping.pipelines.CheckHIVPipeline': 300}
        }

    def start_requests(self):
        return [scrapy.Request(dom, callback=self.hiv_check) for dom in self._load_domains_to_check()]

    def hiv_check(self, response): #parse method
        word_dump = ''.join([txt.lower() for txt in response.xpath('//text()').extract()])

        yield {'domain' : trim_url(response.request.url),
               'text_dump' : word_dump}


    def _load_domains_to_check(self):
        doms = pd.read_csv('domains.csv')
        doms = doms[doms['to_crawl'].isnull()].sort_values(by='references')['domain'].tolist()

        print "-----------------------"
        print "%s NEW DOMAINS TO CHECK FOR HIV" % str(len(doms))
        print "-----------------------"

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
            print
            print "----------------------- NEW SATELITTE SPIDER -----------------------"
            print "start_urls : %s" % self.start_urls[0]
            print "allowed domains : %s" % self.allowed_domains[0]
            print "----------------------- --- ------ -----------------------"
            print

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


