# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
import re
import pandas as pd
import numpy as np


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
    #TODO : Change customsetting when not debugging
    name = 'hiv_bootstraper'
    custom_settings = {
        'ITEM_PIPELINES': {'hiv_scraping.pipelines.HivBootstrapScrapingPipeline': 300},
        'CLOSESPIDER_ITEMCOUNT': 100
    }

    allowed_domains = ['www.nacosa.org.za']#, 'www.aids.org.za','hivsa.com','www.caprisa.org']
    start_urls = ['http://www.nacosa.org.za/']#,'https://www.aids.org.za' ,'http://hivsa.com/','http://www.caprisa.org/Default']
    saved_domains = []
    dead_ends = {}
    restricted_sections = []

    def parse(self, response):
        links = LinkExtractor(allow=(), deny= self.allowed_domains + self.saved_domains).extract_links(response)

        for link in links:
            self.saved_domains.append(get_domain(link.url))
            orgwebsite = OrgWebsite(link = link.url, domain = trim_url(link.url), referer = trim_url(response.request.url) )

            yield orgwebsite


        next_links = LinkExtractor(allow= self.allowed_domains, deny = self.restricted_sections).extract_links(response)
        if len(links) == 0 :
            try :
                self.dead_ends[response.request.url] +=1
            except :
                self.dead_ends[response.request.url] =1

            self._update_restrictions()
        else :
            for link in next_links :
                yield scrapy.Request(link.url, callback = self.parse)



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
        doms = doms[doms['to_crawl'].isnull()]['domain'].tolist()

        return doms

class HIVSatellite(scrapy.Spider):
    #TODO : update the dead-end mechanism to also check whether the new pages are relevant
    name = 'hiv_satellite'

    custom_settings = { 'ITEM_PIPELINES': {'hiv_scraping.pipelines.HivSatScrapingPipeline': 300},
                        'CLOSESPIDER_PAGECOUNT' : 200}

    saved_domains = []
    dead_ends = {}
    restricted_sections = []

    def __init__(self, **kw):
        super(HIVSatellite, self).__init__(**kw)
        self.start_urls = self._get_start_url()
        self.allowed_domains = [get_domain(self.start_urls[0])]

        print
        print "----------------------- NEW SPIDER -----------------------"
        print "start_urls : %s" % self.start_urls[0]
        print "allowed domains : %s" % self.allowed_domains[0]
        print "----------------------- --- ------ -----------------------"
        print


    def parse(self, response):
        links = LinkExtractor(allow=(), deny=self.allowed_domains + self.saved_domains).extract_links(response)

        for link in links:
            self.saved_domains.append(get_domain(link.url))
            orgwebsite = OrgWebsite(link=link.url, domain=trim_url(link.url),
                                    referer=trim_url(response.request.url))

            yield orgwebsite

        next_links = LinkExtractor(allow=self.allowed_domains, deny=self.restricted_sections).extract_links(
            response)
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

    def _get_start_url(self):

        doms = pd.read_csv('domains.csv')
        eligible_doms = doms[np.logical_and(doms['to_crawl'] == 1, doms['crawled'] == 0)]['domain'].tolist()

        # take first result
        chosen_dom = eligible_doms[0]
        # update file
        doms.loc[doms['domain'] == chosen_dom, 'crawled'] = 1
        doms.to_csv('domains.csv', index=False)

        return [chosen_dom]


