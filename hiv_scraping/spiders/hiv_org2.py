# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
import re
import pandas as pd


def stripurl(url):
    matched = re.match('^(?:http[s]?://)+[^/]*', url).group(0)

    return matched.split('://')[-1]

class OrgWebsite(scrapy.Item):
    link = scrapy.Field()
    domain = scrapy.Field()
    referer = scrapy.Field()



class HivOrg2Spider(scrapy.Spider):
    name = 'hiv_orgs'
    custom_settings = {
        'ITEM_PIPELINES': {'hiv_scraping.pipelines.HivScrapingPipeline': 300}
        }

    allowed_domains = ['www.nacosa.org.za', 'www.aids.org.za','hivsa.com','www.caprisa.org']
    start_urls = ['http://www.nacosa.org.za/','https://www.aids.org.za' ,'http://hivsa.com/','http://www.caprisa.org/Default']
    saved_domains = []
    dead_ends = {}
    restricted_sections = []

    def parse(self, response):
        links = LinkExtractor(allow=(), deny= self.allowed_domains + self.saved_domains).extract_links(response)

        for link in links:
            self.saved_domains.append(stripurl(link.url))
            orgwebsite = OrgWebsite(link = link.url, domain = stripurl(link.url), referer = stripurl(response.request.url) )

            yield orgwebsite


        next_links = LinkExtractor(allow= self.allowed_domains, deny = self.restricted_sections).extract_links(response)
        if len(links) == 0 :
            try :
                self.dead_ends[response.request.url] +=1
            except :
                self.dead_ends[response.request.url] =1

            self.update_restrictions()
        else :
            for link in next_links :
                yield scrapy.Request(link.url, callback = self.parse)



    def update_restrictions(self):
        self.restricted_sections = [k for k in self.dead_ends.keys() if self.dead_ends[k] > 3]


class HIVChecker(scrapy.Spider) :

    name = 'hiv_checker'
    start_urls= []
    custom_settings = {
        'ITEM_PIPELINES': {'hiv_scraping.pipelines.CheckHIVPipeline': 300}
        }

    def start_requests(self):
        return [scrapy.Request(dom, callback=self.hiv_check) for dom in self.load_domains_to_check()]

    def hiv_check(self, response): #parse method
        word_dump = ''.join([txt.lower() for txt in response.xpath('//text()').extract()])

        yield {'domain' : stripurl(response.request.url),
               'text_dump' : word_dump}


    def load_domains_to_check(self):
        doms = pd.read_csv('domains.csv')
        return doms[doms['to_crawl'].isnull()]['domain'].tolist()