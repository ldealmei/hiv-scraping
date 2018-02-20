# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
import re

class HivOrg2Spider(scrapy.Spider):
    name = 'hiv_orgs'
    allowed_domains = ['www.nacosa.org.za']
    start_urls = ['http://www.nacosa.org.za/']
    saved_links = []
    dead_ends = {}
    restricted_sections = []

    def parse(self, response):
        links = LinkExtractor(allow=(), deny= self.allowed_domains + self.saved_links).extract_links(response)

        for link in links:
            self.saved_links.append(link.url)

            yield {'link': link.url,
                   'domain': self.stripurl(link.url)}


        next_links = LinkExtractor(allow= self.allowed_domains, deny = self.restricted_sections).extract_links(response)
        if len(links) == 0 :
            try :
                self.dead_ends[response.request.url] +=1
            except :
                self.dead_ends[response.request.url] =1

            print
            print self.restricted_sections
            print
            self.update_restrictions()
        else :
            for link in next_links :
                yield scrapy.Request(link.url, callback = self.parse)

    def stripurl(self, url):
        matched = re.match('^(?:http[s]?://)+[^/]*', url).group(0)

        return matched.split('://')[-1]

    def update_restrictions(self):
        self.restricted_sections = [k for k in self.dead_ends.keys() if self.dead_ends[k] > 3]