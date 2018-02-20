# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
import re

class HivOrg2Spider(scrapy.Spider):
    name = 'hiv_org2'
    allowed_domains = ['www.nacosa.org.za']
    start_urls = ['http://www.nacosa.org.za/']
    saved_links = []

    def parse(self, response):
        links = LinkExtractor(allow=(), deny= self.allowed_domains).extract_links(response)

        for link in links:
            self.saved_links.append(link.url)

            yield {'link': link.url,
                   'domain': self.stripurl(link.url)}
        next_links = LinkExtractor(allow= self.allowed_domains).extract_links(response)

        for link in next_links :
            yield scrapy.Request(link.url, callback = self.parse)

    def stripurl(self, url):
        matched = re.match('^(?:http[s]?://)+[^/]*', url).group(0)

        return matched.split('://')[1]
