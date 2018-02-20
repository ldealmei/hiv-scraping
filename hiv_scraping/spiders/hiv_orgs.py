# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule, CrawlSpider
import re

class HivOrgsSpider(CrawlSpider):
    name = 'hiv_orgs'
    allowed_domains = ['www.nacosa.org.za']
    start_urls = ['http://www.nacosa.org.za/']
    no_follow_links = []
    
    rules = (Rule(LinkExtractor(allow=allowed_domains, deny = no_follow_links ), callback = 'parse_page', follow = True),)

    def parse_page(self, response):
        links = LinkExtractor(allow=(), deny = self.no_follow_links + self.allowed_domains).extract_links(response)
        
        if len(links) == 0 :
            leafs = LinkExtractor(allow= (), deny = self.no_follow_links  ).extract_links(response)
            for l in leafs :
                s = l.url.split('/')
                s_re = '/'.join(s[:-1]) + '/*'
                
                self.no_follow_links.append(s_re)
    
        for link in links:
            self.no_follow_links.append(link.url)
            yield { 'link' : link.url,
                'domain' : self.stripurl(link.url)}

    def stripurl(self, url) :
        matched = re.match('^(?:http[s]?://)+[^/]*', url).group(0)
        
        return matched.split('://')[1]
