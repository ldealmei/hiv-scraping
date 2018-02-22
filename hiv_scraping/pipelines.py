# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exporters import CsvItemExporter
import re

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