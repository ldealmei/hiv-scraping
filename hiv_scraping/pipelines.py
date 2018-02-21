# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exporters import CsvItemExporter
import re

class HivScrapingPipeline(object):
    def open_spider(self, spider):
        #TODO : make the orgs filename depend on which execution it is
        self.file = open("orgs.csv", 'wb')
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
        self.file = open("homepage_check.csv", 'wb')
        self.exporter = CsvItemExporter(self.file, unicode)
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        #TODO : make a better, more advanced nlp processing of the text dump
        words_of_interest = ['aids','hiv','health','gov','ngo','data']

        txt = item['text_dump']
        page_scan={word : len(re.findall(word,txt)) for word in words_of_interest}

        self.exporter.export_item(page_scan)
        pass

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()