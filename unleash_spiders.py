import scrapy
from scrapy.crawler import CrawlerProcess
import sys
sys.path.insert(0,'hiv_scraping/spiders')
from hiv_org2 import HivOrg2Spider


# --------------- PHASE 1 : Crawl initial list --------------------
process = CrawlerProcess({'USER_AGENT' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)'})

process.crawl(HivOrg2Spider)
process.start()

# --------------- PHASE 2 : Select New Domains --------------------

# --------------- PHASE 3 : Unleash Spiders in newly found domains --------------------
