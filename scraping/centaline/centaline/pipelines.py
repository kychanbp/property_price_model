# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exporters import CsvItemExporter


class CentalinePipeline:
    def process_item(self, item, spider):
        return item

class MultiCSVItemPipeline(object):
    SaveTypes = ['CentalineTransactionsItem','CentalineTransactionsDetailItem','CentalineBuildingInfo']

    def __init__(self, file_dir):
        self.file_dir = file_dir

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            file_dir=crawler.settings.get('FILE_DIR'),
        )

    def open_spider(self, spider):
        self.files = dict([ (name, open(self.file_dir+name+'.csv','w+b')) for name in self.SaveTypes ]) #name to be change
        self.exporters = dict([ (name,CsvItemExporter(self.files[name])) for name in self.SaveTypes])
        [e.start_exporting() for e in self.exporters.values()]

    def close_spider(self, spider):
        [e.finish_exporting() for e in self.exporters.values()]
        [f.close() for f in self.files.values()]

    def process_item(self, item, spider):
        what = type(item).__name__
        if what in set(self.SaveTypes):
            self.exporters[what].export_item(item)
        return item
    