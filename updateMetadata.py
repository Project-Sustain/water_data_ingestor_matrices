
from ThreadedDocumentProcessor import ThreadedDocumentProcessor
import pymongo, sys

mongo = pymongo.MongoClient("mongodb://lattice-100:27018/")
db = mongo["sustaindb"]
metadata = db['Metadata']

class WaterQualityDataDistributor(ThreadedDocumentProcessor):
    def __init__(self, collection, numberOfThreads, query):
        super().__init__(collection, numberOfThreads, query, WaterQualityDataDistributor.updateaMetadata)

    def updateaMetadata(self, document):
        return None


def main(collection):
    metadataUpdater = WaterQualityDataDistributor('Metadata', 5, {'collection': collection})
    metadataUpdater.run()


if __name__ == '__main__':
    main(sys.argv[1])
