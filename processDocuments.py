
from ThreadedDocumentProcessor import ThreadedDocumentProcessor
import pymongo, sys, utils


siteLineMatrix = utils.getJSON('siteLineMatrix.json')
sitePolygonMatrix = utils.getJSON('sitePolygonMatrix.json')

mongo = pymongo.MongoClient("mongodb://lattice-100:27018/")
db = mongo["sustaindb"]
pipesCollection = db['water_quality_pipes']
riversAndStreamsCollection = db['water_quality_rivers_and_streams']
bodiesOfWaterCollection = db['water_quality_bodies_of_water']


class DocumentProcessor(ThreadedDocumentProcessor):
    def __init__(self, collection, numberOfThreads, query):
        super().__init__(collection, numberOfThreads, query, DocumentProcessor.processDocument)

    def processDocument(self, document):
        target_site_id = document['MonitoringLocationIdentifier']
        dataIsAssociatedWithPolygon = False
        dataIsAssociatedWithLine = False

        for entry in sitePolygonMatrix:
            if dataIsAssociatedWithPolygon: break
            if target_site_id in list(entry.values())[0]:
                document['BodyOfWater'] = list(entry.keys())[0]
                dataIsAssociatedWithPolygon = True
                break

        for entry in siteLineMatrix:
            if dataIsAssociatedWithLine or dataIsAssociatedWithPolygon: break
            if target_site_id in list(entry.values())[0]:
                document['BodyOfWater'] = list(entry.keys())[0]
                dataIsAssociatedWithLine = True
                break

        if dataIsAssociatedWithPolygon:
            bodiesOfWaterCollection.insert_one(document)
        elif dataIsAssociatedWithLine:
            riversAndStreamsCollection.insert_one(document)
        else:
            pipesCollection.insert_one(document)


def main(collection, numberOfThreads):
    query = {}
    dataDistributor = DocumentProcessor(collection, numberOfThreads, query)
    dataDistributor.run()


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(f'Usage: python3 {sys.argv[0]} <collection_to_iterate> <number_of_threads>')
    collection = sys.argv[1]
    numberOfThreads = sys.argv[2]
    main(collection, numberOfThreads)
