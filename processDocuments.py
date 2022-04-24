
import sys, utils, pymongo
from ThreadedDocumentProcessor import ThreadedDocumentProcessor

siteLineMatrix = utils.getJSON('siteLineMatrix.json')
sitePolygonMatrix = utils.getJSON('sitePolygonMatrix.json')

mongo = pymongo.MongoClient("mongodb://lattice-100:27018/")
db = mongo["sustaindb"]
pipesCollection = db['water_quality_pipes']
riversAndStreamsCollection = db['water_quality_rivers_and_streams']
bodiesOfWaterCollection = db['water_quality_bodies_of_water']

class DocumentProcessor(ThreadedDocumentProcessor):
    def __init__(self, collection, number_of_threads, query):
        super().__init__(collection, number_of_threads, query, DocumentProcessor.processDocument)

    def processDocument(self, document):
        '''
        This is the function that will be called by each thread on each document.
        If this function returns something, it must be a dictionary. 
        Said dictionary will be written in JSON format to the output.json file.

        Update this function to perform whatever actions you need to on each document.
        '''

        # This needs to be optimized
        # Consider getting the state that target_site_id is in, only look at BodyOfWater id's from that same state

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


def main(collection, number_of_threads):
    query = {} # Update the `query` field to specify a mongo query
    documentProcessor = DocumentProcessor(collection, number_of_threads, query)
    documentProcessor.run()


if __name__ == '__main__':
    if len(sys.argv) == 3:
        collection = sys.argv[1]
        number_of_threads = int(sys.argv[2])
        main(collection, number_of_threads)
    else:
        print(f'Invalid args. Check the `README.md` file for program usage')

