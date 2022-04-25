
import json
import os
import sys, utils
from ThreadedDocumentProcessor import ThreadedDocumentProcessor

siteLineMatrix = utils.getJSON('siteLineMatrix.json')
sitePolygonMatrix = utils.getJSON('sitePolygonMatrix.json')

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

        for entry in siteLineMatrix:
            if dataIsAssociatedWithLine: break
            if target_site_id in list(entry.values())[0]:
                document['BodyOfWater'] = list(entry.keys())[0]
                dataIsAssociatedWithLine = True
                break

        for entry in sitePolygonMatrix:
            if dataIsAssociatedWithLine or dataIsAssociatedWithPolygon: break
            if target_site_id in list(entry.values())[0]:
                document['BodyOfWater'] = list(entry.keys())[0]
                dataIsAssociatedWithPolygon = True
                break

        del document['_id']

        if dataIsAssociatedWithPolygon:
            destination = os.path.join('outputFiles/outputBodies.json')
        elif dataIsAssociatedWithLine:
            destination = os.path.join('outputFiles/outputRivers.json')
        else:
            destination = os.path.join('outputFiles/outputPipes.json')
            
        with self.lock:
            with open(destination, 'a') as f:
                f.write(',\n\t')
                f.write(json.dumps(document))


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

