
import sys, utils, os, json
from ThreadedDocumentProcessor import ThreadedDocumentProcessor

sorted_line_matrix = utils.getJSON('sortedLineMatrix.json')
sorted_polygon_matrix = utils.getJSON('sortedPolygonMatrix.json')

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

        target_site_id = document['MonitoringLocationIdentifier']
        dataIsAssociatedWithPolygon = False
        dataIsAssociatedWithLine = False

        body_of_water_in_sorted_line_matrix = utils.binary_search(sorted_line_matrix, 0, len(sorted_line_matrix)-1, target_site_id)
        if body_of_water_in_sorted_line_matrix:
            document['BodyOfWater'] = body_of_water_in_sorted_line_matrix
            dataIsAssociatedWithLine = True
        
        else:
            body_of_water_in_sorted_polygon_matrix = utils.binary_search(sorted_polygon_matrix, 0, len(sorted_polygon_matrix)-1, target_site_id)
            if body_of_water_in_sorted_polygon_matrix:
                document['BodyOfWater'] = body_of_water_in_sorted_polygon_matrix
                dataIsAssociatedWithPolygon = True

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

