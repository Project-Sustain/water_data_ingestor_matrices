
import sys, utils, os, json
from os.path import exists
from ThreadedDocumentProcessor import ThreadedDocumentProcessor

sorted_line_matrix = utils.getJSON('sortedLineMatrix.json')
sorted_polygon_matrix = utils.getJSON('sortedPolygonMatrix.json')

class DocumentProcessor(ThreadedDocumentProcessor):
    def __init__(self, collection, number_of_threads, query, firstBody, firstRiver, firstPipe):
        super().__init__(collection, number_of_threads, query, firstBody, firstRiver, firstPipe, DocumentProcessor.processDocument)

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
            destination = os.path.join('outputFiles/bodies.json')
            with self.lock:
                with open(destination, 'a') as f:
                    if self.firstBody:
                        f.write('\t')
                        self.firstBody = False
                    else:
                        f.write(',\n\t')
                    f.write(json.dumps(document))

        elif dataIsAssociatedWithLine:
            destination = os.path.join('outputFiles/rivers.json')
            with self.lock:
                with open(destination, 'a') as f:
                    if self.firstRiver:
                        f.write('\t')
                        self.firstRiver = False
                    else:
                        f.write(',\n\t')
                    f.write(json.dumps(document))

        else:
            destination = os.path.join('outputFiles/pipes.json')
            with self.lock:
                with open(destination, 'a') as f:
                    if self.firstPipe:
                        f.write('\t')
                        self.firstPipe = False
                    else:
                        f.write(',\n\t')
                    f.write(json.dumps(document))


def main(collection, number_of_threads):
    if not exists('outputFiles/bodies.json'):
        with open('outputFiles/bodies.json', 'a') as f:
            f.write('[\n')
            firstBody = True
    else:
        firstBody = False
    if not exists('outputFiles/rivers.json'):
        with open('outputFiles/rivers.json', 'a') as f:
            f.write('[\n')
            firstRiver = True
    else:
        firstRiver = False
    if not exists('outputFiles/pipes.json'):
        with open('outputFiles/pipes.json', 'a') as f:
            f.write('[\n')
            firstPipe = True
    else:
        firstPipe = False

    query = {} # Update the `query` field to specify a mongo query
    documentProcessor = DocumentProcessor(collection, number_of_threads, query, firstBody, firstRiver, firstPipe)
    documentProcessor.run()


if __name__ == '__main__':
    if len(sys.argv) == 3:
        collection = sys.argv[1]
        number_of_threads = int(sys.argv[2])
        main(collection, number_of_threads)
    else:
        print(f'Invalid args. Check the `README.md` file for program usage')

