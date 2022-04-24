
from abc import ABC
import pymongo, logging, json
from time import sleep
from threading import Thread, Lock
from pymongo.errors import CursorNotFound
import utils


class ThreadedDocumentProcessor(ABC):

    def __init__(self, collection_name, number_of_threads, query, processDocumentFunction):

        self.first_write = True
        self.processDocument = processDocumentFunction
        self.lock = Lock()
        self.collection_name = collection_name
        self.number_of_threads = number_of_threads
        self.error_file = 'error.log'
        self.output_file = 'output.json'

        logging.basicConfig(filename=self.error_file, level=logging.DEBUG, format='%(levelname)s %(name)s %(message)s')
        self.error_logger = logging.getLogger(__name__)
        
        mongo = pymongo.MongoClient('mongodb://lattice-100:27018/')
        self.db = mongo['sustaindb']
        self.query = query
        self.number_of_documents = self.db[collection_name].count_documents(query)

    
    def run(self):
        threads = []
        for i in range(1, self.number_of_threads+1):
            thread = Thread(target=ThreadedDocumentProcessor.iterateDocuments, args=(self, i))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()

        with open(self.output_file, 'a') as f:
            f.write('\n]')


    def iterateDocuments(self, thread_number, document_number=0, documents_processed_by_this_thread=0):
      
        total_documents_for_this_thread = utils.totalNumberOfDocumentsThisThreadMustProcess(thread_number, self.number_of_documents, self.number_of_threads)
        cursor = self.db[self.collection_name].find(self.query, no_cursor_timeout=True).skip(document_number)

        try:
            for document in cursor:
                document_number += 1

                if utils.documentShouldBeProcessedByThisThread(thread_number, document_number, self.number_of_threads):
                    try:
                        object_to_write = self.processDocument(self, document) # This is where we call the `processDocument()` fuction written in `processDocuments.py`
                        if object_to_write: # If your `processDocument()` function returns a dictionary, write it to the output file
                            with self.lock: # Thread-safe access to the output file
                                with open(self.output_file, 'a') as f:
                                    if self.first_write:
                                        f.write('[\n\t')
                                        f.write(json.dumps(object_to_write))
                                        self.first_write = False
                                    else:
                                        f.write(',\n\t')
                                        f.write(json.dumps(object_to_write))

                    except Exception as e:
                        utils.logError(self.error_logger, e, thread_number)

                    documents_processed_by_this_thread += 1
                    utils.logProgress(documents_processed_by_this_thread, total_documents_for_this_thread, thread_number, document_number)
                        
        except CursorNotFound as e:
            utils.logError(self.error_logger, e, thread_number)
            ThreadedDocumentProcessor.iterateDocuments(self, thread_number, document_number=document_number, documents_processed_by_this_thread=documents_processed_by_this_thread)

        except Exception as e:
            utils.logError(self.error_logger, e, thread_number)
            cursor.close()
            sleep(5)
            ThreadedDocumentProcessor.iterateDocuments(self, thread_number, document_number=document_number, documents_processed_by_this_thread=documents_processed_by_this_thread)
            
        cursor.close()

        print(f'{utils.getTimestamp()} [Thread-{thread_number}] Completed')