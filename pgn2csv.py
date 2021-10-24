import datetime
import logging
import queue
import threading

log = logging.getLogger('pgn2csv')
# parsed headers
pgn_headers = ['Date', 'White', 'Black', 'Result', 'BlackElo', 'WhiteElo', 'ECO', 'Opening', 'Variation', 'WhiteFideId',
               'BlackFideId']

class pgn2csv:
    """
    Main class for simple pgn to csv converter
    The class is intended to generate only tabular header data with the most important and rudimentary information
    For anything more sophisticated pgn2data library is to be used

    usage:
        converter = pgn2csv(source_file, target_file)
        converter.convertPgn()
    """

    def __init__(self, source, target=None, test_mode = False):
        self._source = source
        self._target = target
        self._q = queue.Queue()
        self._test_mode = test_mode

    def create_writer(self):
        # creating file in overwrite mode
        writer = open(self._target, 'w', encoding='utf=8')
        return writer

    def create_reader(self):
        # creating file in overwrite mode
        reader = open(self._source, 'r', encoding='utf=8')
        return reader

    def close_file(self, target_file):
        target_file.close()

    def write_csv(self, writer):
        # takes items from the queue and writes them out in csv format line by line
        # for now everything is -expected- to be wrapped in double quotes, as comma will be used as a separator
        log.info('writer thread started, writer: ' + str(writer))
        #writing headers
        header = ','.join(pgn_headers) + '\n'
        writer.write(header)

        while True:
            item = self._q.get()  # item is a dictionary with the keys being the column values in order
            if item == None:
                log.info('None found, exiting')
                self._q.task_done()
                break
            else:
                #filtering dict for the needed headers -> should rework for a more elegant dict filtering
                row_items = {}
                for header in pgn_headers:
                    value = ''
                    if header in item:
                        value = item[header]
                    row_items[header] = value
                row = ','.join(x for x in row_items.values()) + '\n'

                #log.info('writing data: ' + row)
                writer.write(row)
                self._q.task_done()

    def read_csv(self, reader):
        # test mode just adds 3000 items
        if self._test_mode:
            for i in range(3000):
                self._q.put({'a': str(i), 'b': str(i + 1)})
            self._q.put(None)
            pass

        #non-test mode, using the files received as constructor params
        game = None
        for line in reader:
            if line.startswith('['):
                #check if first header row
                if game == None:
                    game = {} #this will contain the header keys - values
                #split the line at the first whitespace
                index = line.find(' ')
                k = line[:index].replace('[','')
                v = line[index+1:].replace(']','').replace('\n','')
                game[k] =v

            if line == '\n' and game!=None:
                #newline, the first after the game header, add header data
                self._q.put(game)
                game = None
            #ignore all other content, comments, moves, etc.

        self._q.put(None)

    def convertPgn(self):
        """
        converts the source pgn file to target csv by finding and writing out main headers
        maintains a sequential reader and a writer through a queue
        rather than reading and keeping everything in memory
        expected to work with 3GB files
        :return: None or exception if there is an error
        """
        log.info('Convert pgn 2 csv started at {}, source file: {}, target file: {}'.format(datetime.datetime.now(),
                                                                                            self._source, self._target))
        log.info(self._q)
        file_reader = self.create_reader()
        file_writer = self.create_writer()

        try:
            # starting reader thread
            threading.Thread(target=self.read_csv(reader=file_reader), daemon=True).start()

            # starting writer thread
            threading.Thread(target=self.write_csv(writer=file_writer), daemon=True).start()

            self._q.join()
        finally:
            self.close_file(file_reader)
            self.close_file(file_writer)

        log.info('everything done, finished at: ' + str(datetime.datetime.now()))


