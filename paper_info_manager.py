
import os
import time
import json


class PaperInfoManager:

    RECORD_FIELD_SEPARATOR = '#'
    RECORD_STRUCTURE_YEAR_LENGTH = 4
    RECORD_STRUCTURE_COUNTER_LENGTH = 4
    RECORD_STRUCTURE_NUMBER_OF_CITATION_YEARS = 60
    RECORD_LENGTH = \
        RECORD_STRUCTURE_YEAR_LENGTH \
        + 1 \
        + RECORD_STRUCTURE_NUMBER_OF_CITATION_YEARS * (RECORD_STRUCTURE_YEAR_LENGTH + RECORD_STRUCTURE_COUNTER_LENGTH) \
        + 1

    MAX_PAPERS_IN_STORAGE_FILE = 250000
    OPERATION_LOG_INTERVAL = 1000
    MAX_CACHE_SIZE = 750000
    CACHE_CLEANING_FACTOR = 0.01

    PUBLICATION_YEAR_KEY_NAME = 'y'
    CITATION_INFO_KEY_NAME = 'c'
    STORAGE_FILE_PATH_FORMAT = r'storage/papers_{file_id}.json'
    MAPPING_FILE_PATH = r'storage/papers_name_mapping.json'

    def __init__(self):
        self.__records_in_current_storage_file = 0
        self.__working_storage_file_index = 0
        self.__operation_counter = 0
        self.__paper_storage_mapping = dict()
        self.__file_handlers = dict()
        self.__record_cache = dict()

    def __get_storage_file_handler(self, file_path):
        # check if file already open
        if file_path in self.__file_handlers.keys():
            return self.__file_handlers[file_path]

        else:
            # need to open the file and store its handler
            file_handler = open(file_path, 'w+')
            self.__file_handlers[file_path] = file_handler
            return file_handler

    def __record_data_to_paper_record(self, record_data):
        paper_record = dict()
        current_index = 0

        # parse publication year info
        paper_record[PaperInfoManager.PUBLICATION_YEAR_KEY_NAME] = \
            int(
                record_data[
                    current_index
                    :(current_index + PaperInfoManager.RECORD_STRUCTURE_YEAR_LENGTH)
                ]
            )
        if paper_record[PaperInfoManager.PUBLICATION_YEAR_KEY_NAME] == 0:
            paper_record[PaperInfoManager.PUBLICATION_YEAR_KEY_NAME] = None
        current_index += PaperInfoManager.RECORD_STRUCTURE_YEAR_LENGTH + 1

        # parse citation info
        paper_record[PaperInfoManager.CITATION_INFO_KEY_NAME] = dict()
        for citation_year_index in range(PaperInfoManager.RECORD_STRUCTURE_NUMBER_OF_CITATION_YEARS):
            if record_data[current_index] == PaperInfoManager.RECORD_FIELD_SEPARATOR:
                break

            citation_year = \
                record_data[current_index:current_index+PaperInfoManager.RECORD_STRUCTURE_YEAR_LENGTH]
            current_index += PaperInfoManager.RECORD_STRUCTURE_YEAR_LENGTH
            citation_count = \
                int(
                    record_data[current_index:current_index+PaperInfoManager.RECORD_STRUCTURE_COUNTER_LENGTH]
                    .replace(PaperInfoManager.RECORD_FIELD_SEPARATOR, '')
                )
            current_index += PaperInfoManager.RECORD_STRUCTURE_COUNTER_LENGTH

            paper_record[PaperInfoManager.CITATION_INFO_KEY_NAME][citation_year] = citation_count

        # return parsed record
        return paper_record

    def __paper_record_to_record_data(self, paper_record):
        record_data = str()

        # encode publication year info
        if paper_record[PaperInfoManager.PUBLICATION_YEAR_KEY_NAME] is not None:
            record_data += str(paper_record[PaperInfoManager.PUBLICATION_YEAR_KEY_NAME])
        else:
            record_data += '0'.rjust(PaperInfoManager.RECORD_STRUCTURE_YEAR_LENGTH, '0')
        record_data += PaperInfoManager.RECORD_FIELD_SEPARATOR

        # encode citation history
        if len(paper_record[PaperInfoManager.CITATION_INFO_KEY_NAME].keys()) \
                > PaperInfoManager.RECORD_STRUCTURE_NUMBER_OF_CITATION_YEARS:
            print('WARNING: paper has too many citation years! num={citation_count}'
                  .format(citation_count=len(paper_record[PaperInfoManager.CITATION_INFO_KEY_NAME].keys())))

        # in case there is too many citation years- selects newest years first and skip the beginning
        sorted_keys = list(paper_record[PaperInfoManager.CITATION_INFO_KEY_NAME].keys())
        sorted_keys.sort()
        sorted_keys.reverse()
        year_key_index = 0
        while year_key_index < len(sorted_keys) \
                and year_key_index < PaperInfoManager.RECORD_STRUCTURE_NUMBER_OF_CITATION_YEARS:
            citation_year = sorted_keys[year_key_index]
            citation_count_string = str(paper_record[PaperInfoManager.CITATION_INFO_KEY_NAME][citation_year])
            if len(citation_count_string) > PaperInfoManager.RECORD_STRUCTURE_COUNTER_LENGTH:
                raise Exception('RECORD COUNTER TOO HIGH!!!')

            citation_count_string = citation_count_string.rjust(
                PaperInfoManager.RECORD_STRUCTURE_COUNTER_LENGTH, PaperInfoManager.RECORD_FIELD_SEPARATOR
            )

            record_data += citation_year + citation_count_string
            year_key_index += 1

        # pad spare space
        added_records = len(paper_record[PaperInfoManager.CITATION_INFO_KEY_NAME])
        pad_length = \
            (PaperInfoManager.RECORD_STRUCTURE_NUMBER_OF_CITATION_YEARS - added_records) \
            * (PaperInfoManager.RECORD_STRUCTURE_YEAR_LENGTH + PaperInfoManager.RECORD_STRUCTURE_COUNTER_LENGTH)
        record_data += PaperInfoManager.RECORD_FIELD_SEPARATOR * pad_length

        record_data += PaperInfoManager.RECORD_FIELD_SEPARATOR
        return record_data

    def __get_record_from_storage(self, record_id):
        # split record_id to get storage file index and relative record index
        storage_file_index, record_index = record_id.split('_')
        storage_file_index = int(storage_file_index)
        record_index = int(record_index)

        # calculate record offset from file start
        record_offset = record_index * PaperInfoManager.RECORD_LENGTH

        # build file path and read record data
        storage_file_path = PaperInfoManager.STORAGE_FILE_PATH_FORMAT.format(file_id=storage_file_index)
        storage_file = self.__get_storage_file_handler(storage_file_path)
        storage_file.flush()
        storage_file.seek(record_offset, 0)
        record_data = storage_file.read(PaperInfoManager.RECORD_LENGTH)
        storage_file.flush()

        # parse record_data into paper record structure
        try:
            paper_record = self.__record_data_to_paper_record(record_data)
        except Exception as ex:
            print('Error: failed converting to paper record')
            print('offset= {offset}'.format(offset=record_offset))
            raise ex

        return paper_record

    def __store_record_to_storage(self, record_id, paper_record):
        # split record_id to get storage file index and relative record index
        storage_file_index, record_index = record_id.split('_')
        storage_file_index = int(storage_file_index)
        record_index = int(record_index)

        # calculate record offset from file start
        record_offset = record_index * PaperInfoManager.RECORD_LENGTH

        # build file path and read record data
        storage_file_path = PaperInfoManager.STORAGE_FILE_PATH_FORMAT.format(file_id=storage_file_index)
        record_data = self.__paper_record_to_record_data(paper_record)

        storage_file = self.__get_storage_file_handler(storage_file_path)
        storage_file.flush()
        os.fsync(storage_file)
        storage_file.seek(record_offset, 0)
        storage_file.write(record_data)
        storage_file.flush()
        os.fsync(storage_file)

    def __clean_cache(self, clean_factor=CACHE_CLEANING_FACTOR):
        # calculate how much records to move
        cache_keys = list(self.__record_cache.keys())
        cache_keys.sort()
        records_count = int(len(cache_keys) * clean_factor)

        # move records and remove from cache
        print('cleaning {num_records} records..'.format(num_records=records_count))
        for i in range(records_count):
            if (i % PaperInfoManager.OPERATION_LOG_INTERVAL) == 0:
                print('record #{record_index}'.format(record_index=i))

            self.__store_record_to_storage(cache_keys[i], self.__record_cache.pop(cache_keys[i]))

    def __add_record_to_cache(self, record_id, paper_record):
        # verify there is room for the record
        if len(self.__record_cache.keys()) == PaperInfoManager.MAX_CACHE_SIZE:
            # move records to storage
            self.__clean_cache()

        self.__record_cache[record_id] = paper_record

    def __get_paper_record(self, paper_id):
        # get paper record_id
        paper_record_id = self.get_paper_record_id(paper_id)

        # verify record is in cache
        if paper_record_id not in self.__record_cache.keys():
            # load record form storage
            paper_record = self.__get_record_from_storage(paper_record_id)

            # store record in cache
            self.__add_record_to_cache(paper_record_id, paper_record)

        # return record from cache
        return self.__record_cache[paper_record_id]

    def __store_paper_record(self, paper_id, paper_record):
        # get paper record_id
        paper_record_id = self.get_paper_record_id(paper_id)

        # store paper record
        self.__add_record_to_cache(paper_record_id, paper_record)

    def __create_new_paper_record(self, paper_id, paper_year):
        # check if need to move to new storage file
        if self.__records_in_current_storage_file == PaperInfoManager.MAX_PAPERS_IN_STORAGE_FILE:
            # increase working file index and initiate records counter
            self.__working_storage_file_index += 1
            self.__records_in_current_storage_file = 0

        # allocate record_id
        record_id = '{storage_file_index}_{record_index}'.format(
            storage_file_index=self.__working_storage_file_index,
            record_index=
                str(self.__records_in_current_storage_file)
                    .rjust(len(str(PaperInfoManager.MAX_PAPERS_IN_STORAGE_FILE)), '0')
        )
        self.__records_in_current_storage_file += 1

        # store mapping
        self.__paper_storage_mapping[paper_id] = record_id

        # build record
        paper_record = {
            PaperInfoManager.PUBLICATION_YEAR_KEY_NAME: paper_year,
            PaperInfoManager.CITATION_INFO_KEY_NAME: dict()
        }

        # store record to working file
        self.__store_paper_record(paper_id, paper_record)

    def __add_citation_year(self, paper_id, citation_year):
        # get paper record
        paper_record = self.__get_paper_record(paper_id)

        # add citation year
        if citation_year not in paper_record[PaperInfoManager.CITATION_INFO_KEY_NAME].keys():
            paper_record[PaperInfoManager.CITATION_INFO_KEY_NAME][citation_year] = 0

        paper_record[PaperInfoManager.CITATION_INFO_KEY_NAME][citation_year] += 1

    def add_paper(self, paper_id, paper_year):
        self.__increase_operation_counter()

        # verify a paper is'nt added twice
        if self.get_paper_record_id(paper_id) is not None:

            # get paper record
            paper_record = self.__get_paper_record(paper_id)

            if paper_record[PaperInfoManager.PUBLICATION_YEAR_KEY_NAME] is not None:
                print('ERROR: paper {paper_id} already in storage. pub_year={pub_year} history={citation_history}'
                    .format(
                        paper_id=paper_id, pub_year=paper_record[PaperInfoManager.PUBLICATION_YEAR_KEY_NAME],
                        citation_history=paper_record[PaperInfoManager.CITATION_INFO_KEY_NAME]
                    )
                )
                return False

            else:
                # empty paper record created when other paper cited it before
                # so only need to set the publication year
                paper_record[PaperInfoManager.PUBLICATION_YEAR_KEY_NAME] = paper_year

        else:
            # create new paper record
            self.__create_new_paper_record(paper_id, paper_year)

        return True

    def add_citation(self, paper_id, citation_year):
        self.__increase_operation_counter()

        # verify paper is in  storage
        if self.get_paper_record_id(paper_id) is None:
            # create empty paper record
            self.__create_new_paper_record(paper_id, None)

        # add one to citation count
        self.__add_citation_year(paper_id, citation_year)

    def __increase_operation_counter(self):
        self.__operation_counter += 1
        if (self.__operation_counter % PaperInfoManager.OPERATION_LOG_INTERVAL) == 0:
            print('[{timestamp}] >> op #{op_id} papers: {papers_count}'.format(
                timestamp=time.ctime(),
                op_id=self.__operation_counter, papers_count=len(self.__paper_storage_mapping))
            )

    def get_paper_record_id(self, paper_id):
        if paper_id in self.__paper_storage_mapping.keys():
            return self.__paper_storage_mapping[paper_id]
        else:
            return None

    def store_cache(self):
        print('storing all cache')
        self.__clean_cache(clean_factor=1.0)

        # store mapping
        self.__store_name_mapping()

    def __store_name_mapping(self):
        # convert name mapping to json string
        mapping_as_string = json.dumps(self.__paper_storage_mapping)

        # store string to file
        with open(PaperInfoManager.MAPPING_FILE_PATH, 'wt') as output_file:
            output_file.write(mapping_as_string)
