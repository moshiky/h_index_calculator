
import time


class PaperInfoManager:

    RECORD_FIELD_SEPARATOR = '#'
    RECORD_STRUCTURE_YEAR_LENGTH = 4
    RECORD_STRUCTURE_COUNTER_LENGTH = 4
    RECORD_STRUCTURE_NUMBER_OF_CITATION_YEARS = 40
    RECORD_LENGTH = \
        RECORD_STRUCTURE_YEAR_LENGTH \
        + 1 \
        + RECORD_STRUCTURE_NUMBER_OF_CITATION_YEARS * (RECORD_STRUCTURE_YEAR_LENGTH + RECORD_STRUCTURE_COUNTER_LENGTH) \
        + 1

    MAX_PAPERS_IN_STORAGE_FILE = 320000
    OPERATION_LOG_INTERVAL = 100

    PUBLICATION_YEAR_KEY_NAME = 'y'
    CITATION_INFO_KEY_NAME = 'c'
    STORAGE_FILE_PATH_FORMAT = r'storage/papers_{file_id}.json'

    def __init__(self):
        self.__records_in_current_storage_file = 0
        self.__working_storage_file_index = 0
        self.__operation_counter = 0
        self.__paper_storage_mapping = dict()

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
        for citation_year in paper_record[PaperInfoManager.CITATION_INFO_KEY_NAME].keys():
            citation_count_string = str(paper_record[PaperInfoManager.CITATION_INFO_KEY_NAME][citation_year])
            if len(citation_count_string) > PaperInfoManager.RECORD_STRUCTURE_COUNTER_LENGTH:
                raise Exception('RECORD COUNTER TOO HIGH!!!')

            citation_count_string = citation_count_string.rjust(
                PaperInfoManager.RECORD_STRUCTURE_COUNTER_LENGTH, PaperInfoManager.RECORD_FIELD_SEPARATOR
            )

            record_data += citation_year + citation_count_string

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
        with open(storage_file_path, 'rt') as storage_file:
            storage_file.seek(record_offset, 0)
            record_data = storage_file.read(PaperInfoManager.RECORD_LENGTH)

        # parse record_data into paper record structure
        return self.__record_data_to_paper_record(record_data)

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
        with open(storage_file_path, 'at') as storage_file:
            storage_file.seek(record_offset, 0)
            storage_file.write(record_data)

    def __get_paper_record(self, paper_id):
        # get paper record_id
        paper_record_id = self.get_paper_record_id(paper_id)

        # return record from storage
        return self.__get_record_from_storage(paper_record_id)

    def __store_paper_record(self, paper_id, paper_record):
        # get paper record_id
        paper_record_id = self.get_paper_record_id(paper_id)

        # store paper record
        self.__store_record_to_storage(paper_record_id, paper_record)

    def __create_new_paper_record(self, paper_id, paper_year):
        # check if need to move to new storage file
        if self.__records_in_current_storage_file == PaperInfoManager.MAX_PAPERS_IN_STORAGE_FILE:
            # increase working file index and initiate records counter
            self.__working_storage_file_index += 1
            self.__records_in_current_storage_file = 0

        # allocate record_id
        record_id = '{storage_file_index}_{record_index}'.format(
            storage_file_index=self.__working_storage_file_index, record_index=self.__records_in_current_storage_file
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

        # store changes
        self.__store_paper_record(paper_id, paper_record)

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
                # store changes
                self.__store_paper_record(paper_id, paper_record)

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
