
import json


class PaperInfoManager:

    MAX_PAPERS_IN_STORAGE_FILE = 500000
    PUBLICATION_YEAR_KEY_NAME = 'y'
    CITATION_INFO_KEY_NAME = 'c'
    STORAGE_FILE_PATH_FORMAT = r'storage/papers_{file_id}.json'
    PAPER_INFO_STORAGE_FILE_PATH = r'storage/papers.json'

    def __init__(self):
        # self.__papers = self.__load_paper_storage()

        self.__current_file_index = 0
        self.__file_swaps = 0
        self.__operation_counter = 0

        self.__working_storage = dict()
        self.__working_storage_file_path = \
            PaperInfoManager.STORAGE_FILE_PATH_FORMAT.format(file_id=self.__current_file_index)

        self.__loaded_storage = None
        self.__loaded_storage_file_path = None

        self.__paper_storage_mapping = dict()

    # def __load_paper_storage(self):
    #     return dict()

    def __store_papers_info(self, papers_info, file_path):
        info_to_store = json.dumps(papers_info)
        with open(file_path, 'wt') as loaded_storage_file:
            loaded_storage_file.write(info_to_store)

    def __load_storage_file_from_disk(self, storage_file_path):
        # read file content
        with open(storage_file_path, 'rt') as storage_file:
            storage_file_content = storage_file.read()

        # parse as json and set instance members
        self.__loaded_storage = json.loads(storage_file_content)
        self.__loaded_storage_file_path = storage_file_path

    def __load_storage_file(self, file_path_to_load):
        print('swapping loaded file. sw#{swap_id} op#{op_id}'.format(
            swap_id=self.__file_swaps, op_id=self.__operation_counter))
        self.__file_swaps += 1

        # store current loaded file
        if self.__loaded_storage_file_path is not None:
            self.__store_papers_info(self.__loaded_storage, self.__loaded_storage_file_path)

        # load requested storage file
        self.__load_storage_file_from_disk(file_path_to_load)

        print('done swapping')

    def __get_paper_record(self, paper_id):
        # get paper storage file name
        paper_storage_file_path = self.__paper_storage_mapping[paper_id]

        # first check in working storage
        if paper_storage_file_path == self.__working_storage_file_path:
            return self.__working_storage[paper_id]

        # second check in loaded storage file
        elif paper_storage_file_path == self.__loaded_storage_file_path:
            return self.__loaded_storage[paper_id]

        # if got here- need to switch loaded file
        else:
            self.__load_storage_file(paper_storage_file_path)
            return self.__loaded_storage[paper_id]

    def __set_paper_year(self, paper_id, paper_year):
        # get paper storage file name
        paper_storage_file_path = self.__paper_storage_mapping[paper_id]

        # first check in working storage
        if paper_storage_file_path == self.__working_storage_file_path:
            self.__working_storage[paper_id][PaperInfoManager.PUBLICATION_YEAR_KEY_NAME] = paper_year

        # second check in loaded storage file
        elif paper_storage_file_path == self.__loaded_storage_file_path:
            self.__loaded_storage[paper_id][PaperInfoManager.PUBLICATION_YEAR_KEY_NAME] = paper_year

        # if got here- need to switch loaded file
        else:
            self.__load_storage_file(paper_storage_file_path)
            self.__loaded_storage[paper_id][PaperInfoManager.PUBLICATION_YEAR_KEY_NAME] = paper_year

    def __create_new_paper_record(self, paper_id, paper_year):
        # store working storage in case it reached max records
        if len(self.__working_storage.keys()) == PaperInfoManager.MAX_PAPERS_IN_STORAGE_FILE:
            # store working storage
            self.__store_papers_info(self.__working_storage, self.__working_storage_file_path)

            # move to next storage file
            self.__current_file_index += 1
            self.__working_storage = dict()
            self.__working_storage_file_path = \
                PaperInfoManager.STORAGE_FILE_PATH_FORMAT.format(file_id=self.__current_file_index)

        # add paper record to working storage
        self.__working_storage[paper_id] = {
            PaperInfoManager.PUBLICATION_YEAR_KEY_NAME: paper_year,
            PaperInfoManager.CITATION_INFO_KEY_NAME: dict()
        }

        # update paper storage mapping
        self.__paper_storage_mapping[paper_id] = self.__working_storage_file_path

    def __add_citation_year_to_storage(self, storage, paper_id, citation_year):
        if citation_year not in storage[paper_id][PaperInfoManager.CITATION_INFO_KEY_NAME].keys():
            storage[paper_id][PaperInfoManager.CITATION_INFO_KEY_NAME][citation_year] = 0

        storage[paper_id][PaperInfoManager.CITATION_INFO_KEY_NAME][citation_year] += 1

    def __add_citation_year(self, paper_id, citation_year):
        # get paper storage file name
        paper_storage_file_path = self.__paper_storage_mapping[paper_id]

        # first check in working storage
        if paper_storage_file_path == self.__working_storage_file_path:
            self.__add_citation_year_to_storage(self.__working_storage, paper_id, citation_year)

        # second check in loaded storage file
        elif paper_storage_file_path == self.__loaded_storage_file_path:
            self.__add_citation_year_to_storage(self.__loaded_storage, paper_id, citation_year)

        # if got here- need to switch loaded file
        else:
            self.__load_storage_file(paper_storage_file_path)
            self.__add_citation_year_to_storage(self.__loaded_storage, paper_id, citation_year)

    def add_paper(self, paper_id, paper_year):
        self.__operation_counter += 1

        # verify a paper is'nt added twice
        if paper_id in self.__paper_storage_mapping.keys():

            # get paper record
            paper_record = self.__get_paper_record(paper_id)

            if paper_record[PaperInfoManager.PUBLICATION_YEAR_KEY_NAME] is not None:
                raise Exception('paper {paper_id} already in storage'.format(paper_id=paper_id))

            else:
                # empty paper record created when other paper cited it before
                # so only need to set the publication year
                self.__set_paper_year(paper_id, paper_year)

        else:
            # create new paper record
            self.__create_new_paper_record(paper_id, paper_year)

    def add_citation(self, paper_id, citation_year):
        self.__operation_counter += 1

        # verify paper is in  storage
        if paper_id not in self.__paper_storage_mapping.keys():
            # create empty paper record
            self.__create_new_paper_record(paper_id, None)

        # add one to citation count
        self.__add_citation_year(paper_id, citation_year)

    def store_active_storage(self):
        # store working storage
        self.__store_papers_info(self.__working_storage, self.__working_storage_file_path)

        # store current loaded file
        self.__store_papers_info(self.__loaded_storage, self.__loaded_storage_file_path)

    # def store_paper_info(self):
    #     print('num of papers: ' + str(len(self.__papers.keys())))
    #     info_as_string = json.dumps(self.__papers)
    #
    #     with open(PaperInfoManager.PAPER_INFO_STORAGE_FILE_PATH, 'wt') as storage_file:
    #         storage_file.write(info_as_string)
