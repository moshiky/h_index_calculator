
import json


class PaperInfoManager:

    MAX_PAPERS_IN_STORAGE_FILE = 100000
    PUBLICATION_YEAR_KEY_NAME = 'pub_year'
    CITATION_INFO_KEY_NAME = 'citations'
    STORAGE_FILE_PATH_FORMAT = r'storage/papers_{file_id}.json'
    PAPER_INFO_STORAGE_FILE_PATH = r'storage/papers.json'

    def __init__(self):
        self.__papers = self.__load_paper_storage()

        self.__current_file_index = 0

        self.__working_storage = dict()
        self.__working_storage_file_path = \
            PaperInfoManager.STORAGE_FILE_PATH_FORMAT.format(file_id=self.__current_file_index)

        self.__loaded_storage = None
        self.__loaded_storage_file_path = None

        self.__paper_storage_mapping = dict()

    def __load_paper_storage(self):
        return dict()

    def add_paper(self, paper_id, paper_year):
        # verify a paper is'nt added twice
        if paper_id in self.__papers.keys():
            if self.__papers[paper_id][PaperInfoManager.PUBLICATION_YEAR_KEY_NAME] is not None:
                raise Exception('paper {paper_id} already in storage'.format(paper_id=paper_id))

            else:
                # empty paper record created when other paper cited it before
                # so only need to set the publication year
                self.__papers[paper_id][PaperInfoManager.PUBLICATION_YEAR_KEY_NAME] = paper_year

        else:
            # create new paper record
            self.__papers[paper_id] = {
                PaperInfoManager.PUBLICATION_YEAR_KEY_NAME: paper_year,
                PaperInfoManager.CITATION_INFO_KEY_NAME: dict()
            }

    def add_citation(self, paper_id, paper_year):
        # verify paper is in  storage
        if paper_id not in self.__papers.keys():
            # create empty paper record
            self.__papers[paper_id] = {
                PaperInfoManager.PUBLICATION_YEAR_KEY_NAME: None,
                PaperInfoManager.CITATION_INFO_KEY_NAME: dict()
            }

        # add one to citation count
        if paper_year not in self.__papers[paper_id][PaperInfoManager.CITATION_INFO_KEY_NAME].keys():
            self.__papers[paper_id][PaperInfoManager.CITATION_INFO_KEY_NAME][paper_year] = 0
        self.__papers[paper_id][PaperInfoManager.CITATION_INFO_KEY_NAME][paper_year] += 1

    def store_paper_info(self):
        print('num of papers: ' + str(len(self.__papers.keys())))
        info_as_string = json.dumps(self.__papers)

        with open(PaperInfoManager.PAPER_INFO_STORAGE_FILE_PATH, 'wt') as storage_file:
            storage_file.write(info_as_string)
