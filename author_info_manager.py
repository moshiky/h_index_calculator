
import json


class AuthorInfoManager:

    PAPERS_KEY_NAME = 'papers'
    CO_AUTHORS_KEY_NAME = 'co_authors'
    AUTHOR_STORAGE_FILE_PATH = r'storage/authors.json'

    def __init__(self):
        self.__authors = self.__load_author_storage()

    def __load_author_storage(self):
        # todo: load authors file
        return dict()

    def add_author_publication(self, author_id, paper_id, co_authors):
        # validate author record's existence
        if author_id not in self.__authors.keys():
            self.__authors[author_id] = {
                AuthorInfoManager.PAPERS_KEY_NAME: [],
                AuthorInfoManager.CO_AUTHORS_KEY_NAME: []
            }

        # add paper id
        if paper_id in self.__authors[author_id][AuthorInfoManager.PAPERS_KEY_NAME]:
            raise Exception('author {author_id} already has paper {paper_id}'.format(
                author_id=author_id, paper_id=paper_id))
        else:
            self.__authors[author_id][AuthorInfoManager.PAPERS_KEY_NAME].append(paper_id)

        # add co-authors
        for co_author_id in co_authors:
            if co_author_id not in self.__authors[author_id][AuthorInfoManager.CO_AUTHORS_KEY_NAME]:
                self.__authors[author_id][AuthorInfoManager.CO_AUTHORS_KEY_NAME].append(co_author_id)

    def store_author_info(self):
        print('num of authors: ' + str(len(self.__authors.keys())))
        info_as_string = json.dumps(self.__authors)

        with open(AuthorInfoManager.AUTHOR_STORAGE_FILE_PATH, 'wt') as storage_file:
            storage_file.write(info_as_string)
