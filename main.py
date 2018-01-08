
import json
from author_info_manager import AuthorInfoManager
from paper_info_manager import PaperInfoManager


PAPER_ID_FIELD_NAME = 'id'
AUTHOR_LIST_FIELD_NAME = 'authors'
PAPER_YEAR_FIELD_NAME = 'year'
REFERENCES_FIELD_NAME = 'references'


def update_paper_records(paper_record, paper_info_manager):
    # extract required fields
    paper_id = paper_record[PAPER_ID_FIELD_NAME]
    paper_year = paper_record[PAPER_YEAR_FIELD_NAME]
    references = paper_record[REFERENCES_FIELD_NAME] if REFERENCES_FIELD_NAME in paper_record.keys() else list()

    # add new paper record
    paper_info_manager.add_paper(paper_id, paper_year)

    # update citation info
    for referenced_paper_id in references:
        paper_info_manager.add_citation(referenced_paper_id, paper_year)


def update_author_records(paper_record, author_info_manager):
    # extract required fields
    paper_id = paper_record[PAPER_ID_FIELD_NAME]
    author_list = list(set(paper_record[AUTHOR_LIST_FIELD_NAME]))

    # iterate paper's authors and update each one
    for author_id in author_list:
        # make author list without current author
        co_authors = list(author_list)
        co_authors.remove(author_id)

        # update author information
        author_info_manager.add_author_publication(author_id, paper_id, co_authors)


def process_dataset_file(dataset_file_path, author_info_manager, paper_info_manager):
    # read file
    with open(dataset_file_path, 'rt') as dataset_file:
        for file_line in dataset_file:
            # parse line as json
            paper_attributes = json.loads(file_line)

            # update authors
            update_author_records(paper_attributes, author_info_manager)

            # update papers
            update_paper_records(paper_attributes, paper_info_manager)


def main(dataset_file_path):
    # initiate info managers
    print('create managers')
    author_info_manager = AuthorInfoManager()
    paper_info_manager = PaperInfoManager()

    # process dataset file
    print('process dataset file')
    process_dataset_file(dataset_file_path, author_info_manager, paper_info_manager)

    # store processed info
    print('store processed info')
    author_info_manager.store_author_info()
    paper_info_manager.store_paper_info()

    print('done')


if __name__ == '__main__':
    db_file_path = r'dataset\dblp.v10\dblp-ref\dblp-ref-3.json'
    main(db_file_path)
