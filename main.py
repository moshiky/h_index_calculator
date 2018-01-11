
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
    paper_year = str(paper_record[PAPER_YEAR_FIELD_NAME])
    references = paper_record[REFERENCES_FIELD_NAME] if REFERENCES_FIELD_NAME in paper_record.keys() else list()

    # add new paper record
    try:
        paper_info_manager.add_paper(paper_id, paper_year)
    except Exception as ex:
        print('Error: failed to add paper')
        raise ex

    # update citation info
    added_references = list()
    for referenced_paper_id in references:
        try:
            paper_info_manager.add_citation(referenced_paper_id, paper_year)
        except Exception as ex:
            print('Error: failed to add paper citation. referenced paper id={ref_id} added={added} all={all_refs}'
                  .format(ref_id=referenced_paper_id, added=added_references, all_refs=references))
            raise ex
        added_references.append(referenced_paper_id)

def update_author_records(paper_record, author_info_manager):
    # extract required fields
    paper_id = paper_record[PAPER_ID_FIELD_NAME]
    author_list = list(set(paper_record[AUTHOR_LIST_FIELD_NAME]))

    # iterate paper's authors and update each one
    added_authors = list()
    for author_id in author_list:
        # make author list without current author
        co_authors = list(author_list)
        co_authors.remove(author_id)

        # update author information
        try:
            author_info_manager.add_author_publication(author_id, paper_id, co_authors)
        except Exception as ex:
            print('ERROR: failed to add author publication. author={author_id} paper={paper_id} added={added} all={all_authors}'
                  .format(author_id=author_id, paper_id=paper_id, added=added_authors, all_authors=author_list))
            raise ex
        added_authors.append(author_id)


def process_dataset_file(dataset_file_path, author_info_manager, paper_info_manager):
    # read file
    line_index = 0
    with open(dataset_file_path, 'rt') as dataset_file:
        for file_line in dataset_file:
            try:
                # parse line as json
                try:
                    paper_attributes = json.loads(file_line)
                except Exception as ex:
                    print('ERROR: failed to load line as json')
                    raise ex

                # update papers
                try:
                    update_paper_records(paper_attributes, paper_info_manager)
                except Exception as ex:
                    print('ERROR: failed to update papers')
                    raise ex

                # update authors
                paper_attributes[PAPER_ID_FIELD_NAME] = \
                    paper_info_manager.get_paper_record_id(paper_attributes[PAPER_ID_FIELD_NAME])
                try:
                    update_author_records(paper_attributes, author_info_manager)
                except Exception as ex:
                    print('ERROR: failed to update authors. paper record id={record_id}'
                          .format(record_id=paper_attributes[PAPER_ID_FIELD_NAME]))
                    raise ex

                # increase line index
                line_index += 1

            except Exception as ex:
                print('line#{line_index} file={file_path} line="{line_text}"'
                      .format(line_index=line_index, file_path=dataset_file_path, line_text=file_line))
                raise ex


def main(db_file_path_list):
    # initiate info managers
    print('create managers')
    author_info_manager = AuthorInfoManager()
    paper_info_manager = PaperInfoManager()

    # process dataset file
    print('process dataset files')
    for db_file_path in db_file_path_list:
        print('processing file: {file_path}'.format(file_path=db_file_path))
        process_dataset_file(db_file_path, author_info_manager, paper_info_manager)

    # store processed info
    print('store author info')
    author_info_manager.store_author_info()
    print('store cached paper info')
    paper_info_manager.store_cache()

    print('done')


if __name__ == '__main__':
    db_files = [
        r'dataset\dblp.v10\dblp-ref\dblp-ref-3.json',
        r'dataset\dblp.v10\dblp-ref\dblp-ref-0.json',
        r'dataset\dblp.v10\dblp-ref\dblp-ref-1.json',
        r'dataset\dblp.v10\dblp-ref\dblp-ref-2.json',
    ]
    main(db_files)
