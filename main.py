
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


def process_dataset_file(dataset_file_info, author_info_manager, paper_info_manager):
    # read file
    line_index = 0
    dataset_file_path = dataset_file_info[0]
    first_line = dataset_file_info[1]
    failed_lines = list()

    with open(dataset_file_path, 'rt') as dataset_file:
        for file_line in dataset_file:
            if line_index < first_line:
                line_index += 1
                continue

            try:
                # parse line as json
                try:
                    paper_attributes = json.loads(file_line)
                except Exception as ex:
                    print('ERROR: failed to load line as json')
                    raise ex

                # validate required fields exists
                record_ok = True
                required_fields = [
                    PAPER_ID_FIELD_NAME,
                    AUTHOR_LIST_FIELD_NAME,
                    PAPER_YEAR_FIELD_NAME
                ]
                for field_name in required_fields:
                    if field_name not in paper_attributes.keys():
                        print('Warning: missing paper information. '
                              'line#{line_index} line={line} paper record={paper_info}'
                              .format(line_index=line_index, line=file_line, paper_info=paper_attributes))
                        record_ok = False
                        break
                if not record_ok:
                    print('skipping paper')
                    continue

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
                      .format(line_index=line_index, file_path=dataset_file_info, line_text=file_line))
                print('exception: {ex}'.format(ex=ex))
                failed_lines.append({
                    'file_path': dataset_file_path,
                    'line_index': line_index,
                    'line_text': file_line,
                    'exception': ex
                })
                print('failed lines in current file: {num_lines}'.format(num_lines=len(failed_lines)))

    return failed_lines


def make_clean_exit(author_info_manager, paper_info_manager):
    # store processed info
    print('store author info')
    author_info_manager.store_author_info()
    print('store cached paper info')
    paper_info_manager.store_cache()


def main(db_file_info_list, should_load_state=False):
    # initiate info managers
    print('create managers')
    author_info_manager = AuthorInfoManager()
    paper_info_manager = PaperInfoManager()

    # load state if needed
    if should_load_state:
        author_info_manager.load_author_info()
        paper_info_manager.restore_stored_state()

    # process dataset file
    failed_lines = list()
    print('process dataset files')
    for db_file_info in db_file_info_list:
        print('processing file: {file_info}'.format(file_info=db_file_info))
        file_failed_lines = process_dataset_file(db_file_info, author_info_manager, paper_info_manager)
        failed_lines.append(file_failed_lines)

    # print failed lines
    if len(failed_lines) > 0:
        print('#### {num_lines} failed lines'.format(num_lines=len(failed_lines)))
        # print(failed_lines)

    # store volatile information
    make_clean_exit(author_info_manager, paper_info_manager)

    print('done')


if __name__ == '__main__':
    db_files = [
        [r'dataset\dblp.v10\dblp-ref\dblp-ref-0.json', 0],
        [r'dataset\dblp.v10\dblp-ref\dblp-ref-1.json', 0],
        [r'dataset\dblp.v10\dblp-ref\dblp-ref-2.json', 0],
        [r'dataset\dblp.v10\dblp-ref\dblp-ref-3.json', 0],
    ]
    main(db_files)
