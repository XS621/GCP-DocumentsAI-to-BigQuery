import pandas as pd
import re
from typing import Iterator, Optional, Sequence, Tuple

import os
from dateutil.parser import parse
from google.cloud import documentai_v1 as documentai
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime

#define parameters!
BUCKET_NAME = os.environ['BUCKET_NAME']
STORAGE_FOLDER_NAME = os.environ['STORAGE_FOLDER_NAME']
project_id = os.environ['PROJECT_ID']
location = os.environ['PROJECT_LOCATION']
processor_id = os.environ['PROCESSOR_ID']

#def move_file
def move_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name):
    """Moves a blob from one bucket to another with a new name."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The ID of your GCS object
    # blob_name = "your-object-name"
    # The ID of the bucket to move the object to
    # destination_bucket_name = "destination-bucket-name"
    # The ID of your new GCS object (optional)
    # destination_blob_name = "destination-object-name"

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )
    source_bucket.delete_blob(blob_name)

    print(
        "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )

#parameters got from environ
SUPPORTED_EXTENSIONS = ['.pdf', '.jpg','.jpeg','.png']
PROCESSOR_NAME = f'projects/{project_id}/locations/{location}/processors/{processor_id}'
KEYWORDS = ['number','id', 'date','#','invoice']
bqclient = bigquery.Client()
#fungsi buat list files inside target folder
def list_files(bucket_name):
    file_name_list = []
    storage_client = storage.Client()
    prefix = STORAGE_FOLDER_NAME+"/"
    blobs = storage_client.list_blobs(bucket_name, prefix='test-docai/')
    for blob in blobs:
        if blob.name!=prefix:
            file_name_list.append(blob.name)
    return file_name_list

#fungsi buat parse text dari document ai
def get_text(doc_element:dict, document:dict):
    response = ""
    for segment in doc_element.text_anchor.text_segments:
        start_index = (
            int(segment.start_index)
            if segment in doc_element.text_anchor.text_segments
            else 0
        )
        end_index = int(segment.end_index)
        response += document.text[start_index:end_index]
    return response


#fungsi utk open file dari gcs bucket ke memory
def open_file_in_memory(file_name, bucket_name):
    for extension in SUPPORTED_EXTENSIONS:
        if file_name[-len(extension):] in extension:
            if extension=='.pdf':
                mime_type='application/pdf'
            elif extension=='.png':
                mime_type='image/png'
            else:
                mime_type="image/jpeg"
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(file_name)
            contents = blob.download_as_string()
            return contents, mime_type

#fungsi untuk memilah text yg di parse ke informasi yang berguna
def get_important_fields(document: documentai.Document):
    sorted_form_fields = form_fields_sorted_by_ocr_order(document)
    data = form_field_tabular_data(sorted_form_fields, document)
    return list(data)
    
#FUNGSI INI KUCNINYA
def form_field_tabular_data(
    form_fields: Sequence[documentai.Document.Page.FormField], document: documentai.Document,) -> Iterator[Tuple[str, str, str]]:
    if not form_fields:
        yield ("-", "-", "-")
        return
    for form_field in form_fields:
        name = text_from_anchor(form_field.field_name.text_anchor, document)
        value = text_from_anchor(form_field.field_value.text_anchor, document)
        findkeyword = re.compile('|'.join([r'\b%s\b' % w for w in KEYWORDS]), flags=re.I)
        x = findkeyword.findall(name.lower())
        if len(x)>0:
            yield (name, value)

#ini unchanged, kepake buat nge sort text terdeteksi berdasarkan indexnya
def form_fields_sorted_by_ocr_order(document: documentai.Document,) -> Sequence[documentai.Document.Page.FormField]:
    def sort_key(form_field):
        # Sort according to the field name detected position
        text_anchor = form_field.field_name.text_anchor
        return text_anchor.text_segments[0].start_index if text_anchor else 0

    form_fields = (
        form_field for page in document.pages for form_field in page.form_fields
    )
    return sorted(form_fields, key=sort_key)

#fungsi buat mecah document text jadi text2 berdasarkan start index and end index
def text_from_anchor(text_anchor: documentai.Document.TextAnchor, document: documentai.Document) -> str:
    text = "".join(
        document.text[segment.start_index : segment.end_index]
        for segment in text_anchor.text_segments
    )
    return text[:-1] if text.endswith("\n") else text

#MAIN FUNCTION
def process_item(filename):
    #client options. dikosongin karena kita pake processor region us
    #define the documentai client
    docai_client = documentai.DocumentProcessorServiceClient(client_options={})

    #download previously detected files from bigquery
    query_string = "SELECT Filename FROM `datalabs-int-bigdata.demo_docai_result.table_2`"
    done_file_list = [i[0] for i in (bqclient.query(query_string).result().to_dataframe(create_bqstorage_client=False)).values.tolist()]
    
    filename_split = filename.split(sep='/')[-1]
    #check apakah calon file ini sudah diproses/blm
    if  filename_split in done_file_list:
        print('Already detected')
        return 0
    else:
        #prepare temporary dataframe to store information before submission
        temp_df = pd.DataFrame(columns = ['ID', 'Date','Filename','LoadedDate'])

        #open file on memory, before process
        file_content, retrv_mime = open_file_in_memory(filename, BUCKET_NAME)
        raw_document = {"content":file_content, "mime_type":retrv_mime}
        request = {"name":PROCESSOR_NAME, "raw_document":raw_document}

        #fetch result from documentai
        result = docai_client.process_document(request=request)
        dokumen = result.document
      
        #parse result ke bentuk yang lebih mudah diproses
        #hasilnya : (field, value)
        important_fields = get_important_fields(dokumen)
        if important_fields is not None:
            tmp_id = ''
            tmp_date = ''
            for item in important_fields:
                if 'number' in item[0].lower() or '#' in item[0].lower() or 'no' in item[0].lower():
                    tmp_id=int(item[1])
                if 'date' in item[0].lower():
                    tmp_date = parse(str(item[1])).strftime('%m-%d-%Y')
                    
            if tmp_id=="":
                tmp_id = 0
            print(tmp_id, tmp_date)
            temp_df = temp_df.append({'ID':tmp_id, 'Date':tmp_date, 'Filename':filename_split, 'LoadedDate':(datetime.now()).strftime('%m-%d-%Y')}, ignore_index=True)
              
            job_config = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField('ID', bigquery.enums.SqlTypeNames.INT64),
                    bigquery.SchemaField('Date', bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField('Filename', bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField('LoadedDate', bigquery.enums.SqlTypeNames.STRING)

                ],
                write_disposition="WRITE_TRUNCATE"
            )

            #define dest.table
            table = 'datalabs-int-bigdata.demo_docai_result.table_2'
            try:
                job = bqclient.load_table_from_dataframe(temp_df, table, job_config=job_config)
                destination_filename = 'test-docai-processed'+'/'+filename_split
                move_blob(BUCKET_NAME, filename, BUCKET_NAME, destination_filename)
                return 1
            except Exception as e:
                return 0
