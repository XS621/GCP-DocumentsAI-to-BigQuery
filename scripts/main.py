from job import process_item

def hello_gcs(event, context):
    file = event
    result = process_item(file['name'])
    if result==1:
        print(f"Success processing file: {file['name']}.")
    else:
        print(f"Failed processing file: {file['name']}.")