import streamlit as st
import boto3
from io import BytesIO
import time
import os


def analyze_expense_async(s3_bucket, s3_file):
    # Initialize the Textract client
    textract_client = boto3.client('textract')
    print(s3_bucket, s3_file, "s#T")
    try:
        # Start the asynchronous expense analysis
        response = textract_client.start_expense_analysis(
            DocumentLocation={
                'S3Object': {
                    'Bucket': s3_bucket,
                    'Name': s3_file
                }
            }
        )

        job_id = response['JobId']
        print(f'Started job with ID: {job_id}')

        # Check the status of the job
        while True:
            status = textract_client.get_expense_analysis(JobId=job_id)
            job_status = status['JobStatus']

            if job_status in ['SUCCEEDED', 'FAILED']:
                break

            print('Job status:', job_status)
            time.sleep(5)

        if job_status == 'SUCCEEDED':
            # Get the results of the expense analysis
            result_pages = []
            next_token = None

            while True:
                if next_token:
                    result = textract_client.get_expense_analysis(JobId=job_id, NextToken=next_token)
                else:
                    result = textract_client.get_expense_analysis(JobId=job_id)

                result_pages.append(result)
                next_token = result.get('NextToken')

                if not next_token:
                    break

            print('Expense analysis completed successfully.')
            output_file = s3_file + "_op"
            # Save the response to a text file
            with open(output_file, 'w') as f:
                for page in result_pages:
                    f.write(str(page) + '\n')

            with open(output_file, 'r') as f:
                return f.read()
        else:
            print('Expense analysis failed.')
            return None

    except Exception as e:
        print(f'Error: {e}')
        return None


def parse_response(response):
    # print(response)
    parsed = {"Invoice_Details": {}, "Items": []}

    for expense_document in response.get('ExpenseDocuments', []):
        for summary_field in expense_document.get('SummaryFields', []):
            field_type = summary_field.get('Type', {}).get('Text')
            field_value = summary_field.get('ValueDetection', {}).get('Text')
            field_label = summary_field.get('LabelDetection', {}).get('Text')
            if field_type not in parsed['Invoice_Details']:
                parsed['Invoice_Details'][field_type] = field_value
                parsed['Invoice_Details'][f"{field_type}_LABEL"] = field_label

        for line_item_group in expense_document.get('LineItemGroups', []):
            for line_item in line_item_group.get('LineItems', []):
                item_details = {}
                for line_item_field in line_item.get('LineItemExpenseFields', []):
                    field_type = line_item_field.get('Type', {}).get('Text')
                    field_value = line_item_field.get('ValueDetection', {}).get('Text')
                    field_label = line_item_field.get('LabelDetection', {}).get('Text')
                    item_details[field_type] = field_value
                    item_details[f"{field_type}_LABEL"] = field_label
                parsed['Items'].append(item_details)

    return parsed


def main():
    st.markdown("<div style='text-align: center; font-size: 50px; font-weight: bold;'>Expense Analyzer</div>",
                unsafe_allow_html=True)

    st.write("")
    document_type = st.selectbox('Select the Document Type', ('Invoice', 'Bill'))
    st.markdown("""
            <div style='text-align: center; margin-bottom: 20px;'>
                <h1 style='font-size: 20px; font-weight: bold;'>Please upload Invoice/Bill</h1>
            </div>
        """, unsafe_allow_html=True)
    st.write("")
    ea_file = st.file_uploader("", type=['pdf', 'jpg', 'png'])
    s3_client = boto3.client('s3')

    if ea_file is not None:
        file_name = ea_file.name
        file_bytes = BytesIO(ea_file.getvalue())
        s3_client.upload_fileobj(file_bytes, st.secrets['BUCKET_NAME'], file_name)
        output = analyze_expense_async(st.secrets['BUCKET_NAME'], ea_file.name)
        parsed_response=parse_response(output)
        st.write("parsed resp",parsed_response)

        col1, col2, col3 = st.columns([15, 10, 15])
        with col2:
            st.download_button(
                label="Download Raw Response",
                data=output,
                file_name=os.path.splitext(file_name)[0] + "_textract.txt",
                mime='text/plain',
            )


if __name__ == "__main__":
    main()
