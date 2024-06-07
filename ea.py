import streamlit as st
import boto3
from io import BytesIO
import time
import os
import re
import datetime
from datetime import datetime, timedelta
from dateutil.parser import parse
import pandas as pd
import math

french_months = {
    'Janvier': 'January', 'Février': 'February', 'Mars': 'March', 'Avril': 'April',
    'Mai': 'May', 'Juin': 'June', 'Juillet': 'July', 'Août': 'August',
    'Septembre': 'September', 'Octobre': 'October', 'Novembre': 'November', 'Décembre': 'December'
}

german_months = {
    'Januar': 'January', 'Februar': 'February', 'März': 'March', 'April': 'April',
    'Mai': 'May', 'Juni': 'June', 'Juli': 'July', 'August': 'August',
    'September': 'September', 'Oktober': 'October', 'November': 'November', 'Dezember': 'December'
}

indonesian_months = {
    'Januari': 'January', 'Februari': 'February', 'Maret': 'March', 'April': 'April',
    'Mei': 'May', 'Juni': 'June', 'Juli': 'July', 'Agustus': 'August',
    'September': 'September', 'Oktober': 'October', 'November': 'November', 'Desember': 'December'
}

malay_months = {
    'Januari': 'January', 'Februari': 'February', 'Mac': 'March', 'April': 'April',
    'Mei': 'May', 'Jun': 'June', 'Julai': 'July', 'Ogos': 'August',
    'September': 'September', 'Oktober': 'October', 'November': 'November', 'Disember': 'December'
}

filipino_months = {
    'Enero': 'January', 'Pebrero': 'February', 'Marso': 'March', 'Abril': 'April',
    'Mayo': 'May', 'Hunyo': 'June', 'Hulyo': 'July', 'Agosto': 'August',
    'Setyembre': 'September', 'Oktubre': 'October', 'Nobyembre': 'November', 'Disyembre': 'December'
}

italian_months = {
    'Gennaio': 'January', 'Febbraio': 'February', 'Marzo': 'March', 'Aprile': 'April',
    'Maggio': 'May', 'Giugno': 'June', 'Luglio': 'July', 'Agosto': 'August',
    'Settembre': 'September', 'Ottobre': 'October', 'Novembre': 'November', 'Dicembre': 'December'
}

spanish_months = {
    'Enero': 'January', 'Febrero': 'February', 'Marzo': 'March', 'Abril': 'April',
    'Mayo': 'May', 'Junio': 'June', 'Julio': 'July', 'Agosto': 'August',
    'Septiembre': 'September', 'Octubre': 'October', 'Noviembre': 'November', 'Diciembre': 'December'
}


def replace_nan_with_none(data):
    if isinstance(data, list):
        return [replace_nan_with_none(item) for item in data]
    elif isinstance(data, dict):
        return {key: replace_nan_with_none(value) for key, value in data.items()}
    elif isinstance(data, (float, int)) and math.isnan(data):
        return None
    else:
        return data


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
                return result, f.read()

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


def convert_to_required_format(parsed_response, document_type):
    # print(parsed_response)
    # The base structure

    total_amt = parsed_response['Invoice_Details'].get('TOTAL', '0')
    subtotal_amt = parsed_response['Invoice_Details'].get('SUBTOTAL', '0')

    # check if "." is a thousand separator
    flag1 = determine_flag(total_amt)
    flag2 = determine_flag(subtotal_amt)

    dot_is_thousand_separator = False

    if flag1 or flag2:
        dot_is_thousand_separator = True

    bill_date = parsed_response['Invoice_Details'].get('INVOICE_RECEIPT_DATE')
    due_date = parsed_response['Invoice_Details'].get('DUE_DATE')
    terms = extract_number(parsed_response['Invoice_Details'].get('PAYMENT_TERMS', '0'), dot_is_thousand_separator)
    # logger.info(f"terms : {terms}")
    if int(terms) > 0:
        terms = int(terms)
    else:
        terms = None

    receiver_address_first_line = parsed_response['Invoice_Details'].get('RECEIVER_ADDRESS', '').split('\n', 1)[0]
    vendor_address_first_line = parsed_response['Invoice_Details'].get('VENDOR_ADDRESS', '').split('\n', 1)[0]
    # other_first_line = parsed_response['Invoice_Details'].get('OTHER', '').split('\n', 1)[0]
    vendor_name = parsed_response['Invoice_Details'].get('VENDOR_NAME', "")

    if document_type == 'INVOICE':
        contact_name = parsed_response['Invoice_Details'].get('RECEIVER_NAME')
        if not contact_name:  # If RECEIVER_NAME is missing, use the first line of RECEIVER_ADDRESS
            contact_name = receiver_address_first_line
        # if not contact_name:
        #     contact_name = other_first_line
    elif document_type == 'BILL':
        contact_name = parsed_response['Invoice_Details'].get('VENDOR_NAME')
        if not contact_name:  # If VENDOR_NAME is missing, use the first line of VENDOR_ADDRESS
            contact_name = vendor_address_first_line
        # if not contact_name:
        #     contact_name = other_first_line
    else:
        contact_name = None

    # print(f"receiver : {parsed_response['Invoice_Details'].get('RECEIVER_NAME')}")
    # print(f"vendor : {parsed_response['Invoice_Details'].get('VENDOR_NAME')}")
    # print(parsed_response)
    contact_resource_id = None
    contact_name_db = contact_name
    currency = None

    # if contact_resource_id:
    #     default_contact_term = get_default_terms(contact_resource_id)
    # else:
    #     default_contact_term = None

    bill_date_2, due_date_2 = process_dates(bill_date, due_date, None, terms)

    if terms:
        terms = f'Net {int(terms)}'

    required_format = {
        "purchase_ref": parsed_response['Invoice_Details'].get('INVOICE_RECEIPT_ID'),
        "items": [],
        "bill_details": {
            "Subtotal": extract_number(parsed_response['Invoice_Details'].get('SUBTOTAL', '0'),
                                       dot_is_thousand_separator),
            "Shipping": extract_number(parsed_response['Invoice_Details'].get('SHIPPING_HANDLING_CHARGE', '0'),
                                       dot_is_thousand_separator),
            "VAT": extract_number(parsed_response['Invoice_Details'].get('TAX', '0'), dot_is_thousand_separator),
            "total_amount": extract_number(parsed_response['Invoice_Details'].get('TOTAL', '0'),
                                           dot_is_thousand_separator),
            "bill_date": bill_date_2,
            "due_date": due_date_2,
            "notes": None,
            "terms": None

        },
        "contact_details": {
            "name": contact_name_db,
            "notes": None,
            "industry": None,
            "contact_resource_id": contact_resource_id,
            "currency": currency
        },
        "vendor_details": {
            "vendor_name": vendor_name
        }
    }

    # Add each item
    for item in parsed_response['Items']:
        item_name = item.get('ITEM')
        discount = 0
        unit = None
        other_label = item.get('OTHER_LABEL')
        if other_label:
            other_label = other_label.lower()
            if other_label in ['discount', 'discount(%)']:
                discount = float(extract_number(item.get('OTHER', '0'), dot_is_thousand_separator))
            elif other_label in ['uom', 'unit']:
                unit = item.get('OTHER')

        if float(extract_number(item.get('QUANTITY', '0'), dot_is_thousand_separator)) == 0 or float(
                extract_number(item.get('QUANTITY', '0'), dot_is_thousand_separator)) is None:
            quantity = 1
        else:
            quantity = float(extract_number(item.get('QUANTITY', '0'), dot_is_thousand_separator))

        if float(extract_number(item.get('PRICE', '0'), dot_is_thousand_separator)) == 0 and float(
                extract_number(item.get('UNIT_PRICE', '0'), dot_is_thousand_separator)) != 0:
            amount = float(extract_number(item.get('UNIT_PRICE', '0'), dot_is_thousand_separator)) * quantity
            unit_price = amount / quantity
        else:
            amount = float(extract_number(item.get('PRICE', '0'), dot_is_thousand_separator))
            unit_price = float(extract_number(item.get('UNIT_PRICE', '0'), dot_is_thousand_separator))

        if amount != 0 and (unit_price is None or unit_price == 0):
            unit_price = amount / quantity

        if quantity == 1 and amount > unit_price:
            unit_price = amount

        required_format['items'].append({
            "item_name": item_name,
            "unit": unit,
            "quantity": quantity,
            "unit_price": unit_price,
            "discount": 0,
            "amount": amount,
            "tags": None
        })

    return required_format


def determine_flag(total_amount):
    # Check if total_amount has exactly three digits after a dot
    return bool(re.search(r'\.\d{3}$', total_amount))


def process_dates(document_date_str=None, due_date_str=None, default_terms=None, terms_identified=None):
    # Convert document_date
    try:
        document_date_millis = validate_date_range(convert_date_to_millis(document_date_str))
        # document_date_millis = convert_date_to_millis(document_date_str)
        if document_date_millis is None:
            # if document_date after conversion is None, set it to today's date
            document_date_millis = int(datetime.now().timestamp() * 1000)

        # Process due_date based on the rules provided
        due_date_millis = validate_date_range(convert_date_to_millis(due_date_str))
        if due_date_millis is None:
            if terms_identified is not None and terms_identified > 0:
                due_date = datetime.fromtimestamp(document_date_millis / 1000) + timedelta(days=terms_identified)
                due_date_millis = int(due_date.timestamp() * 1000)
            elif default_terms is not None and default_terms > 0:
                due_date = datetime.fromtimestamp(document_date_millis / 1000) + timedelta(days=default_terms)
                due_date_millis = int(due_date.timestamp() * 1000)
            else:
                due_date_millis = document_date_millis

        return document_date_millis, due_date_millis
    except Exception as e:

        return int(datetime.now().timestamp() * 1000), int(datetime.now().timestamp() * 1000)


def extract_number(input_string, treat_dots_as_thousands=False):
    # If treat_dots_as_thousands is True, remove all dots (they're thousands separators)
    if treat_dots_as_thousands:
        input_string = input_string.replace('.', '')

    # Normalize by removing commas (either thousands separators or misplaced)
    input_string = input_string.replace(',', '')

    # Regex to find numbers in brackets, ignoring any characters before the digits
    # \([^0-9]* matches any non-digit characters inside the brackets before the number
    match = re.search(r'\([^0-9]*(-?\d+(\.\d+)?)[^0-9]*\)', input_string)
    if match:
        # Convert the captured number to negative
        return '-' + match.group(1).strip('-')

    # If no bracketed number is found, look for regular positive numbers
    match = re.search(r'(-?\d+(\.\d+)?)', input_string)
    return match.group(1) if match else '0'


def convert_date_to_millis(date_str):
    if not date_str:
        return None

    month_translations = {
        **french_months, **german_months, **indonesian_months, **malay_months,
        **filipino_months, **italian_months, **spanish_months
    }

    # Replace any non-English month names with their English equivalents
    for non_english, english in month_translations.items():
        date_str = date_str.replace(non_english, english)

    try:
        # Try to parse the date string
        parsed_date = parse(date_str, dayfirst=True)
        # Convert it to milliseconds since epoch
        millis = int(parsed_date.timestamp() * 1000)
        return millis
    except Exception as e:
        print(f"Error parsing date {date_str} due to {str(e)}")
        return None


def validate_date_range(date_millis):
    if date_millis is not None:
        year = datetime.fromtimestamp(date_millis / 1000).year
        current_year = datetime.now().year
        if year < 1970 or year > current_year + 10:
            return None
    return date_millis


def process_result(result, document_type):
    if result is None:
        response_dict = {
            'status': 'FAILED',
            'created_by': None,
            'processed_payload': 'No text detected in document',
            'OCR_details': {
                'provider': 'TEXTRACT',
                'mode': 'Sync',  # Or 'ASYNC' based on your process
                'job_id': None,
                'request_time': None,
                'completion_time': None,
                'status': 'Failed'
            }
            # Add any other initial fields here...
        }
        return response_dict
    else:
        if pd.DataFrame(result['items']).empty:
            if result['bill_details']['Subtotal'] or result['bill_details']['total_amount']:
                vendor_name = result['vendor_details'].get('vendor_name', "")
                if document_type == 'INVOICE':
                    item_name = 'Invoice Total'
                else:
                    if vendor_name == "":
                        item_name = 'Bill Total'
                    else:
                        item_name = vendor_name + ' Bill Total'
                # Accessing the subtotal
                total_amount = result['bill_details']['total_amount']
                total_amount = float(total_amount) if total_amount else 0
                # Accessing the total_amount and check if it's None or 0, then use subtotal
                subtotal = result['bill_details'].get('Subtotal', total_amount)  # Use .get for safe access
                if (float(subtotal) == 0 or subtotal is None) and (
                        total_amount != 0 and total_amount is not None):
                    subtotal = total_amount
                    # print(f"Here {total_amount}")
                else:
                    subtotal = float(
                        subtotal) if subtotal else total_amount  # Fallback to subtotal if total_amount is 0 or None

                # If contact found then use contact defaults on No Item

                default_account_resource_id = None
                default_tax_profile_resource_id = None
                default_account_name = None

                print(f"subtotal : {subtotal}")
                print(f"total_amount : {total_amount}")

                subtotal_items = [{
                    "item_name": item_name,
                    "unit": None,
                    "quantity": 1.0,
                    "unit_price": subtotal,
                    "discount": 0,
                    "amount": subtotal,
                    "organization_account_resource_id": default_account_resource_id,
                    "tax_profile_resource_id": default_tax_profile_resource_id,
                    "item_resource_id": None,
                    "confidence_flag": True,
                    "account_name": default_account_name
                }]

                # Since we're overriding items, we assume there's no need to continue with classification
                response_dict = {
                    "bt_reference": result['purchase_ref'],
                    "items": subtotal_items,
                    "attachment_details": result['bill_details'],
                    "contact_details": result['contact_details'],
                    "overall_confidence_flag": False,
                    "agent": "lambda",
                }

                response_dict_final = {
                    'status': 'COMPLETED',
                    'created_by': None,
                    'processed_payload': response_dict,
                    'OCR_details': {
                        'provider': 'TEXTRACT',
                        'mode': 'Sync',
                        'job_id': None,
                        'request_time': None,
                        'completion_time': None,
                        'status': None
                    }
                }

                return response_dict_final

            else:
                response_dict = {
                    "bt_reference": result['purchase_ref'],
                    "items": None,
                    "attachment_details": result['bill_details'],
                    "bt_details": result['bt_details'],
                    "contact_details": result['contact_details'],
                    "overall_confidence_flag": None,
                    "agent": "lambda",
                }

                response_dict_final = {
                    'status': 'COMPLETED',
                    'created_by': None,
                    'processed_payload': response_dict,
                    'OCR_details': {
                        'provider': 'TEXTRACT',
                        'mode': 'Sync',
                        'job_id': None,
                        'request_time': None,
                        'completion_time': None,
                        'status': None
                    }
                }

                return response_dict_final
        else:
            items_df = pd.DataFrame(result['items'])
            # print(items_df)
            # logger.info(f"this is the unique names : {items_df['item_name'].unique()}")
            if items_df['item_name'].isnull().all():
                items_df['organization_account_resource_id'] = None
                items_df['confidence_flag'] = False
                items_df['item_resource_id'] = None
                items_df['tax_profile_resource_id'] = None
                items_df['account_name'] = None
                items_df_copy = items_df.copy()  # Create a copy of df
                items_df_copy = items_df_copy.to_dict(orient='records')
                response_dict = {
                    "bt_reference": result['purchase_ref'],
                    "items": items_df_copy,
                    "attachment_details": result['bill_details'],
                    "bt_details": result['bt_details'],
                    "contact_details": result['contact_details'],
                    "overall_confidence_flag": False,
                    "agent": "lambda",
                }

                response_dict_final = {
                    'status': 'COMPLETED',
                    'created_by': None,
                    'processed_payload': response_dict,
                    'OCR_details': {
                        'provider': 'TEXTRACT',
                        'mode': 'Sync',  # Or 'ASYNC' based on your process
                        'job_id': None,
                        'request_time': None,
                        'completion_time': None,
                        'status': None
                    }
                    # Add any other initial fields here...
                }
                # logger.info(f"response_dict_final : {response_dict_final}")

                return response_dict_final

            else:
                default_account_resource_id = None
                default_tax_profile_resource_id = None
                default_account_name = None

                # for item in merged_items:
                #     if not item["item_name"]:
                #         item["item_name"] = ''
                #         item["confidence_flag"] = False

                filtered_items = []  # We will store the filtered items here

                # print(filtered_items)
                merged_items = items_df.to_dict(orient='records')

                response_dict = {
                    "bt_reference": result['purchase_ref'],
                    "attachment_details": result['bill_details'],
                    "contact_details": result['contact_details'],
                    "items": merged_items,
                    "agent": "lambda",
                }

                response_dict = replace_nan_with_none(response_dict)

                response_dict_final = {
                    'status': 'COMPLETED',
                    'created_by': None,
                    'processed_payload': response_dict,
                    'OCR_details': {
                        'provider': 'TEXTRACT',
                        'mode': 'Sync',  # Or 'ASYNC' based on your process
                        'job_id': None,
                        'request_time': None,
                        'completion_time': None,
                        'status': None
                    }
                }
                return response_dict_final


def main():
    st.markdown("<div style='text-align: center; font-size: 50px; font-weight: bold;'>Expense Analyzer</div>",
                unsafe_allow_html=True)
    st.write("")
    st.markdown("""
            <div style='text-align: center; margin-bottom: 20px;'>
                <h1 style='font-size: 20px; font-weight: bold;'>Please upload Invoice/Bill</h1>
            </div>
        """, unsafe_allow_html=True)
    st.write("")
    document_type = st.selectbox('Select the Document Type', ('INVOICE', 'BILL'))
    ea_file = st.file_uploader("", type=['pdf', 'jpg', 'png'])
    s3_client = boto3.client('s3')

    if ea_file is not None:
        file_name = ea_file.name
        file_bytes = BytesIO(ea_file.getvalue())
        s3_client.upload_fileobj(file_bytes, st.secrets['BUCKET_NAME'], file_name)
        output, txt_output = analyze_expense_async(st.secrets['BUCKET_NAME'], ea_file.name)
        parsed_response = parse_response(output)
        st.write("")
        st.markdown("""
                    <div style='text-align: center; margin-bottom: 20px;'>
                        <h1 style='font-size: 17px; font-weight: bold;'>Formatted Response from Expense Analyzer</h1>
                    </div>
                """, unsafe_allow_html=True)
        st.write("")
        st.write(parsed_response)
        req_for = convert_to_required_format(parsed_response, document_type)
        st.write("")
        st.markdown("""
                        <div style='text-align: center; margin-bottom: 20px;'>
                            <h1 style='font-size: 17px; font-weight: bold;'>Final Classification Response</h1>
                        </div>
                    """, unsafe_allow_html=True)
        st.write("")
        final_resp = process_result(req_for,document_type)
        st.write(final_resp)

        col1, col2, col3 = st.columns([15, 10, 15])
        with col2:
            st.download_button(
                label="Download Raw Response",
                data=txt_output,
                file_name=os.path.splitext(file_name)[0] + "_textract.txt",
                mime='text/plain',
            )


if __name__ == "__main__":
    main()
