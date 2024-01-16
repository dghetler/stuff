
import io
import os
import csv
import string
import unicodedata
from statistics import median
import warnings
import re
import ast
import argparse
from datetime import datetime
from dateutil import parser as dateparser
import pandas as pd
import numpy as np

warnings.filterwarnings("error")

start = datetime.now()

parser = argparse.ArgumentParser(description='Analyze data file and prepare SQL code for loading')
parser.add_argument('-f','--file', help='The file name', required=True)
parser.add_argument('-d','--delimiter', help='The field delimiter', required=True)
args = parser.parse_args()

filename = args.file
delim = args.delimiter
if delim == r'\t':  # Check if delimiter is a tab
    delim = '\t'
#new_delim = '|'

# Function to read the file and change the delmiter if necessary and write to a new file
# Multi-Character delimiter is an issue with csv reader, and erroneous delimiters causing whacky records would
# cause pandas to fail to load the file so also converting multi-character to single character delimiter
def file_reader(file, delim):
    if delim in (',', '|'):
        new_file = file.read()
        new_delim = delim
    else:
        new_delim = '|'
        new_file = file.read().replace(delim, new_delim)
    return new_file, new_delim

# Function to check the line endings and whether there are quoted fields
# Will be used later in building SQL script to toggle appropriate options
# in bulk insert
def determine_line_endings(filename, delim):
    with open(filename, 'r', newline='') as file:
        counter = 0
        quotes = False
        line_endings = set()
        while quotes == False or counter < 10:
            line = file.readline()
            record = line.split(delim)
            for value in record:
                value = value.rstrip('\r\n')
                if value.startswith('"') and value.endswith('"'):
                    quotes = True
            if not line:
                break
            counter += 1
            line_ending = line[-2:]
            if line_ending == '\r\n':
                line_endings.add('CRLF')
            elif '\n' in line_ending:
                line_endings.add('LF')
        if 'CRLF' in line_endings:
            return 'CRLF', quotes
        return 'LF', quotes

line_terminator, quoted = determine_line_endings(filename, delim)

# Check encoding when loading the file
encoding = 'utf-8'
try:
    with open(filename, 'r', encoding=encoding) as file:
        new_file, new_delim = file_reader(file, delim)
except:
    encoding='cp1252'
    with open(filename, 'r', encoding=encoding) as file:
        new_file, new_delim = file_reader(file, delim)
        
# Fix pipe delimiter if it's not escaped 
if '|' in delim and len(delim) > 1:
    delim = delim.replace('|','\|')

# Read header and prepare some variables to populate
header = pd.read_table(filename, sep=delim, engine='python', encoding=encoding, quotechar='"', dtype='str', nrows=0)
field_lengths = {re.sub('[^0-9a-zA-Z]+', '_', str(field)): 0 for field in header.columns}
field_names = list(field_lengths.keys())
possible_unicode_fields = {field: 0 for field in field_names}
errors = []
field_count = []
fields_dtypes = {field: {'dtype': [], 'whole_len': [], 'fract_len': [], 'control_char': False, 'parens': False, 'dollar': False, 'decimals': False, 'dash': False, 'comma': False, 'sci': False} for field in field_names}
num_lines = 0

# Function to check data types
def dataType(str):
    if len(str) == 0:
        return '0BLANK'
    try:
        t = ast.literal_eval(str)
    except:
        if len(str) > 5:
            try:
                if bool(dateparser.parse(str)):
                    if len(str) > 10:
                        return '1DATETIME'
                    else:
                        return '2DATE'
            except:
                return '6TEXT'
        else:
            return '6TEXT'
    else:
        if isinstance(t, (int, float)):
            if isinstance(t, int):
                if str.startswith('0') and len(str) > 1:
                    return '6TEXT'
                elif len(str) < 10:
                    return '3INT'
                else:
                    return '4BIG'
            elif isinstance(t, float):
                return '5FLOAT'
        else:
            return '6TEXT'

# Open file in text/string buffer
with io.StringIO(new_file) as nfile:
    reader = csv.reader(nfile, delimiter=new_delim, quotechar='"')
    next(reader) # skip header
    # Establish a baseline for the correct number of fields in the file by reading the first 100 records
    for i, row in enumerate(reader):
        field_count.append(len(row))
        if i == 100:
            break
    field_count = median(field_count)

    nfile.seek(0) # go back to start of file
    next(reader) # skip header again

    # Check the file for field count, erroneous quotes, non-ascii, and non-printable characters
    for i, row in enumerate(reader):
        num_lines += 1
        if (num_lines % 100000) == 0:
                print(f'Processing line {num_lines:,}')
        if len(row) != field_count:
            errors.append({'Issue': 'Incorrect field count', 'Line': i+1, 'Field Name': 'n/a',
                           'Field Number': str(len(row))+' fields instead of '+str(field_count),
                           'Field Value': 'n/a', 'Character Value': 'n/a', 'Character Name': 'n/a', 'Character Integer Value': 'n/a'})

        # Run though fields and check for irregularities
        for j, field in enumerate(row):

            # Check for erroneous double quotes
            if field.count('"') % 2 != 0:
                errors.append({'Issue': 'Erroneous double-quote', 'Line': i+1, 'Field Name': field_names[j],
                               'Field Number': j+1, 'Field Value': field, 'Character Value': '"',
                               'Character Name': 'Double Quote','Character Integer Value': ord('"')})

            # Check for non-printable or non-ascii
            if any(char not in string.ascii_letters+string.digits+string.punctuation+' ' for char in field):
                non_printable_chars = [char for char in field if char not in string.ascii_letters+string.digits+string.punctuation+' ']
                for char in non_printable_chars:
                    try:
                        char_name = unicodedata.name(char)
                    except:
                        char_name = 'n/a'
                    char2 = char
                    if char in ('\n','\r'):
                        fields_dtypes[field_names[j]]['control_char'] = True
                        if char == '\n':
                            char_name = 'Line Feed'
                            char2 = 'LF'
                        else:
                            char_name = 'Carriage Return'
                            char2 = 'CR'
                    else:
                        possible_unicode_fields[field_names[j]] = possible_unicode_fields[field_names[j]] + 1
                    errors.append({'Issue': 'Non-printable or Control Character', 'Line': i+1,
                                    'Field Name': field_names[j], 'Field Number': j+1, 'Field Value': field.strip('\r\n'),
                                    'Character Value': char2, 'Character Name': char_name, 'Character Integer Value': ord(char)})
                        
            # Update the field lengths and establish data types and max values for float fields
            if len(row) == field_count:
                if len(field) > field_lengths[field_names[j]]:
                    field_lengths[field_names[j]] = len(field)
                clean_field = re.sub(r'[(),$]','',str(field)).strip()
                if clean_field == '-':
                    clean_field = '0'
                    fields_dtypes[field_names[j]]['dash'] = True
                dtype = dataType(clean_field)
                fields_dtypes[field_names[j]]['dtype'].append(dtype)
                fields_dtypes[field_names[j]]['dtype'] = [max(fields_dtypes[field_names[j]]['dtype'])]
                # Numbers output from excel can have varyious formatting. Running some checks to account 
                # for them and make adjustments in SQL script later on to clean-up/fix data when loading
                if fields_dtypes[field_names[j]]['dtype'][0] in ('5FLOAT', '4BIG', '3INT'):
                    if '(' in field:
                        fields_dtypes[field_names[j]]['parens'] = True
                    if '$' in field:
                        fields_dtypes[field_names[j]]['dollar'] = True
                    if '..' in clean_field:
                        fields_dtypes[field_names[j]]['decimals'] = True
                    if ',' in field:
                        fields_dtypes[field_names[j]]['comma'] = True
                    if 'e' or 'E' in field:
                        fields_dtypes[field_names[j]]['sci'] = True
                if fields_dtypes[field_names[j]]['dtype'][0] == '5FLOAT' and clean_field != '' and '..' not in clean_field:
                    dec_string = np.format_float_positional(float(clean_field), trim='0')
                    val = dec_string.split('.')
                    fields_dtypes[field_names[j]]['whole_len'].append(len(val[0]))
                    fields_dtypes[field_names[j]]['fract_len'].append(len(val[1]))
                    fields_dtypes[field_names[j]]['whole_len'] = [max(fields_dtypes[field_names[j]]['whole_len'])]
                    fields_dtypes[field_names[j]]['fract_len'] = [max(fields_dtypes[field_names[j]]['fract_len'])]

# Iterate through fields and get percentage of each field that has non-ascii characters
# so we can assign NVARCHAR fields for unicode data in SQL script later on
for k, v in possible_unicode_fields.items():
    possible_unicode_fields[k] = possible_unicode_fields[k] / num_lines * 100

# Convert errors to dataframe, save to csv, then check for issues
if len(errors) > 0:
    df_errors = pd.DataFrame(errors)
    df_errors.to_csv(filename+'_issues.log', index=False, quoting=2, encoding='utf-8')
    if any(df_errors['Issue'].str.contains('Incorrect|quote', regex=True)):
        print("Looks like there are records with incorrect field counts or erroneous quotes - please review issue log and correct data before re-running script.")
        quit()
    else:
        print('Looks like the only issues are non-ascii, non-printable or control characters - generating SQL load script but please review issue log just in case.')
else:
    print('No issues detected - generating SQL load script.')

# Write SQL load file
sql_code = []

sql_code.append('--===============================\n')
sql_code.append('--SQL LOAD SCRIPT\n')
sql_code.append('--DESC: ?????\n')
sql_code.append(f'--DATE: {datetime.now(tz=None)}\n')
sql_code.append('--BY: ????\n')
sql_code.append('--===============================\n\n\n')

sql_code.append('GO\nDROP TABLE IF EXISTS #SHELL\nCREATE TABLE #SHELL (\n')
for i, field in enumerate(field_names):
    length = 0
    if field_lengths[field] > 0:
        length = field_lengths[field]
    else:
        length = 1
    if possible_unicode_fields[field] > 1: # If unicode % is greater than 1% use NVARCHAR
        sql_code.append(f'\t[{field}]\tNVARCHAR({length})')
    else:
        sql_code.append(f'\t[{field}]\tVARCHAR({length})')
    if i != len(field_names)-1:
        sql_code.append(',\n')
    else:
        sql_code.append('\n);\n\n\n')

if delim == '\t':  # Switch tab character back to raw string before writing sql
    delim = r'\t'

sql_code.append(
    f"GO\nBULK INSERT #SHELL\nFROM '{os.path.join(os.getcwd(), filename)}' WITH (\n")
sql_code.append("CODEPAGE = '65001',\n")
sql_code.append("FIRSTROW = 2,\n")
sql_code.append(f"FIELDTERMINATOR = '{delim}',\n")
if quoted:
    sql_code.append("FORMAT = 'CSV',\n")
    sql_code.append("FIELDQUOTE = '\"',\n")
else:
    sql_code.append("--FORMAT = 'CSV',\n")
    sql_code.append("--FIELDQUOTE = '\"',\n")
if line_terminator == 'LF':
    sql_code.append("ROWTERMINATOR = '0x0a'\n")
else:
    sql_code.append("ROWTERMINATOR = '\\n'\n")
sql_code.append("--DATAFILETYPE = 'char' --(or 'widechar')\n")
sql_code.append(");\n\n\n")

sql_code.append('GO\nDROP TABLE IF EXISTS #FORMAT\nSELECT\n')
for i, field in enumerate(field_names):
    length = 1
    dtype = str(fields_dtypes[field]['dtype'][0])
    cr1 = cr2 = dr1 = dr2 = dc1 = dc2 = co1 = co2 = ''
    # Account for control characters and various excel number scenarios within the field
    if fields_dtypes[field]['control_char']:
        cr1 = "REPLACE(REPLACE("
        cr2 = ",CHAR(10),''),CHAR(13),'')"
    if fields_dtypes[field]['dollar']:
        dr1 = "REPLACE("
        dr2 = ",'$','')"
    if fields_dtypes[field]['decimals']:
        dc1 = "REPLACE("
        dc2 = ",'..','.')"
    if fields_dtypes[field]['comma']:
        co1 = "REPLACE("
        co2 = ",',','')"
    if field_lengths[field] > 0:
        length = field_lengths[field]
    if dtype in ('6TEXT','0BLANK'):
        if possible_unicode_fields[field] > 1:
            sql_code.append(
                f"\tCAST(NULLIF(LTRIM(RTRIM({cr1}[{field}]{cr2})),'') AS NVARCHAR({length})) AS [{field}]")
        else:
            sql_code.append(
                f"\tCAST(NULLIF(LTRIM(RTRIM({cr1}[{field}]{cr2})),'') AS VARCHAR({length})) AS [{field}]")
    if dtype == '4BIG':
        if fields_dtypes[field]['parens'] or fields_dtypes[field]['dash']:
            sql_code.append(
                f"\tCAST(CASE WHEN RTRIM(LTRIM(REPLACE({cr1}[{field}]{cr2},'$',''))) = '-' THEN 0\n \
            WHEN CHARINDEX('(',[{field}]) > 0 THEN RTRIM(LTRIM(REPLACE(REPLACE({co1}{dc1}{dr1}{cr1}[{field}]{cr2}{dr2}{dc2}{co2},'(',''),')',''))) * -1\n \
            ELSE NULLIF(RTRIM(LTRIM({co1}{dc1}{dr1}{cr1}[{field}]{cr2}{dr2}{dc2}{co2})),'')\n \
            END AS BIGINT) AS [{field}]")
        else:
            sql_code.append(
                f"\tCAST(NULLIF(RTRIM(LTRIM({co1}{dc1}{dr1}{cr1}[{field}]{cr2}{dr2}{dc2}{co2})),'') AS BIGINT) AS [{field}]")
    if dtype == '3INT':
        if fields_dtypes[field]['parens'] or fields_dtypes[field]['dash']:
            sql_code.append(
                f"\tCAST(CASE WHEN RTRIM(LTRIM(REPLACE({cr1}[{field}]{cr2},'$',''))) = '-' THEN 0\n \
            WHEN CHARINDEX('(',[{field}]) > 0 THEN RTRIM(LTRIM(REPLACE(REPLACE({co1}{dc1}{dr1}{cr1}[{field}]{cr2}{dr2}{dc2}{co2},'(',''),')',''))) * -1\n \
            ELSE NULLIF(RTRIM(LTRIM({co1}{dc1}{dr1}{cr1}[{field}]{cr2}{dr2}{dc2}{co2})),'')\n \
            END AS INT) AS [{field}]")
        else:
            sql_code.append(
                f"\tCAST(NULLIF(RTRIM(LTRIM({co1}{dc1}{dr1}{cr1}[{field}]{cr2}{dr2}{dc2}{co2})),'') AS INT) AS [{field}]")
    if dtype.endswith('DATE'):
        sql_code.append(
            f"\tCAST(NULLIF(LTRIM(RTRIM({cr1}[{field}]{cr2})),'') AS DATE) AS [{field}]")
    if dtype.endswith('TIME'):
        sql_code.append(
            f"\tCAST(NULLIF(LTRIM(RTRIM({cr1}[{field}]{cr2})),'') AS DATETIME2) AS [{field}]")
    if dtype == '5FLOAT':
        decimal_len = fields_dtypes[field]['whole_len'][0] + fields_dtypes[field]['fract_len'][0] + 1
        fract_len = fields_dtypes[field]["fract_len"][0]
        if fields_dtypes[field]['parens'] or fields_dtypes[field]['dash']: 
            sql_code.append(
                f"\tCAST(CASE WHEN RTRIM(LTRIM(REPLACE({cr1}[{field}]{cr2},'$',''))) = '-' THEN 0.0\n \
            WHEN CHARINDEX('(',[{field}]) > 0 THEN CAST(RTRIM(LTRIM(REPLACE(REPLACE({co1}{dc1}{dr1}{cr1}[{field}]{cr2}{dr2}{dc2}{co2},'(',''),')',''))) AS FLOAT) * -1.0\n \
            ELSE CAST(NULLIF(RTRIM(LTRIM({co1}{dc1}{dr1}{cr1}[{field}]{cr2}{dr2}{dc2}{co2})),'') AS FLOAT)\n \
            END AS DECIMAL({str(decimal_len)},{str(fract_len)})) AS [{field}]")
        elif fields_dtypes[field]['sci']:
            sql_code.append(
                f"\tCASE WHEN [{field}] LIKE '%e%' THEN CAST(CAST(RTRIM(LTRIM({co1}{dc1}{dr1}{cr1}[{field}]{cr2}{dr2}{dc2}{co2})) AS FLOAT) AS DECIMAL({str(decimal_len)},{str(fract_len)}))\n \
                        ELSE CAST(RTRIM(LTRIM({co1}{dc1}{dr1}{cr1}[{field}]{cr2}{dr2}{dc2}{co2})) AS DECIMAL({str(decimal_len)},{str(fract_len)}))
                        END AS [{field}]")
        else:
            sql_code.append(
                f"\tCAST(RTRIM(LTRIM({co1}{dc1}{dr1}{cr1}[{field}]{cr2}{dr2}{dc2}{co2})) AS DECIMAL({str(decimal_len)},{str(fract_len)})) AS [{field}]")
    if i != len(field_names)-1:
        sql_code.append(',\n')
    else:
        sql_code.append('\nINTO #FORMAT\nFROM #SHELL\n;\n\n')

with open(filename+'_load.sql', 'w', encoding='utf-8') as file:
    for item in sql_code:
        file.write(f'{item}')

total_time = datetime.now() - start
print(f"Processed {num_lines:,} records")
print('Execution time: ', str(total_time))
