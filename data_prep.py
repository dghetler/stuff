
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
from tqdm import tqdm
import pandas as pd
import numpy as np

warnings.filterwarnings("error")

# start = datetime.now()

parser = argparse.ArgumentParser(description='Analyze data file and prepare SQL code for loading')
parser.add_argument('-f','--file', help='The file name', required=True)
parser.add_argument('-d','--delim', help='The field delimiter', required=True)
parser.add_argument('-c', action='store_true', help='Perform Character Analysis')
args = parser.parse_args()

FILENAME = args.file
DELIM = args.delim
PERFCHAR=False
PERFCHAR = args.c
if DELIM == r'\t':  # Check if delimiter is a tab
    DELIM = '\t'
#new_delim = '|'

# Fix pipe delimiter if it's not escaped
if '|' in DELIM and len(DELIM) > 1:
    DELIM = DELIM.replace('|',r'\|')

def _count_generator(lreader):
    b = lreader(1024 * 1024)
    while b:
        yield b
        b = lreader(1024 * 1024)

with open(FILENAME, 'rb') as fp:
    c_generator = _count_generator(fp.raw.read)
    # count each \n
    count = sum(buffer.count(b'\n') for buffer in c_generator)
    TOTAL_LINES = count
    #print('Total lines to process:', count - 1)

def delim_changer(dc_file, dc_delim):
    """ Function to change the delmiter if necessary and write to a new file.
        Multi-Character delimiter is an issue with csv reader, and erroneous delimiters causing 
        whacky records would cause pandas to fail to load the file so also converting 
        multi-character to single character delimiter """
    new_delim = '|'
    new_file = dc_file.read().replace(dc_delim, new_delim)
    return new_file, new_delim

# Check encoding when loading the file
ENCODING = 'utf-8'
try:
    file = open(FILENAME, 'r', encoding=ENCODING)
    if len(DELIM) > 1:
        file, DELIM = delim_changer(file, DELIM)
except:
    ENCODING='cp1252'
    file = open(FILENAME, 'r', encoding=ENCODING)
    if len(DELIM) > 1:
        file, DELIM = delim_changer(file, DELIM)


def determine_line_endings(dle_filename, dle_delim):
    """ Function to check the line endings and whether there are quoted fields
        Will be used later in building SQL script to toggle appropriate options
        in bulk insert"""
    with open(dle_filename, 'r', encoding=ENCODING, newline='') as dle_file:
        counter = 0
        quotes = False
        line_endings = set()
        while quotes is False or counter < 10:
            line = dle_file.readline()
            record = line.split(dle_delim)
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

line_terminator, quoted = determine_line_endings(FILENAME, DELIM)

# Read header and prepare some variables to populate
header = pd.read_table(FILENAME, sep=DELIM, engine='python', encoding=ENCODING,
                       quotechar='"', dtype='str', nrows=0)
field_lengths = {re.sub('[^0-9a-zA-Z]+', '_', str(field)): 0 for field in header.columns}
field_names = list(field_lengths.keys())
possible_unicode_fields = {field: 0 for field in field_names}
errors = []
field_count = []
fields_dtypes = {field: {'dtype': [], 'whole_len': [], 'fract_len': [], 'control_char': False,
                          'parens': False, 'dollar': False, 'decimals': False, 'dash': False, 
                          'comma': False, 'sci': False} for field in field_names}
NUMLINES = 0

def get_datatype(value):
    """ Function to check data types """
    data_types = {0:'0BLANK',1:'1DATETIME',2:'2DATE',3:'3INT',4:'4BIG',5:'5FLOAT',6:'6TEXT'}
    if len(value) == 0:
        datatype = 0
    try:
        t = ast.literal_eval(value)
    except:
        if len(value) > 5:
            try:
                if bool(dateparser.parse(value)):
                    if len(value) > 10:
                        datatype = 1
                    else:
                        datatype = 2
            except:
                datatype = 6
        else:
            datatype = 6
    else:
        if isinstance(t, (int, float)):
            if isinstance(t, int):
                if value.startswith('0') and len(value) > 1:
                    datatype = 6
                elif len(value) < 10:
                    datatype = 3
                else:
                    datatype = 4
            elif isinstance(t, float):
                datatype = 5
        else:
            datatype = 6
    return data_types.get(datatype)

# Create a reader object from the file
reader = csv.reader(file, delimiter=DELIM, quotechar='"')
next(reader) # skip header
# Establish a baseline for the correct number of fields in the file by reading the first 100 records
for i, row in enumerate(reader):
    field_count.append(len(row))
    if i == 100:
        break
field_count = median(field_count)

file.seek(0) # go back to start of file
next(reader) # skip header again

MAXRECS = 250_000
STEP = max(round(TOTAL_LINES / MAXRECS),1)

if PERFCHAR:
    print("Including Character Analysis.")
else:
    print("Excluding Character Analysis.")

# Check the file for field count, erroneous quotes, non-ascii, and non-printable characters
for i, row in enumerate(tqdm(reader, total=TOTAL_LINES-1, desc="Processing")):
    if i % STEP == 0:
        if len(row) != field_count:
            errors.append({'Issue': 'Incorrect field count', 'Line': i+1, 'Field Name': 'n/a',
                            'Field Number': str(len(row))+' fields instead of '+str(field_count),
                            'Field Value': 'n/a', 'Character Value': 'n/a', 'Character Name': 'n/a', 
                            'Character Integer Value': 'n/a'})

        # Run though fields and check for irregularities
        for j, field in enumerate(row):
            # Check for erroneous double quotes
            if field.count('"') % 2 != 0:
                errors.append({'Issue': 'Erroneous double-quote', 'Line': i+1,
                            'Field Name': field_names[j], 'Field Number': j+1,
                            'Field Value': field, 'Character Value': '"',
                            'Character Name': 'Double Quote',
                            'Character Integer Value': ord('"')})

            # Check for non-printable or non-ascii
            if PERFCHAR:
                if any(char not in string.ascii_letters+string.digits+string.punctuation+' '
                    for char in field):
                    non_printable_chars = [char for char in field if char not in
                                        string.ascii_letters+string.digits+string.punctuation+' ']
                    for char in non_printable_chars:
                        try:
                            CHARNAME = unicodedata.name(char)
                        except:
                            CHARNAME = 'n/a'
                        CHAR2 = char
                        if char in ('\n','\r'):
                            fields_dtypes[field_names[j]]['control_char'] = True
                            if char == '\n':
                                CHARNAME = 'Line Feed'
                                CHAR2 = 'LF'
                            else:
                                CHARNAME = 'Carriage Return'
                                CHAR2 = 'CR'
                        else:
                            possible_unicode_fields[field_names[j]] = possible_unicode_fields[field_names[j]] + 1
                        errors.append({'Issue': 'Non-ASCII or Control Character', 'Line': i+1,
                                        'Field Name': field_names[j], 'Field Number': j+1, 
                                        'Field Value': field.strip('\r\n'),
                                        'Character Value': CHAR2, 'Character Name': CHARNAME, 
                                        'Character Integer Value': ord(char)})

            # Update the field lengths and establish data types and max values for float fields
            if len(row) == field_count:
                if len(field) > field_lengths[field_names[j]]:
                    field_lengths[field_names[j]] = len(field)
                CLEAN_FIELD = re.sub(r'[(),$]','',str(field)).strip()
                if CLEAN_FIELD == '-':
                    CLEAN_FIELD = '0'
                    fields_dtypes[field_names[j]]['dash'] = True
                DTYPE = get_datatype(CLEAN_FIELD)
                fields_dtypes[field_names[j]]['dtype'].append(DTYPE)
                fields_dtypes[field_names[j]]['dtype'] = [max(fields_dtypes[field_names[j]]['dtype'])]
                # Numbers output from excel can have varyious formatting. Running some checks to account
                # for them and make adjustments in SQL script later on to clean-up/fix data when loading
                if fields_dtypes[field_names[j]]['dtype'][0] in ('5FLOAT', '4BIG', '3INT'):
                    if '(' in field:
                        fields_dtypes[field_names[j]]['parens'] = True
                    if '$' in field:
                        fields_dtypes[field_names[j]]['dollar'] = True
                    if '..' in CLEAN_FIELD:
                        fields_dtypes[field_names[j]]['decimals'] = True
                    if ',' in field:
                        fields_dtypes[field_names[j]]['comma'] = True
                    if 'e' in field.lower():
                        fields_dtypes[field_names[j]]['sci'] = True
                if fields_dtypes[field_names[j]]['dtype'][0] == '5FLOAT' and CLEAN_FIELD != '' and '..' not in CLEAN_FIELD:
                    dec_string = np.format_float_positional(float(CLEAN_FIELD), trim='0')
                    val = dec_string.split('.')
                    fields_dtypes[field_names[j]]['whole_len'].append(len(val[0]))
                    fields_dtypes[field_names[j]]['fract_len'].append(len(val[1]))
                    fields_dtypes[field_names[j]]['whole_len'] = [max(fields_dtypes[field_names[j]]['whole_len'])]
                    fields_dtypes[field_names[j]]['fract_len'] = [max(fields_dtypes[field_names[j]]['fract_len'])]

# Close the file
file.close()

# Iterate through fields and get percentage of each field that has non-ascii characters
# so we can assign NVARCHAR fields for unicode data in SQL script later on
for k, v in possible_unicode_fields.items():
    possible_unicode_fields[k] = (possible_unicode_fields[k] / (TOTAL_LINES if TOTAL_LINES <= MAXRECS else MAXRECS)) * 100

# Convert errors to dataframe, save to csv, then check for issues
if len(errors) > 0:
    df_errors = pd.DataFrame(errors)
    df_errors.to_csv(FILENAME+'_issues.log', index=False, quoting=2, encoding='utf-8')
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
    LENGTH = 0
    if field_lengths[field] > 0:
        LENGTH = field_lengths[field]
    else:
        LENGTH = 1
    if possible_unicode_fields[field] > 1: # If unicode % is greater than 1% use NVARCHAR
        sql_code.append(f'\t[{field}]\tNVARCHAR({LENGTH})')
    else:
        sql_code.append(f'\t[{field}]\tVARCHAR({LENGTH})')
    if i != len(field_names)-1:
        sql_code.append(',\n')
    else:
        sql_code.append('\n);\n\n\n')

if DELIM == '\t':  # Switch tab character back to raw string before writing sql
    DELIM = r'\t'

sql_code.append(
    f"GO\nBULK INSERT #SHELL\nFROM '{os.path.join(os.getcwd(), FILENAME)}' WITH (\n")
sql_code.append("CODEPAGE = '65001',\n")
sql_code.append("FIRSTROW = 2,\n")
sql_code.append(f"FIELDTERMINATOR = '{DELIM}',\n")
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
    LENGTH = 1
    DTYPE = str(fields_dtypes[field]['dtype'][0])
    CR1 = CR2 = DR1 = DR2 = DC1 = DC2 = CO1 = CO2 = ''
    # Account for control characters and various excel number scenarios within the field
    if fields_dtypes[field]['control_char']:
        CR1 = "REPLACE(REPLACE("
        CR2 = ",CHAR(10),''),CHAR(13),'')"
    if fields_dtypes[field]['dollar']:
        DR1 = "REPLACE("
        DR2 = ",'$','')"
    if fields_dtypes[field]['decimals']:
        DC1 = "REPLACE("
        DC2 = ",'..','.')"
    if fields_dtypes[field]['comma']:
        CO1 = "REPLACE("
        CO2 = ",',','')"
    if field_lengths[field] > 0:
        LENGTH = field_lengths[field]
    if DTYPE in ('6TEXT','0BLANK'):
        if possible_unicode_fields[field] > 1:
            sql_code.append(
                f"\tCAST(NULLIF(LTRIM(RTRIM({CR1}[{field}]{CR2})),'') AS NVARCHAR({LENGTH})) AS [{field}]")
        else:
            sql_code.append(
                f"\tCAST(NULLIF(LTRIM(RTRIM({CR1}[{field}]{CR2})),'') AS VARCHAR({LENGTH})) AS [{field}]")
    if DTYPE == '4BIG':
        if fields_dtypes[field]['parens'] or fields_dtypes[field]['dash']:
            sql_code.append(
                f"\tCAST(CASE WHEN RTRIM(LTRIM(REPLACE({CR1}[{field}]{CR2},'$',''))) = '-' THEN 0\n \
            WHEN CHARINDEX('(',[{field}]) > 0 THEN RTRIM(LTRIM(REPLACE(REPLACE({CO1}{DC1}{DR1}{CR1}[{field}]{CR2}{DR2}{DC2}{CO2},'(',''),')',''))) * -1\n \
            ELSE NULLIF(RTRIM(LTRIM({CO1}{DC1}{DR1}{CR1}[{field}]{CR2}{DR2}{DC2}{CO2})),'')\n \
            END AS BIGINT) AS [{field}]")
        else:
            sql_code.append(
                f"\tCAST(NULLIF(RTRIM(LTRIM({CO1}{DC1}{DR1}{CR1}[{field}]{CR2}{DR2}{DC2}{CO2})),'') AS BIGINT) AS [{field}]")
    if DTYPE == '3INT':
        if fields_dtypes[field]['parens'] or fields_dtypes[field]['dash']:
            sql_code.append(
                f"\tCAST(CASE WHEN RTRIM(LTRIM(REPLACE({CR1}[{field}]{CR2},'$',''))) = '-' THEN 0\n \
            WHEN CHARINDEX('(',[{field}]) > 0 THEN RTRIM(LTRIM(REPLACE(REPLACE({CO1}{DC1}{DR1}{CR1}[{field}]{CR2}{DR2}{DC2}{CO2},'(',''),')',''))) * -1\n \
            ELSE NULLIF(RTRIM(LTRIM({CO1}{DC1}{DR1}{CR1}[{field}]{CR2}{DR2}{DC2}{CO2})),'')\n \
            END AS INT) AS [{field}]")
        else:
            sql_code.append(
                f"\tCAST(NULLIF(RTRIM(LTRIM({CO1}{DC1}{DR1}{CR1}[{field}]{CR2}{DR2}{DC2}{CO2})),'') AS INT) AS [{field}]")
    if DTYPE.endswith('DATE'):
        sql_code.append(
            f"\tCAST(NULLIF(LTRIM(RTRIM({CR1}[{field}]{CR2})),'') AS DATE) AS [{field}]")
    if DTYPE.endswith('TIME'):
        sql_code.append(
            f"\tCAST(NULLIF(LTRIM(RTRIM({CR1}[{field}]{CR2})),'') AS DATETIME2) AS [{field}]")
    if DTYPE == '5FLOAT':
        decimal_len = fields_dtypes[field]['whole_len'][0] + fields_dtypes[field]['fract_len'][0] + 1
        fract_len = fields_dtypes[field]["fract_len"][0]
        if fields_dtypes[field]['parens'] or fields_dtypes[field]['dash']:
            sql_code.append(
                f"\tCAST(CASE WHEN RTRIM(LTRIM(REPLACE({CR1}[{field}]{CR2},'$',''))) = '-' THEN 0.0\n \
            WHEN CHARINDEX('(',[{field}]) > 0 THEN CAST(RTRIM(LTRIM(REPLACE(REPLACE({CO1}{DC1}{DR1}{CR1}[{field}]{CR2}{DR2}{DC2}{CO2},'(',''),')',''))) AS FLOAT) * -1.0\n \
            ELSE CAST(NULLIF(RTRIM(LTRIM({CO1}{DC1}{DR1}{CR1}[{field}]{CR2}{DR2}{DC2}{CO2})),'') AS FLOAT)\n \
            END AS DECIMAL({str(decimal_len)},{str(fract_len)})) AS [{field}]")
        elif fields_dtypes[field]['sci']:
            sql_code.append(
                f"\tCASE WHEN [{field}] LIKE '%e%' THEN CAST(CAST(RTRIM(LTRIM({CO1}{DC1}{DR1}{CR1}[{field}]{CR2}{DR2}{DC2}{CO2})) AS FLOAT) AS DECIMAL({str(decimal_len)},{str(fract_len)}))\n \
                ELSE CAST(RTRIM(LTRIM({CO1}{DC1}{DR1}{CR1}[{field}]{CR2}{DR2}{DC2}{CO2})) AS DECIMAL({str(decimal_len)},{str(fract_len)}))\n \
                END AS [{field}]")
        else:
            sql_code.append(
                f"\tCAST(RTRIM(LTRIM({CO1}{DC1}{DR1}{CR1}[{field}]{CR2}{DR2}{DC2}{CO2})) AS DECIMAL({str(decimal_len)},{str(fract_len)})) AS [{field}]")
    if i != len(field_names)-1:
        sql_code.append(',\n')
    else:
        sql_code.append('\nINTO #FORMAT\nFROM #SHELL\n;\n\n')

with open(FILENAME+'_load.sql', 'w', encoding='utf-8') as file:
    for item in sql_code:
        file.write(f'{item}')

# total_time = datetime.now() - start
# print(f"Processed {NUMLINES:,} records")
# print('Execution time: ', str(total_time))
