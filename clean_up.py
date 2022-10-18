#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import argparse
import sys 
import pandas as pd 
import csv


def _collect_args(): 
    # Create the parser
    parser = argparse.ArgumentParser()
    # Add an argument group 
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '--input-file', '-i',
        type=argparse.FileType('r'),
        default=sys.stdin,
        help='Input file name containing a valid JSON.')
    group.add_argument(
        'json',
        nargs='?',
        type=str,
        help='Input string containing a valid JSON.')
    # Parse the argument
    args = parser.parse_args()
    data = args.json or args.input_file.read()
    datain=json.loads(data)
    #sys.stdin = open("/dev/tty")
    return datain 

def _read_column_control(): 
    # read in column_names for check 
    with open('column_names.txt', newline='') as f:
        reader = csv.reader(f)
        c_names = [] 
        for name in reader: 
            c_names.append(name[0])
    return c_names 

def _clean_json(data):
    # Iterating through the json
    # list
    # should do other check for format changes 
    results = [] 
    for i in range(0,len(data["results"])):
        results.append(data['results'][i]['metadata']) 
    df = pd.DataFrame(results)
    c_names = _read_column_control() 
    # check all columns are present
    if not set(c_names).issubset(df.columns): 
        ValueError('missing column names expected in file metadata. Format has changed, or JSON is badly formed.')
    # should do check for all columns 
    df = df.drop('R', axis=1)
    df['flowCellLanes'] = [','.join(map(str, l)) for l in df['flowCellLanes']]
    bf_list = [i for i in df.columns if isinstance(df[i][0],list)]
    cleaned_columns = [df[column].apply(lambda x: x[0]) for column in bf_list]
    df[bf_list] = pd.concat(cleaned_columns, axis=1)
    bf_dict = [i for i in df.columns if isinstance(df[i][0],dict)]
    normalized_columns = [pd.json_normalize(df[column]) for column in bf_dict]
    normalized_df = pd.concat(normalized_columns, axis=1)
    df = df.drop(columns=bf_dict)
    df = pd.concat([df, normalized_df], axis=1)
    df = df.drop_duplicates()
    return df

def _write_output(data, out_data):
    out_name = data['results'][0]['metadata']['igoRequestId']
    out_data = out_data.loc[:,~out_data.columns.duplicated()].copy()
    out_data.to_csv('{out_name}.csv'.format(out_name=out_name), index=False)
    test_json_names = ['igoId', 'cmoSampleName', 'sampleName', 'cmoSampleClass', 'cmoPatientId', 'investigatorSampleId', 'oncoTreeCode', 'tumorOrNormal', 'tissueLocation', 'specimenType', 'sampleOrigin', 'preservation', 'collectionYear', 'sex', 'species', 'tubeId', 'cfDNA2dBarcode', 'baitSet', 'qcReports', 'barcodeId', 'barcodeIndex', 'libraryIgoId', 'libraryVolume', 'libraryConcentrationNgul', 'dnaInputNg', 'captureConcentrationNm', 'captureInputNg', 'captureName']
    
    out_data['specimenType'] = None 
    out_data['qcReports'] = [[] for _ in range(len(out_data))]
    out_data = out_data.rename(columns={'oncotreeCode': 'oncoTreeCode', 'igoRequestId': 'igoId', 'sampleClass':'cmoSampleClass'})
    out_data = out_data[test_json_names]
    #sys.stdin = open("/dev/tty")
    out_data = out_data.to_dict('records')
    with open('{out_name}.json'.format(out_name=out_name), 'w') as fout:
        json.dump(out_data , fout, indent=4)
if __name__ == '__main__':
    # get args
    data = _collect_args()
    # clean json 
    out_data = _clean_json(data)
    # write data out
    _write_output(data, out_data)
    