import json
import pandas as pd 
from collections import defaultdict
from urllib.parse import urljoin
from pathlib import Path
import warnings

C_NAMES = [
    "igoRequestId",
    "cmoSampleName",
    "sampleName",
    "sampleClass",
    "cmoPatientId",
    "investigatorSampleId",
    "oncotreeCode",
    "tumorOrNormal",
    "tissueLocation",
    "sampleOrigin",
    "preservation",
    "collectionYear",
    "sex",
    "species",
    "tubeId",
    "cfDNA2dBarcode",
    "baitSet",
    "qcReports",
    "barcodeId",
    "barcodeIndex",
    "libraryIgoId",
    "libraryVolume",
    "libraryConcentrationNgul",
    "dnaInputNg",
    "captureConcentrationNm",
    "captureInputNg",
    "captureName"
]


def _clean_json(data, arguments):
    force = arguments.get('--force')
    # Iterating through the json
    # list
    # should do other check for format changes 
    results = [] 
    for i in range(0,len(data["results"])):
        results.append(data['results'][i]['metadata']) 
    df = pd.DataFrame(results)
    # subset to important columns
    # check all columns are present
    if not set(C_NAMES).issubset(df.columns): 
        missing_columns = set(C_NAMES) - set(df.columns)
        present_columns = set(df.columns) - set(C_NAMES)
        if force: 
            # warn of missing 
            warnings.warn('missing columns: {missing_columns}, which are expect in file metadata. Metadata may be malformed, or format has changed in beagle.'.format(missing_columns = missing_columns))
            df = df[present_columns]
        else:
            raise Exception('missing columns: {missing_columns} which are expect in file metadata. Metadata may be malformed, or format has changed in beagle. Use `--force` option if missing columns are acceptable.'.format(missing_columns = missing_columns))
    else: 
        df = df[C_NAMES]
    # normalize columns 
    bf_list = [i for i in df.columns if isinstance(df[i][0],list)]
    cleaned_columns = [df[column].apply(lambda x: x[0] if isinstance(x, list) else x) for column in bf_list]
    df[bf_list] = pd.concat(cleaned_columns, axis=1)
    bf_dict = [i for i in df.columns if isinstance(df[i][0],dict)]
    normalized_columns = [pd.json_normalize(df[column]) for column in bf_dict]
    normalized_df = pd.concat(normalized_columns, axis=1)
    df = df.drop(columns=bf_dict)
    df = pd.concat([df, normalized_df], axis=1)
    df = df.drop_duplicates()
    return df

def _write_output(data, out_data):
    # csv
    out_name = data['results'][0]['metadata']['igoRequestId']
    out_data = out_data.loc[:,~out_data.columns.duplicated()].copy()
    out_data.to_csv('{out_name}.csv'.format(out_name=out_name), index=False)
    # json 
    out_data = out_data.to_dict('records')
    with open('{out_name}.json'.format(out_name=out_name), 'w') as fout:
        json.dump(out_data , fout, indent=4)



def clean_json_comands(results, arguments):
    # get args
    datain=json.loads(results)
    # clean json
    dataout = _clean_json(datain, arguments)
    # write out 
    _write_output(datain, dataout)
    