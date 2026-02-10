import re
import sys
import os
import bin.access_beagle_endpoint as beagle_api

BEAGLE = beagle_api.AccessBeagleEndpoint()


def create_check_script(file_path,
                        output_name="tempo_check_script.sh",
                        output_root_path="/juno/work/tempo/wes_repo/Results/v2.0.x/bams"):
    """
    Args:
        file_path: path to file with ciTags of samples we need to check
        output_name: name of the generated bash script
        output_root_path: path to output directory

    Returns:
        bash script that checks are the bam files in expected location
    """
    samples = get_list_of_samples_from_cohort_file(file_path)

    with open(output_name, "w") as f:
        f.write("#!/bin/bash\n")
        f.write("non_existent_files_count=0\n")

        for s in samples:
            ci_tag = s.rstrip()
            f.write(f'if ! test -f {output_root_path}/{ci_tag}/{ci_tag}.bam; then echo \'{ci_tag}\'; ((non_existent_files_count++)); fi\n')

        f.write('echo "Total missing samples $non_existent_files_count"')


def create_remove_script(file_path,
                         output_name="tempo_remove_script.sh",
                         output_root_path="/juno/work/tempo/wes_repo/Results/v2.0.x/bams"):
    samples = get_list_of_samples_from_cohort_file(file_path)
    with open(output_name, "w") as f:
        f.write("#!/bin/bash\n")
        for s in samples:
            ci_tag = s.rstrip()
            f.write(f'rm -rf {output_root_path}/{ci_tag}\n')


def get_list_of_samples_from_cohort_file(file_path):
    """
    Args:
        file_path: path to cohort file

    Returns:
        List of ciTags from cohort file. List doesn't contain duplicates
    """
    samples = set()
    with open(file_path, 'r') as f:
        lines = f.readlines()
    # Clean up rows with # sign
    filtered_list = [s.rstrip() for s in lines if not s.startswith('#')]
    for line in filtered_list:
        cleaned_content = re.sub(r'\s+', '\t', line)
        t, n = cleaned_content.split("\t")
        samples.add(t)
        samples.add(n)
    return list(samples)


def cmo_sample_name_to_primary_ids(samples, file_group):
    """
    Args:
        samples: list of cmoSampleName

    Returns:

    """
    total_number_of_samples = len(samples)
    primary_ids = []
    for idx, cmo_sample_name in enumerate(samples, start=1):
        files = BEAGLE.get_files_by_metadata(f"cmoSampleName:{cmo_sample_name}", file_group)
        if not files:
            print(f"Unable to locate cmoSampleName:{cmo_sample_name}")
            continue
        primary_id = files[0]["metadata"]["primaryId"]
        print(f"Fetching {cmo_sample_name}:{primary_id}. Remaining {total_number_of_samples - idx}...")
        primary_ids.append(primary_id)
    return primary_ids


def parse_cohort_file(input_files, directory_path, output_file, file_group="b54d035d-f63c-4ea8-86fb-9dbc976bb7fe"):
    all_sample_ids = []
    
    # Process cohort files
    for input_file in input_files:
        samples = get_list_of_samples_from_cohort_file(input_file)
        all_sample_ids.extend(samples)

    # Write all parsed sample names to output file
    with open(output_file, "w") as f:
        for sample in all_sample_ids:
            f.write(f"{sample}\n")
 
    print(f"File {output_file} successfully generated. Number of samples to run: {len(all_sample_ids)}")
    
    # List directories
    all_directories = [f for f in os.listdir(directory_path) if os.path.isdir(os.path.join(directory_path, f))]
    
    # Compare outputs
    samples_set = set(all_sample_ids)
    directories_set = set(all_directories)
    
    unique_to_samples = samples_set - directories_set
    #print(f"Unique to samples: {unique_to_samples}")

    
    # Convert missing sample names to primaryIds
    primary_ids = cmo_sample_name_to_primary_ids(
        list(unique_to_samples),
        file_group
    )

    # Append missing primaryIds to the same output file
    with open(output_file, "a") as f:
        f.write("\n# Missing primaryIds\n")
        for primary_id in primary_ids:
            f.write(f"{primary_id}\n")

    print(f"Appended {len(primary_ids)} missing primaryIds to {output_file}")

    # Return both sets for downstream use if needed
    return {
        "unique_to_samples": unique_to_samples,
        "primary_ids": primary_ids,
    }

 
HELP = """USAGE:
python3 parse_cohort_files.py parse <input> <directory_path> <output> [<file_group_id>]
    - <input_files> can be a single file, multiple files, or a wildcard (e.g., /path/to/files/*.txt)
python3 parse_cohort_files.py remove <input> [<output>]
python3 parse_cohort_files.py check <input> [<output>]
python3 parse_cohort_files.py list_dir <directory> [<output>]
python3 parse_cohort_files.py compare <file1.txt> <file2.txt> <report_file>

"""

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print(HELP)
        exit(1)
    command = sys.argv[1]
    if command == "parse":
        input_files = sys.argv[2:-2] 
        directory_path = sys.argv[-2]
        output_file = sys.argv[-1]    
        parse_cohort_file(input_files, directory_path, output_file)
    elif command == "remove":
        input_file = sys.argv[2]
        if len(sys.argv) > 2:
            output_file = sys.argv[3]
            create_remove_script(input_file, output_file)
        else:
            create_remove_script(input_file)
    elif command == "check":
        input_file = sys.argv[2]
        if len(sys.argv) > 2:
            output_file = sys.argv[3]
            create_check_script(input_file, output_file)
        else:
            create_check_script(input_file)   
    else:
        print(HELP)
    