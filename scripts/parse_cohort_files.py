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


def ci_tags_to_primary_ids(samples, file_group):
    """
    Args:
        samples: list of ciTags

    Returns:

    """
    total_number_of_samples = len(samples)
    primary_ids = []
    for idx, ci_tag in enumerate(samples, start=1):
        files = BEAGLE.get_files_by_metadata(f"ciTag:{ci_tag}", file_group)
        if not files:
            print(f"Unable to locate ciTag:{ci_tag}")
            continue
        primary_id = files[0]["metadata"]["primaryId"]
        print(f"Fetching {ci_tag}:{primary_id}. Remaining {total_number_of_samples - idx}...")
        primary_ids.append(primary_id)
    return primary_ids


def parse_cohort_file(input_files, output_file, directory_path=None, file_group="b54d035d-f63c-4ea8-86fb-9dbc976bb7fe"):
    all_sample_ids = []
    
    # Process cohort files
    for input_file in input_files:
        # Parse cohort file
        samples = get_list_of_samples_from_cohort_file(input_file)
        # Convert from ciTags to primaryIds if needed
        # primary_ids = ci_tags_to_primary_ids(samples, file_group)
        all_sample_ids.extend(samples)
    
    with open(output_file, "w") as f:
        for sample in all_sample_ids:
            f.write(f"{sample}\n")
    
    print(f"File {output_file} successfully generated. Number of samples to run: {len(all_sample_ids)}")
    
    # List directories (optional)
    if directory_path:
    
        all_directories = [f for f in os.listdir(directory_path) if os.path.isdir(os.path.join(directory_path, f))]
    
    # Compare outputs
    samples_set = set(all_sample_ids)
    directories_set = set(all_directories)
    
    unique_to_samples = samples_set - directories_set
    unique_to_directories = directories_set - samples_set
    
    print(f"Unique to samples: {unique_to_samples}")
    print(f"Unique to directories: {unique_to_directories}")

    return {
        "unique_to_samples": unique_to_samples,
        "unique_to_directories": unique_to_directories
    }

 
HELP = """USAGE:
python3 parse_cohort_files.py parse <input> [<directory_path>] <output> [<file_group_id>]
    - <input_files> can be a single file, multiple files, or a wildcard (e.g., /path/to/files/*.txt)
python3 parse_cohort_files.py remove <input> [<output>]
python3 parse_cohort_files.py check <input> [<output>]
python3 parse_cohort_files.py list_dir <directory> [<output>]
python3 parse_cohort_files.py compare <file1.txt> <file2.txt> <report_file>

"""

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(HELP)
        exit(1)
    command = sys.argv[1]
    if command == "parse":
        input_files = sys.argv[2:-2]
        output_file = sys.argv[-1]
    # directory is optional
        if len(sys.argv) > 4 and os.path.isdir(sys.argv[-2]):
            directory_path = sys.argv[-2]
            input_files = sys.argv[2:-2]    
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
    