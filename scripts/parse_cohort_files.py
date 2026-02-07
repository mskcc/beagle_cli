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


def parse_cohort_file(input_files, output_file, file_group="b54d035d-f63c-4ea8-86fb-9dbc976bb7fe"):
    all_sample_ids = []
    for input_file in input_files:
        # Parse cohort file
        samples = get_list_of_samples_from_cohort_file(input_file)
        # Convert from ciTags to primaryIds
        #primary_ids = ci_tags_to_primary_ids(samples, file_group)
        all_sample_ids.extend(samples)
    with open(output_file, "w") as f:
        for sample in all_sample_ids:
            f.write(f"{sample}\n")
    print(f"File {output_file} successfully generated. Number of samples to run {len(all_sample_ids)}")


def list_directories(directories, output_file):
        all_directories = [f for f in os.listdir(directories) if os.path.isdir(os.path.join(directories, f))]

        with open(output_file, "w") as f:
            for directory in all_directories:
                f.write(f"{directory}\n")
        print(f"File {output_file} successfully generated. Number of directories in BAM folder {len(all_directories)}")

def compare_files(file_1, file_2, report_file):
    try:
        # Read parsed output in read mode
        with open(file_1, 'r') as f:  
            output1 = {line.strip() for line in f if line.strip()}
        
        # Read directory listing output in read mode
        with open(file_2, 'r') as f:  
            output2 = {line.strip() for line in f if line.strip()}
        
        # Compare
        unique_to_file1 = output1 - output2
        unique_to_file2 = output2 - output1
        
        # Debugging prints to check the differences
        print(f"Unique to {file_1}: {unique_to_file1}")
        print(f"Unique to {file_2}: {unique_to_file2}")
        
        # Write results to file in write mode
        with open(report_file, "w") as f:  # 'w' means write mode
            f.write(f"Elements only in {file_1}:\n")
            f.write("\n".join(sorted(unique_to_file1)) + "\n\n")
            
            f.write(f"Elements only in {file_2}:\n")
            f.write("\n".join(sorted(unique_to_file2)) + "\n")
        
        print(f"Comparison complete. Results written to {report_file}")
    
    except FileNotFoundError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


 
HELP = """USAGE:
python3 parse_cohort_files.py parse <input> <output> [<file_group_id>]
python3 parse_cohort_files.py remove <input> [<output>]
python3 parse_cohort_files.py check <input> [<output>]
python3 parse_cohort_files.py list_dir <directory> [<output>]
python3 parse_cohort_files.py compare <file1.txt> <file2.txt> <report_file>

"""

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(HELP)
        exit(1)
    command = sys.argv[1]
    if command == "parse":
        input_files = sys.argv[2:-1]  # all input files
        output_file = sys.argv[-1]    # last argument
        parse_cohort_file(input_files, output_file)
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
    elif command == "list_dir":
        directories = sys.argv[2]
        if len(sys.argv) > 3:
            output_file = sys.argv[3]
            list_directories(directories, output_file)
        else:
            list_directories(directories)
    elif command == "compare":
        if len(sys.argv) < 5:  # At least two files and one report file
            print("Usage: python script.py compare file1 file2 report_file")
            sys.exit(1)
        file_1 = sys.argv[2]
        file_2 = sys.argv[3]
        report_file = sys.argv[4]
        compare_files(file_1, file_2, report_file)
    else:
        print(HELP)
        