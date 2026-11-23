import re
import sys
import bin.access_beagle_endpoint as beagle_api

BEAGLE = beagle_api.AccessBeagleEndpoint()


def format_sample_name(sample_name, specimen_type, ignore_sample_formatting=False):
    """
    Formats a given sample_name to legacy ROSLIN naming conventions, provided that
    it is in valid CMO Sample Name format (see sample_pattern regex value, below)

    Current format is to prepend sample name with "s_" and convert all hyphens to
    underscores

    If it does not meet sample_pattern requirements OR is not a specimen_type=="CellLine",
    return 'sampleMalFormed'

    ignore_sample_formatting is applied if we want to return a sample name regardless of
    formatting
    """
    sample_pattern = re.compile(r"^[^0-9].*$")

    if not ignore_sample_formatting:
        try:
            if "s_" in sample_name[:2]:
                return sample_name
            elif (
                bool(sample_pattern.match(sample_name)) or "cellline" in specimen_type.lower()
            ):  # cmoSampleName is formatted properly
                sample_name = "s_" + sample_name.replace("-", "_")
                return sample_name
            return sample_name
        except TypeError:
            return "sampleNameMalformed"
    else:
        return sample_name


def get_files(primary_id, file_group="<add_file_group_here>"):
    """
    Retrieves all Beagle files for a given primaryId within a file group.

    primary_id: the primaryId metadata value to search for
    file_group: UUID of the file group to search

    Returns the list of matching file records.
    """
    return  BEAGLE.get_files_by_metadata(f"primaryId:{primary_id}", file_group)


def patch_files_cmo_sample_name(primary_id, old_sample_name, new_sample_name):
    """
    Updates cmoSampleName (and the derived ciTag) on files of a primaryId.

    Only files whose current cmoSampleName equals old_sample_name are patched;
    files that no longer match are reported as already updated and left alone.
    The new ciTag is built with format_sample_name().
    """
    files = get_files(primary_id)

    for f in files:
        if f["metadata"]["cmoSampleName"] == old_sample_name:
            print(f"cmoSampleName outdated, updating to {new_sample_name}")
            ci_tag = format_sample_name(new_sample_name, f["metadata"]["sampleClass"])
            print(f"Updating {f['id']} {primary_id} with cmoSampleName:{new_sample_name} and ciTag:{ci_tag}")
            new_metadata = {
                "metadata": {
                    "cmoSampleName": new_sample_name,
                    "ciTag": ci_tag
                }
            }
            print(BEAGLE.patch_file_metadata(f['id'], new_metadata))
        else:
            print(f"cmoSampleName already updated {new_sample_name}")

def patch_cmo_sample_name_and_patient_id(primary_id, new_sample_name, new_patient_id):
    """
    Updates cmoSampleName, cmoPatientId and ciTag on all files of a primaryId.

    Unlike patch_files_cmo_sample_name(), every file found for the primaryId is
    patched unconditionally. The new ciTag is built with format_sample_name().
    """
    files = get_files(primary_id)
    for f in files:
        print(f"{primary_id} cmoSampleName {f['metadata']['cmoSampleName']} and cmoPatientId {f['metadata']['cmoPatientId']} outdated, updating to {new_sample_name}")
        ci_tag = format_sample_name(new_sample_name, f["metadata"]["sampleClass"])
        print(f"Updating {f['id']} {primary_id} with cmoSampleName:{new_sample_name} ciTag:{ci_tag} and cmoPatientId:{new_patient_id}")
        new_metadata = {
            "metadata": {
                "cmoSampleName": new_sample_name,
                "cmoPatientId": new_patient_id,
                "ciTag": ci_tag
            }
        }
        print(BEAGLE.patch_file_metadata(f['id'], new_metadata))


if __name__ == "__main__":
    """
    Change the update method based on the update_file_path file format
    """
    update_file_path = sys.argv[1]
    with open(update_file_path, "r") as f:
        update = f.readlines()
    for sample in update:
        primary_id, new_label, new_patient_id = sample.split("\t")
        new_patient_id = new_patient_id.rstrip()
        patch_cmo_sample_name_and_patient_id(primary_id, new_label, new_patient_id)
