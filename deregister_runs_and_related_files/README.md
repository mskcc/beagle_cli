This will retrieve runs and files associated with a `request_id` and then builds two strings that can be used to delete jobs through `beaglecli`

Prerequisite:

    The following env vars must be set:
        - DEREGISTER_USER
        - DEREGISTER_PW
        - BEAGLE_ENDPOINT

DEREGISTER_USER and DEREGISTER_PW are Beagle credentials allowed to delete.

Usage:

```
python3 deregister_jobs_and_files.py <request_id>
```


