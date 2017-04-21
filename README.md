# find_duplicate_content
Find duplicate content in a directory tree

# Install

  - It requires Python3.5
  - Go to the **find_duplicate_content** and run `python find_duplicated_content.py --help` to see all the options

~~~~
usage: find_duplicated_content.py [-h] [--save-file-path SAVE_FILE_PATH]
                                  [--continue]
                                  dir

Find duplicated content.

positional arguments:
  dir                   The directory to start looking into

optional arguments:
  -h, --help            show this help message and exit
  --save-file-path SAVE_FILE_PATH
                        The file path to save the process state
  --continue            Indicates wether it should continue any given stopped process
~~~~

The process uses a pipeline to retrieve the files and generates SHA1 hashes, so it can do a content lookup in O(1) and the following pipeline process.
The pipeline operations are atomic, and you should be able to rollback to the previous state in case of failure.
If for some reason the process fails you can easily continue the processing from the last state calling with the parameter `--continue`

To make it easier the pipeline functions need to be marked with the `@pipeline_process` decorator
Then the Pipeline class takes care of start the processing.