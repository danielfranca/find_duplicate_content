from pipeline_lib.pipeline import build_structure
from pipeline_lib.pipeline import get_duplicated_content
from pipeline_lib.utils import restore_state
from pipeline_lib.pipeline import Pipeline
import argparse
import os

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find duplicated content.')
    parser.add_argument('dir', help="The directory to start looking into")
    parser.add_argument("--save-file-path", dest="save_file_path", help="The file path to save the process state")
    parser.add_argument("--continue", dest="continue_process", action="store_true",
                      help="Indicates wether it should continue any given stopped process")

    args = parser.parse_args()

    #The pipeline: Build a hash structure to check the file content
    #And then find the duplicated ones
    actions_pipeline = [build_structure, get_duplicated_content]

    if args.save_file_path:
        os.path.abspath(args.save_file_path)

    dir = os.path.abspath(args.dir)
    state = {
        "next_action": 0,
        "root_path": dir,
        "save_file_path": args.save_file_path
    }

    if args.continue_process and args.save_file_path:
        saved_state = restore_state({
            "save_file_path": args.save_file_path
        })
        if dir in saved_state and "state" in saved_state[dir]:
            state = saved_state[dir]["state"]

    pipeline = Pipeline(actions_pipeline)
    final_state = pipeline.start(state=state)

    if (final_state.get("last_error")):
        print("Error during pipeline process: {}".format(final_state["last_error"]))
    else:
        duplicated_content = final_state.get("duplicated_content", [])
        print("\n\n" + ("*" * 80))
        if len(duplicated_content) > 0:
            print("Repeated items found: ")
            for repeated_group in final_state["duplicated_content"]:
                print("*" * 60)
                print(", ".join(repeated_group))
        else:
            print("No Repeated items found.")

