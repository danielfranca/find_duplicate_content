from pipeline_lib.pipeline import build_structure
from pipeline_lib.pipeline import get_duplicated_content
from pipeline_lib.utils import restore_state
from pipeline_lib.pipeline import Pipeline
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find duplicated content.')
    parser.add_argument('dir', nargs="+", help="The directory to start looking into")
    parser.add_argument("--save-path", dest="save_path", help="The path to save the process state")
    parser.add_argument("--continue", dest="continue_process", action="store_true",
                      help="Indicates wether it should continue any given stopped process")

    args = parser.parse_args()

    actions_pipeline = [build_structure, get_duplicated_content]


    # It must get values from command line
    # duplicate_finder --continue --save-path <root_path>
    state = {
        "next_action": 0
    }

    if not args.save_path:
        print("Missing save path")
        exit(-1)

    if args.continue_process:
        state = restore_state({
            "save_path": args.save_path
        })

    payload = {
        "root_path": args.dir,
        "save_path": args.save_path
    }

    pipeline = Pipeline(actions_pipeline)
    final_state = pipeline.start(state=state, payload=payload)

    if (final_state["last_error"]):
        print("Error during pipeline process: {}".format(final_state["last_error"]))
    else:
        print("Repeated items found: ")
        print(", ".join(repeated) for repeated in final_state["duplicated_content"])
