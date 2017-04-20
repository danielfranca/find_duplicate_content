from pipeline_lib.pipeline import build_structure
from pipeline_lib.pipeline import get_duplicated_content
from pipeline_lib.pipeline import Pipeline

if __name__ == "__main__":
    actions_pipeline = [build_structure, get_duplicated_content]

    # It must get values from command line
    # duplicate_finder --continue --save-path <root_path>
    state = {}

    payload = {
        "root_path": ""
    }

    pipeline = Pipeline(actions_pipeline)
    final_state = Pipeline.start(state=state, payload=payload)

    if (final_state["last_error"]):
        print("Error during pipeline process: {}".format(final_state["last_error"]))
    else:
        print("Repeated items found: ")
        print(", ".join(repeated) for repeated in final_state["duplicated_content"])
