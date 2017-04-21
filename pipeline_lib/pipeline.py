from .utils import run_next_action
from .utils import generate_content_hash
from .utils import pipeline_process

import os


class Pipeline():
    def __init__(self, pipeline_processes):
        self.pipeline_processes = pipeline_processes

    def start(self, state, payload):
        next_action_idx = state.get("next_action", 0)
        return run_next_action(state, payload, self.pipeline_processes[next_action_idx:])


##########################
# Go through all the directory tree building a SHA1 structure
##########################
@pipeline_process(required_parameters=["root_path", "save_path"])
def build_structure(state: dict, payload: dict, actions: list):
    state["hash_structure"] = {}
    root_path = payload.get("root_path")

    for dir, sub_dirs, file_names in os.walk(root_path):
        for file_name in file_names:
            f = open(file_name, "rb")
            if not f:
                raise Exception("File not found: {}".format(file_name))

            sha1 = generate_content_hash(f)
            state["hash_structure"].setdefault(sha1, [])
            state["hash_structure"][sha1].append(file_name)

    return run_next_action(state, payload, actions)


########################################################
# Find duplicated content in a pre-built hash structure
########################################################
@pipeline_process(required_parameters=["hash_structure"])
def get_duplicated_content(state: dict, payload: dict, actions: list):
    hash_structure = payload.get("hash_structure", {})
    state["hash_structure"] = hash_structure
    state["duplicated_content"] = sorted([paths for paths in hash_structure.values() if len(paths) > 1])

    return run_next_action(state, {}, actions)


