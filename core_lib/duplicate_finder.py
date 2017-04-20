import os
import copy
import hashlib
import json

####################
# State
# {
#   root_path
#   next_action (0..1)
#   hash_structure
#   duplicated_content
#   last_error
#   save_path
# }
####################
CHUNK_SIZE = 262144 # 256kb

# TODO: Use decorator to register the steps
# TODO: Separate into duplicate_finder_pipeline and utils(decorator/run_next_action/save/restore/generate_hash)


def run_next_action(state: dict, actions: list):
    if len(actions) > 0:
        state["next_action"] += 1
        return actions[0](state, actions[1:])
    else:
        state["next_action"] = 0
        return state


#############################
# Generate a SHA1 hash for a given file content
# Read in chunks to avoid huge memory consuming
#############################
def generate_content_hash(f):

    sha1 = hashlib.sha1()
    while True:
        content = f.read(CHUNK_SIZE)
        if not content:
            break
        sha1.update(content)

    return sha1.hexdigest()


##########################
# Go through all the directory tree building a SHA1 structure
##########################
def build_structure(state: dict, actions: list):
    # clone the state, if something is broken return the original state
    try:
        new_state = copy.deepcopy(state)
        new_state["hash_structure"] = {}

        root_path = new_state.get("root_path")
        if not root_path:
            raise Exception("No root path provided")

        for dir, sub_dirs, file_names in os.walk(root_path):
            for file_name in file_names:
                with open(file_name, "rb") as f:
                    if not f:
                        raise Exception("File not found: {}".format(file_name))

                    sha1 = generate_content_hash(open(file_name))
                    new_state.hash_structure.setdefault(sha1, [])
                    new_state.hash_structure[sha1].append(file_name)

        return run_next_action(new_state, actions)
    except Exception as ex:
        state["last_error"] = str(ex)
        return state


########################################################
# Find duplicated content in a pre-built hash structure
########################################################
def get_duplicated_content(state: dict, actions: list):
    try:
        new_state = copy.deepcopy(state)
        hash_structure = state.get("hash_structure", {})

        new_state["duplicated_content"] = sorted([paths for paths in hash_structure.values() if len(paths) > 1])

        return run_next_action(new_state, actions)
    except Exception as ex:
        state["last_error"] = str(ex)
        return state


#######################################
# Save the app state into a json file
#######################################
def save_state(state: dict):
    try:
        if not state.save_path:
            raise "No save path defined"

        saved_state = restore_state(state)

        if saved_state:
            new_state = copy.deepcopy(saved_state)
            new_state[state.root_path] = {
                "next_action": state.next_action,
                "hash_structure": state.hash_structure,
                "duplicated_content": state.duplicated_content,
                "last_error": state.last_error,
                "save_path": state.save_path
            }

            with open(state.save_path, "w") as f:
                json.dump(new_state, f, indent=4)
                return True, None

    except Exception as ex:
        return False, ex


#####################################
# Restore the app state
#####################################
def restore_state(state):
    saved_state = {}
    with open(state.save_path, "r") as f:
        saved_state = json.load(f)

    return saved_state

