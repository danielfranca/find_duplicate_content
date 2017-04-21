import hashlib
import copy
import json
from datetime import datetime

CHUNK_SIZE = 262144 # 256kb


#################################################
# Run the next action in the pipeline
# and update the action index
#################################################
def run_next_action(state: dict, payload: dict, actions: list):
    now = datetime.now().isoformat()
    print("\n{} - state: {}".format(now, json.dumps(state, indent=4)))
    print("{} - payload: {}".format(now, json.dumps(payload, indent=4)))

    save_state(state, payload)

    if len(actions) > 0:
        state["next_action"] += 1
        return actions[0](state, payload, actions[1:])
    else:
        state["next_action"] = 0
        return state


###########################################
# Decorator to run a pipeline process
###########################################
def pipeline_process(required_parameters=[]):
    def decorator(fn):
        def wrapper(*args, **kwargs):
            state, payload, actions = args
            try:
                validate_required_parameters(required_parameters, payload)
                new_state = copy.deepcopy(state)
                return fn(state=new_state, payload=payload, actions=actions)
            except Exception as ex:
                state["last_error"] = str(ex)
                return state
        return wrapper

    return decorator

#####################################################
# Check if the parameters are correclty being passed
# to the pipeline process
#####################################################
def validate_required_parameters(parameters: list, payload: dict):
    for parameter in parameters:
        if parameter not in payload:
            raise Exception("Missing required parameter: {}".format(parameter))


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


#####################################
# Restore the app state
#####################################
def restore_state(state: dict):
    saved_state = {}
    save_path = state.get("save_path")

    if save_path:
        try:
            with open(save_path, "r") as f:
                saved_state = json.load(f)
        except:
            return saved_state

    return saved_state


#######################################
# Save the app state into a json file
#######################################
def save_state(state: dict, payload: dict):
    save_path = state.get("save_path")
    if not save_path:
        return False

    saved_state = restore_state(state)
    new_state = copy.deepcopy(saved_state) if saved_state else {}

    new_state[state["root_path"]] = {
        "state": {
            "next_action": state["next_action"],
            "hash_structure": state["hash_structure"],
            "duplicated_content": state["duplicated_content"],
            "last_error": state["last_error"],
            "save_path": state["save_path"]
        },
        "payload": payload
    }

    with open(state["save_path"], "w") as f:
        json.dump(new_state, f, indent=4)
        return True

    return False


