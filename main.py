from core_lib.duplicate_finder import build_structure, get_duplicated_content, save_state, restore_state

if __name__ == "__main__":
    actions_pipeline = [build_structure, get_duplicated_content]

    # It must get values from command line
    state = {
        "root_path": ""
    }

    final_state = actions_pipeline[0](state, actions_pipeline[1:])
    if (final_state["last_error"]):
        print("Error during pipeline process: {}".format(final_state["last_error"]))
    else:
        save_path = final_state.get("save_path", "[Non specified]")
        if save_state(final_state):
            print("State saved at: {}".format(save_path))
        else:
            print("Not possible to save state at: {}".format(save_path))
