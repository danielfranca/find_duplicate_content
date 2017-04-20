from pipeline_lib.pipeline import build_structure
from pipeline_lib.pipeline import get_duplicated_content
from pipeline_lib.pipeline import generate_content_hash
from pipeline_lib.pipeline import run_next_action
from pipeline_lib.utils import save_state
import unittest
from unittest import mock
import hashlib
import json


class DuplicateFinderTestCase(unittest.TestCase):

    def test_build_structure_missing_parameter(self):
        initial_state = {
            "last_error": None
        }

        expected_state = {
            "last_error": "Missing required parameter: root_path"
        }

        self.assertEqual(build_structure(initial_state, {}, []), expected_state)

    def test_build_structure(self):
        with mock.patch('os.walk') as os_walk:
            os_walk.return_value = [
                ('/dir1', ('subdir1',), ('filename1','filename2')),
                ('/dir2', (), ('singlefile',)),
                ('/dir3', ('subdir2', 'subdir3'), ('filename1equal',)),
            ]
            initial_state = {
                "next_action": 0,
                "last_error": None
            }

            expected_state = {
                "next_action": 0,
                "hash_structure": {
                    "92af19e633fe03482882098e4ba7619afe09c547": ["filename2"],
                    "f3390fe2e5546dac3d1968970df1a222a3a39c00": ["filename1", "filename1equal"],
                    "fee3dd4822f64d1a12c9b6b945fc1c07ecf4981c": ["singlefile"]
                },
                "last_error": None
            }

            payload = {
                "root_path": "."
            }

            class MockFileReader():
                once = True

                def __init__(self, filename):
                    self.filename = filename

                def read(self, size):
                    if self.once:
                        self.once = False
                        if self.filename == 'filename1' or self.filename == 'filename1equal':
                            return b"FOOBAR"
                        elif self.filename == 'filename2':
                            return b"FILENAME2"
                        elif self.filename == 'singlefile':
                            return b"SINGLEFILE"
                    return None

            def mock_open(filename, mode='r'):
                return MockFileReader(filename)

            self.maxDiff = None
            with mock.patch('pipeline_lib.pipeline.open', mock_open):
                self.assertEqual(build_structure(initial_state, payload, []), expected_state)

    def test_get_duplicated_content(self):
        hash_structure = {
            "da39a3ee5e6b4b0d3255bfef95601890afd80709": ["file1", "file2"],
            "970093678b182127f60bb51b8af2c94d539eca3a": ["file3"],
            "7c4a8d09ca3762af61e59520943dc26494f8941b": ["file4", "file5", "file6"],
            "9a79be611e0267e1d943da0737c6c51be67865a0": ["file7"]
        }

        initial_state = {
            "next_action": 0,
            "duplicated_content": [],
            "last_error": None,
            "save_path": ""
        }

        expected_state = {
            "next_action": 0,
            "hash_structure": hash_structure,
            "duplicated_content": [["file1", "file2"], ["file4", "file5", "file6"]],
            "last_error": None,
            "save_path": ""
        }

        self.maxDiff = None
        payload = {
            "hash_structure": hash_structure
        }
        self.assertEqual(get_duplicated_content(initial_state, payload, []), expected_state)

    def test_get_duplicated_content_empty_hash(self):

        # Empty hash structure
        initial_state = {
            "next_action": 0,
            "duplicated_content": [],
            "last_error": None,
            "save_path": ""
        }

        expected_state = {
            "next_action": 0,
            "hash_structure": {},
            "duplicated_content": [],
            "last_error": None,
            "save_path": ""
        }

        payload = {
            "hash_structure": {}
        }

        self.assertEqual(get_duplicated_content(initial_state, payload, []), expected_state)

    def test_generate_hash_small_file(self):
        class MockFileReader():
            content = b"ILM"
            once = True
            def read(self, size):
                if self.once:
                    self.once = False
                    return self.content
                else:
                    return None

        f = MockFileReader()
        hash = generate_content_hash(f)
        self.assertEqual(len(hash), 40)
        sha1 = hashlib.sha1()
        sha1.update(b"ILM")
        self.assertEqual(hash, sha1.hexdigest())

    def test_generate_hash_huge_file(self):
        FILE_SIZE = 524288
        class MockFileReader():
            idx = 0
            def read(self, size):
                if self.idx >= FILE_SIZE:
                    return None
                else:
                    self.idx += 1
                    return b"A"

        f = MockFileReader()
        hash = generate_content_hash(f)
        self.assertEqual(len(hash), 40)
        sha1 = hashlib.sha1()
        sha1.update(b"A" * FILE_SIZE)
        self.assertEqual(hash, sha1.hexdigest())

    def test_run_no_action(self):
        initial_state = {
            "next_action": 0
        }
        expected_state = {
            "next_action": 0
        }
        self.assertEqual(run_next_action(initial_state, {},  []), expected_state)

    def test_run_next_action(self):
        initial_state = {
            "next_action": 0
        }
        expected_state = {
            "next_action": 1
        }

        called = False

        def action(state, payload, actions):
            nonlocal called
            called = True
            return state

        actions = [action]

        self.assertEqual(run_next_action(initial_state, {}, actions), expected_state)
        self.assertTrue(called)

    def test_run_no_more_actions(self):
        initial_state = {
            "next_action": 1
        }
        expected_state = {
            "next_action": 0
        }

        actions = []

        self.assertEqual(run_next_action(initial_state, {}, actions), expected_state)

    def test_save_and_restore_new_state(self):
        hash_structure = {
            "da39a3ee5e6b4b0d3255bfef95601890afd80709": ["file1", "file2"],
            "970093678b182127f60bb51b8af2c94d539eca3a": ["file3"],
            "7c4a8d09ca3762af61e59520943dc26494f8941b": ["file4", "file5", "file6"],
            "9a79be611e0267e1d943da0737c6c51be67865a0": ["file7"]
        }

        state = {
            "next_action": 0,
            "root_path": "~/dir",
            "save_path": "",
            "hash_structure": hash_structure,
            "duplicated_content": [["file1", "file2"], ["file4", "file5", "file6"]],
            "last_error": None
        }

        payload = {
            "param1": "val1"
        }

        saved_data = None
        self.maxDiff = None

        def mock_json_dump(obj, fp, indent=4):
            nonlocal saved_data
            saved_data = json.dumps(obj, indent=indent, sort_keys=True)

        with mock.patch('json.dump', new=mock_json_dump):
            with mock.patch('pipeline_lib.utils.restore_state', return_value={}):
                self.assertFalse(save_state(state, payload))
                state["save_path"] = "./saved_state.json"
                self.assertTrue(save_state(state, payload))
                self.assertEqual(saved_data, json.dumps({
                    state["root_path"]: {
                        "state": {
                            "next_action": state["next_action"],
                            "hash_structure": state["hash_structure"],
                            "duplicated_content": state["duplicated_content"],
                            "save_path": state["save_path"],
                            "last_error": None,
                        },
                        "payload": payload
                    }
                }, indent=4, sort_keys=True))

    def test_save_and_restore_extra_state(self):
        hash_structure = {
            "da39a3ee5e6b4b0d3255bfef95601890afd80709": ["file1", "file2"],
            "970093678b182127f60bb51b8af2c94d539eca3a": ["file3"],
            "7c4a8d09ca3762af61e59520943dc26494f8941b": ["file4", "file5", "file6"],
            "9a79be611e0267e1d943da0737c6c51be67865a0": ["file7"]
        }

        state = {
            "next_action": 0,
            "root_path": "~/dir",
            "save_path": "",
            "hash_structure": hash_structure,
            "duplicated_content": [["file1", "file2"], ["file4", "file5", "file6"]],
            "last_error": None
        }

        payload = {
            "param1": "val1"
        }

        saved_data = None
        self.maxDiff = None

        def mock_json_dump(obj, fp, indent=4):
            nonlocal saved_data
            saved_data = json.dumps(obj, indent=indent, sort_keys=True)

        restored_state = {
            "/old_dir": {
                "state": {
                    "next_action": 1,
                    "root_path": "/old_dir",
                    "save_path": "./",
                    "hash_structure": hash_structure,
                    "last_error": "Something is broken"
                }
            }
        }

        with mock.patch('json.dump', new=mock_json_dump):
            with mock.patch('pipeline_lib.utils.restore_state', return_value=restored_state):
                self.assertFalse(save_state(state, payload))
                state["save_path"] = "./saved_state.json"
                self.assertTrue(save_state(state, payload))

                restored_state.update({
                    state["root_path"]: {
                        "state": {
                            "next_action": state["next_action"],
                            "hash_structure": state["hash_structure"],
                            "duplicated_content": state["duplicated_content"],
                            "last_error": None,
                            "save_path": state["save_path"]
                        },
                        "payload": payload
                    }
                })

                self.assertEqual(saved_data, json.dumps(restored_state, indent=4, sort_keys=True))


if __name__ == "__main__":
    unittest.main()
