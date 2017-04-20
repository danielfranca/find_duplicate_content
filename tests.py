from core_lib.duplicate_finder import build_structure
from core_lib.duplicate_finder import get_duplicated_content
from core_lib.duplicate_finder import generate_content_hash
from core_lib.duplicate_finder import run_next_action
import unittest
import hashlib


class DuplicateFinderTestCase(unittest.TestCase):

    def test_build_structure(self):
        initial_state = {
            "next_action": 0,
            "hash_structure": {},
            "duplicated_content": [],
            "last_error": None,
            "save_path": ""
        }

        expected_state = {
            "next_action": 0,
            "hash_structure": {},
            "duplicated_content": [],
            "last_error": "No root path provided",
            "save_path": ""
        }

        self.assertEqual(build_structure(initial_state, []), expected_state)

    def test_get_duplicated_content(self):

        hash_structure = {
            "da39a3ee5e6b4b0d3255bfef95601890afd80709": ["file1", "file2"],
            "970093678b182127f60bb51b8af2c94d539eca3a": ["file3"],
            "7c4a8d09ca3762af61e59520943dc26494f8941b": ["file4", "file5", "file6"],
            "9a79be611e0267e1d943da0737c6c51be67865a0": ["file7"]
        }

        initial_state = {
            "next_action": 0,
            "hash_structure": hash_structure,
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

        # self.maxDiff = None
        self.assertEqual(get_duplicated_content(initial_state, []), expected_state)

    def test_get_duplicated_content_empty_hash(self):

        # Empty hash structure
        initial_state = {
            "next_action": 0,
            "hash_structure": {},
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

        self.assertEqual(get_duplicated_content(initial_state, []), expected_state)

    def test_generate_hash_small_file(self):
        class FakeFileReader():
            content = [b"M", b"L", b"I"]
            def read(self, size):
                if len(self.content) > 0:
                    return self.content.pop()
                else:
                    return None

        f = FakeFileReader()
        hash = generate_content_hash(f)
        self.assertEqual(len(hash), 40)
        sha1 = hashlib.sha1()
        sha1.update(b"ILM")
        self.assertEqual(hash, sha1.hexdigest())

    def test_generate_hash_huge_file(self):
        FILE_SIZE = 524288
        class FakeFileReader():
            idx = 0
            def read(self, size):
                if self.idx >= FILE_SIZE:
                    return None
                else:
                    self.idx += 1
                    return b"A"

        f = FakeFileReader()
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
        self.assertEqual(run_next_action(initial_state, []), expected_state)

    def test_run_next_action(self):
        initial_state = {
            "next_action": 0
        }
        expected_state = {
            "next_action": 1
        }

        called = False

        def action(state, actions):
            nonlocal called
            called = True
            return state

        actions = [action]

        self.assertEqual(run_next_action(initial_state, actions), expected_state)
        self.assertTrue(called)

    def test_run_no_more_actions(self):
        initial_state = {
            "next_action": 1
        }
        expected_state = {
            "next_action": 0
        }

        actions = []

        self.assertEqual(run_next_action(initial_state, actions), expected_state)

    def test_save_and_restore_state(self):
        pass

if __name__ == "__main__":
    unittest.main()
