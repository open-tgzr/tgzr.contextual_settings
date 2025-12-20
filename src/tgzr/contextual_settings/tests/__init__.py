from .test_context import test_context
from .test_get_context import test_get_context
from .test_update_context import test_update_context


def do_tests():
    test_context()
    test_get_context()
    test_update_context()
    print("✨ All tests passed ✨")
