import sys

# Hack to avoid detecting current file as firebolt module
old_path = sys.path
sys.path = sys.path[1:]
from firebolt.utils.usage_tracker import UsageTracker

# Back to old path for detection to work properly
sys.path = old_path


def {function_name}():
    print(UsageTracker().user_agent)

{function_name}()