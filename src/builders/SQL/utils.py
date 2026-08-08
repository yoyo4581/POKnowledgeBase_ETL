from functools import wraps
import pyodbc
from utils.colored_text import *

def sql_safe(default=None, label=None):
    def decorator(fn):
        @wraps(fn)
        def wrapper(self, *args, **kwargs):
            try:
                return fn(self, *args, **kwargs)
            except pyodbc.Error as e:
                self.conn.rollback()
                print(f"{RED}Error in {label or fn.__name__} (args={args}, kwargs={kwargs}): {e}{RESET}")
                return default
        return wrapper
    return decorator