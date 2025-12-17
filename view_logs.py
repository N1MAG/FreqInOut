
import os
from freqinout.core.logger import _get_log_file

def main():
    path = _get_log_file()
    print(f"Log file: {path}")
    if os.path.exists(path):
        print(open(path, 'r', encoding='utf-8').read())
    else:
        print("No log file yet.")

if __name__ == "__main__":
    main()
