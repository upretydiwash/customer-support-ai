import json
import sys

def main():
    if len(sys.argv) != 2:
        print("Usage: spark_streaming.py <config_file>")
        sys.exit(1)
    config_file = sys.argv[1]
    with open(config_file) as f:
        config = json.load(f)
        


if __name__ == "__main__":
    main()