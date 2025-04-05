from etl.pipeline import Pipeline
from utils.logging_config import configure_logging

def main():
    configure_logging()
    Pipeline().run()

if __name__ == "__main__":
    main()