import configparser
import shutil
import logging
import time
from sqlalchemy import create_engine
from parser.xml import *
from parser.functions import create_url
from parser.server import FFIDatabase

# logging
logging.basicConfig(filename='C:/Users/Corey/OneDrive/OneDrive - New Mexico Highlands University/Python/FFI/Export-ETL_NEW/log/data.log',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M',
                    level=logging.INFO)


def main():
    # Make sure you have a data/ directory in the main directory for this project before running.
    data_path = 'data'
    base_path = os.getcwd()
    path = os.path.join(base_path, data_path)

    # users need to create their own local config file (see README)
    config = configparser.ConfigParser()
    config.read('config.ini')

    # create database connection
    sql_config = config['FORESTSERV_Staging']
    sql_url = create_url(**sql_config)
    sql_engine = create_engine(sql_url)
    server = FFIDatabase(sql_engine)
    server.insert_codes()
    logging.info(f"Connected to {sql_config['server']} : {sql_config['database']}")

    if not os.path.isdir(processed := os.path.join(path, 'processed')):
        os.mkdir(processed)

    xml_files = [f for f in os.scandir(path)
                 if f.is_file()
                 and '.xml' in f.path]

    for export in xml_files:

        file = export.path
        print(f"Processing {export.name}")
        logging.info(f"Reading in {export.name}")

        s1 = time.perf_counter()
        ffi_data = FFIFile(export)
        e1 = time.perf_counter()
        print(f'Init took {e1-s1} seconds.')

        s2 = time.perf_counter()
        ffi_data.extract()
        e2 = time.perf_counter()
        print(f'Extract took {e2 - s2} seconds.')

        s3 = time.perf_counter()
        ffi_data.transform()
        e3 = time.perf_counter()
        print(f'Transform took {e3 - s3} seconds.')

        s4 = time.perf_counter()
        ffi_data.load(server)
        e4 = time.perf_counter()
        print(f'Load took {e4 - s4} seconds.')

        if len(ffi_data.insert_failed) == 0:
            shutil.move(file, os.path.join(processed, export.name))
            logging.info(f"Moved {export.name} to processed folder")
        else:
            logging.warning(f"{export.name} failed to fully upload. Issues with the following tables: "
                            f"{','.join(ffi_data.insert_failed)}. Review log.")


if __name__ == "__main__":
    main()
