import configparser
import shutil
import logging
from sqlalchemy import create_engine
from parser.xml import *
from parser.functions import create_url
from parser.server import FFIDatabase

# logging
logging.basicConfig(filename='C:/Users/Corey/OneDrive/OneDrive - New Mexico Highlands University/Python/FFI/XMLToCustom/log/data.log',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M')


def main():
    # Make sure you have a data/ directory in the main directory for this project before running.
    data_path = 'data'
    base_path = os.getcwd()
    path = os.path.join(base_path, data_path)

    # users need to create their own local config file (see README)
    config = configparser.ConfigParser()
    config.read('config.ini')

    # create database connection
    sql_config = config['LocalMSSQL']
    sql_url = create_url(**sql_config)
    sql_engine = create_engine(sql_url)
    server = FFIDatabase(sql_engine)
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

        ffi_data = FFIFile(export)

        ffi_data.extract()
        ffi_data.transform()
        ffi_data.load(server)

        if len(ffi_data.insert_failed) == 0:
            shutil.move(file, os.path.join(processed, export.name))
            logging.info(f"Moved {export.name} to processed folder")
        else:
            logging.warning(f"{export.name} failed to fully upload. Issues with the following tables: "
                            f"{','.join(ffi_data.insert_failed)}. Review log.")


if __name__ == "__main__":
    main()
