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
    # Fill this in before running!!!!
    path = 'C:/Users/Corey/OneDrive/OneDrive - New Mexico Highlands University/Python/FFI/XMLToCustom/data'

    # DEBUGGING
    debug = False
    # debug = True

    # users need to create their own local config file (see README)
    config = configparser.ConfigParser()
    config.read('config.ini')

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

        ffi_data.format_tables()

        ffi_data.tables_to_db(server)

        if len(ffi_data.insert_failed) == 0:
            shutil.move(file, os.path.join(processed, export.name))
            logging.info(f"Moved {export.name} to processed folder")
        else:
            logging.warning(f"{export.name} failed to fully upload. Issues with the following tables: "
                            f"{','.join(ffi_data.insert_failed)}. Review log.")


if __name__ == "__main__":
    main()
