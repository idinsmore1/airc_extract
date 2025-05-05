import argparse
import configparser
import sqlite3

from loguru import logger
from pathlib import Path

def main() -> None:
    parser = argparse.ArgumentParser(description="AIRC Data Extractor")
    parser.add_argument('--config', '-c', type=str, default='config.ini', help='Path to the configuration file')
    parser.add_argument('--save-json', '-j', action='store_true', help='Save output as JSON')
    args = parser.parse_args()

    config = _load_config(args.config)    
    _test_connections(config)


def _load_config(config_path: str) -> configparser.ConfigParser:
    # Set up the configuration parser
    config_path = Path(config_path).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file {config_path} not found. Please run create_airc_config to create it.")
    # Load the configuration file
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


def _test_connections(config) -> None:
    """
    Test the connections to the DICOM and data databases.
    """
    # Placeholder for actual connection testing logic
    logger.debug("Testing connections to DICOM and data databases...")
    dicom_db = config.get('GENERAL', 'dicom_db')
    data_db = config.get('GENERAL', 'data_db')
    dicom_db = Path(dicom_db)
    data_db = Path(data_db)
    # If the database paths ar
    if not dicom_db.exists():
        raise FileNotFoundError(f"DICOM database {dicom_db} not found.")
    if not data_db.exists():
        raise FileNotFoundError(f"Data database {data_db} not found.")

    # Test the connections
    with sqlite3.connect(dicom_db) as conn, sqlite3.connect(data_db) as conn2:
        try:
            cursor = conn.cursor()
            logger.debug("Connected to DICOM database.")
        except Exception as e:
            raise sqlite3.Error(f"Error connecting to dicom database {dicom_db}: {e}")
        # Check data database connection
        try:
            cursor = conn2.cursor()
            logger.debug("Connected to data database.")
        except Exception as e:
            raise sqlite3.Error(f"Error connecting to data database {data_db}: {e}")


def create_airc_config() -> None:
    """
    Create a configuration file for the AIRC data extractor.
    """
    parser = argparse.ArgumentParser(description="Create AIRC configuration file")
    parser.add_argument('--dicom-db', '-d', type=str, required=True, help='Path to the DicomConquest database')
    parser.add_argument('--data-db', '-o', type=str, required=True, help='Path to the data database')
    parser.add_argument('--log-level-term', '-l', type=str, default='INFO', help='Terminal logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL')
    parser.add_argument('--log-level-file', '-f', type=str, default='DEBUG', help='File logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL')
    parser.add_argument('--log-dir', type=str, default='.', help='Directory for log files')
    args = parser.parse_args()
    lib_path = Path(__file__).resolve().parent
    config_path = lib_path / 'config.ini'
    config = configparser.ConfigParser()
    dicom_db = Path(args.dicom_db).resolve().absolute()
    data_db = Path(args.data_db).resolve().absolute()
    log_dir = Path(args.log_dir).resolve().absolute()
    config['GENERAL'] = {
        'dicom_db': str(dicom_db),
        'data_db': str(data_db),
        'log_dir': log_dir,
        'log_level': args.log_level_term,
        'log_level_file': args.log_level_file
    }
    with open(config_path, 'w') as configfile:
        config.write(configfile)

    if not data_db.exists():
        create_new_data_db(data_db)
    else:
        logger.info(f"Data database {data_db} already exists. Skipping creation.")
    if not dicom_db.exists():
        raise FileNotFoundError(f"DICOM database {dicom_db} not found.")
    if not log_dir.exists():
        log_dir.mkdir(parents=True, exist_ok=False)
    logger.success(f"Created configuration file at {config_path} with the following settings:")
    logger.success(f"DICOM database: {dicom_db}")
    logger.success(f"Data database: {data_db}")
    logger.success(f"Log directory: {log_dir}")
    logger.success(f"Terminal Log level: {args.log_level_term}")
    logger.success(f"File Log level: {args.log_level_file}")
    


def create_new_data_db(data_db_path: Path|str) -> None:
    """
    Create a new output data database for AIRC data extraction with all required tables.
    :param data_db_path: Path to the new data database
    """
    main = """CREATE TABLE IF NOT EXISTS main (
        series_uid TEXT PRIMARY KEY,
        mrn TEXT,
        accession TEXT,
        study_date TEXT,
        sex TEXT,
        aorta INTEGER,
        spine INTEGER,
        cardio INTEGER,
        lesions INTEGER,
        lung INTEGER
    )"""
    aorta = """CREATE TABLE IF NOT EXISTS aorta (
        series_uid TEXT PRIMARY KEY,
        max_ascending INTEGER,
        max_descending INTEGER,
        sinus_of_valsalva INTEGER,
        sinotubular_junction INTEGER,
        mid_ascending INTEGER,
        proximal_arch INTEGER,
        mid_arch INTEGER,
        proximal_descending INTEGER,
        mid_descending INTEGER,
        diaphragm_level INTEGER,
        celiac_artery_origin INTEGER
    )"""
    spine = """CREATE TABLE IF NOT EXISTS spine (
        series_uid TEXT NOT NULL,
        vertebra TEXT NOT NULL,
        direction TEXT NOT NULL,
        length_mm REAL,
        status TEXT,
        PRIMARY KEY (series_uid, vertebra, direction)
    )"""
    cardio = """CREATE TABLE IF NOT EXISTS cardio (
        series_uid TEXT PRIMARY KEY,
        heart_volume_cm3 REAL,
        coronary_calcification_volume_mm3 REAL
    )"""
    lesions = """CREATE TABLE IF NOT EXISTS lesions (
        series_uid TEXT NOT NULL,
        lesion_id TEXT NOT NULL,
        location TEXT,
        review_status TEXT,
        max_2d_diameter_mm REAL,
        min_2d_diameter_mm REAL,
        mean_2d_diameter_mm REAL,
        max_3d_diameter_mm REAL,
        volume_mm3 REAL,
        PRIMARY KEY (series_uid, lesion_id)
    )"""
    lung = """CREATE TABLE IF NOT EXISTS lung (
        series_uid TEXT NOT NULL,
        location TEXT NOT NULL,
        opacity_score REAL,
        volume_cm3 REAL,
        opacity_volume_cm3 REAL,
        opacity_percent REAL,
        high_opacity_volume_cm3 REAL,
        high_opacity_percent REAL,
        mean_hu REAL,
        mean_hu_opacity REAL,
        low_parenchyma_hu_percent REAL,
        PRIMARY KEY (series_uid, location)
    )"""
    with sqlite3.connect(data_db_path) as conn:
        cursor = conn.cursor()
        for table in [main, aorta, spine, cardio, lesions, lung]:
            try:
                cursor.execute(table)
            except sqlite3.Error as e:
                logger.error(f"Error creating table: {e}")
                print(table)
        conn.commit()
        logger.success(f"Created new data database at {data_db_path} with required tables.")


