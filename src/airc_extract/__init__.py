import sys
import argparse
import configparser
import sqlite3

from datetime import datetime
from loguru import logger
from pathlib import Path
from airc_extract.db_ops import create_new_data_db
from airc_extract.airc_report import AIRCReport


def main() -> None:
    parser = argparse.ArgumentParser(description="AIRC Data Extractor")
    parser.add_argument(
        "--config",
        "-c",
        type=str,
        default=Path(__file__).resolve().parent / "config.ini",
        help="Path to the configuration file. If you ran create_airc_config, this is will point to the default config file.",
    )
    parser.add_argument(
        "--save-json", "-j", action="store_true", help="Save output as JSON. WARNING: This will overwrite an existing JSON output and be a large file."
    )
    args = parser.parse_args()

    config = _load_config(args.config)
    _setup_logging(config)
    _test_connections(config)
    logger.info("Starting AIRC data extraction...")


def _setup_logging(config: configparser.ConfigParser) -> None:
    """
    Set up logging for the AIRC data extractor.
    """
    log_dir = Path(config.get("GENERAL", "log_dir"))
    log_level = config.get("GENERAL", "log_level")
    log_level_file = config.get("GENERAL", "log_level_file")
    today = datetime.today().strftime("%Y_%m_%d")

    # Set up terminal logging
    logger.remove()
    logger.add(
        sys.stderr,
        level=log_level,
        format="<green>{time:YYYY-MM-DD at HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    )

    # Set up file logging
    logger.add(
        Path(log_dir) / f"airc_data_extractor_{today}.log",
        level=log_level_file,
        rotation="1 MB",
        retention="7 days",
        compression="zip",
        format="{time:YYYY-MM-DD at HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    )

def _load_config(config_path: str) -> configparser.ConfigParser:
    # Set up the configuration parser
    config_path = Path(config_path).resolve()
    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file {config_path} not found. Please run create_airc_config to create it."
        )
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
    dicom_db = config.get("GENERAL", "dicom_db")
    data_db = config.get("GENERAL", "data_db")
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
            logger.info("Connected to DICOM database.")
        except Exception as e:
            raise sqlite3.Error(f"Error connecting to dicom database {dicom_db}: {e}")
        # Check data database connection
        try:
            cursor = conn2.cursor()
            logger.info("Connected to data database.")
        except Exception as e:
            raise sqlite3.Error(f"Error connecting to data database {data_db}: {e}")


def create_airc_config() -> None:
    """
    Create a configuration file for the AIRC data extractor.
    """
    parser = argparse.ArgumentParser(description="Create AIRC configuration file")
    parser.add_argument(
        "--dicom-db",
        "-d",
        type=str,
        required=True,
        help="Path to the DicomConquest database",
    )
    parser.add_argument(
        "--data-db", "-o", type=str, required=True, help="Path to the data database"
    )
    parser.add_argument(
        "--log-level-term",
        "-l",
        type=str,
        default="INFO",
        help="Terminal logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL",
    )
    parser.add_argument(
        "--log-level-file",
        "-f",
        type=str,
        default="DEBUG",
        help="File logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL",
    )
    parser.add_argument(
        "--log-dir", type=str, default=".", help="Directory for log files"
    )
    args = parser.parse_args()
    lib_path = Path(__file__).resolve().parent
    config_path = lib_path / "config.ini"
    config = configparser.ConfigParser()
    dicom_db = Path(args.dicom_db).resolve().absolute()
    data_db = Path(args.data_db).resolve().absolute()
    log_dir = Path(args.log_dir).resolve().absolute()
    config["GENERAL"] = {
        "dicom_db": str(dicom_db),
        "data_db": str(data_db),
        "log_dir": log_dir,
        "log_level": args.log_level_term,
        "log_level_file": args.log_level_file,
    }
    with open(config_path, "w") as configfile:
        config.write(configfile)

    if not data_db.exists():
        create_new_data_db(data_db)
    else:
        logger.info(f"Data database {data_db} already exists. Skipping creation.")
    if not dicom_db.exists():
        raise FileNotFoundError(f"DICOM database {dicom_db} not found.")
    if not log_dir.exists():
        log_dir.mkdir(parents=True, exist_ok=False)
    _test_connections(config)
    logger.success(
        f"""Created configuration file at {config_path} with the following settings:
        DICOM database: {dicom_db}
        Data database: {data_db}
        Log directory: {log_dir}
        Terminal Log level: {args.log_level_term}
        File Log level: {args.log_level_file}"""
    )
