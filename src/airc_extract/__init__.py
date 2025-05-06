import os
import sys
import argparse
import configparser
import sqlite3

from datetime import datetime
from loguru import logger
from pathlib import Path, PureWindowsPath
from airc_extract.db_ops import (
    create_new_data_db,
    query_unextracted_data,
    insert_data_to_db,
)
from airc_extract.airc_report import AIRCReport, EmptyReportError


def main() -> None:
    parser = argparse.ArgumentParser(description="AIRC Data Extractor")
    parser.add_argument(
        "--config",
        "-c",
        type=str,
        default=Path(__file__).resolve().parent / "config.ini",
        help="Path to the configuration file. If you ran create_airc_config, this is will point to the default config file.",
    )
    args = parser.parse_args()

    config = _load_config(args.config)
    _setup_logging(config)
    _test_connections(config)
    logger.info("Starting AIRC data extraction...")
    airc_data_extractor(config)


def airc_data_extractor(config: configparser.ConfigParser) -> None:
    """
    Main function to extract AIRC data.
    :param config: Configuration object
    """
    data_dir = Path(config.get("GENERAL", "dicom_data_dir"))
    unextracted_studies = query_unextracted_data(config)
    total_studies = len(unextracted_studies)
    logger.info(f"Found {total_studies} unextracted studies in the DICOM database.")
    for i, study in enumerate(unextracted_studies, start=1):
        # study_ex = data_dir / Path(PureWindowsPath(study[0]))
        # if study_ex.exists():
        try:
            study = [data_dir / Path(PureWindowsPath(file)) for file in study]
            report = AIRCReport(study)
            report.extract_report()
            insert_data_to_db(report, config)
        except EmptyReportError as e:
            logger.error(
                f"{i}/{total_studies} - {report.series_uid} has no valid dicom files. Skipping extraction. {e}"
            )
            continue
        except Exception as e:
            logger.critical(f"{i}/{total_studies} - {report.series_uid} failed: {e}")
            continue
        logger.success(
            f"{i}/{total_studies} - {report.series_uid} extracted and inserted into database."
        )


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
        enqueue=True,
        format="<green>{time:YYYY-MM-DD at HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    )
    # Set up file logging
    logger.add(
        Path(log_dir) / f"airc_data_extractor_{today}.log",
        level=log_level_file,
        rotation="1 GB",
        enqueue=True,
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
        "--dicom-data-dir",
        "-dd",
        type=str,
        required=True,
        help="Path to the DicomConquest data directory",
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
    dicom_data_dir = Path(args.dicom_data_dir).resolve().absolute()
    data_db = Path(args.data_db).resolve().absolute()
    log_dir = Path(args.log_dir).resolve().absolute()
    config["GENERAL"] = {
        "dicom_db": str(dicom_db),
        "dicom_data_dir": str(dicom_data_dir),
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
        DICOM data directory: {dicom_data_dir}
        Data database: {data_db}
        Log directory: {log_dir}
        Terminal Log level: {args.log_level_term}
        File Log level: {args.log_level_file}"""
    )
