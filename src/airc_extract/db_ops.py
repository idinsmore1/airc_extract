import sqlite3
import polars as pl
from loguru import logger
from configparser import ConfigParser
from pathlib import Path

TABLE_COLUMNS = {
    "main": [
        "series_uid",
        "mrn",
        "accession",
        "study_date",
        "sex",
        "aorta",
        "spine",
        "cardio",
        "lesions",
        "lung",
    ],
    "aorta": [
        "series_uid",
        "max_ascending",
        "max_descending",
        "sinus_of_valsalva",
        "sinotubular_junction",
        "mid_ascending",
        "proximal_arch",
        "mid_arch",
        "proximal_descending",
        "mid_descending",
        "diaphragm_level",
        "celiac_artery_origin",
    ],
    "spine": [
        "series_uid",
        "vertebra",
        "direction",
        "length_mm",
        "status",
    ],
    "cardio": [
        "series_uid",
        "heart_volume_cm3",
        "coronary_calcification_volume_mm3",
    ],
    "lesions": [
        "series_uid",
        "lesion_id",
        "location",
        "review_status",
        "max_2d_diameter_mm",
        "min_2d_diameter_mm",
        "mean_2d_diameter_mm",
        "max_3d_diameter_mm",
        "volume_mm3",
    ],
    "lung": [
        "series_uid",
        "location",
        "opacity_score",
        "volume_cm3",
        "opacity_volume_cm3",
        "opacity_percent",
        "high_opacity_volume_cm3",
        "high_opacity_percent",
        "mean_hu",
        "mean_hu_opacity",
        "low_parenchyma_hu_percent",
    ],
}
# These are the columns that are not part of the primary key for each table
# We define these to allow .get() to work on the report data
DATA_COLUMNS = {
    "aorta": [
        "max_ascending",
        "max_descending",
        "sinus_of_valsalva",
        "sinotubular_junction",
        "mid_ascending",
        "proximal_arch",
        "mid_arch",
        "proximal_descending",
        "mid_descending",
        "diaphragm_level",
        "celiac_artery_origin",
    ],
    "spine": [
        "length_mm",
        "status",
    ],
    "cardio": [
        "heart_volume_cm3",
        "coronary_calcification_volume_mm3",
    ],
    "lesions": [
        "location",
        "review_status",
        "max_2d_diameter_mm",
        "min_2d_diameter_mm",
        "mean_2d_diameter_mm",
        "max_3d_diameter_mm",
        "volume_mm3",
    ],
    "lung": [
        "opacity_score",
        "volume_cm3",
        "opacity_volume_cm3",
        "opacity_percent",
        "high_opacity_volume_cm3",
        "high_opacity_percent",
        "mean_hu",
        "mean_hu_opacity",
        "low_parenchyma_hu_percent",
    ],
}


def create_new_data_db(data_db_path: Path | str) -> None:
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
        logger.success(
            f"Created new data database at {data_db_path} with required tables."
        )


def query_unextracted_data(config: ConfigParser) -> list[list]:
    """
    Query the DicomConquest database for unextracted dicom files.
    :param config: Configuration object
    :return: List of lists. Each inner list contains the dicom file paths for a single study.
    """
    dicom_db = config.get("GENERAL", "dicom_db")
    data_db = config.get("GENERAL", "data_db")
    conn = sqlite3.connect(dicom_db)
    with conn:
        cursor = conn.cursor()
        cursor.execute(f"ATTACH DATABASE '{data_db}' AS data_db")
        query = """SELECT main.DICOMImages.SeriesInst as series_uid, main.DICOMImages.ObjectFile as filepath
                FROM main.DICOMImages
                INNER JOIN main.DICOMSeries
                ON main.DICOMImages.SeriesInst = main.DICOMSeries.SeriesInst
                LEFT JOIN data_db.main ON main.DICOMImages.SeriesInst = data_db.main.series_uid
                WHERE data_db.main.series_uid IS NULL
                AND main.DICOMSeries.Modality = 'SR'
                """

        cursor.execute(query)
        unextracted = (
            pl.read_database(query, conn)
            .group_by("series_uid")
            .agg(pl.col("filepath").unique().alias("filepaths"))["filepaths"]
            .to_list()
        )
        cursor.execute("DETACH DATABASE data_db")
    conn.close()
    return unextracted


def insert_data_to_db(report, config: ConfigParser) -> None:
    """
    Insert AIRC Report into the database.
    :param report: AIRC Report Object
    :param config: Configuration object
    """
    airc_data = report.report_data
    data_db = config.get("GENERAL", "data_db")
    conn = sqlite3.connect(data_db)
    with conn:
        for table in TABLE_COLUMNS:
            if table not in airc_data:
                logger.debug(
                    f"{table.title()} data not found in {report.series_uid}. Skipping database insert."
                )
                continue
            insert_statement = get_insert_statement(table)
            formatted_data = format_table_input(airc_data, table)
            try:
                conn.executemany(insert_statement, formatted_data)
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error inserting {report.series_uid} into {table}: {e}")
    conn.close()
    logger.debug(f"{report.series_uid} inserted into database.")


def format_table_input(report_data: dict, table_name: str) -> tuple:
    """
    Format the input data for a given table.
    :param report_data: Dictionary containing the report data
    :param table_name: Name of the table to format the data for
    :return: Tuple of formatted data
    """
    data_cols = DATA_COLUMNS.get(table_name)
    match table_name:
        case "main":
            formatted = [tuple(report_data.get(table_name).values())]
        case "lesions":
            formatted = []
            for lesion, data in report_data.get("lesions").items():
                row = (
                    report_data.get("series_uid"),
                    lesion,
                    *[data.get(col) for col in data_cols],
                )
                formatted.append(row)
        case "spine":
            formatted = []
            for vertebra, measurements in report_data.get("spine").items():
                for direction, data in measurements.items():
                    row = (
                        report_data.get("series_uid"),
                        vertebra,
                        direction,
                        *[data.get(col) for col in data_cols],
                    )
                    formatted.append(row)
        case "lung":
            formatted = []
            for location, data in report_data.get("lung").items():
                row = (
                    report_data.get("series_uid"),
                    location,
                    *[data.get(col) for col in data_cols],
                )
                formatted.append(row)
        case _:
            formatted = [
                (
                    report_data.get("series_uid"),
                    *[report_data[table_name].get(col) for col in data_cols],
                )
            ]
    return formatted


def get_insert_statement(table_name: str) -> str:
    """
    Get the insert statement for a given table name.
    :param table_name: Name of the table
    :return: Insert statement
    """
    columns = ", ".join(TABLE_COLUMNS[table_name])
    placeholders = ", ".join(["?"] * len(TABLE_COLUMNS[table_name]))
    return f"REPLACE INTO {table_name} ({columns}) VALUES({placeholders})"
