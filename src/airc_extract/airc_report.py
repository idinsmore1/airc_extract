import pydicom as dcm
import polars as pl

from datetime import date
from hashlib import sha256
from pathlib import Path
from loguru import logger


class EmptyReportError(FileNotFoundError):
    def __init__(self, message: str):
        super().__init__(message)
        logger.error(message)


class ContentMissingError(ValueError):
    def __init__(self, message: str):
        super().__init__(message)
        logger.error(message)


class AircReport:
    code_map = {
        "CHESTCT0203": "lung_parenchyma",
        "CHESTCT0304": "coronary_calcium",
        "CHESTCT0410": "aortic_diameters",
        "CHESTCT0502": "spine_measurements",
        "CHESTCT0611": "pulmonary_densities",
        "CHESTCT0999": "lung_lesions",
    }

    def __init__(self, dicom_dir: Path | str):
        self.report_data = {}
        self.dicom_dir = dicom_dir if isinstance(dicom_dir, Path) else Path(dicom_dir)

        # Check if the directory exists
        if not dicom_dir.exists():
            error_message = f"{dicom_dir} does not exist."
            logger.error(error_message)
            raise FileNotFoundError(error_message)
        # Check if the directory is empty
        self.dicom_files = list(dicom_dir.glob("*.dcm"))
        if not self.dicom_files:
            self.dicom_files = list(dicom_dir.glob("*"))
            if not self.dicom_files:
                error_message = f"{dicom_dir} is empty."
                logger.error(error_message)
                raise EmptyReportError(error_message)
        # Filter out dicoms that cannot be read
        self._validate_dicoms()
        if not self.dicom_data:
            error_message = f"No valid DICOM files found in {dicom_dir}."
            logger.error(error_message)
            raise EmptyReportError(error_message)

    def _validate_dicoms(self):
        """Validate that all dicoms can be read properly and remove those that can't"""
        valid_dicoms = []
        for dicom in self.dicom_files:
            try:
                data = dcm.dcmread(dicom)
                valid_dicoms.append(data)
            except Exception as e:
                logger.error(f"Failed to read {self.dicom_dir / dicom}: {e}")
        self.dicom_data = valid_dicoms

    def extract_report(self):
        """Extract the report data from the dicom files"""
        self.validate_identifiers()
        self.extract_measurements()

    def validate_identifiers(self):
        """validate that the identifiers are present in the dicom data and are equal"""
        # Get reference values from first DICOM
        ref = self.dicom_data[0]
        ref_values = {
            "PatientID": ref.PatientID,
            "AccessionNumber": ref.AccessionNumber,
            "SeriesInstanceUID": ref.SeriesInstanceUID,
            "PatientSex": ref.PatientSex,
            "StudyDate": ref.StudyDate,
        }

        # Compare all other DICOMs against reference
        for data in self.dicom_data[1:]:
            for attr, ref_val in ref_values.items():
                curr_val = getattr(data, attr, None)
                if curr_val is None:
                    continue
                if curr_val != ref_val:
                    error_message = (
                        f"Mismatched {attr}: expected '{ref_val}', got '{curr_val}'"
                    )
                    logger.error(error_message)
                    raise ValueError(error_message)

        # Set the identifiers in the report data
        self.report_data["mrn"] = ref.PatientID
        self.report_data["accession"] = ref.AccessionNumber
        self.report_data["series_instance_uid"] = ref.SeriesInstanceUID
        self.report_data["sex"] = ref.PatientSex
        self.report_data["scan_date"] = date.fromisoformat(ref.StudyDate).strftime(
            "%Y-%m-%d"
        )

    def extract_measurements(self):
        for data in self.dicom_data:
            try:
                measurement, measures = self._extract_measurement_from_dicom_data(data)
                self.report_data[measurement] = measures
            except ContentMissingError as e:
                continue

    def _extract_measurement_from_dicom_data(
        self, data: dcm.DataElement
    ) -> tuple[str, dict]:
        """Extract all appropriate AIRC measurements from one loaded dicom data
        :param data: dcm.DataElement
        :return: a tuple of the matched code and the measurement data
        """
        # data sequence
        if not hasattr(data, "ContentSequence"):
            logger.error(f"No ContentSequence found in {data.filename}")
            raise ContentMissingError("No ContentSequence found in DICOM data")

        content = data.ContentSequence
        id_content = content[0]
        if not hasattr(id_content, "ConceptCodeSequece"):
            logger.error(f"No AIRC Code found in {data.filename}")
            raise ContentMissingError("No AIRC Code found in DICOM data")
        # Match the code to the AIRC code map
        code = id_content.ConceptCodeSequece[0].CodeValue
        if code not in self.code_map:
            logger.error(f"Code {code} not found in AIRC code map")
            raise ContentMissingError("Code not found in AIRC code map")
        # This is one of the 6 AIRC measurements done - will be the key for the output dictionary
        measurement = self.code_map[code]
        # Get the measurement data
        match measurement:
            case "lung_parenchyma":
                measures = self._extract_lung_parenchyma_measurements(content)
            case "coronary_calcium":
                measures = self._extract_coronary_calcium_measurements(content)
            case "aortic_diameters":
                measures = self._extract_aortic_diameter_measurements(content)
            case "spine_measurements":
                measures = self._extract_spine_measurements(content)
            case "pulmonary_densities":
                measures = self._extract_pulmonary_density_measurements(content)
            case "lung_lesions":
                measures = self._extract_lung_lesion_measurements(content)
   
        return measurement, measures

    def _extract_aortic_diameter_measurements(self, content: dcm.DataElement) -> dict:
        """Extract the aortic diameters from the dicom data
        :param content: the dicom data
        :return: a dictionary of the aortic diameters
        """
        # Get the measurements
        location_code_map = {
            'CHESTCT0408': 'asc_max',
            'CHESTCT0409': 'desc_max',
            'C33557': 'sinus_of_valsalva'
        }
        pass

    def _extract_lung_parenchyma_measurements(self, content: dcm.DataElement) -> dict:
        """Extract the lung parenchyma measurements from the dicom data
        :param content: the dicom data
        :return: a dictionary of the lung parenchyma measurements
        """
        # Get the measurements
        pass

    def _extract_coronary_calcium_measurements(self, content: dcm.DataElement) -> dict:
        """Extract the coronary calcium measurements from the dicom data
        :param content: the dicom data
        :return: a dictionary of the coronary calcium measurements
        """
        # Get the measurements
        pass

    def _extract_spine_measurements(self, content: dcm.DataElement) -> dict:
        """Extract the spine measurements from the dicom data
        :param content: the dicom data
        :return: a dictionary of the spine measurements
        """
        # Get the measurements
        pass

    def _extract_pulmonary_density_measurements(self, content: dcm.DataElement) -> dict:
        """Extract the pulmonary density measurements from the dicom data
        :param content: the dicom data
        :return: a dictionary of the pulmonary density measurements
        """
        # Get the measurements
        pass

    def _extract_lung_lesion_measurements(self, content: dcm.DataElement) -> dict:
        """Extract the lung lesion measurements from the dicom data
        :param content: the dicom data
        :return: a dictionary of the lung lesion measurements
        """
        # Get the measurements
        pass
