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
    finding_site_sequence = '363698007'
    tracking_code = '112039'

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

    def extract_report(self) -> dict:
        """Extract the report data from the dicom files"""
        self.validate_identifiers()
        self.extract_measurements()
        return self.report_data

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
        self.report_data["series_uid"] = ref.SeriesInstanceUID
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
        data_content = self._check_for_content(data)
        measurement = self._match_code_to_airc_measurement(data_content)
        measure_content = self._get_measurement_content_sequence(data_content)
        # Get the measurement data
        match measurement:
            case "lung_parenchyma":
                logger.debug(f"Extracting lung parenchyma measurements from {self.current_filename}")
                measures = self._extract_lung_parenchyma_measurements(measure_content)
            case "coronary_calcium":
                logger.debug(f"Extracting coronary calcium measurements from {self.current_filename}")
                measures = self._extract_coronary_calcium_measurements(measure_content)
            case "aortic_diameters":
                logger.debug(f"Extracting aortic diameter measurements from {self.current_filename}")
                measures = self._extract_aortic_diameter_measurements(measure_content)
            case "spine_measurements":
                logger.debug(f"Extracting spine measurements from {self.current_filename}")
                measures = self._extract_spine_measurements(measure_content)
            case "pulmonary_densities":
                logger.debug(f"Extracting pulmonary density measurements from {self.current_filename}")
                measures = self._extract_pulmonary_density_measurements(measure_content)
            case "lung_lesions":
                logger.debug(f"Extracting lung lesion measurements from {self.current_filename}")
                measures = self._extract_lung_lesion_measurements(measure_content)
        # Return the measurement data and the measurement name

        return measurement, measures

    def _check_for_content(self, data):
        if not hasattr(data, "ContentSequence"):
            logger.error(f"No ContentSequence found in {data.filename}")
            raise ContentMissingError("No ContentSequence found in DICOM data")

        content = data.ContentSequence
        self.current_filename = data.filename
        return content

    def _get_measurement_content_sequence(self, content):
        image_measure_code = '126010'
        measure_content = None
        for seq in content:
            if seq.ConceptNameCodeSequence[0].CodeValue == image_measure_code:
                # This is the image measure - we want to extract the data from this
                measure_content = seq
                break
        # If it's empty raise an error
        if not measure_content:
            logger.error(f"No image measure sequence found in {self.current_filename}")
            raise ContentMissingError("No image measure found in DICOM data")
        # If the sequence exists but doesn't have the content sequence, raise an error
        if not hasattr(measure_content, "ContentSequence"):
            logger.error(f"No measurement ContentSequence found in {self.current_filename}")
            raise ContentMissingError("No ContentSequence found in DICOM data")
        return measure_content

    def _match_code_to_airc_measurement(self, content):
        id_content = content[0]
        code_map = AircReport.code_map
        if not hasattr(id_content, "ConceptCodeSequence"):
            logger.error(f"No AIRC Code found in {self.current_filename}")
            raise ContentMissingError("No AIRC Code found in DICOM data")
        # Match the code to the AIRC code map
        code = id_content.ConceptCodeSequence[0].CodeValue
        if code not in code_map:
            logger.error(f"Code {code} not found in AIRC code map for {self.current_filename}")
            raise ContentMissingError("Code not found in AIRC code map")
        # This is one of the 6 AIRC measurements done - will be the key for the output dictionary
        measurement = code_map[code]
        return measurement

    def _extract_aortic_diameter_measurements(self, measure_content: dcm.DataElement) -> dict:
        """Extract the aortic diameters from the dicom data
        :param content: the dicom data
        :return: a dictionary of the aortic diameters
        """
        # Get the measurements
        location_code_map = {
            'CHESTCT0408': 'max_ascending',
            'CHESTCT0409': 'max_descending',
            'C33557': 'sinus_of_valsalva',
            'RID579': 'sinotubular_junction',
            'CHESTCT0401': 'mid_ascending',
            'CHESTCT0402': 'proximal_arch',
            'CHESTCT0403': 'mid_arch',
            'CHESTCT0404': 'proximal_descending',
            'CHESTCT0405': 'mid_descending',
            'CHESTCT0406': 'diaphragm_level',
            'RID905': 'celiac_artery_origin',
        }
        diameters = {}
        aorta_measures = measure_content.ContentSequence
        for measure in aorta_measures:
            # Each measure is itself a sequence of data describing where the measure is taken and the value
            measure_content = measure.ContentSequence
            diameter_sequence = 'RID13432'
            site_location = None
            diameter = None
            # Loop through the sequences to pull out the location and the diameter
            for sequence in measure_content:
                seq_code = sequence.ConceptNameCodeSequence[0].CodeValue
                if seq_code == self.finding_site_sequence:
                    site_code = sequence.ConceptCodeSequence[0].CodeValue
                    # This is just the final code for PACS - not a meausurement
                    if site_code == 'RID480':
                        continue
                    site_location = location_code_map.get(site_code, f'{sequence.ConceptCodeSequence[0].CodeValue}, {sequence.ConceptCodeSequence[0].CodeMeaning}')
                if seq_code == diameter_sequence:
                    # This is the measurement
                    diameter = int(sequence.MeasuredValueSequence[0].NumericValue)
            # If we have both the location and the diameter, add it to the dictionary
            if site_location is not None and diameter is not None:
                diameters[site_location] = diameter
        if not diameters:
            logger.error(f"No aortic diameters found in {self.current_filename} aortic measure report")
            raise ContentMissingError("No aortic diameters found in DICOM data")
        return diameters

    def _extract_lung_lesion_measurements(self, measure_content: dcm.DataElement) -> dict:
        """Extract the lung lesion measurements from the dicom data
        :param measure_content: the dicom data
        :return: a dictionary of the lung lesion measurements
        """
        # Get the measurements
        lesion_data = {}
        lesion_list = measure_content.ContentSequence
        lesion_data['lesion_count'] = len(lesion_list)
        for idx, lesion in enumerate(lesion_list):
            lesion_id, lesion_measurements = self._extract_lung_lesion_measurement(lesion, idx)
            if lesion_id and lesion_measurements:
                lesion_data[lesion_id] = lesion_measurements
        return lesion_data

    def _extract_lung_lesion_measurement(self, lesion: dcm.DataElement, idx: int) -> tuple[str, dict]:
        if not hasattr(lesion, "ContentSequence"):
            logger.warning(f'No ContentSequence found in {self.current_filename} for lesion {idx}')
            return None, None
        lesion_review_status_code = 'CHESTCT0102'
        measurement_type_map = {
            '103339001': 'max_2d_diameter_mm',
            '103340004': 'min_2d_diameter_mm',
            'RID50155': 'mean_2d_diameter_mm',
            'L0JK': 'max_3d_diameter_mm',
            'RID28668': 'volume_mm3',
        }
        lesion_measurements = {
            'location': None,
            'review_status': None,
            'max_2d_diameter_mm': None,
            'min_2d_diameter_mm': None,
            'mean_2d_diameter_mm': None,
            'max_3d_diameter_mm': None,
            'volume_mm3': None,
        }
        for seq in lesion.ContentSequence:
            descriptor = seq.ConceptNameCodeSequence[0]
            if descriptor.CodeValue == self.tracking_code:
                lesion_id = seq.TextValue
            if descriptor.CodeValue == self.finding_site_sequence:
                location = seq.ContentSequence[0].ConceptCodeSequence[0].CodeMeaning
                lesion_measurements['location'] = location
            if descriptor.CodeValue == lesion_review_status_code:
                if seq.TextValue in ('Measurement accepted', 'Measurement auto-confirmed'):
                    review_status = 'accepted'
                else:
                    review_status = seq.TextValue
                lesion_measurements['review_status'] = review_status
            if descriptor.CodeValue in measurement_type_map:
                measurement_type = measurement_type_map[descriptor.CodeValue]
                # Get the value
                if hasattr(seq, "MeasuredValueSequence"):
                    measurement_value = seq.MeasuredValueSequence[0].NumericValue
                    lesion_measurements[measurement_type] = float(measurement_value)
                else:
                    lesion_measurements[measurement_type] = None
        return lesion_id, lesion_measurements
            

    def _extract_lung_parenchyma_measurements(self, measure_content: dcm.DataElement) -> dict:
        """Extract the lung parenchyma measurements from the dicom data
        :param measure_content: the dicom data
        :return: a dictionary of the lung parenchyma measurements
        """
        # Get the measurements
        pass

    def _extract_coronary_calcium_measurements(self, measure_content: dcm.DataElement) -> dict:
        """Extract the coronary calcium measurements from the dicom data
        :param measure_content: the dicom data
        :return: a dictionary of the coronary calcium measurements
        """
        calc_data = {}
        for measure in measure_content.ContentSequence:
            measure_name = None
            measure_value = None
            for seq in measure.ContentSequence:
                if seq.ConceptNameCodeSequence[0].CodeValue == self.tracking_code:
                    # This is the location
                    if seq.TextValue == 'Heart':
                        measure_name = 'heart_volume_cm3'
                    elif seq.TextValue == 'Calcium score':
                        measure_name = 'coronary_calc_mm3'
                if hasattr(seq, "MeasuredValueSequence"):
                    measure_value = seq.MeasuredValueSequence[0].NumericValue
            if measure_name is not None and measure_value is not None:
                calc_data[measure_name] = float(measure_value)
        return calc_data



    def _extract_spine_measurements(self, measure_content: dcm.DataElement) -> dict:
        """Extract the spine measurements from the dicom data
        :param measure_content: the dicom data
        :return: a dictionary of the spine measurements
        """
        # Get the measurements
        pass

    def _extract_pulmonary_density_measurements(self, measure_content: dcm.DataElement) -> dict:
        """Extract the pulmonary density measurements from the dicom data
        :param measure_content: the dicom data
        :return: a dictionary of the pulmonary density measurements
        """
        # Get the measurements
        pass
