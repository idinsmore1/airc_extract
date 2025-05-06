import pydicom as dcm
import polars as pl

from datetime import date
from pathlib import Path
from loguru import logger


class EmptyReportError(FileNotFoundError):
    def __init__(self, message: str):
        super().__init__(message)


class ContentMissingError(ValueError):
    def __init__(self, message: str):
        super().__init__(message)


class AIRCReport:
    code_map = {
        "CHESTCT0203": "lung_parenchyma",
        "CHESTCT0304": "cardio",
        "CHESTCT0410": "aorta",
        "CHESTCT0502": "spine",
        "CHESTCT0611": "pulmonary_densities",
        "CHESTCT0999": "lesions",
    }
    lung_location_map = {
        "BothLungs": "both_lungs",
        "LeftUpperLobe": "left_upper_lobe",
        "LeftLowerLobe": "left_lower_lobe",
        "RightUpperLobe": "right_upper_lobe",
        "RightMiddleLobe": "right_middle_lobe",
        "RightLowerLobe": "right_lower_lobe",
    }
    finding_site_sequence = "363698007"
    tracking_code = "112039"

    def __init__(self, dicom_files: list[Path | str]):
        self.report_data = {}
        self.dicom_files = dicom_files
        self.series_uid = str(dicom_files[0]).split("/")[-1].split("_")[0]

    def validate_dicoms(self):
        """Validate that all dicoms can be read properly and remove those that can't"""
        valid_dicoms = []
        for dicom in self.dicom_files:
            try:
                data = dcm.dcmread(dicom)
                valid_dicoms.append(data)
            except Exception as e:
                continue
        if not valid_dicoms:
            raise EmptyReportError("")
        self.dicom_data = valid_dicoms

    def extract_report(self) -> dict:
        """Extract the report data from the dicom files"""
        self.validate_dicoms()
        self.validate_identifiers()
        self.extract_measurements()
        logger.debug(f"{self.series_uid} AIRC Report extracted successfully")

    def validate_identifiers(self) -> None:
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

    def extract_measurements(self) -> None:
        for data in self.dicom_data:
            try:
                measurement, measures = self._extract_measurement_from_dicom_data(data)
                self.report_data[measurement] = measures
            except Exception as e:
                logger.warning(
                    f"Error extracting measurement from DICOM data: {e}"
                )
                continue
        self._merge_lung_data()
        self._create_main_dict()

    def _merge_lung_data(self):
        if (
            self.report_data.get("pulmonary_densities") is not None
            and self.report_data.get("lung_parenchyma") is not None
        ):
            # If we have both pulmonary densities and lung parenchyma, merge them
            combined_data = {}
            for location in self.lung_location_map.values():
                pulm_data = self.report_data["pulmonary_densities"].get(location, {})
                par_data = self.report_data["lung_parenchyma"].get(location, {})
                combined_data[location] = {**pulm_data, **par_data}
            self.report_data["lung"] = combined_data
            # Remove the old data
            del self.report_data["pulmonary_densities"]
            del self.report_data["lung_parenchyma"]
        # If we only have one of the two, rename it to lung_data
        else:
            if self.report_data.get("pulmonary_densities") is not None:
                self.report_data["lung"] = self.report_data["pulmonary_densities"]
                del self.report_data["pulmonary_densities"]
            elif self.report_data.get("lung_parenchyma") is not None:
                self.report_data["lung"] = self.report_data["lung_parenchyma"]
                del self.report_data["lung_parenchyma"]

    def _create_main_dict(self) -> None:
        """Create a dictionary of the main table data for the output database"""
        self.report_data["main"] = {
            "series_uid": self.report_data.get("series_uid"),
            "mrn": self.report_data.get("mrn"),
            "accession": self.report_data.get("accession"),
            "study_date": self.report_data.get("scan_date"),
            "sex": self.report_data.get("sex"),
            "aorta": 1 if self.report_data.get("aorta") else 0,
            "spine": 1 if self.report_data.get("spine") else 0,
            "cardio": 1 if self.report_data.get("cardio") else 0,
            "lesions": 1 if self.report_data.get("lesions") else 0,
            "lung": 1 if self.report_data.get("lung") else 0,
        }

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
                logger.debug(
                    f"Extracting lung parenchyma measurements from {self.current_filename}"
                )
                measures = self._extract_lung_parenchyma_measurements(measure_content)
            case "cardio":
                logger.debug(
                    f"Extracting cardio measurements from {self.current_filename}"
                )
                measures = self._extract_coronary_calcium_measurements(measure_content)
            case "aorta":
                logger.debug(
                    f"Extracting aortic diameter measurements from {self.current_filename}"
                )
                measures = self._extract_aortic_diameter_measurements(measure_content)
            case "spine":
                logger.debug(
                    f"Extracting spine measurements from {self.current_filename}"
                )
                measures = self._extract_spine_measurements(measure_content)
            case "pulmonary_densities":
                logger.debug(
                    f"Extracting pulmonary density measurements from {self.current_filename}"
                )
                measures = self._extract_pulmonary_density_measurements(measure_content)
            case "lesions":
                logger.debug(
                    f"Extracting lung lesion measurements from {self.current_filename}"
                )
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
        image_measure_code = "126010"
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
            logger.error(
                f"No measurement ContentSequence found in {self.current_filename}"
            )
            raise ContentMissingError("No ContentSequence found in DICOM data")
        return measure_content

    def _match_code_to_airc_measurement(self, content):
        id_content = content[0]
        code_map = AIRCReport.code_map
        if not hasattr(id_content, "ConceptCodeSequence"):
            logger.error(f"No AIRC Code found in {self.current_filename}")
            raise ContentMissingError("No AIRC Code found in DICOM data")
        # Match the code to the AIRC code map
        code = id_content.ConceptCodeSequence[0].CodeValue
        if code not in code_map:
            logger.error(
                f"Code {code} not found in AIRC code map for {self.current_filename}"
            )
            raise ContentMissingError("Code not found in AIRC code map")
        # This is one of the 6 AIRC measurements done - will be the key for the output dictionary
        measurement = code_map[code]
        return measurement

    def _extract_aortic_diameter_measurements(
        self, measure_content: dcm.DataElement
    ) -> dict:
        """Extract the aortic diameters from the dicom data
        :param content: the dicom data
        :return: a dictionary of the aortic diameters
        """
        # Get the measurements
        location_code_map = {
            "CHESTCT0408": "max_ascending",
            "CHESTCT0409": "max_descending",
            "C33557": "sinus_of_valsalva",
            "RID579": "sinotubular_junction",
            "CHESTCT0401": "mid_ascending",
            "CHESTCT0402": "proximal_arch",
            "CHESTCT0403": "mid_arch",
            "CHESTCT0404": "proximal_descending",
            "CHESTCT0405": "mid_descending",
            "CHESTCT0406": "diaphragm_level",
            "RID905": "celiac_artery_origin",
        }
        diameters = {}
        if not hasattr(measure_content, "ContentSequence"):
            logger.debug(
                f"No aortic diameters found in {self.current_filename} aortic measure report"
            )
            return None
        aorta_measures = measure_content.ContentSequence
        for measure in aorta_measures:
            # Each measure is itself a sequence of data describing where the measure is taken and the value
            if not hasattr(measure, "ContentSequence"):
                continue
            measure_content = measure.ContentSequence
            diameter_sequence = "RID13432"
            site_location = None
            diameter = None
            # Loop through the sequences to pull out the location and the diameter
            for sequence in measure_content:
                seq_code = sequence.ConceptNameCodeSequence[0].CodeValue
                if seq_code == self.finding_site_sequence:
                    site_code = sequence.ConceptCodeSequence[0].CodeValue
                    # This is just the final code for PACS - not a meausurement
                    if site_code == "RID480":
                        continue
                    site_location = location_code_map.get(
                        site_code,
                        f"{sequence.ConceptCodeSequence[0].CodeValue}, {sequence.ConceptCodeSequence[0].CodeMeaning}",
                    )
                if seq_code == diameter_sequence:
                    # This is the measurement
                    diameter = int(sequence.MeasuredValueSequence[0].NumericValue)
            # If we have both the location and the diameter, add it to the dictionary
            if site_location is not None and diameter is not None:
                diameters[site_location] = diameter
        if not diameters:
            logger.debug(
                f"No aortic diameters found in {self.current_filename} aortic measure report"
            )
            return None
        return diameters

    def _extract_lung_lesion_measurements(
        self, measure_content: dcm.DataElement
    ) -> dict:
        """Extract the lung lesion measurements from the dicom data
        :param measure_content: the dicom data
        :return: a dictionary of the lung lesion measurements
        """
        # Get the measurements
        lesion_data = {}
        if not hasattr(measure_content, "ContentSequence"):
            logger.debug(
                f"No lung lesion measurements found in {self.current_filename}"
            )
            return None
        lesion_list = measure_content.ContentSequence
        # lesion_data['lesion_count'] = len(lesion_list)
        for idx, lesion in enumerate(lesion_list):
            lesion_id, lesion_measurements = self._extract_lung_lesion_measurement(
                lesion, idx
            )
            if lesion_id == 'No finding':
                # Skip this lesion as it is not a valid measurement
                continue
            if lesion_id is not None and lesion_measurements is not None:
                lesion_data[lesion_id] = lesion_measurements
        if not lesion_data:
            return None
        return lesion_data

    def _extract_lung_lesion_measurement(
        self, lesion: dcm.DataElement, idx: int
    ) -> tuple[str, dict]:
        if not hasattr(lesion, "ContentSequence"):
            logger.debug(
                f"No ContentSequence found in {self.current_filename} for lesion {idx}"
            )
            return None, None
        lesion_review_status_code = "CHESTCT0102"
        measurement_type_map = {
            "103339001": "max_2d_diameter_mm",
            "103340004": "min_2d_diameter_mm",
            "RID50155": "mean_2d_diameter_mm",
            "L0JK": "max_3d_diameter_mm",
            "RID28668": "volume_mm3",
        }
        lesion_measurements = {
            "location": None,
            "review_status": None,
            "max_2d_diameter_mm": None,
            "min_2d_diameter_mm": None,
            "mean_2d_diameter_mm": None,
            "max_3d_diameter_mm": None,
            "volume_mm3": None,
        }
        lobe_map = {
            "Upper lobe of left lung": "left_upper_lobe",
            "Lower lobe of left lung": "left_lower_lobe",
            "Upper lobe of right lung": "right_upper_lobe",
            "Middle lobe of right lung": "right_middle_lobe",
            "Lower lobe of right lung": "right_lower_lobe",
        }
        for seq in lesion.ContentSequence:
            descriptor = seq.ConceptNameCodeSequence[0]
            # Get the lesion ID
            if descriptor.CodeValue == self.tracking_code:
                lesion_id = seq.TextValue
            # Get the location
            if descriptor.CodeValue == self.finding_site_sequence:
                location = seq.ContentSequence[0].ConceptCodeSequence[0].CodeMeaning
                lesion_measurements["location"] = lobe_map.get(location, location)
            # Get the review status
            if descriptor.CodeValue == lesion_review_status_code:
                if seq.TextValue in (
                    "Measurement accepted",
                    "Measurement auto-confirmed",
                ):
                    review_status = "accepted"
                else:
                    review_status = seq.TextValue
                lesion_measurements["review_status"] = review_status
            if descriptor.CodeValue in measurement_type_map:
                measurement_type = measurement_type_map[descriptor.CodeValue]
                # Get the value
                if hasattr(seq, "MeasuredValueSequence"):
                    measurement_value = seq.MeasuredValueSequence[0].NumericValue
                    lesion_measurements[measurement_type] = float(measurement_value)
                else:
                    lesion_measurements[measurement_type] = None
        return lesion_id, lesion_measurements

    def _extract_lung_parenchyma_measurements(
        self, measure_content: dcm.DataElement
    ) -> dict:
        """Extract the lung parenchyma measurements from the dicom data
        :param measure_content: the dicom data
        :return: a dictionary of the lung parenchyma measurements
        """
        # Get the measurements
        if not hasattr(measure_content, "ContentSequence"):
            logger.debug(
                f"No parenchyma measurements found in {self.current_filename}"
            )
            return None
        parenchyma_data = {}

        meausure_code = "CHESTCT0201"
        for location in measure_content.ContentSequence:
            location_content = location.ContentSequence
            for seq in location_content:
                descriptor = seq.ConceptNameCodeSequence[0]
                if descriptor.CodeValue == self.tracking_code:
                    location_id = self.lung_location_map.get(seq.TextValue)
                    if location_id is None:
                        continue
                if descriptor.CodeValue == meausure_code:
                    if not hasattr(seq, "MeasuredValueSequence"):
                        # If there is no measurement, skip this sequence
                        continue
                    # Get the measurement value
                    measurement_value = seq.MeasuredValueSequence[0].NumericValue
                    parenchyma_data[location_id] = {
                        "low_parenchyma_hu_percent": float(measurement_value)
                    }
        return parenchyma_data

    def _extract_coronary_calcium_measurements(
        self, measure_content: dcm.DataElement
    ) -> dict:
        """Extract the coronary calcium measurements from the dicom data
        :param measure_content: the dicom data
        :return: a dictionary of the coronary calcium measurements
        """
        calc_data = {}
        if not hasattr(measure_content, "ContentSequence"):
            logger.debug(
                f"No coronary calcium measurements found in {self.current_filename}"
            )
            return None
        for measure in measure_content.ContentSequence:
            measure_name = None
            measure_value = None
            for seq in measure.ContentSequence:
                if seq.ConceptNameCodeSequence[0].CodeValue == self.tracking_code:
                    # This is the location
                    if seq.TextValue == "Heart":
                        measure_name = "heart_volume_cm3"
                    elif seq.TextValue == "Calcium score":
                        measure_name = "coronary_calcification_volume_mm3"
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
        spine_data = {}
        if not hasattr(measure_content, "ContentSequence"):
            logger.debug(f"No spine measurements found in {self.current_filename}")
            return None
        # Each sequence in this content is a vertebra's measurements
        for vertebra in measure_content.ContentSequence:
            vertebra_name, vertebra_measurements = self._extract_vertebra_measurement(
                vertebra
            )
            if not vertebra_name or not vertebra_measurements:
                continue
            # If we have a vertebra name and measurements, add them to the dictionary
            spine_data[vertebra_name] = vertebra_measurements
        return spine_data

    def _extract_vertebra_measurement(
        self, vertebra: dcm.DataElement
    ) -> tuple[str, dict]:
        """Extract the vertebra measurements from the dicom data
        :param vertebra: the vertebra content sequence
        :return: a tuple of the vertebra name and the measurements
        """
        if not hasattr(vertebra, "ContentSequence"):
            logger.warning(
                f"No ContentSequence found in for a vertebra in {self.current_filename}"
            )
            return None, None
        # These are the internal codes used by the AIRC for the spine measurements
        measurement_seq_code = "121207"
        direction_code = "106233006"
        status_code = "CHECTCT0001"

        vertebra_name = None
        vertebra_measurements = {}
        for seq in vertebra.ContentSequence:
            descriptor = seq.ConceptNameCodeSequence[0]
            if descriptor.CodeValue == self.tracking_code:
                vertebra_name = seq.TextValue
            if descriptor.CodeValue == measurement_seq_code:
                if not hasattr(seq, "MeasuredValueSequence"):
                    # If there is no measurement, skip this sequence
                    continue
                if not hasattr(seq, "ContentSequence"):
                    # If there is no content sequence, skip this sequence as we need to know the direction and status
                    continue
                # Get the measurement value
                measurement_value = seq.MeasuredValueSequence[0].NumericValue
                direction = None
                status = None
                for content in seq.ContentSequence:
                    if content.ConceptNameCodeSequence[0].CodeValue == direction_code:
                        direction = content.ConceptCodeSequence[0].CodeMeaning.lower()

                    if content.ConceptNameCodeSequence[0].CodeValue == status_code:
                        status = content.ConceptCodeSequence[0].CodeMeaning.lower()
                if direction is None or status is None:
                    # If we don't have a direction or status, skip this measurement
                    continue
                vertebra_measurements[direction] = {
                    "length_mm": float(measurement_value),
                    "status": status,
                }
        if not vertebra_name or not vertebra_measurements:
            # logger.warning(
            #     f"No vertebra name or measurements found for a vertebra in {self.current_filename}"
            # )
            return None, None
        return vertebra_name, vertebra_measurements

    def _extract_pulmonary_density_measurements(
        self, measure_content: dcm.DataElement
    ) -> dict:
        """Extract the pulmonary density measurements from the dicom data
        :param measure_content: the dicom data
        :return: a dictionary of the pulmonary density measurements
        """
        # Get the measurements
        if not hasattr(measure_content, "ContentSequence"):
            logger.debug(
                f"No parenchyma measurements found in {self.current_filename}"
            )
            return None
        density_data = {}
        density_code_map = {
            "CHESTCT0601": "opacity_score",
            "CHESTCT0602": "volume_cm3",
            "CHESTCT0603": "opacity_volume_cm3",
            "CHESTCT0604": "opacity_percent",
            "CHESTCT0605": "high_opacity_volume_cm3",
            "CHESTCT0606": "high_opacity_percent",
            "CHESTCT0607": "mean_hu",
            "CHESTCT0608": "mean_hu_opacity",
        }
        for location in measure_content.ContentSequence:
            location_data = {}
            location_id = None
            location_content = location.ContentSequence
            for seq in location_content:
                descriptor = seq.ConceptNameCodeSequence[0]
                if descriptor.CodeValue == self.tracking_code:
                    location_id = self.lung_location_map.get(seq.TextValue)
                if descriptor.CodeValue in density_code_map:
                    meausure_name = density_code_map[descriptor.CodeValue]
                    if not hasattr(seq, "MeasuredValueSequence"):
                        # If there is no measurement, skip this sequence
                        continue
                    # Get the measurement value
                    measurement_value = seq.MeasuredValueSequence[0].NumericValue
                    if measurement_value == "n/a":
                        measurement_value = None
                    else:
                        measurement_value = float(measurement_value)
                    location_data[meausure_name] = measurement_value
            if location_id is not None and location_data:
                density_data[location_id] = location_data
        if not density_data:
            logger.warning(
                f"No pulmonary density measurements found in {self.current_filename}"
            )
            return None
        return density_data
