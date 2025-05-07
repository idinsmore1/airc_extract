# AI-Rad Companion Chest CT Extractor

This is a python library and command line tool for extracting the results of Siemens' AI-Rad companion Chest CT software into a relational database.  
*NOTICE: Command line tool only for use with an appropriate DicomConquest PACS*  

To install, simply use  
```bash
pip install airc_extract
```

This will install the package and the two necessary command line programs to your python environment. 
## Usage
### Python API
If you simply want to use the extraction method stored
in the `AIRCReport` class, you can import and use it in any python script with
```python
from airc_extract.airc_report import AIRCReport
# This is the list of structured report (SR) dicoms that make up the AIRC Chest CT output
report_dicoms = ['dcm1.dcm', 'dcm2.dcm', ...]
report = AIRCReport(report_dicoms)
report.extract_report()  # This is the method to pull the data
print(report.report_data)  # A dictionary containing the results
```
### Command Line
If you are using a [DicomConquest](https://github.com/marcelvanherk/Conquest-DICOM-Server) server, you can use the two command line tools provided 
in this package to automatically create and update a SQLite database, storing the AIRC results in 6 tables. More information on these tables can be found
[here](https://github.com/idinsmore1/airc_extract/edit/main/src/airc_extract/db_ops.py).  

To start, you will need to run `airc-create-config`.
```bash
airc-create-config \
--dicom-db /path/to/conquest.db3 \ # Required (usually dicomserver/data/dbase/conquest.db3)
--dicom-data-dir /path/to/conquest/data \ # Required (usually dicomserver/data)
--data-db /path/to/output/database.db3 \ # Required. Can NOT be on a network share.
--log-level-term INFO # Terminal logging level \
--log-level-file DEBUG # Log file logging level \
--log-dir . # Directory for log files to be stored
```
This will setup a package `config.ini` file that will be used for extraction, as well as test the connection to the DicomConquest database and the output database. 
It will create a new output database with all necessary tables at the specified path if it does not exist.  

After this, just run 
```bash
airc-extract
```
and all the series not found in the output database will be extracted and inserted into the output database.
