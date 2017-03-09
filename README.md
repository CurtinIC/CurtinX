# CurtinX
The folder contains a python script to process the CurtinX files within the EventData folder to output a CSV file for each defined course. The fields of the parsed event files are translate into columns of the outputted CSV file. It consists of a setting file "curtinSettings.py" and of the actual script "eventExtractor.py".

## Getting Started
The following modules needs to be installed:
* pandas
* numpy

The files needs to included within the unzipped edX folder and will create a "ObfuscatedCSV" folder in the same path with the CSV inside:
```{r basicconsole}
/curtinX unzipped folder
   /curtinx-2016-06-26 (or whichever date the logs were downloaded)
      /...
   /EventData
      /...
   curtinSettings.py
   eventExtractor.py
   /ObfuscatedCSV
      /course1.CSV
      /course2.CSV
      /...
```

The script can then be run simply with
```{r basicconsole}
python eventExtractor.py
```

## Settings
There are a number of parameters that can be tuned in order to modify the way files are processed.

### curtinSettings.py
If the script is located in a different folder or if the folders within the unzipped CurtinX logs have a different names, the following lines have to be edited accordingly:
```{r basicconsole}
folder_base = os.path.dirname(os.path.realpath("__file__"))
folder_preprocessed_mooc = folder_base + "/ObfuscatedCSV"
if os.path.isdir(folder_preprocessed_mooc) == False: # if no "ObfuscatedCSV" folder, create it
	os.makedirs(folder_preprocessed_mooc)
folder_sql = folder_base + "/curtinx-2016-06-26" # "folder_sql" points to the folder with all sql files
folder_logs = folder_base + "/EventData" # "folder_logs" points at the folder with the event files to analyze
```

The courses for which a CSV file has to produced have to be listed in:
```
moocs_available = ["course-v1:CurtinX+MKT1x+1T2016", "course-v1:CurtinX+DBAEx+3T2015"]
```

and for each of these, there should be a class definition (name convention "course-v1:CurtinX+MKT1x+1T2016" -> "MKT1T1T2016") and a dedicated if case in the function "get(course)" which point to the right class.

#### class definition
To define a class for a new course, it is sufficient to copy and paste one of the existing class. Certain paramethers of the class have to be modified accordingly:
* self.staff: is a list containing the emails of anyone who is not a regular enrolled student for the course. Users whose emails is listed here will not appear in the CSV file (because we are interested in student interactions mostly). If empty, no users will be skipped;
* self.enroll_date: datetime of the date the course become open for enrollment. Format: datetime.datetime(YYYY, mm, dd, HH, MM, SS);
* self.start_date: datetime of the date the course start. Same format as self.enroll_date;
* self.end_date = datetime of the date the course ended. Same format as self.enroll_date.

### eventExtractor.py
The script is "ready to use" as it is. The core function is the "event_reader", which lunc parallel jobs to read the files in parallel. The number of parallel jobs is specified by the "nJobs" variable:
```
nJobs = multiprocessing.cpu_count() - 2
```

The commented pieces of code includes specific functionality to filter out certain interactions. Particularly, the "lof_cleaner(logs)" function, allows to modify which columns to keep and which interactions to discard. It is sufficient to outcomment and eventually edit the particular piece of code.
