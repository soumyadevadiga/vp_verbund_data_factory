## VP Verbund Lead Model ETL Process

### Table of Contents

- [VP Verbund Lead Model ETL Process](#vp-verbund-lead-model-etl-process)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Installation and setup](#installation-and-setup)
  - [Usage](#usage)
  - [Sample output](#sample-output)
  - [Have Questions?](#have-questions)

### Introduction
This repository contains the code and instructions for creating a data pipeline to extract, transform and load the lead data. The ETL for lead events is performed using Azure Data Factory, Snowflake and Python.

All the tasks are performed according to the description provided in [Data Engineer Task VP](Data_Engineer_Task_VP.pdf)

### Installation and setup

The SQL import, Snowflake and Azure Data Factory pipeline is setup according to the task description. The script files for these tasks are provided in the folders ``SQL_scripts`` and ``azure_data_factory`` respectively.

Data transformation is performed in python. The script files for transformation are provided in the folder ``BIT``. These python scripts have some dependencies for the packages, which can be installed using either of the following based on the user environment:

```bash
# Use the terminal and enter the following command for installing the dependencies
pip install -r requirements.txt
```

**or** 

```bash
# Use the terminal or an Anaconda Prompt for the following steps:
# 1. Create the environment from the "BIT/src/environment.yml" file:
conda env create -f environment.yml

# The first line of the yml file sets the new environment's name, in this case etl_env
# 2. Activate the new environment: 
conda activate etl_env

# 3. Verify that the new environment was installed correctly:
conda env list
```

The connection parameters for the Snowflake DB can be configured using the ``JSON`` file ``BIT/src/connection.json``. Other variables or parameters for example: source and destination table names, column names, and other constants can be configured using the ``JSON`` file ``BIT/src/config.json``.


### Usage
```bash
# Example usage for the python script from the terminal within the "BIT/src/." folder
python .\main.py
```  

### Sample output
```
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
|"EVENTTYPE"                |"EVENTEMPLOYEE"      |"EVENTDATE"          |"LEADID"                              |"UPDATEDDATEUTC"     |"ID"                                  |
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
|LeadCancelled              |hij@pflegehilfe.de   |2023-08-01 10:24:00  |1A527929-D64C-C031-104D-08DB8D27C11A  |2023-08-01 10:24:00  |aa5a483a-4f6d-4f40-9120-9b3c84ae4529  |
|LeadSold                   |xyz@pflegehilfe.de   |2023-07-25 15:56:49  |1A527929-D64C-C031-104D-08DB8D27C11A  |2023-08-01 10:24:00  |e5176687-3e81-442c-9069-9497f36cfb3d  |
|LeadRequestedCancellation  |Unknown              |2023-08-01 10:24:00  |028BC4B5-4FC6-CE70-2F06-08DB903B8B25  |2023-08-01 10:24:00  |556ca624-6d8a-4b5d-8515-d27fd5ee15c1  |
|LeadSold                   |abcd@pflegehilfe.de  |2023-08-01 10:21:14  |C817066B-AA29-CD75-D786-08DB92790896  |2023-08-01 10:21:14  |7f9e26ba-18da-4c9f-98f4-aed3c31377e4  |
|LeadRequestedCancellation  |Unknown              |2023-08-01 09:46:00  |38E2EC8E-3C1E-C414-CD02-08DB926F32FD  |2023-08-01 09:46:00  |af9ca0fe-088b-4cae-a0be-016ae62077de  |
|LeadRequestedCancellation  |Unknown              |2023-08-01 10:24:00  |1A527929-D64C-C031-104D-08DB8D27C11A  |2023-08-01 10:24:00  |ecebb364-f073-4b9b-b674-2181562ae19a  |
|LeadSold                   |efg@pflegehilfe.de   |2023-08-01 09:10:00  |38E2EC8E-3C1E-C414-CD02-08DB926F32FD  |2023-08-01 09:46:00  |9238262d-2b0d-4707-9552-5773b4585ef2  |
|LeadSold                   |xyz@pflegehilfe.de   |2023-07-29 13:56:02  |028BC4B5-4FC6-CE70-2F06-08DB903B8B25  |2023-08-01 10:24:00  |ad5ff903-82ac-42c3-86bd-1d4c83e91ef7  |
|LeadCancellationRejected   |Unknown              |2023-08-01 10:24:00  |028BC4B5-4FC6-CE70-2F06-08DB903B8B25  |2023-08-01 10:24:00  |630fbc6d-010e-4857-abcb-266571540859  |
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

### Have Questions?

Contact: soumya.mdevadiga@gmail.com