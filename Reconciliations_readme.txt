STEP1: Create virtual environment inside project folder
A> get the miniconda file path. Open anaconda terminal and run find conda. Get the conda executable file path
B> Create a folder inside editor, open terminal and run & "C:\ProgramData\miniconda3\Scripts\conda.exe" create --prefix ./venv python=3.10
C> Initiate the conda inside project folder & "C:\ProgramData\miniconda3\Scripts\conda.exe" init powershell
D> Activate the venv conda activate ./venv

STEP2: Create a extraction.py for loading, cleaning. Use pandas/spark or whatever to make it easy.
Create a requirements.txt file and execute using pip install -r requirements.txt (can use docker for whole process if required)
db_config, schema, bootstrap, extraction (from different sources), executes extraction and performs manipulation before inserting into schema

STEP3: Use db.config for db initiation and engine and db url can be migrated to postgres later.

STEP4: extraction and transformation.py s these can be used for data loading and transformation for reading from different sources, transforming and loading with normalized checks.

STEP5: Finally executed using main.py() and using sql.py() to see output and DDL or DML

STEP6: for connecting to xero API(fetch only), first login to xero developer, connect to web application, provide company url:https://github.com/Arunjeet-htdigital and retrieve url:http://localhost:8080/callback. Then go to configurations and save client id "FE73E4C7FB2F477C94BC0BE1C70B511D" and client secret: "PO5kQcwtcOphCWAw41T3O8oawL0rUTZe9AV4QoMn6Cv6ZhRc" after this execute the run the bootsrap.py script after setting the below environment variables
$env:XERO_CLIENT_ID="FE73E4C7FB2F477C94BC0BE1C70B511D"
$env:XERO_CLIENT_SECRET="PO5kQcwtcOphCWAw41T3O8oawL0rUTZe9AV4QoMn6Cv6ZhRc"
$env:XERO_REDIRECT_URI "http://localhost:8080/callback"
WHILE GIVING AUTHORIZATION select your entity...
which stores as the environment variable and then run the xerobootstrap.py python file which activates the token credentials and starts the connection.(only once need to provide authorization) and finally run the script uninterrupted for 60 Days...


conn.executescript("""
BEGIN;




COMMIT;
""")

GET https://api.xero.com/api.xro/2.0/Reports/ProfitAndLoss?fromDate=2017-02-01&toDate=2017-02-28
