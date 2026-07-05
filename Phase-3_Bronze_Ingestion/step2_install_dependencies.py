"""
=============================================================================
PHASE 3 — STEP 2: INSTALL SQL SERVER ODBC DRIVER + PYTHON PACKAGES
=============================================================================
Purpose  : Install Microsoft ODBC Driver 18 for SQL Server on Ubuntu
           and all Python packages needed to connect to RDS
Run      : python step2_install_dependencies.py
           (this script prints the commands — run each in terminal)
=============================================================================
"""

INSTRUCTIONS = """
╔══════════════════════════════════════════════════════════════════╗
║  PHASE 3 — STEP 2: Install SQL Server ODBC Driver on Ubuntu     ║
╚══════════════════════════════════════════════════════════════════╝

Run ALL commands below in your Ubuntu terminal IN ORDER.
Copy-paste each block, wait for it to finish, then do the next.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCK 1: Install Microsoft ODBC Driver 18 for SQL Server
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -

curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | \\
    sudo tee /etc/apt/sources.list.d/mssql-release.list

sudo apt-get update

sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

sudo apt-get install -y unixodbc-dev

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCK 2: Verify ODBC driver installed correctly
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

odbcinst -q -d -n "ODBC Driver 18 for SQL Server"

  Expected output:
  [ODBC Driver 18 for SQL Server]
  Description=Microsoft ODBC Driver 18 for SQL Server
  Driver=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.x.so.x.x

  If nothing shows: the install in Block 1 failed. Check internet connection.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCK 3: Install Python packages (in your virtual environment)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Make sure your venv is active first:
  source myenv/bin/activate   (adjust path if different)

Then install:
  pip install pyodbc==5.1.0
  pip install sqlalchemy==2.0.23
  pip install pandas boto3 pyarrow python-dotenv tabulate

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCK 4: Verify Python can import pyodbc
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

python3 -c "import pyodbc; print('pyodbc version:', pyodbc.version); print('Drivers:', pyodbc.drivers())"

  Expected output includes:
  pyodbc version: 5.x.x
  Drivers: ['ODBC Driver 18 for SQL Server']

  If Drivers list is empty: Block 1 install failed. Re-run Block 1.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TROUBLESHOOTING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ERROR: "curl: command not found"
FIX:   sudo apt-get install curl

ERROR: "Unable to locate package msodbcsql18"
FIX:   Check your Ubuntu version:  lsb_release -a
       If Ubuntu 24.04, use this URL instead:
       curl https://packages.microsoft.com/config/ubuntu/24.04/prod.list | \\
           sudo tee /etc/apt/sources.list.d/mssql-release.list
       Then re-run the apt-get update and install commands.

ERROR: "ImportError: libodbc.so.2: cannot open shared object file"
FIX:   sudo apt-get install unixodbc
       sudo ldconfig

AFTER ALL BLOCKS COMPLETE:
  Run: python step3_test_rds_connection.py
"""

print(INSTRUCTIONS)
