"""
=============================================================================
PHASE 3 — STEP 1: AWS CONSOLE INSTRUCTIONS — CREATE RDS SQL SERVER
=============================================================================
THIS IS A MANUAL STEP — DO IT IN THE AWS CONSOLE BROWSER
No script needed. Follow each instruction exactly in order.
Estimated time: 20-30 minutes
=============================================================================

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PART A: CREATE THE RDS SQL SERVER INSTANCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STEP 1: Open RDS Console
  → Go to: https://console.aws.amazon.com/rds
  → Make sure region is: ap-south-1 (Mumbai) — top right corner
  → Click: "Create database" (orange button)

STEP 2: Choose creation method
  → Select: "Standard create"
  → (NOT Easy create — we need full control)

STEP 3: Engine options
  → Engine type:    Microsoft SQL Server
  → Edition:        SQL Server Express Edition   ← FREE TIER
  → Version:        SQL Server 2019 (latest available)

STEP 4: Templates
  → Select: "Free tier"
  → This automatically limits to db.t3.micro (free for 12 months)

STEP 5: Settings
  → DB instance identifier:  ameriprise-bank-sqlserver
  → Master username:          admin
  → Master password:          BankAdmin#2025       ← remember this!
  → Confirm password:         BankAdmin#2025

STEP 6: Instance configuration
  → DB instance class: db.t3.micro   ← should be auto-selected by free tier
  → (1 vCPU, 1 GB RAM — enough for our project)

STEP 7: Storage
  → Storage type:          General Purpose SSD (gp2)
  → Allocated storage:     20 GB   ← minimum, free tier
  → Enable storage autoscaling: UNCHECK this (keep costs zero)

STEP 8: Connectivity
  → Virtual private cloud (VPC): Default VPC   ← keep default
  → DB subnet group:             default
  → Public access:               YES   ← IMPORTANT: set to YES
                                         (so you can connect from laptop)
  → VPC security group:          Create new
  → New VPC security group name: ameriprise-bank-rds-sg
  → Availability Zone:           No preference
  → RDS Proxy:                   Disabled

STEP 9: Database authentication
  → Select: Password authentication

STEP 10: Additional configuration
  → Initial database name:   LEAVE BLANK   (SQL Server creates master by default)
  → Backup retention:        1 day
  → Enable automated backups: checked
  → Monitoring: UNCHECK "Enable Enhanced monitoring" (costs extra)
  → Maintenance: defaults are fine

STEP 11: Create!
  → Click: "Create database" (orange button at bottom)
  → Wait 10-15 minutes for status to change from "Creating" to "Available"
  → DO NOT CLOSE the browser tab

STEP 12: Note your endpoint
  → Once status = "Available", click your DB: ameriprise-bank-sqlserver
  → Under "Connectivity & security" tab, copy the ENDPOINT:
    Example: ameriprise-bank-sqlserver.xxxxxxxxx.ap-south-1.rds.amazonaws.com
  → Copy this — you will need it in every script below


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PART B: CONFIGURE SECURITY GROUP (ALLOW YOUR LAPTOP TO CONNECT)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

By default, nobody can connect to RDS. You must open port 1433
(SQL Server port) for your specific IP address.

STEP 1: Find YOUR current public IP address
  → Open browser → go to: https://whatismyip.com
  → Note your IP: e.g. 103.45.67.89

STEP 2: Open Security Group
  → RDS Console → click your DB instance
  → "Connectivity & security" tab
  → Under "Security" → click the security group link
    (e.g. sg-xxxxxxxxx  ameriprise-bank-rds-sg)
  → This opens EC2 Console → Security Groups

STEP 3: Add inbound rule
  → Click "Inbound rules" tab
  → Click "Edit inbound rules"
  → Click "Add rule"
  → Fill in:
      Type:        Custom TCP
      Protocol:    TCP
      Port range:  1433
      Source:      My IP   ← click the dropdown, select "My IP"
                            (auto-fills your current IP)
      Description: My laptop - SQL Server access
  → Click "Save rules"

STEP 4: Verify
  → You should now see one inbound rule:
      Type: Custom TCP | Port: 1433 | Source: YOUR.IP.ADDRESS/32

NOTE: If your home/office IP changes (common with ISP),
      you need to update this rule again. Just edit and select "My IP".


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PART C: INSTALL DBEAVER (FREE SQL CLIENT FOR UBUNTU)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DBeaver is a free database GUI tool — like SSMS but works on Ubuntu.
You will use this to run your 3 SQL scripts visually.

INSTALL (run in Ubuntu terminal):
  sudo snap install dbeaver-ce

OR download from: https://dbeaver.io/download/
  → Choose: Linux Debian package (.deb)
  → sudo dpkg -i dbeaver-ce_*.deb

LAUNCH: Search "DBeaver" in Ubuntu applications

CONNECT TO RDS:
  → DBeaver → New Connection (plug icon top left)
  → Select: SQL Server
  → Fill in:
      Host:     YOUR-RDS-ENDPOINT.ap-south-1.rds.amazonaws.com
      Port:     1433
      Database: master
      Username: admin
      Password: BankAdmin#2025
  → Click "Test Connection"
  → If first time: DBeaver asks to download drivers → click Download
  → Should say "Connected"
  → Click Finish

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PART D: RUN THE 3 SQL SCRIPTS IN DBEAVER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

IMPORTANT: Run in this exact order. FK constraints require it.

Script 1 — Create Tables:
  → DBeaver → File → Open File → select 01_Create_Tables.sql
  → Press Ctrl+A (select all)
  → Press Ctrl+Enter (execute)
  → Expected: "banking" schema created, 4 tables created
  → Verify: Left panel → master → Schemas → banking → Tables
            Should show: branches, customers, accounts, transactions

Script 2 — Insert Historical Data:
  → DBeaver → File → Open File → select 02_Insert_Historical_data.sql
  → Press Ctrl+A → Ctrl+Enter
  → Expected: ~500 customers, 4 branches, 1000+ accounts, 30000+ transactions inserted
  → Takes 1-2 minutes to run

Script 3 — Insert Incremental Data:
  → DBeaver → File → Open File → select 03_Incrementat_data.sql
  → Press Ctrl+A → Ctrl+Enter
  → Expected: BR005 Chennai branch added, new customers added
  → Takes 30-60 seconds

VERIFY DATA LOADED:
  Run these quick check queries in DBeaver (New SQL script tab):

  SELECT COUNT(*) AS branch_count  FROM banking.branches;      -- Expected: 5
  SELECT COUNT(*) AS customer_count FROM banking.customers;    -- Expected: 500+
  SELECT COUNT(*) AS account_count  FROM banking.accounts;     -- Expected: 1000+
  SELECT COUNT(*) AS txn_count      FROM banking.transactions; -- Expected: 30000+
  SELECT * FROM banking.branches;  -- Should show BR001-BR005 including Chennai

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREDENTIALS SUMMARY (save this somewhere safe)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RDS Endpoint : ameriprise-bank-sqlserver.xxxxxxxx.ap-south-1.rds.amazonaws.com
Port         : 1433
Database     : master
Schema       : banking
Username     : admin
Password     : BankAdmin#2025
Security Grp : ameriprise-bank-rds-sg (port 1433 open to your IP)

NEXT STEP: Run step2_install_dependencies.py
"""

print(__doc__)
