BT4301 Project – Group 10

### 1. Prerequisites

- **Operating system (recommended)**:
  - Ubuntu running **inside Docker** (e.g. dev container), or
  - Ubuntu on Windows via **WSL2 (Windows Subsystem for Linux)**.
- **Python**: 3.10 (or compatible 3.10.x)
- **MySQL Server**: 8.x (or compatible)
- **pip**: bundled with Python

You should have MySQL credentials (host, port, user, password, database) that you can use for this project.

Follow the **BT4301 SLAs Week 6** document for:

- Installing **MySQL** on Ubuntu.
- Ensuring the MySQL service is running and accessible from your Ubuntu environment.

### 1.1. Clone this repository in Ubuntu

Inside your Ubuntu environment (Docker container or WSL2):

```bash
# Create a workspace folder (only once)
mkdir -p ~/bt4301
cd ~/bt4301

# Clone this repository
git clone <REPOSITORY_URL> BT4301-Project-Group-10
cd BT4301-Project-Group-10
```

Replace `<REPOSITORY_URL>` with the Git URL provided for your group (e.g. from GitHub or your course Git server).

### 2. Create a Python venv and install dependencies

In your **remote Ubuntu environment** (e.g. Docker or WSL2), from the project root:

```bash
cd BT4301-Project-Group-10   # or your clone path

# Create a Python virtual environment (only once)
python3 -m venv venv

# Activate the venv (required every time you open a new terminal)
source venv/bin/activate

# Install dependencies from dataops
pip install -r dataops/requirements.txt
```

To deactivate the venv later: `deactivate`.

For Jupyter notebook usage from this venv, install `ipykernel` (if not already installed) and register the kernel:

```bash
pip install ipykernel
python -m ipykernel install --user --name bt4301-venv --display-name "Python (bt4301-venv)"
```

Then, in your notebook UI, select the kernel **“Python (bt4301-venv)”** so the notebook uses this virtual environment.

### 3. Start MySQL (Ubuntu / Docker)

In the **same Ubuntu terminal** (or before running the pipeline), start the MySQL service:

```bash
sudo service mysql start
```

If you use **Ubuntu in Docker**, run this inside the container so the notebook can connect to `localhost` MySQL. Check status with: `sudo service mysql status`.

### 4. MySQL setup

1. **Create database**

   In MySQL:

   ```sql
   CREATE DATABASE home_credit;
   USE home_credit;
   ```

2. **Configure connection in the code**

   In the `dataops/home_credit_pipeline.ipynb` notebook, ensure the `MYSQL_CONFIG` (or equivalent config) matches your MySQL instance, for example:

   ```python
   MYSQL_CONFIG = {
       "host": "localhost",
       "user": "your_user",
       "password": "your_password",
       "database": "home_credit",
       "port": 3306,
   }
   ```

### 5. SQLTools connection (view ingested tables)

Use **SQLTools** in Cursor/VS Code to connect to MySQL and view the tables after the pipeline has ingested data.

1. **Install SQLTools and the MySQL driver**
   - Install the **SQLTools** extension (and **SQLTools MySQL/MariaDB** or **SQLTools MySQL** driver) from the Extensions view in Cursor/VS Code.

2. **Create a new connection**
   - Open the **SQLTools** sidebar (database icon or “SQLTools” in the activity bar).
   - Click **“Add new connection”** (or the + icon).
   - Choose **MySQL** (or MySQL/MariaDB) as the driver.
   - Fill in the connection details to match your `MYSQL_CONFIG` (and the database where the pipeline writes):
     - **Host:** `localhost` (or your MySQL host; if MySQL runs inside Docker, use the host that Cursor can reach, e.g. `localhost` if port 3306 is forwarded).
     - **Port:** `3306`
     - **User:** same as in `MYSQL_CONFIG` (e.g. `bt4301`).
     - **Password:** same as in `MYSQL_CONFIG`.
     - **Database:** `home_credit`
   - Give the connection a name (e.g. “Home Credit”) and save.

3. **Connect and view tables**
   - Select the new connection and click **Connect** (or right‑click → Connect).
   - Once connected, expand the connection → **home_credit** → **Tables** to see the ingested tables: **application_train**, **application_test**, **bureau**, **bureau_balance**.
   - You can run `SELECT` queries or open table data from the explorer.

Ensure MySQL is running (`sudo service mysql start` in Ubuntu/Docker) and that the host/port are reachable from where Cursor runs (e.g. port 3306 forwarded from the container if MySQL is inside Docker).

### 6. Running the pipeline notebook

1. In your **remote Ubuntu terminal** (Docker or WSL2):
   - Activate the venv and ensure MySQL is running:

   ```bash
   cd BT4301-Project-Group-10
   source venv/bin/activate
   sudo service mysql start
   ```

2. Start your notebook server (if needed) and open `dataops/home_credit_pipeline.ipynb`.

3. Ensure the kernel is **“Python (bt4301-venv)”**.

4. Run the notebook cells in order:
   - Environment and imports
   - Database connection and table creation
   - Data loading
   - Feature engineering / modelling steps

After the pipeline has loaded data, use the **SQLTools** connection (Section 5) to confirm the tables **application_train**, **application_test**, **bureau**, and **bureau_balance** are present and to run queries.

This should fully set up the Python environment, MySQL database, SQLTools connection, and notebook execution for the project.