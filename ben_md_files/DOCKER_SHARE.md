# Sharing the Docker Container with Teammates

If a teammate cannot set up the environment locally, you can export the entire Docker container (MySQL data, code, venv, everything) and share it as a single file.

---

## On your machine — export the container

**1. Find your container ID:**

```bash
docker ps -a
```

**2. Commit the container's current state to an image:**

```bash
docker commit <container-id> bt4301-group10
```

**3. Save the image to a tar file:**

```bash
docker save bt4301-group10 > bt4301-group10.tar
```

**4. Share the `.tar` file** with your teammate via USB, Google Drive, etc.

---

## On your teammate's machine — load and run it

**1. Load the image:**

```bash
docker load < bt4301-group10.tar
```

**2. Run the container:**

```bash
docker run -it -p 8089:8089 bt4301-group10 /bin/bash
```

**3. Once inside the container, start MySQL and Airflow:**

```bash
service mysql start
bash /root/start_labs.sh
```

**4. Open the Airflow UI** at `http://localhost:8089`

- **Username:** `admin`
- **Password:** found inside the container at `~/bt4301/BT4301-Project-Group-10/dataops/airflow/simple_auth_manager_passwords.json.generated`

---

## Caveats

| Issue | Detail |
|---|---|
| **File size** | The image will likely be 3–6 GB+ (venv, 1M-row CSV, and MySQL data all included) |
| **MySQL doesn't auto-start** | Must run `service mysql start` manually each time the container starts |
| **Airflow password** | Share the generated password file or have your teammate check it inside the container |
| **Port conflict** | If port `8089` is already in use on their machine, change the mapping: `-p <other-port>:8089` |

---

## Lighter alternative — share just the database dump

If you only need to share the MySQL data (not the full environment), export a dump instead:

**Export:**

```bash
mysqldump -u bt4301 -ppassword customer_churn > customer_churn_dump.sql
```

**Teammate imports it** after completing the normal setup guide (`AIRFLOW_SETUP.md`):

```bash
mysql -u bt4301 -ppassword customer_churn < customer_churn_dump.sql
```

This is much smaller than sharing the full container image.
