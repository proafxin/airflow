# Basic Apache Airflow usage

## Environment setup

Use a virtual environment `virtualenv env`. Activate it using `source env/bin/activate` on linux/mac or `./env/bin/activate` on windows. Then run `pip install -r requirements.txt` to install necessary packages. The config files were generated for the first time using `airflow db init`. However, the `airflow.db` file is ignored here. User creation: `airflow users create  --username masum  --firstname Masum  --lastname Billal  --role Admin  --email billalmasum93@gmail.com`.
