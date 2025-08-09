import os
import json
import requests
import snowflake.connector
from dotenv import load_dotenv

# Load credentials from .env 
load_dotenv()

ctx = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

cs = ctx.cursor()
insert_sql = "INSERT INTO raw_data.state_demographics_raw(survey_year, json_payload) SELECT %s, PARSE_JSON(%s)"

YEARS = [2018, 2019, 2020, 2021, 2022, 2023]
FIELDS = "NAME,B19013_001E,B01003_001E"

for year in YEARS:
    url = f"https://api.census.gov/data/{year}/acs/acs5?get={FIELDS}&for=state:*"
    data = requests.get(url).json()
    for row in data[1:]:
        cs.execute(insert_sql, [year, json.dumps(row)])

ctx.commit()
cs.close()
ctx.close()