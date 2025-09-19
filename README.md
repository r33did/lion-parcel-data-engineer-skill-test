# lion-parcel-data-engineer-skill-test
Technical Test submission for Lion Parcel - Data Engineer


For the first task i use postgresql as a source data, clickhouse for the data warehouse, and python for the ETL in `etl_job.py` .

I dockerize the whole services but in the same network (for my convinience, if you want to i can make whole new topology to simulate diff networking). The ETL services run hourly using CRON setup in the dockerfile so we can easily change that setup.

If you want to run this on your side, you must create the .env for yourself, _which is i ignore in this repo_, if you want to i can share it after we discuss in our next interview.

Honestly, my work involved use of AI, i'm not proud of it but also i'm covering that from you guys (there is some inconvenience of time on my side). If you still want to test my technical skill in other way, please do!

And also i finished the bonus task to cleanse json file using python and pandas stack in `bonus_etl_cleanse_json.py`