import os

from dotenv import load_dotenv


cwd = os.getcwd()
dotenv_path = os.path.join(cwd, os.getenv('ENVIRONMENT_FILE', '.env.development'))
load_dotenv(dotenv_path=dotenv_path, override=True)

BUCKET_NAME = os.environ.get('BUCKET_NAME')
OBJECT_NAME = os.environ.get('OBJECT_NAME')
QUEUE_URL_LIST = os.environ.get('QUEUE_URL_LIST').split(',') if os.environ.get('QUEUE_URL_LIST') is not None else []
APP_CONSUMER_ID = os.environ.get('APP_CONSUMER_ID').split(',') if os.environ.get('APP_CONSUMER_ID') is not None else []
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
DB_NAME = os.environ.get('DB_NAME')
DB_USERNAME = os.environ.get('USERNAME')
DB_PASSWORD = os.environ.get('PASSWORD')
SQL_QUERY_GET_TIDNID = os.environ.get('SQL_QUERY_GET_TIDNID')
SQL_QUERY_GET_EVENTS = os.environ.get('SQL_QUERY_GET_EVENTS')
SQL_QUERY_SAVE_EVENTS = os.environ.get('SQL_QUERY_SAVE_EVENTS')
