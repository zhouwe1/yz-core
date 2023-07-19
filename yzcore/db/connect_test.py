import os
from yzcore.default_settings import default_setting as settings
from yzcore.logger import get_logger

try:
    from yzcore.db.db_session import get_db_engine
except ImportError:
    pass

try:
    from pymongo import MongoClient
except ImportError:
    pass


logger = get_logger(__name__)


def db_connect_test(pg_uri=None, redis_conn=None, mongo_uri=None):
    conn = False
    for i in range(3):
        try:
            if pg_uri:
                engine = get_db_engine(pg_uri)
                conn = engine.connect()
                conn.close()
                print('pg 连接成功')
                break
            elif redis_conn:
                redis_conn.ping()
                print('redis 连接成功')
                break
            elif mongo_uri:
                client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
                print(client.server_info())
                print('mongo 连接成功')
                break
        except Exception as e:
            logger.error(e)

            # 重试3次后，仍然连不上直接干掉端口和引起k8s的pod异常
            if not conn and i == 2:
                base_url = settings.BASE_URL
                port = base_url.split(":")[-1]
                # command = "kill -9 $(netstat -nlp | grep :" + str(
                #     port) + " | awk '{print $7}' | awk -F'/' '{{ print $1 }}')"
                command = f"kill -kill $(lsof -t -i :{port})"
                print(command)
                os.system(command)
        else:
            conn = True
    return conn
