import os
from yzcore.default_settings import default_setting as settings
from yzcore.logger import get_logger

logger = get_logger(__name__)


def db_connect_test(pg_engine=None, redis_conn=None, mongo_client=None, is_raise=False):
    """
    检测数据库连接
    :param pg_engine: pg连接实例
    :param redis_conn: redis连接实例
    :param mongo_client: mongodb连接实例
    :param is_raise: True 抛出异常 / False 杀掉进程引起k8s的pod异常
    """
    conn = False
    for i in range(3):
        try:
            if pg_engine:
                conn = pg_engine.connect()
                print(f'postgresql version: {".".join(map(str, conn.dialect.server_version_info))}')
                conn.close()
                break
            elif redis_conn:
                print(f'redis version: {redis_conn.info("Server")["redis_version"]}')
                break
            elif mongo_client:
                print(f'mongodb version: {mongo_client.server_info()["version"]}')
                break
        except Exception as e:
            logger.error(e)

            # 重试3次后，仍然连不上直接干掉端口和引起k8s的pod异常
            if not conn and i == 2:
                if is_raise:
                    raise e
                base_url = settings.BASE_URL
                port = base_url.split(":")[-1]
                command = "kill -9 $(netstat -nlp | grep :" + str(
                    port) + " | awk '{print $7}' | awk -F'/' '{{ print $1 }}')"
                os.system(command)
        else:
            conn = True
    return conn
