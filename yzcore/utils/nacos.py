"""
nacos sdk 支持版本为Python 2.7 Python 3.6 Python 3.7
"""
import nacos
import logging

class BaseNacosClient:
    def __init__(self,nacos_url:str,username:str,password:str,service_name:str,nacos_ip:str=None,nacos_port:int=None,namespace:str=None,**kwargs):
        """
        param: url (url of nacos)
        param: namespace
        param: username (username of nacos)
        param: password (password of nacos)
        param: service_name
        param: ip (ip of nacos)
        param: port (port of nacos)
        """
        self.url = nacos_url
        self.namespace = namespace
        self.username = username
        self.password = password
        self.service_name = service_name
        self.nacos_ip = nacos_ip
        self.nacos_port = nacos_port
        self.client = nacos.NacosClient(nacos_url,namespace=namespace,username=username,password=password)

    async def send_heartbeat(self):
        """向nacos发送心跳"""
        try:
            self.client.send_heartbeat(
                self.service_name,
                ip=self.nacos_ip,
                port=self.nacos_port
                )
        except Exception:
            # 如果没有收到心跳返回信息则重新注册
            self.client.add_naming_instance(
                service_name=self.service_name,
                ip=self.nacos_ip,
                port=self.nacos_port
            )


    def register_nacos(self):
        """微服务注册nacos"""
        try:
            self.client.add_naming_instance(
                service_name=self.service_name,
                ip=self.nacos_ip,
                port=self.nacos_port
            )
        except Exception as ex:
            logging.error("Failed to register to nacos : %s", ex)
