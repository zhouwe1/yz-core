# readme-template

--------------

## Introduction

yzcore 目的为了开发后端服务时，提供一种代码结构规范参考。
可以通过`startproject`和`startapp`两个命令快速创建工程和内部的接口应用模块。
**安装模块**
```shell
$ pip install yz-core2
```
示例：
- 创建工程：
```shell
$ yzcore startproject myproject
```
- 创建工程内部应用：
```shell
$ yzcore startapp myapp ./src/apps/
```

代码结构介绍：
```
.
├── docs		        说明文档、接口文档等文档的存放目录
├── migrations		    数据表迁移文件存放目录
├── src
│   ├── apps 接口应用程序的主目录
│   │   ├── __init__.py
│   │   ├── myapp01
│   │   │   ├── __init__.py
│   │   │   ├── controllers.py  控制层：封装数据交互操作
│   │   │   ├── models.py       模型层：实现数据表与模型的定义
│   │   │   ├── schemas.py      模式层：定义接口数据参数
│   │   │   ├── tests.py        测试文件
│   │   │   └── views.py        视图层：接口定义层
│   │   └── myapp02
│   ├── conf		配置文件的存放目录
│   ├── const		公共常量存放目录
│   ├── tests		测试文件的存放目录
│   ├── main.py		程序的入口文件
│   ├── settings.py	程序的设置文件
│   └── utils		抽离出的公共代码模块存放目录
├── .gitignore
├── requirements.txt
└── README.md
```

## Quick start

Quick Start 部分主要包括两部分内容：简易的安装部署说明(Deployment)和使用案例(Example)。特别是对于一些基础库，必须包括Example模块。


## Documentation

Documentation 部分是核心的文档，对于大型项目可以使用超链接，如使用以下这种形式：

For the full story, head over to the [documentation](https://git.k8s.io/community/contributors/devel#readme).

## 数据库迁移操作
```
# pip install alembic

alembic init migrations                             # 创建迁移环境
alembic revision --autogenerate -m "commit content" # 自动生成迁移文件
alembic upgrade head                                # 升级到最近版本
alembic upgrade <revision_id>                       # 升级到指定版本
alembic downgrade <revision_id>                     # 回退到指定版本
```