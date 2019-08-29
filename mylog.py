# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

# 日志
import logging
# import coloredlogs
# coloredlogs.install()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s %(filename)s:%(levelname)s:%(message)s",datefmt="%d-%M-%Y %H:%M:%S")
ch.setFormatter(formatter)
logger.addHandler(ch)