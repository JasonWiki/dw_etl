#coding=utf-8

u'''
日志级别
    CRITICAL > ERROR > WARNING > INFO > DEBUG > NOTSET
'''
import logging


class Logger:

     @staticmethod
     def init(level = logging.DEBUG):
        logging.basicConfig(level=level,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S')

     @staticmethod
     def debug(message):
        logging.debug(message)

     @staticmethod
     def info(message):
         logging.info(message)

     # 警告
     @staticmethod
     def warning(message):
         logging.warning(message)

     # 错误的
     @staticmethod
     def error(message):
         logging.error(message)

     # 危险的
     @staticmethod
     def critical(message):
         logging.critical(message)
