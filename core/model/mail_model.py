#coding=utf-8
u'''
日期模型对象，所有日期对象都从此类出
'''
from core.model.conf_model import ConfModel
from core.util.mail.mail import Mail

class MailModel:

    @staticmethod
    def DefautlSendMail(subject,content):
        systemConf =  ConfModel.getSystemConf()

        mailBaseInfo = {
            'smtpServer' : systemConf['bi_mail']['smtp_server'],
            'username' : systemConf['bi_mail']['username'],
            'password' : systemConf['bi_mail']['password'],
            'sender' : systemConf['bi_mail']['sender'],
            'receiver' : systemConf['bi_mail']['receiver'].split(','),
            'subject' : subject,
            'content' : content
        }

        Mail.SendMail(mailBaseInfo)

