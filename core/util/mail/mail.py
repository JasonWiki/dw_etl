#coding=utf-8
import smtplib
from email.mime.text import MIMEText


u'''
Created on 2015年10月20日

@author: Jason
'''
class Mail:


    u'发送邮件主方法'
    @staticmethod
    def SendMail(mailInfo,mailType = 'html'):

        if (mailType == 'html'):
            msg = Mail.Html(mailInfo['subject'],mailInfo['content'])
        elif (mailType == 'other'):
            pass

        Mail.BaseMail(
            mailInfo['smtpServer'],
            mailInfo['username'],
            mailInfo['password'],
            mailInfo['sender'],
            mailInfo['receiver'],
            msg.as_string()
        )


    u'''
    发送邮件
        @par smtpServer 邮件服务器地址
        @par username 用户名
        @par password 密码
        @par sender 发件人
        @par receiver 收件人 | 群发 ['***','****',……]
        @par content 内容，需要 msg.as_string() 转换后再输入
    '''
    @staticmethod
    def BaseMail(smtpServer,username,password,sender,receiver,msg):
        smtp = smtplib.SMTP()
        smtp.connect(smtpServer)
        smtp.login(username, password)
        smtp.sendmail(sender, receiver, msg)
        smtp.quit()



    @staticmethod
    def Html(subject,content):
        msg =  MIMEText(content,'html','utf-8')
        msg['Subject'] = subject 
        return msg

        
        

