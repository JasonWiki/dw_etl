#coding=utf-8
u'''
Created on 2015年10月20日

@author: Jason
'''
class Camel:
     
     @staticmethod
     def camelToUnderline(camelFormat):
         '''
             驼峰命名格式转下划线命名格式
         '''
         underlineFormat=''
         if isinstance(camelFormat, str):
             for _s_ in camelFormat:
                 underlineFormat += _s_ if _s_.islower() else '_'+_s_.lower()

         return underlineFormat[1:]

     @staticmethod
     def underlineToCamel(underlineFormat):
         '''
             下划线命名格式驼峰命名格式
         '''
         camelFormat = ''
         if isinstance(underlineFormat, str):
             for _s_ in underlineFormat.split('_'):
                 camelFormat += _s_.capitalize()
         return camelFormat
     
    