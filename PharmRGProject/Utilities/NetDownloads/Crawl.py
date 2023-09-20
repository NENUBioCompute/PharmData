#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2022/10/9 20:51
# @Author  : ouyangxike
# @FileName: Crawl.py
# @Software: PyCharm

'''
Before you can use this class, you need to perform the following steps:
1.install selenium package
2.insatll chromedriver
'''

from selenium.webdriver.common.by import By
from selenium import webdriver
import re
from urllib import error,request

class Crawl:

    def __init__(self, url):
        self.CheckURL(url)
        self.url = url
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('headless')
        self.driver = webdriver.Chrome(chrome_options=self.options)
        self.driver.get(url)

    def CheckURL(self, url):
        '''
        Check the correctness of the url
        @param url: Web address
        @return:
        '''
        if re.match(r'^https?:/{2}\w.+$', url):
            try:
                request.urlopen(url)
                return True
            except error.URLError:
                return False
        else:
            return False

    def GetPropertyValue(self,label, property):
        '''
        @param label: label element
        @param property: label property
        @return: property value
        '''
        return label.get_attribute(property)

    def GetLabelText(self, label):
        '''
        @param label: label
        @return: label text
        '''
        return label.text

    def GetLabelXpath(self, path)->object:
        '''
        Locate the tag via xpath
        @param args: label path
        @return:label
        '''
        return self.driver.find_element(By.XPATH, path)


    def GetLableSelector(self, label_name:str, **kwargs):
        '''
        Locate tags via CSS_SELECTOR
        @param label_name:label name
        @param kwargs: key:property name,value:property value
        @return:label
        '''
        pro_str = label_name
        for key in kwargs:
            value = kwargs[key]
            pro_str = pro_str + f'[{key}="{value}"]'
        return self.driver.find_element(By.CSS_SELECTOR, pro_str)

    def GetLabelList(self, path):
        '''
        @param path: label path
        @return: label list
        '''
        return self.driver.find_elements(By.XPATH, path)

if __name__=='__main__':

    craw = Crawl("https://transportal.compbio.ucsf.edu/transporters/ABCB1/")

    elements = craw.GetLabelList('/html/body/table[4]/tbody/tr/td/a')
    table = craw.GetLableSelector('table', style='text-align:center; margin-left:20pt', border=1)
    text = craw.GetLabelText(table)
    for element in elements:
        print(craw.GetPropertyValue(element, 'href'))
    # value = craw.get_property_value(element, 'href')
    # print(text, value)