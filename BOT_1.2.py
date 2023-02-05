#!/usr/bin/env python
# coding: utf-8

# In[18]:


import telebot
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials	
#from __future__ import print_function
import gspread
import pandas as pd
import json
from telebot import types # для указание типов
from datetime import datetime, timedelta
import datetime as dt


json = 'warm-buckeye-373611-99f227c7f73c.json'


Sheet_id_st1  = '1uA7PLQeOR9ejKp-8brO9Sor1JeyzX5IuP5phBKRqDiA'
Sheet_id_akd  = '1j6uTwIduNBWm1mXW_Qc6CfbPJVlJ_655SKh0tN0DuZs'
Sheet_id_ubi  = '1964QSJo_5E6e3aCDVc2BQQhU0rH8J3aaRoWkZuJ5TFc'
Sheet_id_lun  = '1_qShVgqOUMVD8gtQIUKN_UMTRvMLKFu-ajaF8knW9XE'
Sheet_id_kolc = '1Xe1IKBz-tdGEfLRD3DLI68ofXDJa-WvyCesAf7HqJ94'
Sheet_id_micro = '13lcE_jEyJbaI8TY28cpfJ3CtSU4qA9mc3oZsmO8Z7ek'

scope = ['https://spreadsheets.google.com/feeds']
credentials = ServiceAccountCredentials.from_json_keyfile_name('warm-buckeye-373611-99f227c7f73c.json', scope)
gc = gspread.authorize(credentials)
bot = telebot.TeleBot('5694708420:AAFGTkmnoT5mxFih3ylJRzgbiCXlu10I1SM')

def time(x):    
    djd = dt.timedelta(x)
    dublin_epoch = dt.datetime(1899, 12, 30)
    x = dublin_epoch + djd
    return str(x)

def import_data(x): #import and rename
    work_sheet = gc.open_by_key(x)\
     .sheet1.get_all_records(value_render_option="UNFORMATTED_VALUE") #для стартовой
    df = pd.DataFrame(work_sheet).replace( '',np.nan,regex= True )
    df = df.rename(columns={'Работник': 'worker',
                                        'Дата':'date',
                                        'Утро в кассе':'morning', 'Приход': 'arrival',
                                         'Назначение П': 'neme_arrival','Комент Р':'coment_p', 'Расход':'spend',
                                        'Назначение Р':'neme_payment', 'касса вечер': 'cash_evning',
                                        'Безнал':'card', ' Выручка':'revenue','Выручка':'revenue'})
    return df


def Payment_data(x):  # lasts payments
    Payment_data = x[['date','spend','neme_payment','coment_p']]
    Payment_data['date'] = Payment_data['date'].fillna(method = 'ffill')
    
    Payment_data.date = Payment_data['date'].apply(lambda x: time(x)[:10] )
    Payment_data.date = pd.to_datetime(Payment_data['date'], errors ='coerce')
    
    Last_day_data = Payment_data.date.dropna().iloc[-5] #Дата 5 дней назад с последней
    Last_payment = Payment_data.dropna().loc[Payment_data.date > Last_day_data]
    
    pd.options.mode.chained_assignment = None
    return Last_payment
def report(x):  # lasts payments
    report_data = x[['date','worker','spend','neme_payment','coment_p','revenue']]
    report_data['date'] = report_data['date'].fillna(method = 'ffill')
    
    report_data.date = report_data['date'].apply(lambda x: time(x)[:10] )
    report_data.date = pd.to_datetime(report_data['date'], errors ='coerce')
    
    Last_day_report_data = report_data.date.dropna().iloc[-1] #последняя дата
    Last_report_data = report_data.loc[report_data.date == Last_day_report_data]
    
    
    pd.options.mode.chained_assignment = None
    return Last_report_data #by last date in df

def report_2(x):  # lasts payments
    report_data = x[['date','worker','spend','neme_payment','coment_p','revenue']]
    report_data['date'] = report_data['date'].fillna(method = 'ffill')
    
    report_data.date = report_data['date'].apply(lambda x: time(x)[:10] )
    report_data.date = pd.to_datetime(report_data['date'], errors ='coerce')
    
    Last_day_report_revenue = report_data.revenue.dropna().iloc[-1] #последняя дата
    Last_report_data = report_data.loc[report_data.revenue == Last_day_report_revenue]
    
    
    pd.options.mode.chained_assignment = None
    return Last_report_data #by last revenue
#выдает последний день где есть выручка



@bot.message_handler(commands=['start'])
def start(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    btn2 = types.KeyboardButton("Стартовая")
    btn3 = types.KeyboardButton("Академ")
    btn4 = types.KeyboardButton("Юбилейная")
    btn5 = types.KeyboardButton("Лунная")
    btn6 = types.KeyboardButton("Кольцово")
    markup.add(btn2,btn3,btn4,btn5,btn6)
    bot.send_message(message.chat.id, text="Привет, {0.first_name}!".format(message.from_user), reply_markup=markup)


@bot.message_handler(content_types=['text'])
def func(message):

    if message.text == "Стартовая":
        bot.send_message(message.chat.id, f"""
<b>Стартовая {str(report_2(import_data(Sheet_id_st1)).date.iloc[0])[:10]}</b>:

<b>Последняя наличка вечер</b>: {import_data(Sheet_id_st1).cash_evning.dropna().iloc[-1]}
работник: {str(report_2(import_data(Sheet_id_st1)).worker.iloc[0])}
выручка: {report_2(import_data(Sheet_id_st1)).revenue.sum()}

<b>Расходы за последнее время:</b>
{Payment_data(import_data(Sheet_id_st1))}
         """,parse_mode="html")
    elif message.text == "Академ":
        bot.send_message(message.chat.id, f"""
<b>Академ {str(report_2(import_data(Sheet_id_akd)).date.iloc[0])[:10]}</b>:

работник: {str(report_2(import_data(Sheet_id_akd)).worker.iloc[0])}
выручка: {report_2(import_data(Sheet_id_akd)).revenue.sum()}
<b>Последняя наличка вечер</b>: {import_data(Sheet_id_akd).cash_evning.dropna().iloc[-1]}

<b>Расходы за последнее время:</b>
{Payment_data(import_data(Sheet_id_akd))}
              """,parse_mode="html")
        
        
    elif message.text == "Юбилейная":
        bot.send_message(message.chat.id, f"""
<b>Юбилейная {str(report_2(import_data(Sheet_id_ubi)).date.iloc[0])[:10]}</b>:
        
<b>Работник</b>: {str(report_2(import_data(Sheet_id_ubi)).worker.iloc[0])}
<b>Выручка</b>: {report_2(import_data(Sheet_id_ubi)).revenue.sum()}
<b>Последняя наличка вечер</b>: {import_data(Sheet_id_ubi).cash_evning.dropna().iloc[-1]}
        
<b>Расходы за последнее время:</b>
{Payment_data(import_data(Sheet_id_ubi))}
        
              """,parse_mode="html")
    elif message.text == "Лунная":
        bot.send_message(message.chat.id, f"""
<b>Лунная {str(report_2(import_data(Sheet_id_lun)).date.iloc[0])[:10]}</b>:
        
Последняя наличка вечер: {import_data(Sheet_id_lun).cash_evning.dropna().iloc[-1]}
работник: {str(report_2(import_data(Sheet_id_lun)).worker.iloc[0])}
выручка: {report_2(import_data(Sheet_id_lun)).revenue.sum()}
        
<b>Расходы за последнее время:</b>
{Payment_data(import_data(Sheet_id_lun))}
        
       
              """,parse_mode="html")
    elif message.text == "Кольцово":
        bot.send_message(message.chat.id, f"""
<b>Кольцово {str(report_2(import_data(Sheet_id_kolc)).date.iloc[0])[:10]}</b>:

Последняя наличка вечер: {import_data(Sheet_id_kolc).cash_evning.dropna().iloc[-1]}
работник: {str(report_2(import_data(Sheet_id_kolc)).worker.iloc[0])}
выручка: {report_2(import_data(Sheet_id_kolc)).revenue.sum()}

<b>Расходы за последнее время:</b>
{Payment_data(import_data(Sheet_id_kolc))}
              """,parse_mode="html")
               
bot.polling(none_stop=True)


