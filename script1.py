import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pandas as pd
#install packages
import time
from oauth2client.service_account import ServiceAccountCredentials
#import schedule 
from multiprocessing import Pool
from multiprocessing import Manager
import sys
sys.setrecursionlimit(25000)
import requests
from bs4 import BeautifulSoup
import re
import csv
import pandas as pd
import json
import gspread_dataframe as gd





scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

credentials = Credentials.from_service_account_file('trusty-mantra-375121-23cfd47e0a4c.json', scopes=scopes)

gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

# open a google sheet
gs = gc.open_by_key("189PDYXSLaMLyqmfg6lDAs-PjW6OfB8hsqIptk8oh8mY")
# select a work sheet from its name
worksheet1 = gs.worksheet('Sheet1')
def product(url):
    attributes={}
    response=requests.get(url)
    soup=BeautifulSoup(response.text)
    try:
        title=soup.find("h1",{"itemprop":"name"}).contents[0]
        attributes['title']=str(title).replace(',',"")
    except:
        attributes['title']="title"
    try:
        sku=soup.find("p",{"class":"sku"}).contents[0]
        attributes['sku']=str(sku).replace(',',"")
    except:
        attributes['sku']="sku"
    try:
        ul=soup.find("ul",{"class":"attribute-list"})

        for element in ul:
            try:
                key=(str(element.strong.contents[0]).replace(':',"").replace(',',""))
                if (key=="Tema"):
                    value=str(element.span.a.contents[0]).replace("\n","").replace("   ","").replace("\t",'').replace(',',"")
                else:
                    value=str(element.span.contents[0]).replace("\n","").replace("   ","").replace("\t",'').replace(',',"")
                attributes[key]=value
            except:
                pass
    except:
        pass
    try:
        products_option=soup.find("div",{"class":"product-options"})
        try:
            regular_price=float(products_option.find("span",{"class":"regular-price"}).find("span",{"class":"price"}).contents[0])+float(str(products_option.find("span",{"class":"regular-price"}).find("span",{"class":"price"}).find("span",{"class":"precision"}).contents[0]).replace(",","0."))
            attributes['regular_price']=str(regular_price).replace(',',"")
        except:
            attributes['regular_price']="regular_price"

        #old_price
        try:
            old_price=float(str((products_option.find("p",{"class":"old-price"}).find("span",{"class":"price"}).contents[0])).replace("\t",""))+float(str(products_option.find("p",{"class":"old-price"}).find("span",{"class":"price"}).find("span",{"class":"precision"}).contents[0]).replace(",","0."))
            attributes['old_price']=str(old_price).replace(',',"*")
        except:
            attributes['old_price']=str("old_price").replace(',',"*")
        #special_price
        try:
            special_price=float(str((products_option.find("p",{"class":"special-price"}).find("span",{"class":"price"}).contents[0])).replace("\t",""))+float(str(products_option.find("p",{"class":"special-price"}).find("span",{"class":"price"}).find("span",{"class":"precision"}).contents[0]).replace(",","0."))
            attributes['special_price']=str(special_price).replace(',',"*")

        except:
            attributes['special_price']="special_price"
    except:
        pass
    try:
        delivery_date=soup.find("span",{"class":"delivery-dates"})
        delivery=str(delivery_date.find("span").text)+"-"+str(delivery_date.find("span").find_next("span").text)
        attributes['delivery']=delivery
    except:
        attributes['delivery']="delivery"

    try:
        tables=soup.find_all("table",{"class":"stylized attributes"})
        for table in tables:
            trs=table.find_all("tr")
            for tr in trs:
                key=str(tr.th.text).replace("\t","").replace(',',"")
                value=str((tr.td.text)).replace("\t","").replace(',',"")
                attributes[key]=value
    except:
        pass
    try:
        image=soup.find("img",{"class":"main-image-nosrc"})["data-lazy"]
        attributes["image"]=image
    except:
        attributes["image"]="image"
    return attributes
def product_in_categorie(url):
    url=url+"?disponibil=in-depozit&limit=100"
    print(url)
    data=[]
    response=requests.get(url)
    soup=BeautifulSoup(response.text)
    liste=soup.find_all("a",{"class":"product-box"})
    if(len(liste)>0):
        for element in liste:
            data.append(product(element['href']))
            print(len(data))
    df = pd.DataFrame(data)
    ss = gc.open("Sheet1") 
    ws = ss.worksheet("Sheet1")
    existing = gd.get_as_dataframe(ws)
    updated = existing.append(df)
    gd.set_with_dataframe(ws, updated)

def func():
  try:
    product_in_categorie("https://www.ozone.ro/puzzle-uri/")
  except:
    pass
  try:
    product_in_categorie("https://www.ozone.ro/jocuri-pentru-computere-si-console/")
  except:
    pass
  try:
    product_in_categorie("https://www.ozone.ro/accesorii-de-gaming/")
  except:
    pass
  try:
   product_in_categorie("https://www.ozone.ro/librarie/")
  except:
    pass 
  try:
   product_in_categorie("https://www.ozone.ro/librarie/")
  except:
    pass
  try:
   product_in_categorie("https://www.ozone.ro/tv-monitori/")
  except:
    pass
  try:
   product_in_categorie("https://www.ozone.ro/filme-muzica/")
  except:
    pass
  try:
   product_in_categorie("https://www.ozone.ro/audio-si-hi-fi")
  except:
    pass
  try:
   product_in_categorie("https://www.ozone.ro/merchandising")
  except:
    pass
  try:
   product_in_categorie("https://www.ozone.ro/jocuri-de-masa-101")
  except:
    pass
  try:
   product_in_categorie("https://www.ozone.ro/jucarii")
  except:
    pass
  try:
   product_in_categorie("https://www.ozone.ro/mama-si-bebe")
  except:
    pass  
  try:
   product_in_categorie("https://www.ozone.ro/aparate-electrice-mici")
  except:
    pass  

func()
