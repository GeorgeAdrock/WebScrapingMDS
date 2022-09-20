# Tratamiento de datos
# ==============================================================================
import numpy as np
import pandas as pd
import re
import time
from datetime import date
import pyarrow
import os
# Manejo Web, paginas y webScrapping
# ==============================================================================
import urllib.request
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup as bs

# Gráficos
# ==============================================================================
from matplotlib import pyplot as plt
from threading import Thread

# Configuración warnings
# ==============================================================================\n",
import warnings
warnings.filterwarnings('ignore')

def ExtraerLink(linkPage,patron):
    lista = []
    for tag in linkPage:
        valor = tag.get('href')
        if(str(valor).find(patron) != -1):
            lista.append(valor)
    df = pd.DataFrame (lista, columns = ['url'])
    df = df.drop_duplicates()
    return df
def leerUrl(pagina):    
    soup = bs(urllib.request.urlopen(pagina).read().decode())
    #print(str(soup) )
    time.sleep(5)
  
    return  soup

from selenium import webdriver 
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.support.select import Select
from webdriver_manager.chrome import ChromeDriverManager

# Se define la ruta de descarga
#chromeOptions = webdriver.ChromeOptions()
#path = os.path.join(os.getcwd(), "output\\")
#prefs = {"download.default_directory" : path,  "directory_upgrade": True}
#chromeOptions.add_experimental_option("prefs",prefs)

# Creamos carpeta para el Producto Analizado
CurrentDirectory = os.getcwd()
#CurrentDirectoryFolder = CurrentDirectory + '\\' + 'output' #PARAMETRO_NOMBRE
CurrentDirectoryFolder = 'output/' #PARAMETRO_NOMBRE
# definir ruta_descarga a gusto
ruta_descarga = CurrentDirectoryFolder 
options = Options()
options.add_experimental_option("prefs", {
  "download.default_directory": ruta_descarga, #Donde descargara
  "download.prompt_for_download": False,
  "download.directory_upgrade": True
})

options.add_argument("--headless")

if __name__ == '__main__': 
    start_time = time.time()
    #edgeBrowser = webdriver.Edge(CurrentDirectory+"//msedgedriver.exe")
    driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(),  chrome_options=options)

    driver.get('https://www.linkedin.com/jobs/search/?keywords=Data%20Scientist&location=Chile&locationId=&geoId=104621616&f_TPR=r86400&position=1&pageNum=0') 
    linkedin_soup = bs(driver.page_source.encode("utf-8"), "html")
    #print(linkedin_soup)
    patron = '/jobs/view/'
    df = ExtraerLink(linkedin_soup('a'),patron)
    #df.info()
    columns = ["url", "contenido"]
    dffinal = pd.DataFrame(columns=columns)

    for i in range(len(df)-1):
        link = str(df.iloc[i]['url'])

        #name = get_name(driver,link)   
        #print(name)
        linkedin_soup1 = leerUrl(link.split('?')[0]) 
        #print(linkedin_soup1)
        if str(linkedin_soup1) == "Not Found":
            break
    
        parametro = 'show-more-less-html__markup'
        links_divs = linkedin_soup1.findAll("div", {"class": parametro})
        links_divs = str(links_divs)
        dffinal = dffinal.append(
                    {
                        "url": link,
                        "contenido": links_divs,
                    },
                    ignore_index=True,
                )
    
    dffinal.to_parquet(CurrentDirectory +"df_"+date.today().strftime("%d%m%Y")+'.parquet', engine='pyarrow')
    print("--- %s seconds ---" % (time.time() - start_time))
    driver.close()

