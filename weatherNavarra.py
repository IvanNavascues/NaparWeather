import datetime
import math
import urllib.request
import re
from unicodedata import normalize
from datetime import datetime
import pandas as pd
import requests
import chardet
import DBConection as db

## Aux Functions

def parseDateFromText(text):
    for fmt in ('%d/%m/%Y', '%d/%m/%Y %H:%M:%S', '%Y/%m/%d'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError('no valid date format found')

def getRegionOfStation(IDStation):
    tierraEstella = dict(name="Tierra Estella", IDRegion = 1, IDStations = [5,7,11,21,33,251,267,268,271,423,424])
    baztanBidasoa = dict(name="Baztan-Bidasoa", IDRegion = 2, IDStations = [25,42,460,497,517])
    zonaMedia = dict(name="Zona Media", IDRegion = 3, IDStations = [9,30,35,242,257,265,270,275])
    ribera = dict(name="Ribera", IDRegion = 4, IDStations = [26,31,36,258,272,273,274,276])
    riberaAlta = dict(name="Ribera Alta", IDRegion = 5, IDStations = [4,6,259,269])
    pamplona = dict(name="Pamplona", IDRegion = 6, IDStations = [243,405,455])
    valdizarbeNovenera = dict(name="Valdizarbe-Novenera", IDRegion = 7, IDStations = [24,28,263,264,266])
    larraunLeitzaldea = dict(name="Larraun-Leitzaldea", IDRegion = 8, IDStations = [37,461,499,518])
    pirineo = dict(name="Pirineo", IDRegion = 9, IDStations = [32,249,502,519])
    prepirineo = dict(name="Prepirineo", IDRegion = 10, IDStations = [12,34])
    sakana = dict(name="Sakana", IDRegion = 11, IDStations = [8,22,29,250])
    sanguesa = dict(name="Sangüesa", IDRegion = 12, IDStations = [10,23,246,247,262])
    
    regionList = [tierraEstella,baztanBidasoa,zonaMedia,ribera,riberaAlta,pamplona,valdizarbeNovenera,larraunLeitzaldea,pirineo,prepirineo,sakana,sanguesa]
    
    found = False
    i=0
    while not found and i < len(regionList):
        region = regionList[i]
        if int(IDStation) in region.get("IDStations"):
            found = True
            IDRegion = region.get("IDRegion")
        else:
            i=i+1
    if found:
        return IDRegion
    else:
        return 0    
    

def getTemperaturesOfStation(df,IDStation,IDRegion):
    header = '('+IDRegion+','+IDStation+',{IDFecha},{tempMaxima},{tempMedia},{tempMinima})'
    dateQuery = '(select IDFecha from '+db.dbName+'.fechas where '+db.dbName+'.fechas.ano = {año} and '+db.dbName+'.fechas.mes = {mes} and '+db.dbName+'.fechas.dia = {dia})'
    statement = ""
    maxRow = -1
    medRow = -1
    minRow = -1
    i = 0
    for column in df.columns:  #Veo que columnas estan y cuales no
        if column == "Temperatura máxima ºC":
            maxRow = i
        elif column == "Temperatura media ºC":
            medRow = i
        elif column == "Temperatura mínima ºC":
            minRow = i
        i = i+1
    
    for index, row in df.iterrows():
        #Depuracion de datos 
        if maxRow != -1:
            if type(row[maxRow]) is str:
                tempMax = float(row[maxRow].replace(',','.'))
            else:
                tempMax = row[maxRow]
                
            if math.isnan(tempMax):
                tempMax = "NULL"
            
            elif tempMax > 50:
                tempMax = "NULL"
                
        else:
            tempMax = "NULL"
            
        if medRow != -1:
            if type(row[medRow]) is str:
                tempMed =  float(row[medRow].replace(',','.'))
            else:
                tempMed = row[medRow]
                
            if math.isnan(tempMed):
                tempMed = "NULL"
        else:
            tempMed = "NULL"
            
        if minRow != -1:
            if type(row[minRow]) is str:
                tempMin =  float(row[minRow].replace(',','.'))
            else:
                tempMin =  row[minRow]
                
            if math.isnan(tempMin):
                tempMin = "NULL"
            
            elif tempMin < -30:
                tempMin = "NULL"
        else:
            tempMin = "NULL"
        
        if not isinstance(tempMax, str) and not isinstance(tempMin, str):
             if tempMax < tempMin:
                 tempMin = "NULL"
                 tempMax = "NULL"
                 
        if not isinstance(tempMax, str) and not isinstance(tempMed, str):
             if tempMax < tempMed:
                 tempMax = "NULL"
                 tempMed = "NULL"
                 
        if not isinstance(tempMin, str) and not isinstance(tempMed, str):
             if tempMin > tempMed:
                 tempMin = "NULL"
                 tempMed = "NULL"
                 
        if not (isinstance(tempMin, str) and isinstance(tempMed, str) and isinstance(tempMax, str)):
            fecha = parseDateFromText(index)
            statement = statement + header.format(IDFecha=dateQuery.format(año=fecha.year,mes=fecha.month,dia=fecha.day),tempMaxima=tempMax,tempMedia=tempMed,tempMinima=tempMin) + ','
      
    if statement == "":
        return " "
    else: 
        return statement[:-1]
    
## Parse Web

def readCSV(link):
    
    url = "http://meteo.navarra.es"
    link = link.replace("\\","")
    link = link.replace(" ","%20")
    link = link.replace("xc3xa1", "á")
    link = link.replace("xc3xa9", "é")
    link = link.replace("xc3xad", "í")
    link = link.replace("xc3xb3", "ó")
    link = link.replace("xc3xb1", "ñ")
    r = requests.get(url+link, allow_redirects=True)
    open('prueba.csv', 'wb').write(r.content)
    
    with open('prueba.csv', 'rb') as f:
        enc = chardet.detect(f.read())  # or readline if the file is large
        
    df = pd.read_csv('prueba.csv', encoding = enc['encoding'], index_col=0, sep = ';')
    return df

def getStationsNames():

    link = "http://meteo.navarra.es/estaciones/descargardatos.cfm"
    f = urllib.request.urlopen(link)
    myfile = f.read()
    myfile = str(myfile)

    res = re.findall('AUTOMATICAS(.*)MANUALES',myfile)
    res = str(res)

    provinciasSinParse = re.split("IDEstacion=",res)
    provinciasSinParse.pop(0)
    
    info = []

    for x in provinciasSinParse:
        primSplit = re.split('"> |<',x)
        info.append(re.split('">',primSplit[0]))
    
    return info

def getStationInfo(IDStation):
    link = "http://meteo.navarra.es/estaciones/descargardatos_estacion.cfm?IDEstacion="+IDStation
    f = urllib.request.urlopen(link)
    myfile = f.read()
    myfile = str(myfile)
    
    res = re.split("script",myfile)
    
    yearMaps = re.split("d.add",res[14])
    if len(yearMaps) > 4:
        yearMaps.pop(0)
        yearMaps.pop(0)
        yearMaps.pop(0)
        yearMaps.pop(0)
        
        years = re.findall(r"\d+",yearMaps[0])
        actualYear = years[len(years)-1]
        
        linkList = []
        for x in yearMaps:
            years = re.findall(r"\d+",x)
            actualYear = years[len(years)-1]
            if int(actualYear) >= 2004:
                links = re.split(',',x)
                actualLink = re.split('\'\);',links[3])
                linkList.append(actualLink[0][2:])
            
        return linkList
            
## Main functions (create and populate DB)

def obtainDates(startDate,endDate,conection):
    createTable = "CREATE TABLE IF NOT EXISTS `fechas` (`IDFecha` int(5) NOT NULL AUTO_INCREMENT,`dia` int(3) NOT NULL,`semana` int(3) NOT NULL,`mes` int(3) NOT NULL,`ano` int(5) NOT NULL, PRIMARY KEY (`IDFecha`))DEFAULT CHARSET=utf8;"
    conection.cursor().execute(createTable)
    conection.commit()
    statement = "INSERT INTO "+db.dbName+".fechas (`dia`, `semana`, `mes`, `ano`) VALUES "
    header = '({dia},{semana},{mes},{año})'
    dates = pd.date_range(start=startDate,end=endDate,freq='d')
    statement = statement + header.format(dia=int(dates[0].day),semana = int(dates[0].week),mes=int(dates[0].month),año=int(dates[0].year))
    for date in dates[1:]:
        statement = statement + ',' + header.format(dia=int(date.day),semana = int(date.week),mes=int(date.month),año=int(date.year))
    return statement

def obtainStations(stations,conection):
    createTable = "CREATE TABLE IF NOT EXISTS `estaciones` (`IDEstacion` int(11) NOT NULL,`nombre` varchar(50) NOT NULL, PRIMARY KEY (`IDEstacion`))DEFAULT CHARSET=utf8;"
    conection.cursor().execute(createTable)
    conection.commit()
    
    statement = "INSERT INTO "+db.dbName+".estaciones (`IDEstacion`, `nombre`) VALUES "
    header = '({ID},"{nombre}")'
    statement = statement + header.format(ID=int(stations[0][0]),nombre=stations[0][1])
    for x in stations[1:]:
        x[1] = x[1].replace("\\", "")
        x[1] = x[1].replace("xc3xa1", "á")
        x[1] = x[1].replace("xc3xa9", "é")
        x[1] = x[1].replace("xc3xad", "í")
        x[1] = x[1].replace("xc3xb3", "ó")
        x[1] = x[1].replace("xc3xb1", "ñ")
        statement = statement + ',' + header.format(ID=int(x[0]),nombre=x[1])
    
    return statement

def obtainRegions(conection):
    createTable = "CREATE TABLE IF NOT EXISTS `comarcas` (`IDComarca` int(11) NOT NULL,`nombre` varchar(60) NOT NULL, PRIMARY KEY (`IDComarca`))DEFAULT CHARSET=utf8;"
    conection.cursor().execute(createTable)
    conection.commit()
    statement = "INSERT INTO "+db.dbName+".comarcas (`IDComarca`, `nombre`) VALUES "
    header = '({IDComarca},"{nombre}")'
    regionNames = ['Tierra Estella','Baztan-Bidasoa','Zona Media','Ribera','Ribera Alta','Pamplona','Valdizarbe-Novenera','Larraun-Leitzaldea','Pirineo','Prepirineo','Sakana','Sangüesa']
    statement = statement + header.format(IDComarca=1,nombre=str(regionNames[0]))
    for i in range(2,13):
        statement = statement + ',' + header.format(IDComarca=i,nombre=str(regionNames[i-1]))
    return statement

def obtainGeneralTemperatures(stations,conection):
    createTable = "CREATE TABLE IF NOT EXISTS temperatura (IDEstacion int NOT NULL,IDFecha int NOT NULL,IDComarca int NOT NULL,tempMaxima float,tempMedia float,tempMinima float,FOREIGN KEY (IDEstacion) REFERENCES estaciones(IDEstacion),FOREIGN KEY (IDFecha) REFERENCES fechas(IDFecha),FOREIGN KEY (IDComarca) REFERENCES comarcas(IDComarca))DEFAULT CHARSET=utf8;"
    conection.cursor().execute(createTable)
    conection.commit()
    
    CSVList = []
    usedStationsList = []
    
    for station in stations:
        stationInfo = getStationInfo(station[0])
        if (stationInfo is not None):
            CSVList.append(stationInfo)
            usedStationsList.append(station[0])

    i = 0
    header = "INSERT INTO "+db.dbName+".temperatura (`IDComarca`, `IDEstacion`, `IDFecha`, `tempMaxima`, `tempMedia`, `tempMinima`) VALUES "
    for stationList in CSVList:
        IDRegion = getRegionOfStation(usedStationsList[i])
        if IDRegion != 0:
            for link in stationList:
                df = readCSV(link)
                statement = getTemperaturesOfStation(df,usedStationsList[i],str(IDRegion))
                if statement != " ":
                    conection.cursor().execute(header+statement)
                    conection.commit()
            
            print("Estaciones insertadas: "+str(i+1)+"/"+str(len(usedStationsList)))
            i = i+1
        else:
            print("ERROR, No existe comarca")

def printOptionsMenu():
    print("1: Insertar fechas en BBDD")
    print("2: Insertar estaciones en BBDD")
    print("3: Insertar comarcas en BBDD")
    print("4: Insertar temperaturas en BBDD")
    print("0: Salir")
    return input("Elige una opcion: ")

###########     MAIN        ##########
option = printOptionsMenu()

stationNames = getStationsNames()

conection = db.connect()

while True:
    if int(option) == 1:     ##INSERCION EN BBDD DE TODAS LAS FECHAS DISPONIBLES (2004 A 2021)
        print("Comenzando...")
        startDate = '2004-01-01'
        endDate = '2021-12-31'
        statement = obtainDates(startDate,endDate,conection)
        cursor = conection.cursor()
        cursor.execute(statement)
        conection.commit()
        print("Listo!")
        
    elif int(option) == 2:   ##INSERCION EN BBDD DE TODAS LAS ESTACIONES AUTOMATICAS (TABLA ESTACIONES)
        print("Comenzando...")
        statement = obtainStations(stationNames,conection)
        conection.cursor().execute(statement)
        conection.commit()
        print("Listo!")

    elif int(option) == 3:  ##INSERCION EN BBDD DE TODAS LAS COMARCAS
        print("Comenzando...")
        statement = obtainRegions(conection)
        conection.cursor().execute(statement)
        conection.commit()
        print("Listo!")

    elif int(option) == 4:   ##INSERCION EN BBDD DE TODAS LAS TEMPERATURAS CON SUS RESPECTIVAS RELACIONES
        print("Comenzando...")
        obtainGeneralTemperatures(stationNames,conection)
        print("Listo!")
        
    elif int(option) == 0:
        break
    
    option = printOptionsMenu()
