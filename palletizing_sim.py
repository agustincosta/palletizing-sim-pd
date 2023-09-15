import numpy as np
import math
import datetime
import pandas as pd
from decorators import timer
import itertools
from typing import List
from matplotlib import pyplot as plt
import logging

layersPerPallet = 15
traysPerLayer = 4

logging.basicConfig(filename='simulation.log', encoding='utf-8', level=logging.DEBUG)

class DataAnalysis:

  def __init__(self, filePath:str) -> None:
    """Inicializacion de clase con lectura de CSV

    Args:
        filePath (str): Ruta al archivo csv
    """
    dtypes = {'Nro Orden': np.int64, 'Fe y Hr Creac': str, 'Destino': str, 'SKU': np.int64, 'Cant. Orignial Ordenada': np.int64,\
              'Cantidad Ordenada': np.int64, 'Cantidad Asignada': np.int64, 'Cant. Empacada': np.int64, 'BG Enviada': np.int64, \
              'Cantidad Cancelada': np.int64, 'CEVE': str, 'Fecha Comercial': str, 'Mes': np.int64, 'Sem': np.int64}
    #Series de fechas
    #self.datesCol = pd.read_csv(filePath, sep=';', dtype=dtypes, parse_dates=[11], dayfirst=True, encoding='latin-1')['Fecha Comercial']
    #self.days = self.datesCol.unique()
    #self.days = pd.Series(self.days).dropna()
    #Series de clientes
    self.clientsCol = pd.read_csv(filePath, sep=';', dtype=dtypes, encoding='latin-1')['Destino']
    self.destinations = self.clientsCol.unique()
    self.destinations = pd.Series(self.destinations).dropna()
    #DataFrame general
    self.fileDF = pd.read_csv(filePath, sep=';', dtype=dtypes, parse_dates=[11], dayfirst=True, encoding='latin-1')
    self.fileDF = self.fileDF.sort_index()
    #Se eliminan columnas no importantes
    self.fileDF = self.fileDF.drop(labels=['Nro Orden', 'Fe y Hr Creac', 'Cant. Orignial Ordenada', 'Cantidad Ordenada', 'Cantidad Asignada', 'Cant. Empacada', 'Cantidad Cancelada', 'CEVE', 'Mes', 'Sem'], axis=1)
    #Renombrado de columnas
    self.fileDF.rename(columns = {'BG Enviada' : 'Cantidad', 'Fecha Comercial' : 'Fecha'}, inplace = True)
    self.fileDF = self.fileDF.set_index('Fecha')
    #Modulo 60 para eliminar pallets completos
    self.fileDF['Cantidad'] = self.fileDF['Cantidad'] % (layersPerPallet*traysPerLayer)
    pd.options.display.float_format = '{:.2f}'.format
    #Eliminar filas con cantidad 0
    self.fileDF = self.fileDF[self.fileDF.Cantidad > 0]
    self.skus = self.fileDF['SKU'].unique()
    #Serie de fechas sin los dias con cantidad 0
    self.days = np.unique(self.fileDF.index.values)
    self.days = pd.Series(self.days).dropna()

  def filterByDate(self, date:np.datetime64, df:pd.DataFrame=pd.DataFrame()) -> pd.DataFrame:
    """Filtra dataset por día

    Args:
        date (np.datetime64): Fecha seleccionada
        df (pd.DataFrame, optional): DataFrame a usar en lugar del self.fileDF. Defaults to None.

    Returns:
        pd.DataFrame: Dataset filtrado por día
    """
    dia = pd.Timestamp(date).day
    mes = pd.Timestamp(date).month
    anio = pd.Timestamp(date).year

    if df.empty:
      filtered_dataset = self.fileDF.loc[(self.fileDF.index>=pd.Timestamp(date))&(self.fileDF.index<(datetime.datetime(year=anio,month=mes,day=dia, hour=23, minute=59, second=59)))]
    else:
      filtered_dataset = df.loc[(df.index>=pd.Timestamp(date))&(df.index<(datetime.datetime(year=anio,month=mes,day=dia, hour=23, minute=59, second=59)))]

    return filtered_dataset

  def countDifferentProducts(self, productsSeries:pd.Series) -> int:
    """Retorna la cuenta de productos distintos dado un dataset 

    Args:
        productsSeries (pd.Series): Array conteniendo solamente los productos, ya filtrado por tiempo

    Returns:
        int: Cantidad de productos distintos
    """
    u = productsSeries.nunique()
    return u

  def productQuantity(self, dataset:pd.DataFrame, asc:bool=False) -> pd.DataFrame:
    """Filtra dataset y obtiene cantidad de cada producto

    Args:
        dataset (pd.DataFrame): Dataset con columna de 'Producto' y 'Cantidad'
        asc (bool, optional): Ordenar ascendente if True, descendente if False. Defaults to True

    Returns:
        pd.DataFrame: Dataset filtrado
    """
    self.grouped = dataset.reset_index()
    smallDataset = self.grouped.drop(labels=['Fecha', 'Destino'], axis=1)
    groupedDF = smallDataset.groupby(['SKU']).sum()
    outputDataset = groupedDF.sort_values(by=['Cantidad'], ascending=asc)

    return outputDataset

  def getTopSKUs(self, dataset:pd.DataFrame, qty:int) -> pd.DataFrame:
    """Filtra dataset y obtiene el top n de SKUs acorde a su cantidad

    Args:
        dataset (pd.DataFrame): Dataset con columnas de 'Fecha', 'Destino', 'SKU' y 'Cantidad'
        qty (int): Cantidad de SKUs a guardar por cada cliente por día

    Returns:
        pd.DataFrame: Dataset filtrado
    """
    outputDF = pd.DataFrame(columns=['Fecha', 'SKU', 'Cantidad', 'CapasEnteras'])  #Dataset una fila por sku por dia
    topDF = pd.DataFrame(columns=['Fecha', 'CapasEnteras', 'SumaCapasDia', '%Top', 'Bandejas', 'CantSKU'])     #Dataset con una fila por dia pero con datos agregados solo para n SKUs
    #dailyDF = pd.DataFrame(columns=['Fecha', 'SKU', 'Cantidad', 'CapasEnteras'])
    for date in self.days:
      dailyDF = pd.DataFrame(columns=['Fecha', 'SKU', 'Cantidad', 'CapasEnteras'])
      dayDF = self.filterByDate(date).set_index('Destino')
      self.lp = dayDF
      for client in self.destinations:
        if client in dayDF.index:
          if isinstance(dayDF.loc[client], pd.DataFrame):  #Si ese día para ese cliente hay un único SKU
            day_client_df = dayDF.loc[client].groupby(['SKU']).sum()
          else:
            day_client_df = dayDF.loc[client].to_frame().T
            day_client_df = day_client_df.reset_index()
            #day_client_df = day_client_df.drop(labels=['index'])
            self.arr = day_client_df
          day_client_df['Fecha'] = date
          self.tre = day_client_df
          if isinstance(dayDF.loc[client], pd.DataFrame):
            day_client_df['CapasEnteras'] = (day_client_df['Cantidad']/traysPerLayer).apply(math.trunc)
            day_client_df = day_client_df.reset_index()
            try:
              day_client_df = day_client_df.drop(labels=['level_0', 'index'])
            except Exception as e:
              pass
            day_client_df = day_client_df.set_index(['Fecha'])
            
          else:
            self.rr = day_client_df
            day_client_df['CapasEnteras'] = math.trunc(day_client_df['Cantidad'].sum()/traysPerLayer)
            day_client_df = day_client_df.reset_index()
            try:
              day_client_df = day_client_df.drop(labels=['level_0', 'index'])
            except Exception as e:
              pass
            day_client_df = day_client_df.set_index(['Fecha'])
          
          if isinstance(dayDF.loc[client], pd.DataFrame):
            dailyDF = pd.concat([dailyDF, day_client_df])

      if len(dailyDF) > 0:
        dailyDF = dailyDF.groupby(['SKU']).sum()
      self.drd = dailyDF
      self.drd_date = date
      dailyDF = dailyDF.sort_values(by=['CapasEnteras'], ascending=False)

      x = dailyDF.iloc[0:qty]['CapasEnteras'].sum()/dailyDF['CapasEnteras'].sum() if dailyDF['CapasEnteras'].sum() > 0 else 0

      d = {'Fecha':[date], 'CapasEnteras':[dailyDF.iloc[0:qty]['CapasEnteras'].sum()], \
           'SumaCapasDia':[dailyDF['CapasEnteras'].sum()], '%Top':[x],\
           'Bandejas':[dailyDF['Cantidad'].sum()], 'CantSKU':[dailyDF['Cantidad'].count()]}

      auxDF = pd.DataFrame(data = d)

      topDF = pd.concat([topDF, auxDF])

      outputDF = pd.concat([outputDF, dailyDF])

    #outputDF = outputDF.set_index(['Fecha'])
    #topDF = outputDF.set_index(['Fecha'])

    return outputDF, topDF

  def palletsPerDay(self, dataset:pd.DataFrame) -> pd.DataFrame:
    """Crea dataset con columna de fecha y pallets por día 

    Args:
        dataset (pd.DataFrame): Dataset con columnas de 'Fecha', 'Destino', 'SKU' y 'Cantidad'

    Returns:
        pd.DataFrame: Dataset modificado
    """
    outputDF = pd.DataFrame(columns=['Fecha', 'Capas enteras', 'Pallets'])
    for i in range(len(self.days)):
      date = self.days[i]
      dayDF = self.filterByDate(date)
      outputDF.loc[i] = [date, math.trunc(dayDF['Cantidad'].sum()/traysPerLayer), math.trunc(dayDF['Cantidad'].sum()/(traysPerLayer*layersPerPallet))]

    return outputDF

  def dailySKUStats(self, dataset:pd.DataFrame, skuQty:int):
    """Por cada día suma cantidades de SKUs (para todos los clientes) y calcula capas enteras y pallets

    Args:
        dataset (pd.DataFrame): Dataset con columnas de 'Fecha', 'Destino', 'SKU' y 'Cantidad'
        skuQty (int): Cantidad de SKUs a guardar por día

    Returns:
        pd.DataFrame: Dataset filtrado
    """
    outputDF = pd.DataFrame(columns=['Fecha', 'SKU', 'Cantidad', 'Capas enteras', 'Pallets'])
    topNDF = pd.DataFrame(columns=['Fecha', 'Capas enteras', 'Suma dia', '%Top', 'Bandejas', 'CantSKU'])

    for i in range(len(self.days)):
      date = self.days[i]
      dayDF = self.filterByDate(date)
      dailyDF = dayDF.drop(labels=['Destino'], axis=1)
      dailyDF = dailyDF.groupby(['SKU']).sum()
      dailyDF['Fecha'] = date
      dailyDF['Capas enteras'] = (dailyDF['Cantidad']/traysPerLayer).apply(math.trunc)
      dailyDF['Pallets'] = (dailyDF['Cantidad']/(traysPerLayer*layersPerPallet)).apply(math.trunc)
      dailyDF = dailyDF.reset_index()
      dailyDF = dailyDF.sort_values(by=['Capas enteras'], ascending=False)
      self.tre = dailyDF
      
      outputDF = pd.concat([outputDF, dailyDF])

      x = dailyDF.iloc[0:skuQty]['Capas enteras'].sum()/dailyDF['Capas enteras'].sum() if dailyDF['Capas enteras'].sum() > 0 else 0

      d = {'Fecha':[date], 'Capas enteras':[dailyDF.iloc[0:skuQty]['Capas enteras'].sum()], \
           'Suma dia':[dailyDF['Capas enteras'].sum()], '%Top':[x],\
           'Bandejas':[dailyDF['Cantidad'].sum()], 'CantSKU':[dailyDF['Cantidad'].count()]}

      
      auxDF = pd.DataFrame(data = d)
      
      topNDF = pd.concat([topNDF, auxDF])

    outputDF = outputDF.set_index(['Fecha'])
    topNDF = topNDF.set_index(['Fecha'])

    return outputDF, topNDF

  @timer
  def datasetForRobot(self, topNumber:int) -> pd.DataFrame:
    """Genera dataset para paletizado de robot. Filtra por día para quedarse solo con los SKU en el top 20 de cantidades
    Args:
        topNumber (int): Cantidad de SKUs a guardar por día

    Returns:
        pd.DataFrame: Dataset filtrado
    """
    outputDF = pd.DataFrame(columns=['Destino', 'SKU', 'Cantidad'])
    for date in self.days:
      dailyTopSkus = pd.DataFrame(columns=['Fecha', 'SKU'])
      auxDF = pd.DataFrame(columns=['SKU', 'CapasEnteras'])
      dayDF = self.filterByDate(date)                                       #Comienza con columnas de Fecha (index), Destino, SKU y Cantidad. Se elimina la columna de Fecha
      dayDF = dayDF.dropna()                                      #Nuevo
      dayDF = dayDF.drop_duplicates(subset=['Destino', 'SKU'])    #Nuevo
      dayDF_noDest = dayDF.drop(labels=['Destino'], axis=1)                 #Se elimina la columna de Destino
      dayDF_noDest = dayDF_noDest.groupby(['SKU']).sum(numeric_only=True)   #Se suma Cantidad y quedan solo SKU y Cantidad
      dayDF_noDest = dayDF_noDest.reset_index()                             #Tenía index SKU y se resetea

      auxDF['SKU'] = dayDF_noDest['SKU']                                    #Se copia la columna de SKU
      auxDF['CapasEnteras'] = (dayDF_noDest['Cantidad']/traysPerLayer).apply(math.trunc)  #Se le asigna la cantidad de capas enteras haciendo división entera de cantidad
      auxDF = auxDF.sort_values(by=['CapasEnteras'], ascending=False)       #Se ordena por cantidad de capas enteras

      dailyTopSkus = auxDF[0:topNumber].copy()                              #Se hace el slice del dataset por los 20 SKU de más cantidad de capas. Tiene columna de SKU y CapasEnteras
      dailyTopSkus['Fecha'] = date                                          #Al nuevo dataset para el día se asigna la Fecha (la misma para todas las filas porque es solo el día)
      dailyTopSkus = dailyTopSkus.drop(labels=['Fecha'], axis=1)
      dailyTopSkus = dailyTopSkus.set_index(['SKU'])

      #Hay que eliminar líneas de SKU duplicadas por día y por cliente


      dailyDF = pd.merge(dayDF, dailyTopSkus, how='inner', on=['SKU'])      #Intersección de tablas, se obtiene el dataset del día filtrado por los SKU del dataset top 20. Ya viene con 
                                                                            # la columna de fecha de la tabla de top 20.
      dailyDF['Fecha'] = date                                               #Se agrega columna de fecha 
      dailyDF = dailyDF.drop(labels=['CapasEnteras'], axis=1)               #Se elimina la de capas enteras que solo servía para filtar el top 20
      dailyDF = dailyDF.set_index(['Fecha'])                                #Se asigna fecha al índice

      outputDF = pd.concat([outputDF, dailyDF])                             #Se concatena con todos los días para formar el dataset deseado

    return outputDF

  @timer
  def bestCasePalletizing(self, robotDataset:pd.DataFrame):
    """Calcula el mejor caso de que porcentaje de movimientos se podrían hacer con el robot considerando pallets completos de "base"

    Args:
        robotDataset (pd.DataFrame): Dataset filtrado para robot con top 20 SKU

    Returns:
        float: Porcentaje de mejora de movimientos
    """
    movements_client_DF = pd.DataFrame(columns=['Fecha', 'Destino', 'Movimientos', 'CapasTotales'])
    movements_day_DF = pd.DataFrame(columns=['Fecha', 'Movimientos', 'CapasTotales'])
    for date in self.days:                                                  #Iteración por día
      dailyDF = pd.DataFrame(columns=['Fecha', 'SKU', 'Cantidad', 'CapasEnteras'])
      dayDF = self.filterByDate(date, df=robotDataset).set_index(['Destino'])   #Dataframe del día con Destino, SKU y Cantidad
      for client in self.destinations:                                      #Iteración por destino  
        if client in dayDF.index:                                           #No todos los días tienen envíos a todos los destinos
          if isinstance(dayDF.loc[client], pd.DataFrame):                   #Si ese día para ese cliente hay un único SKU va a devolver una Series no DataFrame
            day_client_df = dayDF.loc[client].groupby(['SKU']).sum()        #Se agrupa por SKU (aunque no debería haber repetidos)
          else:
            day_client_df = dayDF.loc[client].to_frame().T                  #Si hay solo un SKU se convierte la Series devuelta a DataFrame por compatibilidad
            day_client_df = day_client_df.set_index(['SKU'])                #Cambio de índice para asimilar al caso de DF
          
          day_client_df['Fecha'] = date                                     #Columnas de fecha y CapasEnteras
          day_client_df['CapasEnteras'] = (day_client_df['Cantidad']/traysPerLayer).apply(math.trunc)
          #print("Aham")                                                     #Aham
          total_layers = 0
          base_layers = 0
          total_layers = day_client_df['CapasEnteras'].sum()                #Capas (Movimientos) totales por día por cliente
          total_pallets = math.trunc(total_layers/layersPerPallet)          #Pallets derivados de esas capas
          ordered_dcdf = day_client_df.sort_values(by=['CapasEnteras'], ascending=False)[0:total_pallets].copy()  #Pedazo de DF con los que consideramos "base"
          base_layers = ordered_dcdf['CapasEnteras'].sum()
          client_min_movements = total_layers - base_layers

          d1 = {'Fecha':[date], 'Destino':[client], 'Movimientos':[client_min_movements], 'CapasTotales':[total_layers]}
          auxDF1 = pd.DataFrame(data = d1)
          movements_client_DF = pd.concat([movements_client_DF, auxDF1])

      singleDayDF = movements_client_DF[movements_client_DF['Fecha']==date]
      d2 = {'Fecha':[date], 'Movimientos':[singleDayDF['Movimientos'].sum()], 'CapasTotales':singleDayDF['CapasTotales'].sum()}
      auxDF2 = pd.DataFrame(data = d2)
      movements_day_DF = pd.concat([movements_day_DF, auxDF2])

    movements_client_DF = movements_client_DF[movements_client_DF['CapasTotales'] > 0]
    x = movements_client_DF['Movimientos']/movements_client_DF['CapasTotales'] if len(movements_client_DF['CapasTotales']) > 1 else 0
    movements_client_DF.insert(3, 'Ratio', x)
    movements_day_DF.insert(3, 'Ratio', movements_day_DF['Movimientos']/movements_day_DF['CapasTotales'])
    return movements_day_DF, movements_client_DF 

class Capa:
  def __init__(self, SKU:int, layer:int) -> None:
    """Inicialización de clase asignando SKU y numero de capa

    Args:
        SKU (int): Numero de SKU
        layer (int): Numero de capa en pallet
    """
    self.SKU = SKU
    self.layerNumber = layer

class PalletEntrada:
  id_obj = itertools.count()

  def __init__(self, SKU:int) -> None:
    """Inicialización de clase creando lista de objetos Capa

    Args:
        SKU (int): SKU del cual serán todas las capas
    """
    #Inicialización de propiedades
    self.id = next(PalletEntrada.id_obj)
    self.layers = []
    self.currentLayers = 15
    self.empty = False
    self.product = SKU
    #Creación de capas dentro de lista
    self.layers += [Capa(SKU, i) for i in range(self.currentLayers)]

  def __isEmpty(self) -> bool:
    """Retorna si el pallet tiene 0 filas o no

    Returns:
        bool: Pallet vacío
    """
    return self.currentLayers == 0
  
  def subtractLayer(self) -> Capa:
    """Retira la última capa del pallet

    Raises:
        ValueError: Si se aplica a pallet vacío

    Returns:
        Capa: Objeto capa retirado de pallet
    """
    #Verifica que pallet tenga capas
    if not self.empty:
      removedLayer = self.layers[-1]      #Extrae capa
      del self.layers[-1]                 #Elimina elemento de lista
      self.currentLayers -= 1             #Decrementa contador de capas
      self.empty = self.__isEmpty()       #Actualiza estado de variable empty
    else:
      raise ValueError('No se pueden retirar capas porque el pallet ya esta vacío')
    return removedLayer
    
class PalletSalida:
  #ID incremental
  id_obj = itertools.count()

  def __init__(self, destination:str) -> None:
    """Inicialización de clase creando lista vacia para capas con destino asignado

    Args:
        destination (str): Destino del pallet
    """
    #Inicialización de propiedades
    self.id = next(PalletSalida.id_obj)
    self.layers = []
    self.currentLayers = 0
    self.complete = False
    self.destination = destination
    
  def __isComplete(self) -> bool:
    """Retorna si el pallet tiene 15 filas o no

    Returns:
        bool: Pallet completo
    """
    return self.currentLayers == 15
  
  def addLayer(self, newLayer:Capa) -> None:
    """Agrega capa a pallet de salida

    Args:
        layer (Capa): Capa retirada de pallet de entrada
    """
    #Verifica que el pallet tenga lugar
    if not self.complete:
      newLayer.layerNumber = self.currentLayers + 1   #Actualiza numero de capa
      self.layers.append(newLayer)                    #Agrega capa a la lista
      self.currentLayers += 1                         #Incrementa contador de capas
      self.complete = self.__isComplete()               #Actualiza el estado de la variable complete
    else:
      raise ValueError('No se pueden agregar capas porque el pallet ya esta completo')

  def layerListToDF(self) -> pd.DataFrame:
    """Convierte lista de capas a pandas DataFrame con columnas Capa y SKU

    Returns:
        pd.DataFrame: DataFrame de filas de pallet
    """
    df = pd.DataFrame(columns=['Capa', 'SKU'])          #Definición de DF vacío
    for layer in self.layers:                           #Iteración en lista de capas
      a = {'Capa': layer.layerNumber, 'SKU': layer.SKU} #Genera fila
      df = pd.concat([df, pd.DataFrame(data=a)])        #Concatena cada fila al DF

    return df

  def __skuTotals(self) -> pd.Series:
    """Retorna cantidades de cada SKU en el pallet

    Returns:
        pd.Series: Series de cantidad para cada SKU
    """
    layers_df = self.layerListToDF()

    return layers_df.groupby(['SKU']).count()

class Simulation(DataAnalysis):
  """Clase hija de DataAnalysis. Utiliza las funciones de esta para generar 
  simulación de paletizado

  Args:
      DataAnalysis (class): Clase padre de funciones de estadísticas
  """
  dayDataset = pd.DataFrame(columns=['Destino', 'SKU', 'Cantidad'])
  skuAllocation = pd.DataFrame(columns=['PalletsParciales'], index=['SKU'])
  dayDestinations = []

  #Lista de pallets de entrada
  entryPallets: List[PalletEntrada]
  entryPallets = []

  #Lista de pallets de salida
  exitPallets: List[PalletSalida]
  exitPallets = []

  completedExitPallets: List[PalletSalida]
  completedExitPallets = []

  #Lista de indices para eliminar pallets de salida luego de una iteración completa
  deleteExitPallets: List[int]
  deleteExitPallets = []
  
  #Métricas de simulación
  remainingLayers = 0
  numExitPallets = 0
  numCompletedPallets = 0
  transferedLayers = 0
  batchTransfers = 0
  totalPallets = 0
  totalLayers = 0
  palletChanges = 0
  simRecordIndex = 0
  simulationRecord = pd.DataFrame(columns=['RemLayers', 'ExitPallets', 'CompPallets', 'LayerTransfers', 'BatchTransfers', 'PalletChanges'])


  def __init__(self, filePath: str) -> None:
    """Inicialización de clase Simulation con su respectiva clase padre

    Args:
        filePath (str): Ruta al archivo .csv con datos
    """
    super().__init__(filePath)
    
  def getSimulationDataset(self, skus:int) -> pd.DataFrame:
    """Genera dataset filtrado por Top N SKUs cada día

    Args:
        skus (int): Cantidad de SKUS a incluir

    Returns:
        pd.DataFrame: Dataset filtrado. Columnas: Fecha (index), Destino, SKU, Cantidad
    """
    return self.datasetForRobot(skus)
  
  def getSimulationDayDataset(self, dia:np.datetime64, workingDF:pd.DataFrame) -> None:
    """Filtra por día el dataset dado

    Args:
        dia (np.datetime64): Fecha
        workingDF (pd.DataFrame): Dataset de trabajo. Columnas: Fecha (index), Destino, SKU, Cantidad

    Returns:
        pd.DataFrame: Dataset para ese único día. Columnas: Destino, SKU, Cantidad
    """
    self.dayDataset = self.filterByDate(date=dia, df=workingDF)
    newVals = (self.dayDataset['Cantidad']/traysPerLayer).apply(math.trunc)   #Conversión de bandejas a capas completas
    auxDF = self.dayDataset.copy()
    auxDF[auxDF.columns[2]] = newVals
    self.dayDataset = auxDF
    self.dayDestinations = pd.unique(self.dayDataset['Destino']).tolist()    #Lista de destinos 
  
  def __getSkuAllocation(self) -> pd.DataFrame:
    """Genera tabla con cantidades de capas y pallets para cada SKU

    Args:
        dayDataset (pd.DataFrame): Dataset del día para simulación. Columnas: Destino, SKU, Cantidad

    Returns:
        pd.DataFrame: DataFrame. Columnas: SKU (index), PalletsParciales
    """
    outputDF = pd.DataFrame(columns=['SKU', 'PalletsParciales', 'Asignados'])   #Estructura de DataFrame de retorno
    df = self.dayDataset.drop(labels=['Destino'], axis=1)               #Elimina columna de Destino
    df2 = df.groupby(by=['SKU'], as_index=False).sum()                  #Agrupa por SKU y suma cantidades
    df2 = df2.sort_values(by=['Cantidad'], ascending=False)             #Reordenado por Cantidad descendiente

    outputDF['SKU'] = df2['SKU']                                        #Asigna columna SKU y de pallets
    outputDF['PalletsParciales'] = (df2['Cantidad']/layersPerPallet).apply(math.ceil)
    outputDF['Asignados'] = False                                       #Columna para indicar si este SKU ya no se debe utilizar
    outputDF = outputDF.set_index(['SKU'])                              #SKU como índice

    self.skuAllocation = outputDF

  def __transferLayer(self, originPalletIndex:int, destinationPalletIndex:int) -> None:
    """Transfiere capa entre pallets. Decrementa cuenta de capas para el SKU al destino correspondiente

    Args:
        originPalletIndex (int): Indice en lista de clase de pallet de suministro al que se le retira la capa
        destinationPalletIndex (int): Indice en lista de clase de pallet de pedido al que se le coloca la capa
        dayDF (pd.DataFrame): DataFrame de pedidos del día. Columnas: Destino, SKU, Cantidad

    """
    layer = self.entryPallets[originPalletIndex].subtractLayer()                #Saca capa de pallet de entrada
    self.exitPallets[destinationPalletIndex].addLayer(layer)                   #Coloca en pallet de salida

    outputDF = self.dayDataset.copy()                             #Copia DF de pedidos
    outputDF = outputDF.reset_index()                             #Resetea index
    x = outputDF[(outputDF['SKU'] == self.entryPallets[originPalletIndex].product) & (outputDF['Destino'] == self.exitPallets[destinationPalletIndex].destination)]
    valueIndex = x.index[0]                                       #Obtiene indice de este valor en filas de DF
    newValue = x['Cantidad'].values[0] - 1                        #Resta 1 al valor de Cantidad
    if newValue > 0:
      outputDF.at[valueIndex, 'Cantidad'] = newValue              #Resta 1 al valor de Cantidad
    else:
      outputDF = outputDF.drop(labels=[valueIndex], axis=0)
    outputDF = outputDF.set_index(['index'])                      #Recupera el índice original
    self.dayDataset = outputDF                                    #Copia DF modificado a variable de clase
    self.transferedLayers += 1                                    #Incrementa cuenta de capas transferidas para registro

  def __changeEntryPallets(self) -> None:
    """Intercambio de pallet de entrada cuando se terminan las capas o no se puede usar más.
    La función se debe ejecutar una vez que se realizó toda una iteración por los pallets de entrada y se asignaron todas
    las capas posibles

    Args:
        entryPallets (List[PalletEntrada]): Lista con objetos de tipo PalletEntrada
        skuAllocation (pd.DataFrame): DataFrame de asignación de SKUs a pallets. Columnas: SKU (index), PalletsParciales, Asignados

    Returns:
        Tuple[List[PalletEntrada], pd.DataFrame]: Lista actualizada con pallets intercambiados, DataFrame actualizado de asignación
    """
    nonConsecutivePallets = True
    deleteEntryPallets = []
    activeSKU = []
    assignedSKU = []

    for o in range(len(self.entryPallets)):
      pallet = self.entryPallets[o]
      activeSKU += [pallet.product]               #Obtiene primero lista de productos activos
      self.skuAllocation.at[pallet.product, 'PalletsParciales'] = self.skuAllocation.at[pallet.product, 'PalletsParciales'] - 1    #Resta uno de los pallets asignados a ese SKU

    #Ahora tiene la lista de asignación actualizada con los pallets ya usados la iteración pasada restados

    auxDF = self.skuAllocation.loc[(self.skuAllocation['PalletsParciales'] > 0)]    #DataFrame solo con los que se pueden usar
    
    for i in range(len(self.entryPallets)):                     #Itera para cada pallet en la lista de entrada
      pallet = self.entryPallets[i]

      remainingPallets = self.skuAllocation.loc[pallet.product]['PalletsParciales']

      #Nuevo algoritmo de cambio de pallets
      if nonConsecutivePallets:                                       
        
        #Filtrado por los SKU que se usaron la iteración pasada y los que se asignen esta
        deleteSkuList = activeSKU + assignedSKU
        deleteSkuList = list(set(deleteSkuList) & set(auxDF.index.to_list()))                        
        usablePalletsDF = auxDF.drop(index=deleteSkuList, axis=0)       #Elimina de la lista los que se están usando ahora y los que se asignaron en esta pasada
        
        #No hay resultados con este filtro. Se hace uno menos estricto
        if usablePalletsDF.empty:                                       #Puede que no haya otros pallets para usar y la vista filtrada da vacío
          lastUsablePalletsDF = auxDF.drop(index=assignedSKU, axis=0)   #Filtra el DF anterior pero dejando los que se usaron hasta ahora
          
          #Tampoco hay resultados con este. Se elimina el pallet
          if lastUsablePalletsDF.empty:                                 #Si en este caso tampoco hay pallets se elimina el pallet de entrada
            deleteEntryPallets += [i]
            continue
          
          #Hay resultados, se asigna el pallet
          else:
            randomProduct = lastUsablePalletsDF.sample(n=1).index.values[0]   #Selecciona produco al azar
            pallet = PalletEntrada(randomProduct)                             #Genera pallet de entrada
            assignedSKU += [randomProduct]                                    #Agrega SKU asignado a la lista para filtrar los demás
        
        #Luego de filtrar hay productos disponibles para elegir
        else:
          randomProduct = usablePalletsDF.sample(n=1).index.values[0]   #Selecciona produco al azar
          pallet = PalletEntrada(randomProduct)                         #Genera pallet de entrada
          assignedSKU += [randomProduct]                                #Agrega SKU asignado a la lista para filtrar los demás

      #Viejo algoritmo de cambio de pallets  
      else:
        if pallet.empty:                                          #Si un pallet está vacío

          if remainingPallets > 1:                               #Se evalúa si todavía faltan usar pallets
            #Acá se cambia de lógica, en lugar se seguir ingresando pallets del mismo producto, se pasa a otro que no esté en uso
            self.skuAllocation.at[pallet.product, 'PalletsParciales'] = self.skuAllocation.at[pallet.product, 'PalletsParciales'] - 1    #Resta uno de los pallets asignados a ese SKU
            pallet = PalletEntrada(pallet.product)                #Crea nuevo pallet del mismo producto en esa posición
          
          else:                                                   #Ya se usaron todos los pallets de ese producto
            if len(self.skuAllocation[self.skuAllocation['Asignados']==False].index) == 0:
              break
            auxProd = self.skuAllocation[self.skuAllocation['Asignados']==False].index.values[0] #Se seleccionan todos los SKUs no asignados y se elige el primero
            pallet = PalletEntrada(auxProd)                   #Se crea nuevo pallet con el primer SKU no asignado
            self.skuAllocation.at[auxProd, 'Asignados'] = True    #El nuevo producto queda como asignado
        
        else:                                                     #El pallet no está vacío
          if remainingPallets == 1:                               #Si es el último de la asignación y le quedan capas se cambia
            if len(self.skuAllocation[self.skuAllocation['Asignados']==False].index) == 0:
              deleteEntryPallets += [i]
              break
            auxProd = self.skuAllocation[self.skuAllocation['Asignados']==False].index.values[0] #Se seleccionan todos los SKUs no asignados y se elige el primero
            pallet = PalletEntrada(auxProd)                   #Se crea nuevo pallet con el primer SKU no asignado
            self.skuAllocation.at[auxProd, 'Asignados'] = True    #El nuevo producto queda como asignado

      self.entryPallets[i] = pallet                             #Modifica valor de variable de clase
      self.palletChanges += 1                                   #Registro para métrica de simulación
      #print(self.palletChanges)
    
    for m in sorted(deleteEntryPallets, reverse=True):  #Reordena lista para borrar comenzando por los indices altos
      del self.entryPallets[m]

  def __getDestinationsForSku(self, SKU:int) -> List[str]:
    """Encuentra destinos que requieren capas del SKU dado

    Args:
        SKU (int): SKU seleccionado

    Returns:
        List[str]: Lista de destinos para ese SKU
    """
    df = self.dayDataset
    query_str = f"SKU=={SKU} & Cantidad>=1"
    auxDF = df.query(query_str)
    #destinos_series = df[(df['SKU']==SKU) & (df['Cantidad'] >= 1)]['Destino'].drop_duplicates().dropna()  #Filtrado por filas con el SKU seleccionado sin duplicados ni NA
    destinos_series = auxDF['Destino'].drop_duplicates().dropna()  #Filtrado por filas con el SKU seleccionado sin duplicados ni NA
    return destinos_series.to_list()                                            #Convertido a lista para retornar

  def __layerTransferProcess(self, entryPalletIndex:int, exitPalletIndex:int) -> None:
    """Transferencia de múltiples capas calculando cuantas puede recibir el pallet y utilizando listas de pallets

    Args:
        entryPalletIndex (int): Indice de pallet de entrada a utilizar de la lista
        exitPalletIndex (int): Indice de pallet de salida a utilizar de la lista (ya existente)
    """
    currentDestination = self.exitPallets[exitPalletIndex].destination    #Define el destino del pallet de salida actual
    destinationDF = self.dayDataset.set_index(['Destino'])
    destinationDF = destinationDF.loc[currentDestination]               #Filtra dataset del día por destino actual
    currentSKU = self.entryPallets[entryPalletIndex].product              #Define producto del pallet actual
    if isinstance(destinationDF, pd.Series):
      destinationDF = destinationDF.to_frame().T
    currentLayers = destinationDF[destinationDF['SKU'] == currentSKU].iloc[0]['Cantidad']  #Cuantas capas precisa el destino actual
    availableLayers = len(self.entryPallets[entryPalletIndex].layers)           #Cuantas capas tiene el pallet de entrada para dar
    palletSpace = layersPerPallet-len(self.exitPallets[exitPalletIndex].layers) #Cuantas capas puede aceptar el pallet
    
    layersQty = min([currentLayers, availableLayers, palletSpace])

    if layersQty > 0:                                           #Verificar que no se intenten transferir capas nulas

      self.batchTransfers += 1                                #Registro para estadísticas de simulación

      for k in range(layersQty):
        self.__transferLayer(entryPalletIndex, exitPalletIndex) #Transfiere las capas

    else:
      pass
      #print("Que pija")
  
  def __checkRemainingLayers(self) -> int:
    """Verifica si todavía quedan capas para paletizar

    Returns:
        int: Cantidad de capas que faltan paletizar
    """
    self.remainingLayers = self.dayDataset['Cantidad'].sum()
    return self.remainingLayers
  
  def exitPalletDefinition(self) -> pd.DataFrame:
    """Define armado de pallets de salida que usen hasta cierta cantidad de productos cada uno.
    Para cada destino define como serán los pallets de salida.

    Returns:
        pd.DataFrame: DataFrame con pallets definidos. Columnas: Destino, Pallet, SKU, Cantidad
    """
    palletAssignmentDF = pd.DataFrame(columns=['Destino', 'Pallet', 'SKU', 'Cantidad'])

    for destination in self.dayDestinations:                                      #Comienza iterando para cada destino
      destinationDF: pd.DataFrame
      destinationDF = self.dayDataset[self.dayDataset['Destino']==destination]    #Obtiene capas de cada SKU necesarias
      new_index = list(range(len(destinationDF['Cantidad'])))
      destinationDF.loc[:,'Ind'] = new_index
      destinationDF = destinationDF.set_index(['Ind'])
      destinationDF = destinationDF.sort_values(by=['Cantidad'], ascending=False) #Ordenado por cantidad de capas en forma descendente
      remainingSkus = destinationDF['SKU'].count()                                 #Cantidad de SKUs por destino
            
      currentPallet = 1
      currentLayer = 0

      while remainingSkus > 0:                                          #Loop secundario por destino hasta asignar todos los SKU a un pallet
        
        startNewPallet = False
        #Comienza por el primero de la lista
        currentSKU = destinationDF.iloc[0]['SKU']                                 #SKU actual

        #Asigna las capas de este primer destino
        layersAvailable = destinationDF.iloc[0]['Cantidad']                       #Cantidad de capas que se precisan de ese SKU
        rowData = {'Destino':[destination], 'Pallet':[currentPallet], 'SKU':[currentSKU], 'Cantidad':[layersAvailable]}   #Fila para agregar al DF de asignación
        palletAssignmentDF = pd.concat([palletAssignmentDF, pd.DataFrame(rowData)])   #Agrega fila a DF de asignación
        currentLayer += layersAvailable                                           #Incrementa cantidad de capas en el pallet actual
        layersMissing = layersPerPallet - currentLayer                            #Cantidad de capas que faltan para completar pallet

        #Elimina el SKU del destinationDF
        destinationDF = destinationDF.drop(index=destinationDF.index.values[0])   #Elimina primer elemento de la lista
        
        #Recorre la lista buscando segundo SKU que complemente
        for i in range(len(destinationDF['Cantidad'])):                           
          if (destinationDF.loc[destinationDF.index.values[i], 'Cantidad'] == layersMissing):           #El SKU actual tiene las capas que se precisan?
            currentSKU = destinationDF.iloc[i]['SKU']                                 #SKU actual
            rowData = {'Destino':[destination], 'Pallet':[currentPallet], 'SKU':[currentSKU], 'Cantidad':[layersMissing]}   #Fila para agregar al DF de asignación
            palletAssignmentDF = pd.concat([palletAssignmentDF, pd.DataFrame(rowData)])                               #Agrega fila a DF de asignación
            destinationDF = destinationDF.drop(index=destinationDF.index.values[i])                                   #Elimina actual elemento de la lista
            currentPallet += 1                                                                                        #Incrementa cantidad de capas en el pallet actual
            currentLayer = 0                                                                                          #Resetea valor de capa actual
            startNewPallet = True
            break                                                                                     #Debe comenzar con un nuevo pallet

        if startNewPallet:                                                        #Se completó pallet en la iteración anterior, debe comenzarse con otra
          continue
        
        #No se pudo armar pallet con solo 2 SKUs
        else:                                                                     #No se completó pallet, se debe recorrer la lista para completar con más de 1 SKU         
          rowDropList = []
          for j in range(len(destinationDF['Cantidad'])):
            currentSKU = destinationDF.loc[destinationDF.index.values[i], 'SKU']  #SKU actual
            layersMissing = layersPerPallet - currentLayer                        #Cantidad de capas que faltan para completar pallet            
            layersAvailable = destinationDF.iloc[j]['Cantidad']                   #Cantidad de capas que se precisan de ese SKU
            if (layersPerPallet >= currentLayer + layersAvailable):               #Verifica si esas capas entran en el pallet actual
                                                                    
              currentSKU = destinationDF.iloc[j]['SKU']                                 #SKU actual
              rowData = {'Destino':[destination], 'Pallet':[currentPallet], 'SKU':[currentSKU], 'Cantidad':[layersAvailable]}   #Fila para agregar al DF de asignación
              palletAssignmentDF = pd.concat([palletAssignmentDF, pd.DataFrame(rowData)])                               #Agrega fila a DF de asignación
              rowDropList += [j]
              
              currentLayer += layersAvailable                                                                           #Incrementa cantidad de capas en el pallet actual

              if currentLayer == layersPerPallet:   #Si se completó el pallet
                currentPallet += 1
                currentLayer = 0
                startNewPallet = True
                break
          
          #Elimina elementos usados al final de la iteración
          if len(rowDropList) > 0:
            orderedDroplist = sorted(rowDropList, reverse=True)
            for k in (orderedDroplist):
              destinationDF = destinationDF.drop(index=destinationDF.index.values[k])      #Elimina actual elemento de la lista
            rowDropList = []

        if not startNewPallet:            #Llega el final de la segunda vuelta y si no terminó el pallet igual resetea para armar otro
          currentPallet += 1
          currentLayer = 0 

        remainingSkus = destinationDF['SKU'].count()      #Actualiza valor de SKUs sin asignar

    return palletAssignmentDF

  def getPartialDF(self, inputDF) -> pd.DataFrame:
    """Convierte a DataFrame si la entrada es una Series luego de filtrar un DF y se retorna solo una fila como Series

    Args:
        inputDF (_type_): Objeto de tipo pd.DataFrame o pd.Series

    Raises:
        TypeError: El tipo del inputDF no es de los esperados

    Returns:
        pd.DataFrame: inputDF como pd.DataFrame
    """
    if isinstance(inputDF, pd.Series):
      outputDF = inputDF.to_frame().T
    elif isinstance(inputDF, pd.DataFrame):
      outputDF = inputDF
    else:
      raise TypeError("No es ni un pd.Series ni pd.DataFrame")
    return outputDF

  def entryPalletSelection(self, exitPalletsDF:pd.DataFrame, numPalletsEntry:int) -> List[pd.DataFrame]:
    """Selección de pallets de entrada a partir de pallets de salida esperados

    Args:
        exitPalletsDF (pd.DataFrame): DataFrame con asignación de capas y SKUs a pallets de salida por destino. Columnas: Destino, Pallet, SKU, Cantidad
        numPalletsEntry (int): Cantidad límite de pallets de entrada por grupo

    Returns:
        List[pd.DataFrame]: DataFrames con los pallets de entrada y de salida vinculados por variable de Grupo
    """

    palletsEntrada = pd.DataFrame(columns=['Grupo', 'SKU', 'CantidadPallets'])                  #DF con pallets de entrada
    palletsSalida = pd.DataFrame(columns=['Grupo', 'Destino', 'Pallet', 'SKU', 'Cantidad'])     #DF con pallets de salida referenciados a los de entrada por grupo
    exitPalletAssignmentDF = exitPalletsDF.copy()
    newInd = list(range(exitPalletAssignmentDF.shape[0]))
    exitPalletAssignmentDF = exitPalletAssignmentDF.assign(Ind = newInd)
    exitPalletAssignmentDF = exitPalletAssignmentDF.set_index(['Ind'])


    remainingPallets = exitPalletAssignmentDF.shape[0]
    group = 1

    while remainingPallets > 0:

      #Aca crear while para todos los pallets
      currentDestination = exitPalletAssignmentDF.iloc[0]['Destino']                  #Nombre del primer destino de la lista
      currentPalletNumber  = exitPalletAssignmentDF[exitPalletAssignmentDF['Destino']==currentDestination].iloc[0]['Pallet']      #Número de pallet del 1ro en la lista
      #Lista de SKUs del primer pallet en la lista

      auxDF = exitPalletAssignmentDF[exitPalletAssignmentDF['Destino']==currentDestination]
      currentPalletSkus = auxDF[auxDF['Pallet']==currentPalletNumber]['SKU']

      exitDestinations = exitPalletAssignmentDF['Destino'].unique().tolist()      #Listado de destinos distintos para el día
      otherDestinations = exitDestinations.copy()
      otherDestinations.remove(currentDestination)             #Listado de destintos sin el actual
      
      skusComplete = False

      if len(currentPalletSkus) < numPalletsEntry:  #Comienza por verificar si con los SKUs que se precisan para el pallet seleccionado se cubren todos los de entrada necesarios
        for dest in otherDestinations:              #No hay suficientes SKU entonces busca otro pallet de otro destino
          destPallets = exitPalletAssignmentDF[exitPalletAssignmentDF['Destino']==dest]['Pallet']   #Lista de pallets en destino
          
          for palNum in destPallets:            #Busca en todos los pallets del destino si alguno comparte SKUs con el ya elegido antes
            auxDF = exitPalletAssignmentDF[exitPalletAssignmentDF['Destino']==dest]
            destSkuList = auxDF[auxDF['Pallet']==palNum]['SKU']                        #Lista de SKUs de cada pallet
            intersection = len(set(destSkuList) & set(currentPalletSkus))              #Cantidad de elementos iguales entre ambas listas
            
            if (intersection > 0):                                                                  #Tienen al menos uno en común
              currentPalletSkus = list(set(destSkuList) | set(currentPalletSkus))                   #Añade nuevos SKU a la lista

              if ((len(currentPalletSkus) + len(destSkuList) - intersection) == numPalletsEntry):   #Verifica si ya se completaron los SKUs requeridos
                skusComplete = True

            if skusComplete:        #Condición para cortar loop de pallets dentro de destino
              break
          
          if skusComplete:          #Condición para cortar loop de destinos
            break
      
      else:                         #Si los SKUs del pallet elegido son más de los deseados igual se mantiene esa cantidad
        numPalletsEntry = len(currentPalletSkus)  #Se modifica para este pallet

      dfPalletDropList = []

      #Ya se cuenta con una lista de SKUs para entrada. Se deben definir los pallets de salida que se pueden armar con ellos.
      for dest in exitDestinations:
        destPallets = exitPalletAssignmentDF[exitPalletAssignmentDF['Destino']==dest]['Pallet'].unique()   #Lista de pallets en destino
        
        for pal in destPallets:
          auxDF = exitPalletAssignmentDF[exitPalletAssignmentDF['Destino']==dest]
          destSkuList = auxDF[auxDF['Pallet']==pal]['SKU']                                        #SKUs para este pallet y destino
          
          if set(destSkuList).issubset(set(currentPalletSkus)):                                   #Si el pallet puede ser armado por los SKU elegidos
            partialDF = auxDF[auxDF['Pallet']==pal].copy()                                        #Copia porción de DF que importa
            drop_indexes = auxDF[auxDF['Pallet']==pal].index.values.tolist()                      #Obtiene indices para poder eliminarlos de DF original
            partialDF = partialDF.assign(Grupo=group)                                       #Agrega columna de grupo a copia de DF
            partialDF['Cantidad'] = partialDF['Cantidad'].astype('int64')
            palletsSalida = pd.concat([palletsSalida, partialDF])                                 #Concatena para armar DF de salida
            palletsSalida = palletsSalida.convert_dtypes(convert_integer=True)
            dfPalletDropList += drop_indexes                                                    #Agrega índices a lista para eliminarlos

      #Se definen a partir de los SKU los pallets de entrada
      for sku in currentPalletSkus:
        auxDF2 = palletsSalida[palletsSalida['Grupo']==group]
        qty = auxDF2.groupby(by=['SKU'], as_index=False).sum(numeric_only=True)
        palQty = (qty['Cantidad']/layersPerPallet).apply(math.ceil)
        data = {'Grupo':[group], 'SKU':[sku], 'CantidadPallets':[palQty]}
        auxDF = pd.DataFrame(data)
        palletsEntrada = pd.concat([palletsEntrada, auxDF])

      if len(dfPalletDropList) > 0:                   #Verifica si debe eliminar pallets de salida
        for m in sorted(dfPalletDropList, reverse=True):    #Reordena lista para borrar comenzando por los indices altos
          exitPalletAssignmentDF.drop(index=m, axis=0)      #Lo elimina del DF en uso                        
        dfPalletDropList = []                         #Una vez eliminados se vacía la lista

      group += 1                                              #Pasa al siguiente grupo
      remainingPallets = exitPalletAssignmentDF.shape[0]      #Actualiza cantidad de pallets restantes

    return palletsEntrada, palletsSalida
  
  def unlimitedExitSimulation(self, startingPallets:int) -> None:
    """Simulación de paletizado simple. Se limitan pallets de entrada y se asignan las capas de cada uno hasta completarlo
    y abriendo los pallets de salida necesarios para eso

    Args:
        startingPallets (int): Cantidad de pallets de entrada (SKU distintos)
    """
    #Lista de pallets de entrada
    startPallets = startingPallets if len(self.skuAllocation.index.values) >= startingPallets else len(self.skuAllocation.index.values)
    for i in range(startPallets):
      self.entryPallets += [PalletEntrada(self.skuAllocation.index[i])]
      self.skuAllocation.at[self.skuAllocation.index[i], 'Asignados'] = True

    self.remainingLayers = self.__checkRemainingLayers()
    self.totalPallets = self.skuAllocation['PalletsParciales'].sum()

    #-----------------~~~~~~~~~~~~~~~~~~~~-----------------
    #Loop principal. Idealmente el umbral tiene que ser 0.
    #-----------------~~~~~~~~~~~~~~~~~~~~-----------------
    while (self.remainingLayers > 0) and (self.skuAllocation['PalletsParciales'].sum() > 0):
      self.dayDataset = self.dayDataset[self.dayDataset['Cantidad']>0]
      #----Algoritmo principal----
      if len(self.entryPallets) == 0:
        print("SHIT")
        print(f"Capas {self.remainingLayers}/{self.totalLayers}")
        break
      for i in range(len(self.entryPallets)):                          #Comienza iterando por cada pallet de entrada
        if self.entryPallets[i].empty:                          #Si el pallet está vacío debe cambiarlo y pasar al siguiente pallet de entrada
          continue
        
        currentSKU = self.entryPallets[i].product               #Define producto del pallet actual
        possibleDestinations = self.__getDestinationsForSku(currentSKU)  #Obtiene lista de destinos posibles para el SKU

        if len(possibleDestinations) == 0:                      #Verifica si existen destinos posibles
          continue                                              #Si no existen destinos continúa con el siguiente pallet de entrada
        
        #Comienza loop secundario para que solo cambie de pallet de entrada cuando lo termina o no hay destinos
        while ((not self.entryPallets[i].empty) and (len(possibleDestinations) > 0)):
          
          remainingLayers = self.__checkRemainingLayers()             #Valores para registro de simulación
          self.numExitPallets = len(self.exitPallets)                 #Valores para registro de simulación
          self.numCompletedPallets = len(self.completedExitPallets)   #Valores para registro de simulación

          #Registro de simulación
          tempData = {'RemLayers': [self.remainingLayers], 'ExitPallets': [self.numExitPallets], 'CompPallets':[self.numCompletedPallets],\
                       'LayerTransfers': [self.transferedLayers], 'BatchTransfers': [self.batchTransfers], 'PalletChanges':[self.palletChanges]}
          tempDF = pd.DataFrame(data=tempData, index=[self.simRecordIndex])
          self.simRecordIndex += 1
          self.simulationRecord = pd.concat([self.simulationRecord, tempDF])

          if len(self.exitPallets) > 0:
            palletFound = False

            #Itera por cada pallet de salida
            for j in range(len(self.exitPallets)):
              palletFound = False
              #Verifica si el destino del pallet es de los buscados y si el pallet tiene lugar libre              
              if ((self.exitPallets[j].destination in possibleDestinations) and (not self.exitPallets[j].complete)): 
                palletFound = True                                #Encontró un pallet para asignar capas

              else:
                if self.exitPallets[j].complete:                  #Si el pallet de destino está completo
                  if not (j in self.deleteExitPallets):           #Y si no está para borrar ya
                    self.deleteExitPallets += [j]                 #Agrega indice de pallet a la lista para eliminar

              #Operación cuando hay pallet para transferir capas
              if palletFound:
                self.__layerTransferProcess(i, j)                 #Transferencia de capas entre pallets
                possibleDestinations = self.__getDestinationsForSku(currentSKU)  #Obtiene lista de destinos posibles para el SKU
          
            if len(possibleDestinations) == 0:                      #Verifica si existen destinos posibles
              continue                                              #Si no existen destinos continúa con el siguiente pallet de entrada
        
            #Si iterando por los pallets de salida no se encontró ninguno para transferir capas
            if not palletFound:
              self.exitPallets += [PalletSalida(possibleDestinations[0])]   #Se crea un pallet de salida con el primer destino
              
              self.__layerTransferProcess(i, -1)                  #Transferencia de capas entre pallets. 
                                                                  #Indice -1 para destino referencia pallet recién creado
              possibleDestinations = self.__getDestinationsForSku(currentSKU)  #Obtiene lista de destinos posibles para el SKU
          
          else:                                                   #No existen pallets de salida todavía
            if len(possibleDestinations) == 0:                    #Verifica si existen destinos posibles
              continue                                            #Si no existen destinos continúa con el siguiente pallet de entrada
        
            self.exitPallets += [PalletSalida(possibleDestinations[0])]   #Se crea un pallet de salida con el primer destino
              
            self.__layerTransferProcess(i, -1)                    #Transferencia de capas entre pallets. 
                                                                  #Indice -1 para destino referencia pallet recién creado
            possibleDestinations = self.__getDestinationsForSku(currentSKU)  #Obtiene lista de destinos posibles para el SKU
          
          #Iteración por pallets de salida existentes terminada
          
          for p in range(len(self.exitPallets)):                       #Verifica si hay pallets para cerrar porque no quedan productos
            currentDest = self.exitPallets[p].destination       #Destino de cada pallet de salida para verificar
            if len(self.dayDataset[self.dayDataset['Destino']==currentDest]) == 0:   #No quedan capas por asignar a ese destino
              if not (p in self.deleteExitPallets):             #Y si no está para borrar ya
                    self.deleteExitPallets += [p]               #Agrega indice de pallet a la lista para eliminar

          if len(self.deleteExitPallets) > 0:                   #Verifica si debe eliminar pallets de salida
            for m in sorted(self.deleteExitPallets, reverse=True):  #Reordena lista para borrar comenzando por los indices altos
              self.completedExitPallets += [self.exitPallets[m]]#Guarda pallet en lista de completados
              del self.exitPallets[m]                           #Lo elimina de lista de en uso
            self.deleteExitPallets = []                         #Una vez eliminados se vacía la lista

          #Vuelve a evaluar si continua con el mismo pallet de entrada o cambia
          possibleDestinations = self.__getDestinationsForSku(currentSKU)  #Obtiene lista de destinos posibles para el SKU
         
      
      #Terminó de iterar por todos los pallets de entrada  
      self.__changeEntryPallets()                               #Intercambio de pallets de entrada

      self.remainingLayers = self.__checkRemainingLayers()

    self.aa = self.simulationRecord['ExitPallets'].max()
    #self.simulationRecord.plot(grid=True, style='.-')
    #plt.show()

  def __multilayerTransfer(self, entryPalletIndex:int, exitPalletIndex:int, layerQuantity:int) -> int:
    """Transferencia de múltiples capas entre pallets especificando cantidades

    Args:
        entryPalletIndex (int): Indice de pallet de entrada a utilizar de la lista
        exitPalletIndex (int): Indice de pallet de salida a utilizar de la lista (ya existente)
        layerQuantity (int): Cantidad de capas a transferir
    
    Returns:
        int: Cantidad de capas transferidas
    """
    availableLayers = len(self.entryPallets[entryPalletIndex].layers)           #Cuantas capas tiene el pallet de entrada para dar
    layersQty = min([layerQuantity, availableLayers])                           #Cantidad de capas a transferir finalmente

    if layersQty > 0:                                           #Verificar que no se intenten transferir capas nulas

      self.batchTransfers += 1                                  #Registro para estadísticas de simulación

      for k in range(layersQty):
        self.__transferLayer(entryPalletIndex, exitPalletIndex) #Transfiere las capas

    return layersQty

  def __endSimulationBatch(self, entryPallets:pd.DataFrame, exitPallets:pd.DataFrame, group:int) -> List[pd.DataFrame]:
    """Cambia los pallets de entrada y salida. Para pallets de entrada si quedan en el grupo pone esos, sino cambia de grupo.

    Args:
        entryPallets (pd.DataFrame): DataFrame de pallets de entrada. Columnas: 'Grupo', 'SKU', 'CantidadPallets'
        exitPallets (pd.DataFrame): DataFrame de pallets de salida. Columnas: 'Grupo', 'Destino', 'Pallet', 'SKU', 'Cantidad'
        group (int): Grupo actual

    Returns:
        List[pd.DataFrame]: _description_
    """
    groupEntryPallets = entryPallets[entryPallets['Grupo']==self.group]    #Filtrado de pallets de entrada por grupo
    remPalletsGroup = groupEntryPallets['Cantidad'].sum()                  #Cantidad de pallets que quedan para el grupo   
    totalGroups = pd.unique(palEntr['Grupo'])

    if remPalletsGroup > 0:           #no quedan pallets del grupo, se cambia al siguiente
      self.entryPallets = []          #Hay que vaciar la lista de pallets de entrada para colocar los nuevos
      self.group += 1
    else: 
      self.entryPallets = []          #Hay que vaciar la lista de pallets de entrada para colocar los nuevos
      for m in len(self.exitPallets): #Se guardan los pallets completados
        self.completedExitPallets += [self.exitPallets[m]]
      self.exitPallets = []          #Se vacía la lista de pallets de salida


  def limitedPositionSimulation(self, maxProdsEntry:int) -> None:
    """Simulación de paletizado donde se predefinen los pallets de destino por tandas limitadas
    Se paletiza por tandas intercambiando los pallets de entrada hasta que todos los de salida estén completos

    Args:
        maxProdsEntry (int): Cantidad de pallets de entrada
    """
    exitPalletAssignment = self.exitPalletDefinition()
    # palEntr: 'Grupo', 'SKU', 'CantidadPallets'
    # palSal: 'Grupo', 'Destino', 'Pallet', 'SKU', 'Cantidad'
    palEntr, palSal = self.entryPalletSelection(exitPalletAssignment, maxProdsEntry)

    palletGroups = pd.unique(palEntr['Grupo'])        #Array de grupos
    self.group = 1
    self.remainingLayers = palSal['Cantidad'].sum()

    #Loop principal por grupos de pallets de salida
    while (self.remainingLayers > 0):

      groupEntryPallets = palEntr[palEntr['Grupo']==self.group]                        #Filtrado de pallets de entrada por grupo
      entryPalsQty = maxProdsEntry if groupEntryPallets.shape[0] >= maxProdsEntry else groupEntryPallets.shape[0]     #Verifica cuantos puede crear
      
      for entryPalsIndx in range(entryPalsQty):                                   #Loop de creación de pallets de entrada
        self.entryPallets += [PalletEntrada(groupEntryPallets.iloc[entryPalsIndx]['SKU'])]    #Creación de cada pallet
        palEntr.iat[entryPalsIndx, 2] -= 1                                        #Se le resta uno a la cantidad de pallets de ese SKU para ese grupo

      groupExitPallets = palSal[palSal['Grupo']==self.group]
      exitPalsQty = groupExitPallets.shape[0]                                     #Cantidad de pallets de salida para ese grupo
      
      for exitPalsIndx in range(exitPalsQty):                                                #Loop de creación de pallets de salida y asignación de capas
        self.exitPallets += [PalletSalida(groupExitPallets.iloc[exitPalsIndx]['Destino'])]   #Creación de cada pallet

      #Comienza loop de asignación de capas
      for i in range(len(self.entryPallets)):                   #Recorre para cada pallet de entrada
        if self.entryPallets[i].empty:
          continue

        currentSKU = self.entryPallets[i].product               #Define producto del pallet actual

        for j in range(len(self.exitPallets)):                  #Recorre para cada pallet de salida
          palletDestination = self.exitPallets[j].destination   #Destino de pallet de salida

          #no se si se puede asumir que en cada grupo no va a haber destinos repetidos
          #supongo que hay solo un pallet por destino en un grupo
          query_str = f"Destino=={palletDestination} & SKU=={currentSKU} & Cantidad >= 1"
          palletProducts = groupExitPallets.query(query_str)
          #Devuelve DF con columnas SKU y Cantidad
          
          if not palletProducts.empty:                          #Si dio algún resultado el filtro
            layersToTransfer = palletProducts['Cantidad']
            rowIndex = layersToTransfer.index.values[0]
            transferedLayers = self.__multilayerTransfer(i, j, layersToTransfer)   #Transferencia de capas
            palSal.at[rowIndex, 'Cantidad'] -= transferedLayers
        #Terminó iteración por pallets de salida
      #Terminó de pasar por todos los pallets de entrada

      self.remainingLayers = palSal['Cantidad'].sum()

          

  @timer
  def daySimulation(self, startingPallets:int):
    """Simulación de paletizado para un día

    Args:
        dayDataset (pd.DataFrame): Dataset de capas por cliente para el día dado. Columnas: Destino (index), SKU, Cantidad
        startingPallets (int): Cantidad de pallets de entrada que se utilizarán
    """
    #DataFrame de SKUs y cantidades
    self.__getSkuAllocation()
    self.totalLayers = self.dayDataset['Cantidad'].sum()

    #Simulación simple
    self.unlimitedExitSimulation(startingPallets=startingPallets)

  def resetSimulation(self):
    """Reinicia todas las variables de clase para poder correr una nueva simulación
    """
    self.dayDataset = pd.DataFrame(columns=['Destino', 'SKU', 'Cantidad'])
    self.skuAllocation = pd.DataFrame(columns=['PalletsParciales'], index=['SKU'])

    #Lista de pallets de entrada
    self.entryPallets = []

    #Lista de pallets de salida
    self.exitPallets = []
    self.completedExitPallets = []

    #Lista de indices para eliminar pallets de salida luego de una iteración completa
    self.deleteExitPallets = []
    
    #Métricas de simulación
    self.remainingLayers = 0
    self.numExitPallets = 0
    self.numCompletedPallets = 0
    self.transferedLayers = 0
    self.batchTransfers = 0
    self.totalPallets = 0
    self.palletChanges = 0
    self.simRecordIndex = 0
    self.simulationRecord = pd.DataFrame(columns=['RemLayers', 'ExitPallets', 'CompPallets', 'LayerTransfers', 'BatchTransfers', 'PalletChanges'])

if __name__ == '__main__':

  timestamp = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
  logging.info(f"Comenzando simulación - {timestamp}")

  sim = Simulation("BD  Pedidos  CEDIS MAR-ABR-MAY- 2023.csv")
  dataset_completo = sim.getSimulationDataset(20)
  dias = sim.days
  makeGraph = False
  iters = 15
  openPosDF = pd.DataFrame(columns=dias, index=list(range(iters)))

  
  
  movements_day_DF = pd.DataFrame(columns=['Fecha', 'Movimientos', 'CapasTotales'])
  dayStatsDF = pd.DataFrame(columns=['Fecha', 'CapasTotales', 'MovConPiso', 'MovEnGrupo', 'Transferencias'])
  #movements_day_DF, _ = sim.bestCasePalletizing(dataset_completo)
  batchMovements = []
  layerTransfers = []

  for dia in dias:
    openPosList = []
    
    sim.getSimulationDayDataset(dia, dataset_completo)

    exitPalletAssignment = sim.exitPalletDefinition(2)
    palEntr, palSal = sim.entryPalletSelection(exitPalletAssignment, 6)

    sim.daySimulation(10)
    batchMovements += [sim.batchTransfers]
    layerTransfers += [sim.transferedLayers]
    logging.info(f"{dia} - Remaining layers {sim.remainingLayers}/{sim.totalLayers}")
    if sim.remainingLayers > 0:
      logging.warning(f"No se asignaron todas las capas el día {dia}")
    sim.resetSimulation()
    """
    for i in range(iters):
      sim.getSimulationDayDataset(dia, dataset_completo)
      #sim.exitPalletDefinition(3)
      sim.daySimulation(20)
      print(f"{20} Posiciones de entrada - {sim.aa} Posiciones de salida max")
      openPosList += [sim.aa]     
      sim.resetSimulation()
    openPosDF[dia] = openPosList
    """
  print("FIN!")

  dayStatsDF['Fecha'] = movements_day_DF['Fecha']
  dayStatsDF['CapasTotales'] = movements_day_DF['CapasTotales']
  dayStatsDF['MovConPiso'] = movements_day_DF['Movimientos']
  dayStatsDF['MovEnGrupo'] = batchMovements
  dayStatsDF['Transferencias'] = layerTransfers
  logging.info(f"Capas totales: Min={dayStatsDF.loc[:, 'CapasTotales'].min()} - Mean={dayStatsDF.loc[:, 'CapasTotales'].mean()} - Max={dayStatsDF.loc[:, 'CapasTotales'].max()}")
  logging.info(f"Movimientos dejando piso: Min={dayStatsDF.loc[:, 'MovConPiso'].min()} - Mean={dayStatsDF.loc[:, 'MovConPiso'].mean()} - Max={dayStatsDF.loc[:, 'MovConPiso'].max()}")
  logging.info(f"Porcentaje pisos: Min={movements_day_DF.loc[:, 'Ratio'].min()} - Mean={movements_day_DF.loc[:, 'Ratio'].mean()} - Max={movements_day_DF.loc[:, 'Ratio'].max()}")
  logging.info(f"Movimientos multi capa: Min={dayStatsDF.loc[:, 'MovEnGrupo'].min()} - Mean={dayStatsDF.loc[:, 'MovEnGrupo'].mean()} - Max={dayStatsDF.loc[:, 'MovEnGrupo'].max()}")
  logging.info(f"Transferencias: Min={dayStatsDF.loc[:, 'Transferencias'].min()} - Mean={dayStatsDF.loc[:, 'Transferencias'].mean()} - Max={dayStatsDF.loc[:, 'Transferencias'].max()}")

  dayStatsDF.set_index(['Fecha'], inplace=True) 

  timestamp = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
  logging.info(f"Comenzando simulación - {timestamp}") 

  if makeGraph:
    dayStatsDF.plot(grid=True, style='.-')
    plt.show()
 

