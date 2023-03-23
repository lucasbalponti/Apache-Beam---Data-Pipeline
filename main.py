import pandas as pd
import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

# Criando as funções que serão utilizadas na pipeline

def texto_para_lista(texto, delimitador):
    """
    Recebe uma linha de texto e transforma em uma lista
    """
    return texto.split(delimitador)

def lista_para_dicionario(lista, chaves):
    """
    Recebe uma lista e transforma em dicionário
    """
    return dict(zip(chaves, lista))

def trata_data(dicionario):
    """
    Cria uma entrada no dicionário contendo apenas o mês e o ano da data
    """
    dicionario['ano_mes'] = '-'.join(dicionario['data_iniSE'].split('-')[:2])
    return dicionario

def chave_uf(dicionario):
    """
    Recebe um dicionário e retorna uma tupla contendo a UF e o dicionário
    """
    chave = dicionario['uf']
    return (chave, dicionario)

def casos_dengue(tupla):
    """
    Recebe uma tupla contendo a UF e uma lista de dicionários e retorna uma tupla
    contendo UF-Ano-Mês e contagem de casos
    """
    uf, registros = tupla

    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)


# Lendo o header do arquivo de casos de dengue:
header_dengue = list(pd.read_csv('casos_dengue.txt', sep='|', nrows=1).columns.values)

# Iniciando uma instância de configurações de pipeline e uma instância de pipeline
pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

# Criando a pcollection 'dengue'
dengue = (
    pipeline
    | "Leitura do dataset de casos de dengue" >> ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "Convertendo texto para lista" >> beam.Map(texto_para_lista, delimitador='|')
    | "Convertendo lista para dicionários" >> beam.Map(lista_para_dicionario, chaves=header_dengue)
    | "Criando campo ano_mes" >> beam.Map(trata_data)
    | "Adiciona a chave da uf junto ao dicionario" >> beam.Map(chave_uf)
    | "Agrupando por uf" >> beam.GroupByKey()
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue)
    | "Somar por uf-ano-mes" >> beam.CombinePerKey(sum)
    | "Mostrar resultados" >> beam.Map(print)
)

# Rodando a pipeline
pipeline.run()