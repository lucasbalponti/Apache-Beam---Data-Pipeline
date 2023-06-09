import pandas as pd
import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
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

def lista_chave_uf_ano_mes(lista):
    """
    Recebe uma lista e retorna uma tupla contendo UF-Ano-Mês e mm de chuva
    """
    data, mm, uf = lista
    ano_mes = '-'.join(data.split('-')[:2])

    if float(mm)<0:
        mm = 0.0
    else:
        mm = round(float(mm), 1)
    return (f'{uf}-{ano_mes}', mm)

def filtrar_vazios(tupla):
    """
    Remove elementos com campos vazios
    """
    chave, dados = tupla

    if all((dados['chuvas'], dados['dengue'])):
        return True
    else:
        return False

def descompactar_elementos(tupla):
    """
    Descompacta os elementos para campos de uma tupla
    """
    chave, dados = tupla
    chuvas = dados['chuvas'][0]
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split('-')

    return (uf, ano, mes, str(chuvas), str(dengue))

def preparar_csv(tupla, delimitador):
    """
    Recebe uma tupla e retorna uma string delimitada
    """
    return delimitador.join(tupla)

# Lendo o header do arquivo de casos de dengue:
header_dengue = list(pd.read_csv('casos_dengue.txt', sep='|', nrows=1).columns.values)

# Iniciando uma instância de configurações de pipeline e uma instância de pipeline
pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

# Criando a pcollection 'dengue'
dengue = (
    pipeline
    | "Dengue - Leitura do dataset" >> ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "Dengue - Convertendo texto para lista" >> beam.Map(texto_para_lista, delimitador='|')
    | "Dengue - Convertendo lista para dicionários" >> beam.Map(lista_para_dicionario, chaves=header_dengue)
    | "Dengue - Criando campo ano_mes" >> beam.Map(trata_data)
    | "Dengue - Adiciona a chave da uf junto ao dicionario" >> beam.Map(chave_uf)
    | "Dengue - Agrupando por uf" >> beam.GroupByKey()
    | "Dengue - Descompactando" >> beam.FlatMap(casos_dengue)
    | "Dengue - Somar por uf-ano-mes" >> beam.CombinePerKey(sum)
)

# Criando a pcollection 'chuvas'
chuvas = (
    pipeline
    | "Chuvas - Leitura do dataset" >> ReadFromText('chuvas.csv', skip_header_lines=1)
    | "Chuvas - Convertendo texto para lista" >> beam.Map(texto_para_lista, delimitador=',')
    | "Chuvas - Retornando tupla contando chave uf-ano-mes e mms" >> beam.Map(lista_chave_uf_ano_mes)
    | "Chuvas - Somar por uf-ano-mes" >> beam.CombinePerKey(sum)
)

# Criando a pcollection 'resultado'
resultado = (
    ({'chuvas': chuvas, 'dengue': dengue})
    | "Resultados - Agrupando valores pelo dicionário" >> beam.CoGroupByKey()
    | "Resultados - Filtrando dados vazios" >> beam.Filter(filtrar_vazios)
    | "Resultados - Descompactando elementos" >> beam.Map(descompactar_elementos)
    | "Resultados - Prepara csv" >> beam.Map(preparar_csv, delimitador=';')
)


#Criando Header para exportação
header = 'UF; ANO; MES; CHUVA; DENGUE'

# Exportando os dados
resultado | "Escrevendo o resultado em um arquivo" >> WriteToText('resultado', file_name_suffix='.csv', header = header)

# Rodando a pipeline
pipeline.run()