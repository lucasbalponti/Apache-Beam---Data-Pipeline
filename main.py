import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

# Definindo método para transformar uma linha de texto em uma lista
def texto_para_lista(texto, delimitador):
    return texto.split(delimitador)

# Iniciando uma instância de configurações de pipeline e uma instância de pipeline
pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

# Criando a pcollection 'dengue'
dengue = (
    pipeline
    | "Leitura do dataset de casos de dengue" >> ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "Convertendo texto para lista" >> beam.Map(texto_para_lista, delimitador='|')
    | "Mostrar resultados" >> beam.Map(print)
)

# Rodando a pipeline
pipeline.run()