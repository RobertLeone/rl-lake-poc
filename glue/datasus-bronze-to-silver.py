from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.job import Job

# AWS Glue imports
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Inicializando o GlueContext
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

def input_data(input):
    """
    Função que faz a leitura dos dados na bronze.
    """
    try:
        df = spark.read.json(f"s3://rl-bronze-lake/datasus/{input}/")
    except Exception as e:
        raise(f"Input error: {e}")
    return df

def output_data(df):
    """
    Função que retorna os dados transformar e processados em parquet para a silver.
    """
    try:
        df.write.mode("overwrite").parquet(f"s3://rl-silver-lake/datasus/")
    except Exception as e:
        raise(f"Output error: {e}")

def explode_struct(nested_df):
    """
    Função que transforma os dados dentro da Struct em colunas.
    """
    try:
        flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
        nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

        flat_df = nested_df.select(flat_cols +
                                [col(nc+'.'+c).alias(c)
                                    for nc in nested_cols
                                    for c in nested_df.select(nc+'.*').columns])
    except Exception as e:
        print(f"Explode struct function error: {e}")
    return flat_df

def adjust_json_filed(noisy_df):
    """
    Função para realizar um tratamento necessário, pois cada json vem como lista
    dentro de uma lista, então eu removo o primeiro array, retiro o [] e depois
    transformo a coluna em struct.
    """
    try:
        df_stage = noisy_df
        df_stage = df_stage.withColumn("id", explode_outer("id"))
        df_stage = df_stage.withColumn("id", regexp_replace("id","[\\[\\]]",""))
        df_stage = df_stage.withColumn("id",from_json(col("id"), schema))
        df_stage = df_stage.select("id._source")
    except Exception as e:
        raise(f"Aduste json function error: {e}")
    return df_stage

def transform_int(type, df, *args):
    """
    Função que faz tratamentos do tipo inteiro.
    """
    try:
        dataframe = df
        if type == "int":
            for cols in args:
                dataframe = dataframe.withColumn(f"{cols}",col(f"{cols}").cast(IntegerType()))
        elif type == "long":
            for cols in args:
                dataframe = dataframe.withColumn(f"{cols}",col(f"{cols}").cast(LongType()))
        elif type == "double":
            for cols in args:
                dataframe = dataframe.withColumn(f"{cols}",col(f"{cols}").cast(DoubleType()))
        else:
            raise Exception("Coudn't find type!")
    except Exception as e:
        raise(f"Transform int function error: {e}")
    return dataframe

def transform_date(type,df,*args):
    """
    Função que faz tratamentos do tipo data.
    """
    try:
        dataframe = df
        if type == "timestamp":
            for cols in args:
                dataframe = dataframe.withColumn(f"{cols}",col(f"{cols}").cast(TimestampType()))
        elif type == "date":
            for cols in args:
                dataframe = dataframe.withColumn(f"{cols}",col(f"{cols}").cast(DateType()))
        elif type == "to_date":
            for cols in args:
                dataframe = dataframe.withColumn(f"{cols}",to_date(col(f"{cols}"), "MM/dd/yyyy"))
        elif type == "to_stamp":
            for cols in args:
                dataframe = dataframe.withColumn(f"{cols}",to_timestamp(col(f"{cols}"), "MM/dd/yyyy HH:mm:ss"))
        else:
            raise Exception("Coudn't find type!")
    except Exception as e:
        raise(f"Transform date function error: {e}")
    return dataframe

schema = StructType([
            StructField("_index", StringType(), True),
            StructField("_type", StringType(), True),
            StructField("_id", StringType(), True),
            StructField("_score", StringType(), True),
            StructField("_source", StructType([
                StructField('estabelecimento_municipio_codigo', StringType(), True),
                StructField('estabelecimento_razaoSocial', StringType(), True),
                StructField('vacina_fabricante_referencia', StringType(), True),
                StructField('paciente_racaCor_valor', StringType(), True),
                StructField('estalecimento_noFantasia', StringType(), True),
                StructField('estabelecimento_municipio_nome', StringType(), True),
                StructField('vacina_grupoAtendimento_codigo', StringType(), True),
                StructField('estabelecimento_valor', StringType(), True),
                StructField('paciente_endereco_coPais', StringType(), True),
                StructField('vacina_descricao_dose', StringType(), True),
                StructField('vacina_nome', StringType(), True),
                StructField('data_importacao_rnds', StringType(), True),
                StructField('dt_deleted', StringType(), True),
                StructField('vacina_fabricante_nome', StringType(), True),
                StructField('@timestamp', StringType(), True),
                StructField('paciente_endereco_coIbgeMunicipio', StringType(), True),
                StructField('vacina_lote', StringType(), True),
                StructField('status', StringType(), True),
                StructField('vacina_grupoAtendimento_nome', StringType(), True),
                StructField('vacina_codigo', StringType(), True),
                StructField('paciente_id', StringType(), True),
                StructField('estabelecimento_uf', StringType(), True),
                StructField('id_sistema_origem', StringType(), True),
                StructField('paciente_enumSexoBiologico', StringType(), True),
                StructField('paciente_idade', StringType(), True),
                StructField('vacina_numDose', StringType(), True),
                StructField('vacina_dataAplicacao', StringType(), True),
                StructField('paciente_dataNascimento', StringType(), True),
                StructField('@version', StringType(), True),
                StructField('paciente_endereco_nmPais', StringType(), True),
                StructField('paciente_endereco_uf', StringType(), True),
                StructField('vacina_categoria_nome', StringType(), True),
                StructField('document_id', StringType(), True),
                StructField('sistema_origem', StringType(), True),
                StructField('paciente_endereco_cep', StringType(), True),
                StructField('paciente_racaCor_codigo', StringType(), True),
                StructField('paciente_nacionalidade_enumNacionalidade', StringType(), True),
                StructField('data_importacao_datalake', StringType(), True),
                StructField('paciente_endereco_nmMunicipio', StringType(), True),
                StructField('vacina_categoria_codigo', StringType(), True)
            ])),
            StructField("sort", StringType(), True)

])

def etl_datasus():
    """
    Função que realiza o ETL completo com todas as transformações necessárias.
    """
    try:
        df_datasus = input_data("date=*")
        df_datasus = adjust_json_filed(df_datasus)
        df_datasus = explode_struct(df_datasus)
        df_datasus = transform_int("long", df_datasus, "estabelecimento_municipio_codigo",
                                                    "vacina_grupoAtendimento_codigo",
                                                    "estabelecimento_valor",
                                                    "paciente_endereco_coIbgeMunicipio",
                                                    "id_sistema_origem")
        df_datasus = transform_int("int", df_datasus, "paciente_endereco_coPais",
                                                    "vacina_codigo",
                                                    "paciente_idade",
                                                    "vacina_numDose",
                                                    "@version",
                                                    "paciente_racaCor_codigo",
                                                    "vacina_categoria_codigo")
        df_datasus = transform_date("timestamp", df_datasus, "data_importacao_rnds",
                                                            "@timestamp",
                                                            "vacina_dataAplicacao",
                                                            "data_importacao_datalake")
        df_datasus = transform_date("date", df_datasus, "paciente_dataNascimento")
        df_datasus = df_datasus.distinct()
        output_data(df_datasus)
    except Exception as e:
        raise(f"ETL Datasus error: {e}")

etl_datasus()