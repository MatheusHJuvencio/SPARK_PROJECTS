from pyspark.sql import SparkSession
from pyspark.sql import functions as f


#inciando a sessção Spark
spark = SparkSession.builder.appName('SparkStreaming').getOrCreate()

#comando necessário para que o Spark possa tratar os dados vindos em streaming, para tratar os mesmos, o Spark criará um dataframe;
#onde terá uma coluna values e as linhas será cada tweet;
#readStrem - leitura dos dados; socket - por onde os dados irão passar; host e a porta - onde devem ser encontrados os dados.

lines = spark.readStream\
    .format('socket')\
    .option('host', 'localhost')\
    .option('port', 9009)\
    .load()

#visualização do processo;
#append - esponsável por determinar que somente as novas linhas do dataframe sejam escritas no armazenamento externo;
#console - os dados devem ser escritos e renderizados no console.
#query = lines.writeStream\
#    .outputMode('append')\
#    .format('console')\
#    .start()

#separar por palavras (split) > colocar as palavras em linhas (explode)
words = lines.select(f.explode(f.split(lines.value, ' ')).alias('word'))

#contar as palavras
wordCount = words.groupBy('word').count()

query = wordCount.writeStream\
    .outputMode('complete')\
    .format('console')\
    .start()

query.awaitTermination()