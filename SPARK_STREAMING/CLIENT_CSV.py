from pyspark.sql import SparkSession
import shutil

#tratamento para verificar cada item da pasta, e caso encontre algum erro, o processo não ficará pela metade
for item in ['./csv', './check']:
    try:
        shutil.rmtree(item) #apagar as pastas criadas que armazena os tweet ao rodar uma segunda vez, caso contrário, dará erro
    except OSError as err:
        print(f'Aviso: {err.strerror}')

#inciando a sessção Spark
spark = SparkSession.builder.appName('SparkStreaming').getOrCreate()

tweets = spark.readStream\
    .format('socket')\
    .option('host', 'localhost')\
    .option('port', 9009)\
    .load()

#armazenar os tweet em csv
query = tweets.writeStream\
    .outputMode('append')\
    .option('encoding', 'utf-8')\
    .format('csv')\
    .option('path', './csv')\
    .option('checkpointLocation', './check')\
    .start()

query.awaitTermination()