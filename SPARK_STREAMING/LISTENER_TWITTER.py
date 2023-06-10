import socket
import tweepy

#configurando a porta e o host
HOST = 'localhost'
PORT = 9009

#configurando o socket
s = socket.socket()

#relacionado a porta e host ao socket
s.bind((HOST, PORT))
print(f"Aguardando conexão na porta: {PORT}")

s.listen(5)
connection, address = s.accept()
print(f"Recebendo solicitação de {address}")

#token do twitter develop
token="SEU TOKEN"

#palavra chave de busca
keyword = "basquete"

class GetTweets(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        print(tweet.text)   #visualizar os tweet, porem, só o texto ja o arquivo vem no formato JSON
        print("="*50)
        connection.send(tweet.text.encode('utf-8', 'ignore'))

printer = GetTweets(token)
printer.add_rules(tweepy.StreamRule(keyword)) #palavra filtrada
printer.filter()


connection.close()