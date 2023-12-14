import scrapy
import os
from neo4j import GraphDatabase
from scrapy.exceptions import CloseSpider
import time

class conexaoNeo4j:
  def __init__(self, uri, user, pwd):
    self.__uri = uri
    self.__user = user
    self.__pwd = pwd
    self.__driver = None
    self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))

  def close(self):
    if self.__driver is not None:
      self.__driver.close()

  def query(self, query, parameters=None, db=None):
    assert self.__driver is not None, "Driver não inicializado!"
    session = None
    response = None
    try: 
      session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
      response = list(session.run(query, parameters))
    except Exception as e:
      print("Consulta falhou:", e)
    finally: 
      if session is not None:
        session.close()
    return response


conn = conexaoNeo4j(uri=os.environ['NEO4J_URI'], 
                       user=os.environ['NEO4J_USER'], 
                       pwd=os.environ['NEO4J_PWD'])


class spider(scrapy.Spider):

  name = 'spider'
  start_urls = [
    'https://editorial.rottentomatoes.com/guide/best-horror-movies-of-all-time/'
  ]

  def parse(self, response):

    pesquisarFilme = input("Escolha um filme e iremos recomendar os 5 top filmes parecidos: ")
    encontrarFilmesSimilares(pesquisarFilme)
    time.sleep(8) 

    linhas = response.css('div.article_movie_title')
    for linha in linhas:
      link = linha.css("div > h2 > a::attr(href)")
      porcentagem = linha.css('div > h2 > span.tMeterScore::text').get()
      ano = linha.css('div > h2 > span.subtle.start-year::text').get()
      yield response.follow(link.get(),
                            self.analisarFilme,
                            meta={
                              "porcentagem": porcentagem,
                              "ano": ano
                            })

  def analisarFilme(self, response):
    nome = response.css(
      'div.thumbnail-scoreboard-wrap > score-board-deprecated > h1::text').get(
      )

    genero = response.css('ul#info > li:nth-child(2) > p > span::text').get()

    diretor = response.css(
      'ul#info > li:nth-child(4) > p > span > a::text').get()
    lacamento = response.css(
      'ul#info > li:nth-child(7) > p > span > time::text').get()

    bilheteria = response.css(
      'ul#info > li:nth-child(10) > p > span::text').get()
    duracao = response.css(
      'ul#info > li:nth-child(11) > p > span > time::text').get()
    watch = response.css(
      'section.where-to-watch > bubbles-overflow-container > where-to-watch-meta >where-to-watch-bubble::attr(image) '
    ).getall()

    porcentagem = response.meta['porcentagem']
    ano = response.meta['ano']

    yield {
      "Nome":
      nome,
      "Porcentagem":
      porcentagem,
      "Ano que Saiu":
      ano.replace("(", "").replace(")", ""),
      "Diretor":
      diretor,
      "Data que Saiu":
      lacamento,
      "Bilheteria":
      bilheteria.replace("\n", "").replace(" ", ""),
      "Tamanho do Filme":
      duracao if duracao is None else duracao.replace("\n", "").replace(
        " ", ""),
      "Genero":
      genero.replace("\n", "").replace(" ", ""),
      "Aonde Assistir":
      watch,
    }

    criarFilme = '''
    MERGE (filme:Filme {nome: $nome, ano: $ano})
    ON CREATE SET filme.bilheteria = $bilheteria, filme.duracao = $duracao, filme.porcentagem = $porcentagem

    WITH filme
    MERGE (genero:Genero {nome: $genero})
    MERGE (filme)-[:GENERO]->(genero)

    WITH filme
    MERGE (diretor:Diretor {nome: $diretor})
    MERGE (filme)-[:DIRIGIDO]->(diretor)
    '''

    deletarFilme = '''
    MATCH (filme:Filme {nome: $nome, ano: $ano})
    DETACH DELETE filme
    '''

    conn.query(deletarFilme, parameters={
        'nome': nome,
        'ano': ano.replace("(", "").replace(")", "")
    })

    conn.query(criarFilme, parameters={
        'nome': nome,
        'ano': ano.replace("(", "").replace(")", ""),
        'bilheteria': bilheteria.replace("\n", "").replace(" ", ""),
        'duracao': duracao if duracao is None else duracao.replace("\n", "").replace(" ", ""),
        'genero': genero.replace("\n", "").replace(" ", ""),
        'diretor': diretor,
        'porcentagem': float(porcentagem.replace("%", "").strip()),
    })
    conn.close()

def encontrarFilmesSimilares(movie_name):
  similar_movies_query = '''
  MATCH (filme:Filme)-[:GENERO]->(genero)<-[:GENERO]-(similar:Filme)
  WHERE filme.nome = $filme_nome
  AND toFloat(filme.porcentagem) <= toFloat(similar.porcentagem) + 10
  AND toFloat(filme.porcentagem) >= toFloat(similar.porcentagem) - 10
  RETURN similar.nome AS nome, similar.porcentagem AS percentage
  ORDER BY toFloat(similar.porcentagem) DESC
  LIMIT 5
  '''

  results = conn.query(similar_movies_query, parameters={'filme_nome': movie_name})
  print(f"Filmes similares a {movie_name}: \n")
  for record in results:
    print(f"{record['nome']} - {record['porcentagem']}%")
    print("\n")