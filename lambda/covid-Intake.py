import json
import requests
import gzip
import boto3

# O ideal é deixar o auth como variável de ambiente, porém é uma API Pública :), sinta-se à vontade para utiliza-lá!
auth = {
  'Authorization': 'Basic aW11bml6YWNhb19wdWJsaWM6cWx0bzV0JjdyX0ArI1Rsc3RpZ2k=',
  'Content-Type': 'application/json'
  }

url = "https://imunizacao-es.saude.gov.br/_search"

def lambda_handler(event, context):
  """
  Faz a extração dos dados de acordo com a data e devolve um arquivo json com compressão gz para o S3.

  :param event: dict  
  """
  # Inicio da query no elastic para conseguir pegar o parâmetro sort para ser utilizado no search_after.
  date_time = event['date']
  result = {"id": []}
  elastic_query = json.dumps({
    "size": 10000,
    "query": {
      "range": {
        "vacina_dataAplicacao": {
          "gte": f"{date_time}T00:00:00",
          "lte": f"{date_time}T23:59:59"
        }
      }
    },
    "sort": [
      {
        "@timestamp": "asc"
      }
    ]
  })
  response = requests.request("GET", url, headers=auth, data=elastic_query)
  raw = response.json()
  data = raw['hits']['hits']
  key = data[-1]['sort']
  result['id'].extend(data)
  count = 0
  while len(data) >= 1:
    """
    Loop while, enquanto existir data na query ele irá pegar cada request com 10 mil documentos, arquivar em json e jogar no S3.
    """
    try:
      count += 1
      s3 = boto3.client("s3")
      body = gzip.compress(json.dumps(result).encode('utf-8'))
      s3.put_object(Bucket='rl-bronze-lake', Key=f'datasus/date={date_time}/{key[0]}_2022.json.gz', Body=body)
      print(f"Package {count} retrieved successfully")

      elastic_query_search_after = json.dumps({
      "size": 10000,
      "query": {
        "range": {
          "vacina_dataAplicacao": {
            "gte": f"{date_time}T00:00:00",
            "lte": f"{date_time}T23:59:59"
          }
        }
      },
      "search_after":[key[0]],
      "sort": [
        {
          "@timestamp": "asc"
        }
      ]
      })
      response = requests.request("GET", url, headers=auth, data=elastic_query_search_after)
      raw = response.json()
      data = raw['hits']['hits']
      key = data[-1]['sort']
      result['id'][0] = data
      
    except Exception as error:
      print(f"Error: {error}")
      pass

  print("Rickn and Morty") 