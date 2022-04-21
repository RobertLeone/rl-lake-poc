import requests
import json

auth = {
  'Authorization': 'Basic aW11bml6YWNhb19wdWJsaWM6cWx0bzV0JjdyX0ArI1Rsc3RpZ2k=',
  'Content-Type': 'application/json'
  }

url = "https://imunizacao-es.saude.gov.br/_search"

def lambda_handler():
  """
  Faz a extração dos dados de acordo com a data e devolve um arquivo json com compressão gz para o S3.  
  """
  result = {"id": []}

  payload = json.dumps({
    "size": 10000,
    "query": {
      "range": {
        "vacina_dataAplicacao": {
          "gte": "2022-04-20T00:00:00",
          "lte": "2022-04-20T23:59:59"
        }
      }
    },
    "sort": [
      {
        "@timestamp": "asc"
      }
    ]
  })
  response = requests.request("GET", url, headers=auth, data=payload)
  data = response.json()
  key = data['hits']['hits'][-1]['sort']
  bkp_key = data['hits']['hits']
  result['id'].extend(data['hits']['hits'])
  while len(bkp_key) >= 1:
    try:
      payload = json.dumps({
      "size": 10000,
      "query": {
        "range": {
          "vacina_dataAplicacao": {
            "gte": "2022-04-20T00:00:00",
            "lte": "2022-04-20T23:59:59"
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
      response = requests.request("GET", url, headers=auth, data=payload)
      data = response.json()
      bkp_key = data['hits']['hits']
      key = data['hits']['hits'][-1]['sort']
      result['id'].extend(data['hits']['hits'])
    except Exception as error:
      print(f"Error: {error}")
      pass
  
  print("Rickn and Morty")  

lambda_handler()