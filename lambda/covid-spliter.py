from datetime import date, timedelta
import json
import boto3

def daterange(start_date, end_date):
    """
    Retornar uma lista com um intervalo de datas específicado

    :param start_date: date
    :param end_date: date

    :yield: lista com as datas
    """
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def lambda_handler(event, content):
    """
    Essa função invoca a lambda covid-intake para cada data, assim é possível ter o histórico conforme a data específicada
    de forma paralela.
    """
    start_date = date(2022, 1, 1)
    end_date = date(2022, 4, 1)
    count = 0
    error_count = 0
    for single_date in daterange(start_date, end_date):
        token = str(single_date.strftime("%Y-%m-%d"))
        lambda_aws = boto3.client('lambda')
        payload = {
            "date": token
        }
        count += 1
        try:
            lambda_aws.invoke(
                FunctionName = 'rl-datasus-intake',
                InvocationType = 'Event',
                Payload = json.dumps(payload)
            )
            print(f"Started {count} Functions")
        except Exception as error:
            error_count = 1
            print(f"Error: Failed to start function {count}")
            print(error)
            continue
    
    print("Rickn and Morty") 

    return {
        'statusCode': 200,
        'body': json.dumps(f'Success Triggering Lambda Functions for Covid, Errors: {error_count}')
    }