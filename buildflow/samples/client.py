import requests

response = requests.get(
    'http://localhost:3569/get_job/raysubmit_3fWbqMSRfJfJXQ39')

print(response.text)