import urllib.request, json
import sys
if len(sys.argv) < 3:
    print('Usage: python json_test.py <url> <output.json>')
    print('example:\n'
      +'python json_demo.py https://covid19api.io/api/v1/JohnsHopkinsDataDailyReport test.json')
    sys.exit(-1)

url_endpoint = sys.argv[1]
output_file = sys.argv[2]
with urllib.request.urlopen(url_endpoint) as url:
    #if you want to play with a json object
    data = json.loads(url.read().decode())
    with open(output_file, 'w') as out_stream:
        #if you want to save the json file
        out_stream.write(json.dumps(data))
