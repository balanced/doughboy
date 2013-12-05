import sys
import json

from billy_client import BillyAPI


def main():
    if len(sys.argv) < 3:
        print 'Usage create_company.py ENDPOINT PROCESSOR_KEY'
        sys.exit(-1)

    endpoint = sys.argv[1]
    processor_key = sys.argv[2]

    api = BillyAPI(None, endpoint=endpoint)
    company = api.create_company(processor_key=processor_key)
    print json.dumps(company.json_data)


if __name__ == '__main__':
    main()
