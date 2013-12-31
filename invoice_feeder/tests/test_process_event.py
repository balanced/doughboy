from __future__ import unicode_literals
import os
import json
import unittest

import mock


class TestProcessEvent(unittest.TestCase):

    def setUp(self):
        from invoice_feeder import tests
        test_pkg_dir = os.path.abspath(os.path.dirname(tests.__file__))
        fixture_dir = os.path.join(test_pkg_dir, 'fixtures')
        msg_filename = os.path.join(fixture_dir, 'msg.json')
        self.msg_payload = json.load(open(msg_filename, 'rt'))
        self.config = dict(
            billy=dict(
                endpoint='MOCK_ENDPOINT',
                api_key='MOCK_API_KEY',
                company_guid='MOCK_COMPANY_GUID',
            )
        )

    def make_one(self, *args, **kwargs):
        from invoice_feeder.process_event import EventProcessor
        return EventProcessor(*args, **kwargs)

    @mock.patch('billy_client.BillyAPI')
    def test_process_event(self, api_cls):
        api = mock.MagicMock()
        customer = mock.MagicMock()
        company = mock.MagicMock()

        api_cls.return_value = api
        api.get_company.return_value = company
        api.list_customers.return_value = [customer]

        processor = self.make_one(self.config)
        processor.process(self.msg_payload)

        # create BillyAPI instance
        api_cls.assert_called_with(
            api_key=self.config['billy']['api_key'],
            endpoint=self.config['billy']['endpoint'],
        )
        # api.get_company
        api.get_company.assert_called_with(self.config['billy']['company_guid'])
        # list customers by external_id
        customer_uri = '/v1/customers/{}'.format(
            self.msg_payload['mirrored_customer_guid']
        )
        api.list_customers.assert_called_with(external_id=customer_uri)
        # create invoice
        # TODO: do more tests here
