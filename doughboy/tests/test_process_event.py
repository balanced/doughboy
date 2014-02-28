from __future__ import unicode_literals
import os
import json
import unittest

import mock

from doughboy import tests
from doughboy.process_event import EventProcessor
from doughboy.process_event import EventConsumer 


class TestProcessEvent(unittest.TestCase):

    def setUp(self):
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
        expected_amount = 72 + 10 - 3
        expected_payment_uri = '/v1/bank_accounts/BA2eRgRHV25MuvHWUL4BYOYv'
        expected_items = [
            dict(type='Holds', quantity=1, volume=112, amount=30, name='$0.30 per hold'),
            dict(type='Debits: cards', quantity=1, volume=1221, amount=42, name='3.5% of txn amount'),
            dict(type='Debits: bank accounts', quantity=0, volume=0, amount=0, name='1.0% of txn amount (max $5.00 per debit)'),
            dict(type='Credits: succeeded', quantity=0, volume=0, amount=0, name='$0.25 per credit'),
            dict(type='Credits: failed', quantity=0, volume=0, amount=0, name='$0.00 per failed credit'),
            dict(type='Refunds', quantity=0, volume=0, amount=0, name='3.5% of txn amount returned'),
            dict(type='Reversals', quantity=0, volume=0, amount=0, name='$0.00 per reversal'),
            dict(type='Chargebacks', quantity=0, volume=0, amount=0, name='$15.00 per failed chargeback'),
        ]
        expected_adjustments = [
            dict(
                amount=-10,
                reason='hello',
            ),
            dict(
                amount=3,
                reason='baby',
            ),
        ]
        expected_external_id = 'IV2elPkokX5rRAxobd84fM3t'

        _, kwargs = customer.invoice.call_args
        self.assertEqual(customer.invoice.call_count, 1)
        self.assertEqual(kwargs.pop('title'), 
                         'Balanced Transaction Usage Invoice')
        self.assertEqual(kwargs.pop('amount'), expected_amount)
        self.assertEqual(kwargs.pop('external_id'), expected_external_id)
        self.assertEqual(kwargs.pop('payment_uri'), expected_payment_uri)
        self.assertEqual(kwargs.pop('adjustments'), expected_adjustments)
        self.assertEqual(kwargs.pop('items'), expected_items)
        self.assertFalse(kwargs)


class TestEventConsumer(unittest.TestCase):

    def setUp(self):
        test_pkg_dir = os.path.abspath(os.path.dirname(tests.__file__))
        fixture_dir = os.path.join(test_pkg_dir, 'fixtures')
        msg_filename = os.path.join(fixture_dir, 'msg.json')
        self.msg_payload = json.load(open(msg_filename, 'rt'))

    def make_one(self, *args, **kwargs):
        return EventConsumer(*args, **kwargs)

    def test_process_message(self):
        processor = mock.Mock()
        consumer = self.make_one(
            connection=None, 
            queues=None,
            processor=processor,
        )

        msg = mock.Mock()
        consumer.on_message(self.msg_payload, msg)
        processor.process.assert_called_once()
        msg.ack.assert_called_once()

    def test_error_capture(self):
        processor = mock.Mock()
        processor.process.side_effect = RuntimeError('Boom!')
        consumer = self.make_one(
            connection=None, 
            queues=None,
            processor=processor,
        )

        msg = mock.Mock()
        consumer.on_message(self.msg_payload, msg)
        self.assertFalse(msg.ack.called)

    def test_ignore_other_messages(self):
        processor = mock.Mock()
        consumer = self.make_one(
            connection=None, 
            queues=None,
            processor=processor,
        )

        def assert_ignored(msg_type):
            payload = self.msg_payload.copy()
            payload['type'] = msg_type
            msg = mock.Mock()
            consumer.on_message(payload, msg)
            msg.ack.assert_called_once()
            self.assertFalse(processor.process.called)

        assert_ignored('invoice.created')
        assert_ignored('invoice.updated')
        assert_ignored('invoice.foobared')
        assert_ignored('invoice-no-funding-source')
