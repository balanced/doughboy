from __future__ import unicode_literals
import logging


class EventProcessor(object):
    """Process invoice created events from Balanced API service, create 
    corresponding invoice entities in Billy service

    """

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    def process(self, event_json):
        """Process one invoice event from Balanced API service

        """
        self.logger.info('Processing event %s for marketplace %s', 
                         event_json['_id'], event_json['marketplace_guid'])
        self.logger.log(logging.NOTSET, 'Payload: %r', event_json)
        ev = event_json['entity_view']

        hold_item = dict(
            type='Holds',
            quantity=ev['holds_count'],
            amount=ev['holds_total_amount'],
            name='${:.2f} per hold'.format(ev['hold_fee']),
            total=ev['holds_total_fee'],
        )
        debit_card_item = dict(
            type='Debits: cards',
            quantity=ev['card_debits_count'],
            amount=ev['card_debits_total_amount'],
            name='{}% of txn amount'.format(ev['variable_fee_percentage']),
            total=ev['failed_credits_total_fee'],
        )
        debit_bank_item = dict(
            type='Debits: bank accounts',
            quantity=ev['bank_account_debits_count'],
            amount=ev['bank_account_debits_total_amount'],
            name=(
                '{}% of txn amount'
                .format(ev['bank_account_debit_variable_fee_percentage'])
            ),
            total=ev['bank_account_debits_total_fee'],
        )
        if ev['bank_account_debit_variable_fee_cap']:
            debit_bank_item['name'] += (
                ' (max ${:.2f} per debit)'
                .format(ev['bank_account_debit_variable_fee_cap'] / 100.0)
            )
        # TODO: this only appears in new schema, add it on later
        """
        credit_successed_item = dict(
            type='Credits: succeeded',
            quantity=ev['bank_account_credits_count'],
            amount=ev['bank_account_credits_total_amount'],
            name='${:.2f} per credit'.format(ev['bank_account_credit_fee']),
            total=ev['bank_account_credits_total_fee'],
        )
        print 'Credits: successed', credit_successed_item
        """
        credit_failed_item = dict(
            type='Credits: failed',
            quantity=ev['failed_credits_count'],
            amount=ev['failed_credits_total_amount'],
            name='${:.2f} per failed credit'.format(ev['failed_credit_fee']),
            total=ev['failed_credits_total_fee'],
        )
        refund_item = dict(
            type='Refunds',
            quantity=ev['reversals_count'],
            amount=ev['reversals_total_amount'],
            name='{}% of txn amount returned'.format(ev['variable_fee_percentage']),
            total=ev['refunds_total_fee'],
        )
        reversal_item = dict(
            type='Reversals',
            quantity=ev['reversals_count'],
            amount=ev['reversals_total_amount'],
            name='${:.2f} per reversal'.format(0),
            total=ev['reversals_total_fee'],
        )
        chargeback_item = dict(
            type='Chargebacks',
            quantity=ev['lost_debit_chargebacks_count'],
            amount=ev['lost_debit_chargebacks_total_amount'],
            name='${:.2f} per failed chargeback'.format(
                ev['chargeback_fixed_fee'] / 100.0
            ),
            total=ev['lost_debit_chargebacks_total_fee'],
        )
        items = [
            hold_item,
            debit_card_item,
            debit_bank_item,
            #credit_successed_item,
            credit_failed_item,
            refund_item,
            reversal_item,
            chargeback_item,
        ]
        for item in items:
            self.logger.info(
                'Item: type=%s, quantity=%s, amount=%s, '
                'name=%r, total=%s',
                item['type'],
                item['quantity'],
                item['amount'],
                item['name'],
                item['total'],
            )

        total_fee = ev['total_fee']
        adjustments_total_fee = ev['adjustments_total_fee']

        self.logger.info('Total fee: %s', total_fee)
        self.logger.info('Adjustment fee: %s', adjustments_total_fee)

        adjustments = [
            dict(total=adjustments_total_fee),
        ]

        import balanced
        from billy_client import BillyAPI

        #api = BillyAPI(None, endpoint='http://127.0.0.1:6543')
        #company = api.create_company(processor_key='ef13dce2093b11e388de026ba7d31e6f')
        #print company
        #return

        api = BillyAPI(
            api_key='3pw6Hmo1J6cowcnw8DjmyYMza3yJSKGhsmj5myoEZu86', 
            endpoint='http://127.0.0.1:6543',
        )
        company = api.get_company('CPFfwjvyhiSG39iHG5KMBeoy')
        # TODO: get corresponding customer
        customer = company.create_customer()
        # TODO: should get existing customer payment method
        # tokenlize a payment method
        balanced.configure('ef13dce2093b11e388de026ba7d31e6f')
        card = balanced.Card(
            expiration_month='12',
            security_code='123',
            card_number='5105105105105100',
            expiration_year='2020'
        ).save()
        # call to billy API, create an invoice
        invoice = customer.invoice(
            title='Balanced Transaction Usage Invoice',
            amount=total_fee,
            payment_uri=card.uri,
            items=items,
            adjustments=adjustments,
        )
        print invoice
        # TODO: ack to the message queue
        # TODO: think what about this processing failed? what if we 
        # finished creating an invoice in Billy, but failed to ack to the
        # queue service? then we will have duplicate invoice for the same
        # message... hum....


def main():
    paylod = '''{
  "_cls": "AuditEvent.InvoiceAuditEvent",
  "_id": "EV73df81fc3aaf11e39b69026ba7f8ec28",
  "_types": [
    "AuditEvent",
    "AuditEvent.InvoiceAuditEvent"
  ],
  "callbacks": [
    {
      "status": "succeeded",
      "_types": [
        "AuditEventCallback"
      ],
      "marketplace_guid": "MP5Pg8XIUxdZn4L8gbcIfy2t",
      "attempts": 1,
      "_cls": "AuditEventCallback",
      "attempt_limit": 10,
      "guid": "CB5ff2exrMUY2XicpIZgUr0q",
      "event_guid": "EV73df81fc3aaf11e39b69026ba7f8ec28"
    }
  ],
  "entity_data": {
    "credits_total_amount": 0,
    "prepaid_discount_percentage": 2,
    "lost_debit_chargebacks_total_fee": 0,
    "credits_count": 0,
    "updated_at": "2013-10-22T00:18:19.301184Z",
    "variable_fee_percentage": 0,
    "refunds_count": 0,
    "bank_account_debits_total_amount": 0,
    "holds_count": 0,
    "guid": "IV3wBmYdVrqdG0x3ogb3IJFm",
    "holds_total_amount": 0,
    "reversals_total_fee": 0,
    "marketplace_guid": "MP5Pg8XIUxdZn4L8gbcIfy2t",
    "lost_debit_chargebacks_total_amount": 0,
    "refund_fee": false,
    "reversal_fee": 0,
    "reversals_count": 0,
    "card_debits_total_amount": 0,
    "holds_total_fee": 0,
    "reversals_total_amount": 0,
    "card_debits_count": 0,
    "min_created_at": null,
    "refunds_total_fee": 0,
    "settle_lock": 0,
    "failed_credits_total_amount": 0,
    "failed_credits_count": 0,
    "credits_total_fee": 0,
    "bank_account_debit_fee": 30,
    "adjustments_total_fee": 100,
    "credit_fee": 0,
    "failed_credit_fee": 0,
    "settle_at": "2013-10-22T00:18:18.852405Z",
    "bank_account_debits_total_fee": 0,
    "count": 1,
    "lost_debit_chargebacks_count": 0,
    "hold_fee": 0,
    "chargeback_liability_percentage": 100,
    "refunds_total_amount": 0,
    "created_at": "2013-10-22T00:18:18.907725Z",
    "card_debits_total_fee": 0,
    "bank_account_debit_variable_fee_percentage": 1,
    "bank_account_debit_variable_fee_cap": 500,
    "chargeback_fixed_fee": 1500,
    "adjustments_count": 1,
    "total_fee": 100,
    "max_created_at": null,
    "failed_credits_total_fee": 0,
    "funding_instrument_guid": null,
    "bank_account_debits_count": 0,
    "sequence_number": 19
  },
  "entity_guid": "IV3wBmYdVrqdG0x3ogb3IJFm",
  "entity_view": {
    "credits_total_amount": 0,
    "created_at": "2013-10-22T00:18:18.907725Z",
    "settlement_uri": null,
    "credits_count": 0,
    "adjustments": [],
    "period": [
      null,
      null
    ],
    "lost_debit_chargebacks_total_amount": 0,
    "failed_credits_total_fee": 0,
    "bank_account_debit_variable_fee_cap": 500,
    "refunds_count": 0,
    "bank_account_debits_total_amount": 0,
    "holds_count": 0,
    "id": "IV3wBmYdVrqdG0x3ogb3IJFm",
    "holds_total_amount": 0,
    "reversals_total_fee": 0,
    "marketplace_uri": "/v1/marketplaces/MP5Pg8XIUxdZn4L8gbcIfy2t",
    "failed_credit_fee": 0,
    "source": null,
    "state": "scheduled",
    "reversals_count": 0,
    "card_debits_total_amount": 0,
    "holds_total_fee": 0,
    "reversals_total_amount": 0,
    "card_debits_count": 0,
    "debits_total_fee": 0,
    "_type": "invoice",
    "refunds_total_fee": 0,
    "failed_credits_total_amount": 0,
    "updated_at": "2013-10-22T00:18:19.301184Z",
    "failed_credits_count": 0,
    "credits_total_fee": 0,
    "_uris": {
      "reversals": {
        "_type": "page",
        "key": "rever"
      },
      "debits": {
        "_type": "page",
        "key": "de"
      },
      "refunds": {
        "_type": "page",
        "key": "ref"
      },
      "settlements": {
        "_type": "page",
        "key": "settlem"
      },
      "failed_credits": {
        "_type": "page",
        "key": "failed_cre"
      },
      "credits": {
        "_type": "page",
        "key": "cre"
      },
      "bank_account_debits": {
        "_type": "page",
        "key": "bank_account_de"
      },
      "lost_debit_chargebacks": {
        "_type": "page",
        "key": "lost_debit_chargeb"
      },
      "holds": {
        "_type": "page",
        "key": "h"
      },
      "card_debits": {
        "_type": "page",
        "key": "card_de"
      }
    },
    "adjustments_total_fee": 100,
    "credit_fee": 0,
    "refund_fee": false,
    "settle_at": "2013-10-22T00:18:18.852405Z",
    "bank_account_debits_total_fee": 0,
    "lost_debit_chargebacks_count": 0,
    "hold_fee": 0,
    "refunds_total_amount": 0,
    "debits_total_amount": 0,
    "uri": "/v1/invoices/IV3wBmYdVrqdG0x3ogb3IJFm",
    "card_debits_total_fee": 0,
    "bank_account_debit_variable_fee_percentage": 1,
    "chargeback_fixed_fee": 1500,
    "adjustments_count": 1,
    "total_fee": 100,
    "variable_fee_percentage": 0,
    "debits_count": 0,
    "lost_debit_chargebacks_total_fee": 0,
    "bank_account_debits_count": 0,
    "sequence_number": 19
  },
  "internal": false,
  "marketplace_guid": "MP5Pg8XIUxdZn4L8gbcIfy2t",
  "occurred_at": "2013-10-22T00:18:19.301Z",
  "trace_guid": "TR744f99d83aaf11e3b8c6026ba7c1aba6",
  "type": "invoice.created"
}    
    '''
    import json
    processor = EventProcessor()
    event_json = json.loads(paylod)
    processor.process(event_json)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
