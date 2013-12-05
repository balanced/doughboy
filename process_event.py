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
                         event_json['guid'], event_json['marketplace_guid'])
        self.logger.log(logging.NOTSET, 'Payload: %r', event_json)
        evs = event_json['entity_views']
        ev10 = evs['1.0']

        hold_item = dict(
            type='Holds',
            quantity=ev10['holds_count'],
            amount=ev10['holds_total_amount'],
            name='${:.2f} per hold'.format(ev10['hold_fee']),
            total=ev10['holds_total_fee'],
        )
        debit_card_item = dict(
            type='Debits: cards',
            quantity=ev10['card_debits_count'],
            amount=ev10['card_debits_total_amount'],
            name='{}% of txn amount'.format(ev10['variable_fee_percentage']),
            total=ev10['failed_credits_total_fee'],
        )
        debit_bank_item = dict(
            type='Debits: bank accounts',
            quantity=ev10['bank_account_debits_count'],
            amount=ev10['bank_account_debits_total_amount'],
            name=(
                '{}% of txn amount'
                .format(ev10['bank_account_debit_variable_fee_percentage'])
            ),
            total=ev10['bank_account_debits_total_fee'],
        )
        if ev10['bank_account_debit_variable_fee_cap']:
            debit_bank_item['name'] += (
                ' (max ${:.2f} per debit)'
                .format(ev10['bank_account_debit_variable_fee_cap'] / 100.0)
            )
        credit_successed_item = dict(
            type='Credits: succeeded',
            quantity=ev10['bank_account_credits_count'],
            amount=ev10['bank_account_credits_total_amount'],
            name='${:.2f} per credit'.format(ev10['bank_account_credit_fee']),
            total=ev10['bank_account_credits_total_fee'],
        )
        credit_failed_item = dict(
            type='Credits: failed',
            quantity=ev10['failed_credits_count'],
            amount=ev10['failed_credits_total_amount'],
            name='${:.2f} per failed credit'.format(ev10['failed_credit_fee']),
            total=ev10['failed_credits_total_fee'],
        )
        refund_item = dict(
            type='Refunds',
            quantity=ev10['reversals_count'],
            amount=ev10['reversals_total_amount'],
            name='{}% of txn amount returned'.format(ev10['variable_fee_percentage']),
            total=ev10['refunds_total_fee'],
        )
        reversal_item = dict(
            type='Reversals',
            quantity=ev10['reversals_count'],
            amount=ev10['reversals_total_amount'],
            name='${:.2f} per reversal'.format(0),
            total=ev10['reversals_total_fee'],
        )
        chargeback_item = dict(
            type='Chargebacks',
            quantity=ev10['lost_debit_chargebacks_count'],
            amount=ev10['lost_debit_chargebacks_total_amount'],
            name='${:.2f} per failed chargeback'.format(
                ev10['chargeback_fixed_fee'] / 100.0
            ),
            total=ev10['lost_debit_chargebacks_total_fee'],
        )
        items = [
            hold_item,
            debit_card_item,
            debit_bank_item,
            credit_successed_item,
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

        total_fee = ev10['total_fee']
        adjustments_total_fee = ev10['adjustments_total_fee']
        source_uri = ev10['source_uri']
        marketplace_uri = ev10['marketplace_uri']

        self.logger.info('Total fee: %s', total_fee)
        self.logger.info('Adjustment fee: %s', adjustments_total_fee)
        self.logger.info('Source URI: %s', source_uri)
        self.logger.info('Marketplace URI: %s', marketplace_uri)

        adjustments = [
            dict(total=adjustments_total_fee),
        ]

        import balanced
        from billy_client import BillyAPI
        from billy_client import DuplicateExternalIDError

        #api = BillyAPI(None, endpoint='http://127.0.0.1:6543')
        #company = api.create_company(processor_key='ef13dce2093b11e388de026ba7d31e6f')
        #print company
        #return

        api = BillyAPI(
            api_key='Ehuk9jkcENFxv1Jk2bprUUtrAATv8vNkRw9Fa3m1SkYd', 
            endpoint='http://127.0.0.1:6543',
        )
        company = api.get_company('CPTQhADwa9wJYSspzLDxFEn3')
        # TODO: figure how to create a customer from marketplace URI,
        # they should be 1:1 relation in v1.1 API

        # make sure we won't duplicate customer for the same marketplace
        customers = list(api.list_customers(external_id=marketplace_uri))
        if not customers:
            customer = company.create_customer(
                external_id=marketplace_uri,
            )
        else:
            customer = customers[0]
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
        try:
            invoice = customer.invoice(
                title='Balanced Transaction Usage Invoice',
                amount=total_fee,
                payment_uri=card.uri,
                items=items,
                adjustments=adjustments,
                external_id=event_json['guid'],
            )
        except DuplicateExternalIDError:
            self.logger.warn('The invoice for event %s have already been '
                             'created, just ack the message', 
                             event_json['guid'])
            # TODO: ack here
            return
        print invoice
        # TODO: ack to the message queue


def main():
    paylod = '''{
  "trace_guid": "TR7b433aa35d2f11e3b7b428cfe9603dcf",
  "parent_guid": null,
  "entity_view": {
    "bank_account_credits_count": 12,
    "settlement_uri": "/v1/invoice_settlements/invoice_settlement",
    "lost_debit_chargebacks_total_fee": 0,
    "adjustments": [],
    "updated_at": "2013-11-10T02:01:08.980658Z",
    "lost_debit_chargebacks_total_amount": 0,
    "bank_account_debit_variable_fee_cap": 500,
    "refunds_count": 0,
    "bank_account_debits_total_amount": 0,
    "holds_count": 219,
    "id": "IV4GzXxAvn5WhFBZR9RosL6U",
    "holds_total_amount": 1378117,
    "reversals_total_fee": 0,
    "marketplace_uri": "/v1/marketplaces/MP35BtXWqGuYEsv2RHH2CGtH",
    "refund_fee": true,
    "state": "paid",
    "settle_at": "2013-11-10T01:29:54.315160Z",
    "card_debits_total_amount": 741118,
    "holds_total_fee": 0,
    "reversals_total_amount": 0,
    "card_debits_count": 171,
    "debits_total_fee": 24086,
    "bank_account_credit_fee": 0,
    "refunds_total_fee": 0,
    "source_uri": "/v1/marketplaces/MP35BtXWqGuYEsv2RHH2CGtH/bank_accounts/BA3EcoA5u8GOmm1EipSUDlPM",
    "bank_account_credits_total_fee": 0,
    "period": [
      "2013-11-09T00:01:31.148171Z",
      "2013-11-09T23:57:33.609046Z"
    ],
    "failed_credits_count": 0,
    "refunds_total_amount": 0,
    "_uris": {
      "source_uri": {
        "_type": "bank_account",
        "key": "source"
      },
      "settlement_uri": {
        "_type": "invoice_settlement",
        "key": "settlement"
      }
    },
    "adjustments_total_fee": 0,
    "failed_credit_fee": 0,
    "reversals_count": 0,
    "bank_account_debits_total_fee": 0,
    "bank_account_debits_count": 0,
    "lost_debit_chargebacks_count": 0,
    "hold_fee": 0,
    "bank_account_credits_total_amount": 1166031,
    "debits_total_amount": 741118,
    "uri": "/v1/invoices/IV4GzXxAvn5WhFBZR9RosL6U",
    "card_debits_total_fee": 24086,
    "bank_account_debit_variable_fee_percentage": 1.0,
    "chargeback_fixed_fee": 1500,
    "adjustments_count": 0,
    "total_fee": 24086,
    "variable_fee_percentage": 3.25,
    "failed_credits_total_amount": 0,
    "debits_count": 171,
    "failed_credits_total_fee": 0,
    "_type": "invoice",
    "created_at": "2013-11-10T01:29:54.367778Z",
    "sequence_number": 325
  },
  "entity_views": {
    "1.poshmark": {
      "invoices": [
        {
          "created_at": "2013-11-10T01:29:54.367778Z",
          "bank_account_credits_count": 12,
          "links": {
            "source": "BA3EcoA5u8GOmm1EipSUDlPM",
            "settlement": null
          },
          "adjustments": [],
          "updated_at": "2013-11-10T02:01:08.980658Z",
          "marketplace_uri": "/marketplaces/MP35BtXWqGuYEsv2RHH2CGtH",
          "failed_credits_total_fee": 0,
          "bank_account_debit_variable_fee_cap": 500,
          "refunds_count": 0,
          "bank_account_debits_total_amount": 0,
          "holds_count": 219,
          "id": "IV4GzXxAvn5WhFBZR9RosL6U",
          "holds_total_amount": 1378117,
          "reversals_total_fee": 0,
          "lost_debit_chargebacks_total_amount": 0,
          "failed_credit_fee": 0,
          "state": "paid",
          "chargeback_fixed_fee": 1500,
          "reversals_count": 0,
          "card_debits_total_amount": 741118,
          "holds_total_fee": 0,
          "reversals_total_amount": 0,
          "card_debits_count": 171,
          "debits_total_fee": 24086,
          "bank_account_credit_fee": 0,
          "refunds_total_fee": 0,
          "bank_account_credits_total_fee": 0,
          "period": [
            "2013-11-09T00:01:31.148171Z",
            "2013-11-09T23:57:33.609046Z"
          ],
          "failed_credits_count": 0,
          "refunds_total_amount": 0,
          "adjustments_total_fee": 0,
          "refund_fee": true,
          "settle_at": "2013-11-10T01:29:54.315160Z",
          "bank_account_debits_total_fee": 0,
          "lost_debit_chargebacks_count": 0,
          "hold_fee": 0,
          "bank_account_credits_total_amount": 1166031,
          "debits_total_amount": 741118,
          "uri": "/invoices/IV4GzXxAvn5WhFBZR9RosL6U",
          "card_debits_total_fee": 24086,
          "bank_account_debit_variable_fee_percentage": 1.0,
          "href": "/invoices/IV4GzXxAvn5WhFBZR9RosL6U",
          "adjustments_count": 0,
          "total_fee": 24086,
          "variable_fee_percentage": 3.25,
          "failed_credits_total_amount": 0,
          "debits_count": 171,
          "lost_debit_chargebacks_total_fee": 0,
          "bank_account_debits_count": 0,
          "sequence_number": 325
        }
      ],
      "links": {
        "invoices.source": "/resources/{invoices.source}",
        "invoices.settlement": "/invoice_settlements/{invoices.invoice_settlement}"
      }
    },
    "1.0": {
      "created_at": "2013-11-10T01:29:54.367778Z",
      "bank_account_credits_count": 12,
      "settlement_uri": "/v1/invoice_settlements/invoice_settlement",
      "adjustments": [],
      "updated_at": "2013-11-10T02:01:08.980658Z",
      "marketplace_uri": "/v1/marketplaces/MP35BtXWqGuYEsv2RHH2CGtH",
      "failed_credits_total_fee": 0,
      "bank_account_debit_variable_fee_cap": 500,
      "refunds_count": 0,
      "bank_account_debits_total_amount": 0,
      "holds_count": 219,
      "id": "IV4GzXxAvn5WhFBZR9RosL6U",
      "holds_total_amount": 1378117,
      "reversals_total_fee": 0,
      "lost_debit_chargebacks_total_amount": 0,
      "failed_credit_fee": 0,
      "state": "paid",
      "reversals_count": 0,
      "card_debits_total_amount": 741118,
      "holds_total_fee": 0,
      "reversals_total_amount": 0,
      "card_debits_count": 171,
      "debits_total_fee": 24086,
      "bank_account_credit_fee": 0,
      "refunds_total_fee": 0,
      "source_uri": "/v1/marketplaces/MP35BtXWqGuYEsv2RHH2CGtH/bank_accounts/BA3EcoA5u8GOmm1EipSUDlPM",
      "bank_account_credits_total_fee": 0,
      "period": [
        "2013-11-09T00:01:31.148171Z",
        "2013-11-09T23:57:33.609046Z"
      ],
      "failed_credits_count": 0,
      "refunds_total_amount": 0,
      "_uris": {
        "source_uri": {
          "_type": "bank_account",
          "key": "source"
        },
        "settlement_uri": {
          "_type": "invoice_settlement",
          "key": "settlement"
        }
      },
      "adjustments_total_fee": 0,
      "refund_fee": true,
      "settle_at": "2013-11-10T01:29:54.315160Z",
      "bank_account_debits_total_fee": 0,
      "lost_debit_chargebacks_count": 0,
      "hold_fee": 0,
      "bank_account_credits_total_amount": 1166031,
      "debits_total_amount": 741118,
      "uri": "/v1/invoices/IV4GzXxAvn5WhFBZR9RosL6U",
      "card_debits_total_fee": 24086,
      "bank_account_debit_variable_fee_percentage": 1.0,
      "chargeback_fixed_fee": 1500,
      "adjustments_count": 0,
      "total_fee": 24086,
      "variable_fee_percentage": 3.25,
      "failed_credits_total_amount": 0,
      "debits_count": 171,
      "lost_debit_chargebacks_total_fee": 0,
      "_type": "invoice",
      "bank_account_debits_count": 0,
      "sequence_number": 325
    },
    "1.1": {
      "invoices": [
        {
          "created_at": "2013-11-10T01:29:54.367778Z",
          "bank_account_credits_count": 12,
          "links": {
            "source": "BA3EcoA5u8GOmm1EipSUDlPM",
            "settlement": null
          },
          "adjustments": [],
          "updated_at": "2013-11-10T02:01:08.980658Z",
          "marketplace_uri": "/marketplaces/MP35BtXWqGuYEsv2RHH2CGtH",
          "failed_credits_total_fee": 0,
          "bank_account_debit_variable_fee_cap": 500,
          "refunds_count": 0,
          "bank_account_debits_total_amount": 0,
          "holds_count": 219,
          "id": "IV4GzXxAvn5WhFBZR9RosL6U",
          "holds_total_amount": 1378117,
          "reversals_total_fee": 0,
          "lost_debit_chargebacks_total_amount": 0,
          "failed_credit_fee": 0,
          "state": "paid",
          "chargeback_fixed_fee": 1500,
          "reversals_count": 0,
          "card_debits_total_amount": 741118,
          "holds_total_fee": 0,
          "reversals_total_amount": 0,
          "card_debits_count": 171,
          "debits_total_fee": 24086,
          "bank_account_credit_fee": 0,
          "refunds_total_fee": 0,
          "bank_account_credits_total_fee": 0,
          "period": [
            "2013-11-09T00:01:31.148171Z",
            "2013-11-09T23:57:33.609046Z"
          ],
          "failed_credits_count": 0,
          "refunds_total_amount": 0,
          "adjustments_total_fee": 0,
          "refund_fee": true,
          "settle_at": "2013-11-10T01:29:54.315160Z",
          "bank_account_debits_total_fee": 0,
          "lost_debit_chargebacks_count": 0,
          "hold_fee": 0,
          "bank_account_credits_total_amount": 1166031,
          "debits_total_amount": 741118,
          "uri": "/invoices/IV4GzXxAvn5WhFBZR9RosL6U",
          "card_debits_total_fee": 24086,
          "bank_account_debit_variable_fee_percentage": 1.0,
          "href": "/invoices/IV4GzXxAvn5WhFBZR9RosL6U",
          "adjustments_count": 0,
          "total_fee": 24086,
          "variable_fee_percentage": 3.25,
          "failed_credits_total_amount": 0,
          "debits_count": 171,
          "lost_debit_chargebacks_total_fee": 0,
          "bank_account_debits_count": 0,
          "sequence_number": 325
        }
      ],
      "links": {
        "invoices.source": "/resources/{invoices.source}",
        "invoices.settlement": "/invoice_settlements/{invoices.invoice_settlement}"
      }
    }
  },
  "marketplace_guid": "MP35BtXWqGuYEsv2RHH2CGtH",
  "occurred_at": "2013-11-10T02:01:08.980000Z",
  "entity_guid": "IV4GzXxAvn5WhFBZR9RosL6U",
  "callbacks": [],
  "internal": false,
  "entity_data": {
    "bank_account_credits_count": 12,
    "account_debit_variable_fee_percentage": 1.0,
    "cheque_credits_total_amount": 0,
    "account_debits_total_fee": 0,
    "cheque_credits_count": 0,
    "cheque_credits_total_fee": 0,
    "updated_at": "2013-11-10T02:01:08.980658Z",
    "failed_credits_total_fee": 0,
    "account_debits_total_amount": 0,
    "variable_fee_percentage": 3.25,
    "refunds_count": 0,
    "bank_account_debits_total_amount": 0,
    "holds_count": 219,
    "guid": "IV4GzXxAvn5WhFBZR9RosL6U",
    "holds_total_amount": 1378117,
    "cheque_credit_fixed_fee": 100,
    "account_debit_variable_fee_cap": null,
    "reversals_total_fee": 0,
    "marketplace_guid": "MP35BtXWqGuYEsv2RHH2CGtH",
    "lost_debit_chargebacks_total_amount": 0,
    "refund_fee": true,
    "prepaid_discount_percentage": 2.0,
    "reversal_fee": 0,
    "settle_at": "2013-11-10T01:29:54.315160Z",
    "card_debits_total_amount": 741118,
    "holds_total_fee": 0,
    "reversals_total_amount": 0,
    "card_debits_count": 171,
    "bank_account_credit_fee": 0,
    "min_created_at": "2013-11-09T00:01:31.148171Z",
    "refunds_total_fee": 0,
    "settle_lock": 1,
    "bank_account_credits_total_fee": 0,
    "failed_credits_total_amount": 0,
    "failed_credits_count": 0,
    "refunds_total_amount": 0,
    "bank_account_debit_fee": 30,
    "adjustments_total_fee": 0,
    "failed_credit_fee": 0,
    "reversals_count": 0,
    "bank_account_debits_total_fee": 0,
    "count": 402,
    "lost_debit_chargebacks_count": 0,
    "hold_fee": 0,
    "chargeback_liability_percentage": 100.0,
    "bank_account_credits_total_amount": 1166031,
    "account_debits_count": 0,
    "created_at": "2013-11-10T01:29:54.367778Z",
    "account_debit_fixed_fee": 0,
    "card_debits_total_fee": 24086,
    "bank_account_debit_variable_fee_percentage": 1.0,
    "bank_account_debit_variable_fee_cap": 500,
    "chargeback_fixed_fee": 1500,
    "adjustments_count": 0,
    "total_fee": 24086,
    "max_created_at": "2013-11-09T23:57:33.609046Z",
    "lost_debit_chargebacks_total_fee": 0,
    "funding_instrument_guid": "BA3EcoA5u8GOmm1EipSUDlPM",
    "bank_account_debits_count": 0,
    "sequence_number": 325
  },
  "guid": "EVbe5fb1a15d2d11e38e7428cfe9603dcf",
  "type": "invoice.updated"
}
    '''
    import json
    processor = EventProcessor()
    event_json = json.loads(paylod)
    processor.process(event_json)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
