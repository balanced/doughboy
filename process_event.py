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

        total_fee = ev10['total_fee']
        adjustments_total_fee = ev10['adjustments_total_fee']
        marketplace_uri = ev10['marketplace_uri']
        # it is possible a marketplace exists without a bank account associated
        # with it, in that case, we just create the invoice wihout `payment_uri`
        # and update it later when the marketplace got a bank account
        source_uri = ev10.get('source_uri')

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
            api_key='FXSpohZE8NuUn5W1peb4FhcXQp49Bc1vDpLkCsPstNRS', 
            endpoint='http://127.0.0.1:6543',
        )
        company = api.get_company('CPXVsVvBDY94p3zVNvLSZ21M')
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
        # call to billy API, create an invoice
        try:
            invoice = customer.invoice(
                title='Balanced Transaction Usage Invoice',
                amount=total_fee,
                payment_uri=source_uri,
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
    import os
    import json
    processor = EventProcessor()

    logger = logging.getLogger(__name__)

    input_dir = 'input'
    for filename in os.listdir(input_dir):
        if filename.startswith('.'):
            continue
        filepath = os.path.join(input_dir, filename)
        logger.info('Loading %s', filepath)
        with open(filepath, 'rt') as json_file:
            content = json_file.read()
            if not content.strip():
                logger.warn('Ignore empty file %s', filepath)
                continue
            event_json = json.loads(content)
        processor.process(event_json)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
