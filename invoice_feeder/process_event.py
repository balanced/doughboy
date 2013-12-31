from __future__ import unicode_literals
import os
import json
import logging
import logging.config

from kombu.mixins import ConsumerMixin
from kombu import Queue
from billy_client import BillyAPI
from billy_client import DuplicateExternalIDError


class EventProcessor(object):
    """Process invoice created events from Balanced API service, create 
    corresponding invoice entities in Billy service

    """

    def __init__(self, config, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.config = config

    def process(self, event_json):
        """Process one invoice event from Balanced API service

        """
        # prepare necessary data
        mirrored_customer_guid = event_json['mirrored_customer_guid']
        mirrored_funding_source_guid = event_json['mirrored_funding_source_guid']

        entity_data = event_json['entity_data']
        invoice_guid = entity_data['guid']
        marketplace_guid = entity_data['marketplace_guid']
        marketplace_uri = '/v1/marketplaces/{}'.format(marketplace_guid)
        customer_uri = '/v1/customers/{}'.format(mirrored_customer_guid)
        if mirrored_funding_source_guid.startswith('BA'):
            funding_type = 'bank_accounts'
        elif mirrored_funding_source_guid.startswith('PA'):
            funding_type = 'proxy_accounts'
        else:
            raise ValueError(
                'Unexpected funding source {}'.format(mirrored_customer_guid)
            )
        funding_source_uri = '/v1/{}/{}'.format(
            funding_type,
            mirrored_funding_source_guid,
        )
        total_fee = entity_data['total_fee']
        adjustments_total_fee = entity_data['adjustments_total_fee']

        self.logger.info('Processing invoice %s for marketplace %s', 
                         invoice_guid, marketplace_guid)
        self.logger.log(logging.NOTSET, 'Payload: %r', event_json)

        hold_item = dict(
            type='Holds',
            quantity=entity_data['holds_count'],
            amount=entity_data['holds_total_amount'],
            name='${:.2f} per hold'.format(entity_data['hold_fee'] / 100.0),
            total=entity_data['holds_total_fee'],
        )
        debit_card_item = dict(
            type='Debits: cards',
            quantity=entity_data['card_debits_count'],
            amount=entity_data['card_debits_total_amount'],
            name='{}% of txn amount'.format(entity_data['variable_fee_percentage']),
            total=entity_data['card_debits_total_fee'],
        )
        debit_bank_item = dict(
            type='Debits: bank accounts',
            quantity=entity_data['bank_account_debits_count'],
            amount=entity_data['bank_account_debits_total_amount'],
            name=(
                '{}% of txn amount'
                .format(entity_data['bank_account_debit_variable_fee_percentage'])
            ),
            total=entity_data['bank_account_debits_total_fee'],
        )
        if entity_data['bank_account_debit_variable_fee_cap']:
            debit_bank_item['name'] += (
                ' (max ${:.2f} per debit)'
                .format(entity_data['bank_account_debit_variable_fee_cap'] / 100.0)
            )
        credit_successed_item = dict(
            type='Credits: succeeded',
            quantity=entity_data['bank_account_credits_count'],
            amount=entity_data['bank_account_credits_total_amount'],
            name='${:.2f} per credit'.format(
                entity_data['bank_account_credit_fee'] / 100.0
            ),
            total=entity_data['bank_account_credits_total_fee'],
        )
        credit_failed_item = dict(
            type='Credits: failed',
            quantity=entity_data['failed_credits_count'],
            amount=entity_data['failed_credits_total_amount'],
            name='${:.2f} per failed credit'.format(
                entity_data['failed_credit_fee'] / 100.0
            ),
            total=entity_data['failed_credits_total_fee'],
        )
        refund_item = dict(
            type='Refunds',
            quantity=entity_data['reversals_count'],
            amount=entity_data['reversals_total_amount'],
            name='{}% of txn amount returned'.format(entity_data['variable_fee_percentage']),
            total=entity_data['refunds_total_fee'],
        )
        reversal_item = dict(
            type='Reversals',
            quantity=entity_data['reversals_count'],
            amount=entity_data['reversals_total_amount'],
            name='${:.2f} per reversal'.format(0),
            total=entity_data['reversals_total_fee'],
        )
        chargeback_item = dict(
            type='Chargebacks',
            quantity=entity_data['lost_debit_chargebacks_count'],
            amount=entity_data['lost_debit_chargebacks_total_amount'],
            name='${:.2f} per failed chargeback'.format(
                entity_data['chargeback_fixed_fee'] / 100.0
            ),
            total=entity_data['lost_debit_chargebacks_total_fee'],
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

        ev11 = event_json['entity_views']['1.1']['invoices'][0]
        adjustments = []
        for adjustment in ev11['adjustments']:
            self.logger.info(
                'Adjustment: amount=%s, reason=%s', 
                adjustment['amount'], adjustment['description'],
            )
            adjustments.append(dict(
                amount=adjustment['amount'],
                reason=adjustment['description'],
            ))

        self.logger.info('Marketplace URI: %s', marketplace_uri)
        self.logger.info('Customer URI: %s', customer_uri)
        self.logger.info('Funding source URI: %s', funding_source_uri)
        self.logger.info('Adjustment fee: %s', adjustments_total_fee)
        self.logger.info('Total fee: %s', total_fee)

        billy_cfg = self.config['billy']

        api = BillyAPI(
            api_key=billy_cfg['api_key'], 
            endpoint=billy_cfg['endpoint'],
        )
        company = api.get_company(billy_cfg['company_guid'])

        # retrieval the customer 
        customers = list(api.list_customers(external_id=customer_uri))
        if not customers:
            customer = company.create_customer(external_id=customer_uri)
        elif len(customers) == 1:
            customer = customers[0]
        else:
            raise RuntimeError(
                'WTF? we should only have one customer for that',
            )
        try:
            invoice = customer.invoice(
                # TODO: a better title here
                title='Balanced Transaction Usage Invoice',
                amount=total_fee,
                payment_uri=funding_source_uri,
                items=items,
                adjustments=adjustments,
                external_id=invoice_guid,
            )
            self.logger.info(
                'Created invoice %s in Billy for %s (GUID from Balanced)', 
                invoice.guid, invoice_guid,
            )
        except DuplicateExternalIDError:
            self.logger.warn('The invoice %s (GUID from Balanced) have already '
                             'been created, just ack the message', 
                             invoice_guid)
        self.logger.info('Processed invoice %s (GUID from Balanced) for '
                         'marketplace %s', invoice_guid, marketplace_uri)


class EventConsumer(ConsumerMixin):

    def __init__(self, connection, queues, processor, event_dir , logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.connection = connection
        self.queues = queues
        self.processor = processor
        self.event_dir = event_dir

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(self.queues, callbacks=[self.on_message], accept=['json']),
        ]

    def on_message(self, body, message):
        event_json = body
        event_type = event_json['type']
        if not event_type.startswith('invoice.'):
            self.logger.warn('Ignore unknown event type %r', event_type)
            message.ack()
            return
        if self.event_dir is not None:
            event_path = os.path.join(self.event_dir, event_json['guid'])
            with open(event_path, 'wt') as event_file:
                data = json.dumps(event_json, sort_keys=True,
                                 indent=4, separators=(',', ': '))
                event_file.write(data)
        self.processor.process(event_json)
        message.ack()
        self.logger.info('Ack message %s', event_json['guid'])


def setup_logging(
    default_path='logging.yaml', 
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """Setup logging configuration

    """
    import yaml
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def main():
    import yaml
    from kombu import Connection

    setup_logging()

    cfg_path = os.environ.get('CFG', 'dev_cfg.yaml')
    with open(cfg_path, 'rt') as cfg_file:
        config = yaml.load(cfg_file)

    processor = EventProcessor(config)
    logger = logging.getLogger(__name__)

    logger.info('Start processing events')
    logger.info('Billy Endpoint: %s', config['billy']['endpoint'])
    logger.info('Billy Company GUID: %s', config['billy']['company_guid'])

    event_dir = config.get('event_dir')
    amqp_cfg = config['amqp']
    amqp_uri = amqp_cfg['uri']
    queue = amqp_cfg['queue']
    logger.info('Connecting to message queue server %s', amqp_uri)
    logger.info('Pulling events from queue %s', queue)

    try:
        with Connection(amqp_uri) as conn:
            consumer = EventConsumer(conn, Queue(queue), processor, event_dir)
            consumer.run()
    except (SystemExit, KeyboardInterrupt):
        pass

    logger.info('Stop processing event')

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
