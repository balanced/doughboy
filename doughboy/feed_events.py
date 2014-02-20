from __future__ import unicode_literals
import os
import json
import logging

from kombu import Connection


def main():
    import yaml
    logger = logging.getLogger(__name__)

    cfg_path = os.environ.get('CFG', 'dev_cfg.yaml')
    with open(cfg_path, 'rt') as cfg_file:
        config = yaml.load(cfg_file)

    amqp_cfg = config['amqp']
    amqp_uri = amqp_cfg['uri']
    queue = amqp_cfg['queue']
    input_dir = 'input'

    with Connection(amqp_uri) as conn:
        event_queue = conn.SimpleQueue(queue)

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
            logger.info('Publish event %s', event_json['guid'])
            event_queue.put(event_json)

        event_queue.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
