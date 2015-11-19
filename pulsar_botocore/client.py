import botocore.client
from pulsar import get_event_loop, new_event_loop


class PulsarClientCreator(botocore.client.ClientCreator):

    def __init__(self, loader, endpoint_resolver, user_agent, event_emitter,
                 retry_handler_factory, retry_config_translator,
                 response_parser_factory=None, loop=None):
        super().__init__(loader, endpoint_resolver, user_agent, event_emitter,
                         retry_handler_factory, retry_config_translator,
                         response_parser_factory=response_parser_factory)

        loop = loop or get_event_loop() or new_event_loop()
        self._loop = loop

    # TODO
