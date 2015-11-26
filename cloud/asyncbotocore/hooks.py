import asyncio
import logging

from botocore.hooks import HierarchicalEmitter, _PrefixTrie


logger = logging.getLogger(__name__)


class AsyncHierarchicalEmitter(HierarchicalEmitter):
    def __init__(self):
        super().__init__()

        # We keep a reference to the handlers for quick
        # read only access (we never modify self._handlers).
        # A cache of event name to handler list.
        self._lookup_cache = {}
        self._handlers = _PrefixTrie()
        # This is used to ensure that unique_id's are only
        # registered once.
        self._unique_id_handlers = {}

    @asyncio.coroutine
    def _emit(self, event_name, kwargs, stop_on_response=False):
        handlers_to_call = self._lookup_cache.get(event_name)
        if handlers_to_call is None:
            handlers_to_call = self._handlers.prefix_search(event_name)
            self._lookup_cache[event_name] = handlers_to_call
        elif not handlers_to_call:
            return []
        kwargs['event_name'] = event_name
        responses = []
        for handler in handlers_to_call:
            print(handler)
            logger.debug('Event %s: calling handler %s', event_name, handler)
            response = handler(**kwargs)
            if asyncio.iscoroutine(response):
                response = yield from response
            responses.append((handler, response))
            if stop_on_response and response is not None:
                return responses
        return responses

    @asyncio.coroutine
    def emit(self, event_name, **kwargs):
        """
        Emit an event by name with arguments passed as keyword args.

            >>> responses = yield from emitter.emit(
            ...     'my-event.service.operation', arg1='one', arg2='two')

        :rtype: list
        :return: List of (handler, response) tuples from all processed
                 handlers.
        """
        return (yield from self._emit(event_name, kwargs))

    @asyncio.coroutine
    def emit_until_response(self, event_name, **kwargs):
        """
        Emit an event by name with arguments passed as keyword args,
        until the first non-``None`` response is received. This
        method prevents subsequent handlers from being invoked.

            >>> handler, response = yield from emitter.emit_until_response(
                'my-event.service.operation', arg1='one', arg2='two')

        :rtype: tuple
        :return: The first (handler, response) tuple where the response
                 is not ``None``, otherwise (``None``, ``None``).
        """
        responses = yield from self._emit(event_name, kwargs,
                                          stop_on_response=True)
        if responses:
            return responses[-1]
        else:
            return (None, None)
