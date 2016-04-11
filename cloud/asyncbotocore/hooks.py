import logging
from inspect import isawaitable

from botocore.hooks import HierarchicalEmitter

from ..utils.async import as_coroutine


logger = logging.getLogger(__name__)


class AsyncHierarchicalEmitter(HierarchicalEmitter):

    def async_emit(self, *args, **kwargs):
        return as_coroutine(self.emit(*args, **kwargs))

    def _emit(self, event_name, kwargs, stop_on_response=False):
        responses = []
        # Invoke the event handlers from most specific
        # to least specific, each time stripping off a dot.
        handlers_to_call = self._lookup_cache.get(event_name)
        if handlers_to_call is None:
            handlers_to_call = self._handlers.prefix_search(event_name)
            self._lookup_cache[event_name] = handlers_to_call
        elif not handlers_to_call:
            # Short circuit and return an empty response is we have
            # no handlers to call.  This is the common case where
            # for the majority of signals, nothing is listening.
            return []
        kwargs['event_name'] = event_name
        responses = []
        need_to_wait = False
        for handler in handlers_to_call:
            logger.debug('Event %s: calling handler %s', event_name, handler)
            response = handler(**kwargs)
            if isawaitable(response):
                need_to_wait = True
            responses.append((handler, response))
            if stop_on_response and response is not None:
                break
        return self._wait(responses) if need_to_wait else responses

    async def emit_until_response(self, event_name, **kwargs):
        """
        Emit an event by name with arguments passed as keyword args,
        until the first non-``None`` response is received. This
        method prevents subsequent handlers from being invoked.

            >>> handler, response = await emitter.emit_until_response(
                'my-event.service.operation', arg1='one', arg2='two')

        :rtype: tuple
        :return: The first (handler, response) tuple where the response
                 is not ``None``, otherwise (``None``, ``None``).
        """
        responses = await as_coroutine(self._emit(event_name, kwargs,
                                                  stop_on_response=True))
        if responses:
            return responses[-1]
        else:
            return None, None

    async def _wait(self, responses):
        result = []
        for handler, response in responses:
            if isawaitable(response):
                response = await response
            result.append((handler, response))
        return result
