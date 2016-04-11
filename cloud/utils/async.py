from inspect import isawaitable


async def as_coroutine(value):
    if isawaitable(value):
        value = await value
    return value
