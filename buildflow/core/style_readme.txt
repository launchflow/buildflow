
When implementing an API class and extending its abilities, you should always
indicate the switch from API world -> implementation world with a `*` so any
downstream usage is forced to use kwargs for non-api fields.

Example:

SomeAPI:
    def __init__(self, api_required_field):
        ...


class MyImplementation(SomeApi)

    def __init__(self, api_required_field, *, my_required_field):
        ...



Rule above applies to any configuration options, even in "owned" abstractions.