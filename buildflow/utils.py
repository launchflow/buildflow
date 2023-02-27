from uuid import uuid4


def uuid() -> str:
    return uuid4().hex
