import ast
import builtins
import importlib
import sys
from typing import Any


def find_flow_varname(filename: str) -> str:
    with open(filename, "r") as file:
        tree = ast.parse(file.read())
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
                if hasattr(node.value.func, "id") and node.value.func.id == "Flow":
                    return node.targets[0].id
    return None


def import_from_string(import_str: str) -> Any:
    if import_str.endswith(".py"):
        filename = import_str
        varname = find_flow_varname(filename)
        if varname is not None:
            import_str = f"{filename[:-3]}:{varname}"
        else:
            raise ValueError(f"Could not find Flow object in {filename}")
    if hasattr(builtins, import_str):
        return getattr(builtins, import_str)
    module_str, _, attrs_str = import_str.partition(":")
    if not module_str or not attrs_str:
        message = (
            'Import string "{import_str}" must be in format "<module>:<attribute>".'
        )
        raise ValueError(message.format(import_str=import_str))

    try:
        module = importlib.import_module(module_str)
    except ImportError as exc:
        if exc.name != module_str:
            raise exc from None
        message = 'Could not import module "{module_str}".'
        raise ValueError(message.format(module_str=module_str))

    instance = module
    try:
        for attr_str in attrs_str.split("."):
            instance = getattr(instance, attr_str)
    except AttributeError:
        message = 'Attribute "{attrs_str}" not found in module "{module_str}".'
        raise ValueError(message.format(attrs_str=attrs_str, module_str=module_str))

    # Clear cache to allow reloading of module
    del sys.modules[module_str]

    return instance
