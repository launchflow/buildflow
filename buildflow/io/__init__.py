import dataclasses
import importlib
import inspect
import pkgutil
from typing import List

from buildflow.io.primitive import Primitive


@dataclasses.dataclass
class IOPrimitiveOption:
    module_name: str
    class_name: str
    source: bool
    sink: bool


def list_all_io_primitives() -> List[IOPrimitiveOption]:
    primitives = []
    # Iterate through all submodules in the package
    for importer, module_name, is_pkg in pkgutil.iter_modules(__path__):
        if module_name == "primitive":
            continue
        full_module_name = f"{__name__}.{module_name}"
        module = importlib.import_module(full_module_name)
        # Check each object in the module
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj) and issubclass(obj, Primitive) and obj != Primitive:
                source = obj.source_provider is not Primitive.source_provider
                sink = obj.sink_provider is not Primitive.sink_provider
                primitives.append(
                    IOPrimitiveOption(
                        module_name=module_name,
                        class_name=name,
                        source=source,
                        sink=sink,
                    )
                )
    return primitives
