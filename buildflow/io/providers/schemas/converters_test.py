from dataclasses import asdict, dataclass
import datetime
import json
from typing import Dict, List, Optional, Set
import unittest

from buildflow.core import exceptions
from buildflow.io.providers.schemas import converters


@dataclass
class Nested:
    c: int


@dataclass
class InputDataClass:
    a: int
    b: int
    nested: Optional[Nested] = None
    nested_list: Optional[List[Nested]] = None
    timestamp: Optional[datetime.datetime] = None


class ConvertersTest(unittest.TestCase):
    def test_bytes_to_dict(self):
        expected_dict = {"a": 1, "b": 2}
        intput_bytes = json.dumps(expected_dict).encode("utf-8")

        converter = converters.bytes_to_dict()

        self.assertEqual(expected_dict, converter(intput_bytes))

    def test_bytes_to_dataclass(self):
        expected_output = InputDataClass(
            a=1,
            b=2,
            nested=Nested(c=3),
            nested_list=[Nested(c=4)],
            timestamp=datetime.datetime.now(),
        )
        output_dict = asdict(expected_output)
        output_dict["timestamp"] = output_dict["timestamp"].isoformat()
        intput_bytes = json.dumps(output_dict).encode("utf-8")

        converter = converters.bytes_to_dataclass(InputDataClass)

        self.assertEqual(expected_output, converter(intput_bytes))

    def test_dataclass_to_json(self):
        input_class = InputDataClass(
            a=1,
            b=2,
            nested=Nested(c=3),
            nested_list=[Nested(c=4)],
            timestamp=datetime.datetime.now(),
        )
        expected_output = asdict(input_class)
        expected_output["timestamp"] = expected_output["timestamp"].isoformat()

        converter = converters.dataclass_to_json()

        converted_json = converter(input_class)
        self.assertEqual(expected_output, converted_json)

    def test_json_push_converter_none(self):
        input_dict = {"a": 1, "b": 2}

        converter = converters.json_push_converter(None)

        self.assertEqual(input_dict, converter(input_dict))

    def test_json_push_converter_to_json(self):
        class Custom:
            def __init__(self, a: int) -> None:
                self.a = a

            def to_json(self) -> Dict[str, int]:
                return {"a": self.a}

        input_class = Custom(1)
        output_dict = {"a": 1}

        converter = converters.json_push_converter(Custom)

        self.assertEqual(output_dict, converter(input_class))

    def test_json_push_converter_dataclass(self):
        input_class = InputDataClass(
            a=1, b=2, nested=Nested(c=3), nested_list=[Nested(c=4)]
        )
        expected_output = asdict(input_class)

        converter = converters.json_push_converter(InputDataClass)

        converted_json = converter(input_class)
        self.assertEqual(expected_output, converted_json)

    def test_json_push_converter_list_dataclass(self):
        input_data = [
            InputDataClass(a=1, b=2, nested=Nested(c=3), nested_list=[Nested(c=4)])
        ]
        expected_output = [asdict(input_data[0])]

        converter = converters.json_push_converter(List[InputDataClass])

        converted_json = converter(input_data)
        self.assertEqual(expected_output, converted_json)

    def test_json_push_converter_list_dict(self):
        input_data = [{"a": 1}]

        converter = converters.json_push_converter(List[Dict])

        converted_json = converter(input_data)
        self.assertEqual(input_data, converted_json)

    def test_json_push_converter_set(self):
        input_data = set({"a": 1})

        converter = converters.json_push_converter(Set[Dict])

        converted_json = converter(input_data)
        self.assertEqual(input_data, converted_json)

    def test_json_push_converter_nested_set(self):
        input_data = [set({"a": 1})]

        converter = converters.json_push_converter(List[Set[Dict]])

        converted_json = converter(input_data)
        self.assertEqual(input_data, converted_json)

    def test_json_push_converter_not_supported(self):
        with self.assertRaises(exceptions.CannotConvertSinkException):
            converters.json_push_converter(int)

    def test_bytes_push_converter_none(self):
        input_bytes = "a".encode()

        converter = converters.bytes_push_converter(None)

        self.assertEqual(input_bytes, converter(input_bytes))

    def test_bytes_push_converter_to_bytes(self):
        class Custom:
            def __init__(self, a: int) -> None:
                self.a = a

            def to_bytes(self) -> Dict[str, int]:
                return json.dumps({"a": self.a}).encode("utf-8")

        input_class = Custom(1)
        output_bytes = json.dumps({"a": 1}).encode("utf-8")

        converter = converters.bytes_push_converter(Custom)

        self.assertEqual(output_bytes, converter(input_class))

    def test_bytes_push_converter_dataclass(self):
        input_class = InputDataClass(
            a=1, b=2, nested=Nested(c=3), nested_list=[Nested(c=4)]
        )
        expected_output = json.dumps(asdict(input_class)).encode()

        converter = converters.bytes_push_converter(InputDataClass)

        self.assertEqual(expected_output, converter(input_class))

    def test_bytes_push_converter_not_supported(self):
        with self.assertRaises(exceptions.CannotConvertSinkException):
            converters.bytes_push_converter(int)


if __name__ == "__main__":
    unittest.main()
