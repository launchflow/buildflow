import unittest

import buildflow
from buildflow.api import NodePlan, ProcessorPlan
from buildflow.core.io import empty_io
from buildflow.core.io import datawarehouse_io


class TestDataWarehouseIO(unittest.TestCase):

    def test_gcp_datawarehouse_io_plan(self):
        expected_plan = NodePlan(
            name="",
            processors=[
                ProcessorPlan(
                    name="dw_process",
                    source_resources={
                        "source_type": "EmptySource",
                    },
                    sink_resources={
                        "sink_type": "DataWarehouseSink",
                        "table": {
                            "table_id": "p.buildflow_default.table",
                            "schema": None,
                        },
                    },
                )
            ],
        )
        app = buildflow.ComputeNode()

        @app.processor(
            source=empty_io.EmptySource([]),
            sink=datawarehouse_io.DataWarehouseSink(cloud="gcp",
                                                    name="table",
                                                    project_id="p"),
        )
        def dw_process(elem):
            pass

        plan = app.plan()
        self.assertEqual(expected_plan, plan)


if __name__ == "__main__":
    unittest.main()
