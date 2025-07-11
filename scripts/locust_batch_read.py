import random
import json
from locust import HttpUser, task, between, events

class BatchReadUser(HttpUser):

    table_size = 100000
    batch_size = 100

    @events.init_command_line_parser.add_listener
    def _(parser):
        parser.add_argument("--table-size", type=int, default=100000, help="Set table size")
        parser.add_argument("--batch-size", type=int, default=100, help="Set batch size")
        parser.add_argument("--database-name", type=str, default="benchmark", help="Set database name")

    @events.test_start.add_listener
    def _(environment, **kwargs):
        BatchReadUser.table_size = environment.parsed_options.table_size
        BatchReadUser.db_name = environment.parsed_options.database_name
        BatchReadUser.batch_size = environment.parsed_options.batch_size
        print(f"Starting Locust with table_size={BatchReadUser.table_size}, batch_size={BatchReadUser.batch_size}")

    @task
    def batch_read(self):
        operations = []
        for _ in range(self.batch_size):
            id0 = random.randint(1, self.table_size)
            operations.append({
                "method": "POST",
                "relative-url": f"{BatchReadUser.db_name}/bench_tbl/pk-read",
                "body": {
                    "filters": [{"column": "id0", "value": id0}]
                }
            })

        payload = {"operations": operations}
        headers = {"Content-Type": "application/json"}

        self.client.post("/0.1.0/batch", data=json.dumps(payload), headers=headers)
