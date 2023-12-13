from multiprocessing import Process, Queue
from time import time_ns
from typing import Any


class InfluxLine:
    """Class representing a single InfluxDB measurement"""

    def __init__(self, key: str, value: Any, *tags: tuple[str, Any]) -> None:
        self.key = key
        self.value = value
        self.tags = tags
        self.timestamp = time_ns()


class InfluxLPR:
    """Class for printing measurements in InfluxDB Line Protocol format"""

    def __init__(self) -> None:
        self.queue: Queue[InfluxLine] = Queue()
        self.print_job = Process(target=self._print_task)
        self.print_job.start()

    def print(self, key: str, value: Any, *tags: tuple[str, Any]) -> None:
        """Adds the measurement to the print queue"""
        self.queue.put(InfluxLine(key, value, *tags))

    def _print_task(self) -> None:
        data: dict[str, tuple[tuple[str, Any], ...]] = {}	# dictionary identifier -> tags
        try:
            while True:
                line = self.queue.get(block=True)
                tagDictionary = dict(line.tags)	# convert the list of 2-tuples into a dictionary to get the identifiers
                identifier = str(tagDictionary.get("objectInstanceNumber")) + "_" + str(tagDictionary.get("deviceIdentifier"))

                if line.key != "presentValue":
                    if identifier in data:
                        data[identifier] += ((line.key, line.value),)
                    else:
                        data[identifier] = ((line.key, line.value),)
                    continue
                
				# if line.key == "presentValue"
                if identifier not in data:
                    data[identifier] = ()    # to make sure identifier is a key in data
                
                line.tags += data[identifier]
                self._print_influx_line(line)
                data.pop(identifier)
        except KeyboardInterrupt:
            pass

    @staticmethod
    def _print_influx_line(line: InfluxLine) -> None:
        tags_str = ",".join(f"{tagKey}={tagValue}"
                            for tagKey, tagValue in line.tags)
        tags_str = f",{tags_str}" if tags_str else tags_str
        value = line.value
        if isinstance(value, list):
            for index, inner in enumerate(value):
                print(f"bacnet{tags_str},index={index} "
                      f"{line.key}={inner} {line.timestamp}")
        else:
            print(f"bacnet{tags_str} {line.key}={line.value} {line.timestamp}")
