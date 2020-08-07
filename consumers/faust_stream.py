"""Defines trends calculations for stations"""
import logging

import faust

logger = logging.getLogger(__name__)

# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("postgres-connect-stations", value_type=Station)
out_topic = app.topic("cta.stations", partitions=1)

table = app.Table(
   "cta.stations.table",
   default=int,
   partitions=5,
   changelog_topic=out_topic
)

def convert_line(event: Station):
    if event.blue:
        return "blue"
    elif event.red:
        return "red"
    elif event.green:
        return "green"
    else:
        return None


@app.agent(topic)
async def process(stream):

    async for event in stream:
        line = convert_line(event)

        if line is not None:
            transformed_station = TransformedStation(
                station_id=event.station_id,
                station_name=event.station_name,
                order=event.order,
                line=line
            )

            table[event.station_id] = transformed_station

            await out_topic.send(key=str(event.station_id), value=transformed_station)

if __name__ == "__main__":
    app.main()
