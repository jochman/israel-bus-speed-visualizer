from collections import defaultdict
from pathlib import Path

import gtfs_kit as gk
from bidi.algorithm import get_display

import asyncio
import datetime as dt
import enum

import httpx
import pytz
from typer import Argument, Option, run, secho


class Agency(str, enum.Enum):
    EGGED = "EGGED"
    DAN = "DAN"


provider_to_num = {Agency.EGGED: "3", Agency.DAN: "5"}


LIMIT = 500000


async def get_page_rides_list(
    client: httpx.AsyncClient, start_time, end_time, ids: dict[str, str]
):
    url = "https://open-bus-stride-api.hasadna.org.il/siri_rides/list"
    offset = 0
    while True:
        task = client.get(
            url,
            params={
                "limit": LIMIT,
                "offset": offset,
                "siri_route__line_refs": ",".join(ids.values()),
                "scheduled_start_time_from": start_time,
                "scheduled_start_time_to": end_time,
            },
        )
        resp = await task
        if data := resp.json():
            offset += LIMIT
            yield data
        else:
            return


async def get_page_vehicle_locations(client: httpx.AsyncClient, ride_ids, route_ids):
    url = "https://open-bus-stride-api.hasadna.org.il/siri_vehicle_locations/list"
    offset = 0
    while True:
        task = client.get(
            url,
            params={
                "limit": LIMIT,
                "offset": offset,
                "siri_rides__ids": ",".join(map(str, ride_ids)),
                "siri_routes__ids": ",".join(map(str, route_ids)),
            },
        )
        resp = await task
        if data := resp.json():
            offset += LIMIT
            yield data
        else:
            return


async def get_rides_list(route_ids: dict[str, str], date: dt.datetime):
    start_time = (
        dt.datetime.combine(
            date, dt.datetime.min.time(), tzinfo=pytz.timezone("Israel")
        )
        .astimezone()
        .isoformat()
    )
    end_time = (
        dt.datetime.combine(
            date, dt.datetime.max.time(), tzinfo=pytz.timezone("Israel")
        )
        .astimezone()
        .isoformat()
    )
    data = defaultdict(list)
    async with httpx.AsyncClient() as client:
        async for resp in get_page_rides_list(client, start_time, end_time, route_ids):
            for element in resp:
                data[element["siri_route_id"]].append(element)
    return data


async def get_locations(rides: dict[str, dict]):
    ids = set()
    ids2 = set()
    for ride in rides.values():
        for element in ride:
            ids.add(element["id"])
            ids2.add(element["siri_route_id"])
        break
    data = defaultdict(list)
    async with httpx.AsyncClient() as client:
        async for resp in get_page_vehicle_locations(client, ids, ids2):
            for element in resp:
                data[element["siri_ride__id"]].append(element)
    return data


def main(
    line_number: str = Argument(),
    day: dt.datetime = Option(default_factory=dt.datetime.now, help="day to check"),
    agency: Agency = Option(
        help="in case where there are multiple lines with the same number (as 1), provide an agency",
        default=None,
    ),
    gtfs: Path = Option(exists=True, default="israel-public-transportation.zip"),
):
    feed = gk.read_feed(gtfs, dist_units="km")
    routes = feed.get_routes()
    line = routes.loc[routes["route_short_name"] == line_number]
    if agency:
        agency_id = provider_to_num[agency]
        line = line.loc[line["agency_id"] == agency_id]
    secho(f"checking directions for line {line_number}:", fg="green")
    secho(get_display(line["route_long_name"].to_string()), fg="white")
    line_dict = line.to_dict()
    rides_list = asyncio.run(get_rides_list(line_dict["route_id"], day))
    locations = asyncio.run(get_locations(rides_list))


if __name__ == "__main__":
    run(main)
