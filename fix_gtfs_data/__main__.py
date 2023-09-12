from pathlib import Path
import gtfs_kit as gk

"""
`route_short_name` is line number_1
"""

import asyncio
from typer import run, Option, Argument, secho
import datetime as dt
import httpx
import pytz
import enum


class Agency(enum.StrEnum):
    EGGED = "3"
    DAN = "5"


async def get_data(route_ids: dict[str, str], date: dt.date):
    start_time = (
        dt.datetime.combine(date, dt.datetime.min.time(), pytz.timezone("Israel"))
        .astimezone()
        .isoformat()
    )
    end_time = (
        (
            dt.datetime.combine(date, dt.datetime.min.time(), pytz.timezone("Israel"))
            + dt.timedelta(days=1, minutes=-1)
        )
        .astimezone()
        .isoformat()
    )
    url = "https://open-bus-stride-api.hasadna.org.il/siri_rides/list"

    async with httpx.AsyncClient() as client:
        tasks = [
            client.get(
                url,
                params={
                    "siri_route__line_refs": value,
                    "scheduled_start_time_from": start_time,
                    "scheduled_start_time_to": end_time,
                },
            )
            for value in route_ids.values()
        ]
        resp = await asyncio.gather(*tasks)
    data: dict[int, httpx.Response] = dict(zip(route_ids.keys(), resp))
    for key in data.keys():
        if data[key].status_code == 200:
            data[key] = data[key].json()
    return data


def main(
    line_number: str = Argument(),
    day: str = Option(default_factory=dt.date.today().isoformat, help="day to check"),
    agency: Agency = Option(
        help="in case where there are multiple lines with the same number (as 1), provide an agency",
        default=None,
    ),
    gtfs: Path = Option(exists=True, default="israel-public-transportation.zip"),
):
    feed = gk.read_feed(gtfs, dist_units="km")
    routes = feed.get_routes()
    day_obj = dt.date.fromisoformat(day)
    line = routes.loc[routes["route_short_name"] == line_number]
    if agency:
        line = line.loc[line["agency_id"] == str(agency)]
    secho(f"checking directions for line {line_number}:", fg="green")
    secho(line["route_long_name"].to_string(), fg="white")
    line_dict = line.to_dict()
    resp = asyncio.run(get_data(line_dict["route_id"], day_obj))
    print(resp)


if __name__ == "__main__":
    run(main)
