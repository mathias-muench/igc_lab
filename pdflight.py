from typing import List, Tuple

import pandas as pd

from scraped import ScrapedFlight


def flights_to_dataframes(
    flights: List[ScrapedFlight],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
    metadata_dfs = []
    fixes_dfs = []
    thermals_series_list = []

    for flight in flights:
        if not flight.valid:
            raise ValueError("Flight is invalid. Check flight.notes for details.")

        metadata_df = pd.DataFrame(
            {
                "competition": [flight.competition],
                "competition_class": [flight.competition_class],
                "pilot": [flight.pilot],
                "points": [flight.points],
                "start": [
                    pd.to_datetime(flight.start)
                    .tz_localize("Europe/Vienna")
                    .tz_convert("UTC")
                ],
                "finish": [
                    pd.to_datetime(flight.finish)
                    .tz_localize("Europe/Vienna")
                    .tz_convert("UTC")
                ],
            },
            index=pd.Index(
                [flight.fr_manuf_code + flight.fr_uniq_id + flight.date],
                name="flight",
            ),
        )
        metadata_dfs.append(metadata_df)

        fixes_df = pd.DataFrame(
            (
                {
                    "lat": fix.lat,
                    "lon": fix.lon,
                    "alt": fix.alt,
                    "gsp": fix.gsp,
                    "bearing": fix.bearing,
                    "bearing_change_rate": fix.bearing_change_rate,
                    "flying": fix.flying,
                    "circling": fix.circling,
                }
                for fix in flight.fixes
            ),
            index=pd.to_datetime(
                [fix.timestamp for fix in flight.fixes], unit="s", utc=True
            ),
        )

        fixes_df = fixes_df.resample("1s").nearest()

        fixes_df.index = pd.MultiIndex.from_arrays(
            [
                [flight.fr_manuf_code + flight.fr_uniq_id + flight.date]
                * len(fixes_df.index),
                fixes_df.index,
            ],
            names=["flight", "datetime"],
        )
        fixes_dfs.append(fixes_df)

        thermals_series = pd.Series(
            [
                pd.to_timedelta(thermal.time_change(), unit="s")
                for thermal in flight.thermals
            ],
            index=pd.MultiIndex.from_arrays(
                [
                    [flight.fr_manuf_code + flight.fr_uniq_id + flight.date]
                    * len(flight.thermals),
                    pd.to_datetime(
                        [thermal.enter_fix.timestamp for thermal in flight.thermals],
                        unit="s",
                        utc=True,
                    ),
                ],
                names=["flight", "datetime"],
            ),
            name="duration",
        )
        thermals_series_list.append(thermals_series)

    return (
        pd.concat(metadata_dfs).sort_index(),
        pd.concat(fixes_dfs).sort_index(),
        pd.concat(thermals_series_list).sort_index(),
    )


def shift_datetime(i, dt):
    return (i[0], (i[1] + dt))


def in_task(df: pd.DataFrame, md: pd.DataFrame) -> pd.DataFrame:
    return (
        df.index.droplevel("datetime").map(md["start"])
        <= df.index.get_level_values("datetime")
    ) & (
        df.index.get_level_values("datetime")
        < df.index.droplevel("datetime").map(md["finish"])
    )
