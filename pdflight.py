from typing import Tuple, List

import pandas as pd
from igc_lib.igc_lib import Flight


def flights_to_dataframes(
    flights: List[Flight],
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
                "start": [pd.to_datetime(flight.start).tz_localize("Europe/Vienna").tz_convert("UTC")],
                "finish": [pd.to_datetime(flight.finish).tz_localize("Europe/Vienna").tz_convert("UTC")],
            },
            index=pd.MultiIndex.from_tuples(
                [(flight.fr_manuf_code, flight.fr_uniq_id, flight.date)],
                names=["code", "id", "date"],
            ),
        )
        metadata_dfs.append(metadata_df)

        fixes_df = (
            pd.DataFrame(
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
            )
            .set_index(
                pd.to_datetime(
                    [fix.timestamp for fix in flight.fixes], unit="s", utc=True
                )
            )
            .rename_axis("datetime")
        )

        if fixes_df.index.has_duplicates:
            raise ValueError(
                f"Duplicate indices found in the DataFrame: {flight.fr_manuf_code}{flight.fr_uniq_id} {flight.date}"
            )

        fixes_df = fixes_df.resample("1s").asfreq()
        num_cols = ["lat", "lon", "alt", "gsp", "bearing", "bearing_change_rate"]
        fixes_df[num_cols] = fixes_df[num_cols].interpolate(method="time")
        bool_cols = ["flying", "circling"]
        fixes_df[bool_cols] = fixes_df[bool_cols].ffill()

        fixes_df.index = pd.MultiIndex.from_arrays(
            [
                fixes_df.index,
                [flight.fr_manuf_code] * len(fixes_df.index),
                [flight.fr_uniq_id] * len(fixes_df.index),
                [flight.date] * len(fixes_df.index),
            ],
            names=["datetime", "code", "id", "date"],
        )
        fixes_dfs.append(fixes_df)

        thermals_series = pd.Series(
            [
                pd.to_timedelta(thermal.time_change(), unit="s")
                for thermal in flight.thermals
            ],
            index=pd.to_datetime(
                [thermal.enter_fix.timestamp for thermal in flight.thermals],
                unit="s",
                utc=True,
            ),
            name="duration",
        )
        thermals_series.index = pd.MultiIndex.from_arrays(
            [
                thermals_series.index,
                [flight.fr_manuf_code] * len(thermals_series.index),
                [flight.fr_uniq_id] * len(thermals_series.index),
                [flight.date] * len(thermals_series.index),
            ],
            names=["datetime", "code", "id", "date"],
        )
        thermals_series_list.append(thermals_series)

    return (
        pd.concat(metadata_dfs).sort_index(),
        pd.concat(fixes_dfs).sort_index(),
        pd.concat(thermals_series_list).sort_index(),
    )


def shift_datetime(i, dt):
    return ((i[0] + dt),) + i[1:]

def in_task(df: pd.DataFrame, md: pd.DataFrame) -> pd.DataFrame:
    return (df.index.droplevel("datetime").map(md["start"]) <= df.index.get_level_values("datetime")) & (df.index.get_level_values("datetime") < df.index.droplevel("datetime").map(md["finish"]))
