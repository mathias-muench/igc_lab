"""Utilities for converting Flight objects to pandas DataFrames."""

import pandas as pd

def flights_to_dataframes(flights):
    """Converts a list of Flight objects to concatenated pandas DataFrames for metadata, fixes, and thermals.

    Args:
        flights: A list of Flight objects (from igc_lib.py).

    Returns:
        A tuple of three pandas objects:
        - A DataFrame with flight metadata (pilot, points) for all flights
        - A DataFrame with flight fixes (1-second resampled) for all flights
        - A Series with thermal durations for all flights

        All objects share the same metadata index (code, id, date) in their indices.

    Raises:
        ValueError: If any flight is invalid.
    """
    metadata_dfs = []
    fixes_dfs = []
    thermals_series_list = []

    for flight in flights:
        if not flight.valid:
            raise ValueError(f"Flight is invalid. Check flight.notes for details. Flight: {flight.fr_manuf_code}{flight.fr_uniq_id} {flight.date}")

        # Create metadata DataFrame
        metadata_df = pd.DataFrame({
            "pilot": [flight.pilot],
            "points": [flight.points]
        }, index=pd.MultiIndex.from_tuples(
            [(flight.fr_manuf_code, flight.fr_uniq_id, flight.date)],
            names=["code", "id", "date"]
        ))
        metadata_dfs.append(metadata_df)

        # Create fixes DataFrame
        fixes_df = (
            pd.DataFrame({
                "lat": fix.lat,
                "lon": fix.lon,
                "alt": fix.alt,
                "gsp": fix.gsp,
                "bearing": fix.bearing,
                "bearing_change_rate": fix.bearing_change_rate,
                "flying": fix.flying,
                "circling": fix.circling,
                "task": fix.task,
            } for fix in flight.fixes)
            .set_index(
                pd.to_datetime(
                    [fix.timestamp for fix in flight.fixes],
                    unit="s",
                    utc=True
                )
            )
            .rename_axis("datetime")
        )

        if fixes_df.index.has_duplicates:
            raise ValueError(f"Duplicate indices found in the DataFrame: {flight.fr_manuf_code}{flight.fr_uniq_id} {flight.date}")

        # Resample to 1-second intervals
        fixes_df = fixes_df.resample('s').asfreq()

        # Interpolate numerical columns
        num_cols = ["lat", "lon", "alt", "gsp", "bearing", "bearing_change_rate"]
        fixes_df[num_cols] = fixes_df[num_cols].interpolate(method='time')

        # Forward-fill boolean columns
        bool_cols = ["flying", "circling", "task"]
        fixes_df[bool_cols] = fixes_df[bool_cols].ffill()

        # Add flight metadata to the index
        fixes_df.index = pd.MultiIndex.from_arrays(
            [
                fixes_df.index,
                [flight.fr_manuf_code] * len(fixes_df.index),
                [flight.fr_uniq_id] * len(fixes_df.index),
                [flight.date] * len(fixes_df.index)
            ],
            names=["datetime", "code", "id", "date"]
        )
        fixes_dfs.append(fixes_df)

        # Create thermals Series
        thermals_series = pd.Series(
            (pd.to_timedelta(thermal.time_change(), unit="s") for thermal in flight.thermals),
            index=pd.to_datetime(
                [thermal.enter_fix.timestamp for thermal in flight.thermals],
                unit="s",
                utc=True
            ),
            name="duration"
        )

        # Add flight metadata to the index
        thermals_series.index = pd.MultiIndex.from_arrays(
            [
                thermals_series.index,
                [flight.fr_manuf_code] * len(thermals_series.index),
                [flight.fr_uniq_id] * len(thermals_series.index),
                [flight.date] * len(thermals_series.index)
            ],
            names=["datetime", "code", "id", "date"]
        )
        thermals_series_list.append(thermals_series)

    # Concatenate all flights' data
    metadata_df = pd.concat(metadata_dfs)
    fixes_df = pd.concat(fixes_dfs)
    thermals_series = pd.concat(thermals_series_list)

    return metadata_df, fixes_df, thermals_series
