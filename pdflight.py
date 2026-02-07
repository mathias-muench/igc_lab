"""Utilities for converting Flight objects to pandas DataFrames."""

import pandas as pd

def flight_to_dataframes(flight):
    """Converts a Flight object to pandas DataFrames for metadata, fixes, and thermals.

    Args:
        flight: A Flight object (from igc_lib.py).

    Returns:
        A tuple of three pandas objects:
        - A DataFrame with flight metadata (pilot, points)
        - A DataFrame with flight fixes (1-second resampled)
        - A Series with thermal durations

        All objects share the same metadata index (code, id, date) in their indices.

    Raises:
        ValueError: If the flight is invalid.
    """
    if not flight.valid:
        raise ValueError("Flight is invalid. Check flight.notes for details.")

    # Create metadata DataFrame
    metadata_df = pd.DataFrame({
        "pilot": [flight.pilot],
        "points": [flight.points]
    }, index=pd.MultiIndex.from_tuples(
        [(flight.fr_manuf_code, flight.fr_uniq_id, flight.date)],
        names=["code", "id", "date"]
    ))

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

    return metadata_df, fixes_df, thermals_series
