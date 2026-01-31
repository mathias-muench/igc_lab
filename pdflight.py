"""Utilities for converting Flight objects to pandas DataFrames."""

import pandas as pd

def flight_to_dataframe(flight):
    """Converts a Flight object to a pandas DataFrame.

    Args:
        flight: A Flight object (from igc_lib.py).

    Returns:
        A DataFrame where each row is a GNSSFix, resampled to 1-second intervals,
        with columns for:
        - lat, lon (interpolated)
        - derived attributes: gsp, bearing, bearing_change_rate (interpolated)
        - flying, circling, task (forward-filled)
        - alt (chosen altitude, PRESS or GNSS, interpolated)

        The index is set to the UTC datetime of the fix.

    Raises:
        ValueError: If the flight is invalid.
    """
    if not flight.valid:
        raise ValueError("Flight is invalid. Check flight.notes for details.")

    df = (
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

    # Resample to 1-second intervals
    df = df.resample('s').asfreq()

    # Interpolate numerical columns
    num_cols = ["lat", "lon", "alt", "gsp", "bearing", "bearing_change_rate"]
    df[num_cols] = df[num_cols].interpolate(method='time')

    # Forward-fill boolean columns
    bool_cols = ["flying", "circling", "task"]
    df[bool_cols] = df[bool_cols].ffill()

    # Add flight metadata to the index
    df.index = pd.MultiIndex.from_arrays(
        [
            df.index,
            [flight.fr_manuf_code] * len(df.index),
            [flight.fr_uniq_id] * len(df.index),
            [flight.date] * len(df.index)
        ],
        names=["datetime", "code", "id", "date"]
    )

    return df

def thermals_to_dataframe(flight):
    """Converts a Flight's thermals to a pandas Series of durations.

    Args:
        flight: A Flight object (from igc_lib.py).

    Returns:
        A Series where:
        - index is the UTC datetime of the thermal entry
        - values are the duration of each thermal (as timedelta)

    Raises:
        ValueError: If the flight is invalid.
    """
    if not flight.valid:
        raise ValueError("Flight is invalid. Check flight.notes for details.")

    series = pd.Series(
        (pd.to_timedelta(thermal.time_change(), unit="s") for thermal in flight.thermals),
        index=pd.to_datetime(
            [thermal.enter_fix.timestamp for thermal in flight.thermals],
            unit="s",
            utc=True
        ),
        name="duration"
    )

    # Add flight metadata to the index
    series.index = pd.MultiIndex.from_arrays(
        [
            series.index,
            [flight.fr_manuf_code] * len(series.index),
            [flight.fr_uniq_id] * len(series.index),
            [flight.date] * len(series.index)
        ],
        names=["datetime", "code", "id", "date"]
    )

    return series
