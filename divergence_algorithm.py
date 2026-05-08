import numpy as np
import pandas as pd


def build_turn_mask(series, direction):
    values = pd.Series(series)

    if direction == "top":
        standard_turn = (values > values.shift(1)) & (values > values.shift(-1))
        prev_compare = lambda edge, pivot: edge < pivot
        next_compare = lambda edge, pivot: edge < pivot
    else:
        standard_turn = (values < values.shift(1)) & (values < values.shift(-1))
        prev_compare = lambda edge, pivot: edge > pivot
        next_compare = lambda edge, pivot: edge > pivot

    plateau_turn = pd.Series(False, index=values.index)
    i = 0
    while i < len(values):
        current = values.iloc[i]
        if pd.isna(current):
            i += 1
            continue

        j = i
        while j + 1 < len(values) and np.isclose(
            values.iloc[j + 1], current, rtol=0, atol=1e-9
        ):
            j += 1

        if j > i and i > 0 and j < len(values) - 1:
            prev_val = values.iloc[i - 1]
            next_val = values.iloc[j + 1]
            if (
                pd.notna(prev_val)
                and pd.notna(next_val)
                and prev_compare(prev_val, current)
                and next_compare(next_val, current)
            ):
                plateau_turn.iloc[j] = True

        i = j + 1

    return (standard_turn.fillna(False) | plateau_turn).astype(bool)


def detect_top_divergence(
    df,
    stoch_col="stochrsi",
    high_col="high",
    turn_col="turn",
    signal_col="bl",
):
    df = df.copy()
    df[turn_col] = np.where(build_turn_mask(df[stoch_col], "top"), -1, np.nan)
    df_turn = df[df[turn_col] == -1].copy()
    df_turn[signal_col] = np.where(
        (df_turn[stoch_col].shift(1) > 93)
        & (df_turn[high_col] > df_turn[high_col].shift(1))
        & (df_turn[stoch_col] < df_turn[stoch_col].shift(1)),
        -1,
        np.nan,
    )
    signals = df_turn[df_turn[signal_col] == -1].copy()
    return df, df_turn, signals


def detect_bottom_divergence(
    df,
    stoch_col="stochrsi",
    low_col="low",
    turn_col="turn",
    signal_col="dbl",
):
    df = df.copy()
    df[turn_col] = np.where(build_turn_mask(df[stoch_col], "bottom"), 1, np.nan)
    df_turn = df[df[turn_col] == 1].copy()
    df_turn[signal_col] = np.where(
        (df_turn[stoch_col].shift(1) < 5)
        & (df_turn[low_col] < df_turn[low_col].shift(1))
        & (df_turn[stoch_col] > df_turn[stoch_col].shift(1)),
        1,
        np.nan,
    )
    signals = df_turn[df_turn[signal_col] == 1].copy()
    return df, df_turn, signals
