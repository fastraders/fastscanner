NATS_FLUSH_SECONDS = (
    0.0001,
    0.0005,
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.1,
    float("inf"),
)

INDICATOR_EXTEND_SECONDS = (
    0.0005,
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    1.0,
    float("inf"),
)

HTTP_REQUEST_SECONDS = (
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    float("inf"),
)

DRAGONFLY_COMMAND_SECONDS = (
    0.0001,
    0.0005,
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    float("inf"),
)

WS_PUSH_SECONDS = (
    0.0005,
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    1.0,
    float("inf"),
)

WS_SUBSCRIBE_SECONDS = (
    0.001,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    5.0,
    float("inf"),
)

CANDLE_TO_CLIENT_SECONDS = (
    0.1,
    0.25,
    0.5,
    1.0,
    2.0,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    float("inf"),
)

PARTITION_WRITE_SECONDS = (
    0.001,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    float("inf"),
)

SYMBOL_DURATION_SECONDS = (
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    25.0,
    60.0,
    120.0,
    300.0,
    600.0,
    float("inf"),
)

RATELIMIT_WAIT_SECONDS = (
    0.001,
    0.01,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.0,
    5.0,
    10.0,
    float("inf"),
)

CACHE_SAVE_SECONDS = (
    0.0005,
    0.001,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    1.0,
    5.0,
    float("inf"),
)

SLOW_FETCH_SECONDS = (
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    20.0,
    45.0,
    90.0,
    120.0,
    300.0,
    float("inf"),
)

