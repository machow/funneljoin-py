import pytest
import pandas as pd
import siuba.sql

from funneljoin import after_join
from siuba import collect
from siuba.tests.helpers import assert_frame_sort_equal
from pandas import NA

from pathlib import Path
from helpers import backend, tribble

here = Path(__file__).parent

landed = pd.read_csv(here / "landed.csv")
registered = pd.read_csv(here / "registered.csv")

@pytest.fixture
def data(backend):
    yield backend.load_df(landed), backend.load_df(registered)

# NOTE: to create the target dataframe results, I used this R code...
# res = after_join(
#        landed, registered,
#        by_user = "user_id", by_time = c("timestamp" = "timestamp"),
#        mode = "left", type = "any-any"
# )
# cat(paste(
#        res$user_id,
#        paste0('"', res$timestamp.x, '"'),
#        paste0('"', res$timestamp.y, '"'),
#        sep = ", ", collapse = ",\n"
# ))

# TODO:
#  * first_firstafter
#  * firstwithin
#  * lastbefore_any
#  * lastbefore_firstafter
#  * max_gap

PARAMS = {

        # any-firstafter ----

        ("inner", "any-firstafter"): [
            ("user_id", "timestamp_x", "timestamp_y"),
              1,        "2018-07-01", "2018-07-02",
              3,        "2018-07-02", "2018-07-02",
              4,        "2018-07-01", "2018-07-02",
              5,        "2018-07-10", "2018-07-11",
              6,        "2018-07-07", "2018-07-10",
              6,        "2018-07-08", "2018-07-10",
        ],

        ("left", "any-firstafter"): [
            ("user_id", "timestamp_x", "timestamp_y"),
              1,        "2018-07-01", "2018-07-02",
              2,        "2018-07-01", NA,
              3,        "2018-07-02", "2018-07-02",
              4,        "2018-07-01", "2018-07-02",
              4,        "2018-07-04", NA,
              5,        "2018-07-10", "2018-07-11",
              5,        "2018-07-12", NA,
              6,        "2018-07-07", "2018-07-10",
              6,        "2018-07-08", "2018-07-10",
        ],

        ("right", "any-firstafter"): [
            ("user_id", "timestamp_x", "timestamp_y"),
              1,        "2018-07-01", "2018-07-02",
              3,        "2018-07-02", "2018-07-02",
              4,        NA,           "2018-06-10",
              4,        "2018-07-01", "2018-07-02",
              5,        "2018-07-10", "2018-07-11",
              6,        "2018-07-07", "2018-07-10",
              6,        "2018-07-08", "2018-07-10",
              6,        NA,           "2018-07-11",
              7,        NA,           "2018-07-07",
        ],
        ("anti", "any-firstafter"): [
            ("user_id", "timestamp"),
              2,        "2018-07-01",
              4,        "2018-07-04",
              5,        "2018-07-12",
        ],
        ("semi", "any-firstafter"): [
            ("user_id", "timestamp"),
              1,        "2018-07-01",
              3,        "2018-07-02",
              4,        "2018-07-01",
              5,        "2018-07-10",
              6,        "2018-07-07",
              6,        "2018-07-08"
        ],
        ("full", "any-firstafter"): [
            ("user_id", "timestamp_x", "timestamp_y"),
              1,        "2018-07-01", "2018-07-02",
              2,        "2018-07-01", NA,
              3,        "2018-07-02", "2018-07-02",
              4,        "2018-07-01", "2018-07-02",
              4,        "2018-07-04", NA,
              5,        "2018-07-10", "2018-07-11",
              5,        "2018-07-12", NA,
              6,        "2018-07-07", "2018-07-10",
              6,        "2018-07-08", "2018-07-10",
              4,        NA,           "2018-06-10",
              6,        NA,           "2018-07-11",
              7,        NA,           "2018-07-07"
        ],

        # any-any ----
        ("left", "any-any"): [
            ("user_id", "timestamp_x", "timestamp_y"),
              1,        "2018-07-01", "2018-07-02",
              2,        "2018-07-01",  NA ,
              3,        "2018-07-02", "2018-07-02",
              4,        "2018-07-01", "2018-07-02",
              4,        "2018-07-04",  NA ,
              5,        "2018-07-10", "2018-07-11",
              5,        "2018-07-12",  NA ,
              6,        "2018-07-07", "2018-07-10",
              6,        "2018-07-07", "2018-07-11",
              6,        "2018-07-08", "2018-07-10",
              6,        "2018-07-08", "2018-07-11"
        ],
        ("inner", "any-any"): [
            ("user_id", "timestamp_x", "timestamp_y"),
              1,        "2018-07-01", "2018-07-02",
              3,        "2018-07-02", "2018-07-02",
              4,        "2018-07-01", "2018-07-02",
              5,        "2018-07-10", "2018-07-11",
              6,        "2018-07-07", "2018-07-10",
              6,        "2018-07-07", "2018-07-11",
              6,        "2018-07-08", "2018-07-10",
              6,        "2018-07-08", "2018-07-11"
        ],
        ("anti", "any-any"): [
            ("user_id", "timestamp"),
              2,        "2018-07-01",
              4,        "2018-07-04",
              5,        "2018-07-12"
        ],
        ("semi", "any-any"): [
            ("user_id", "timestamp"),
              1,        "2018-07-01",
              3,        "2018-07-02",
              4,        "2018-07-01",
              5,        "2018-07-10",
              6,        "2018-07-07",
              6,        "2018-07-08"
        ],
        ("full", "any-any"): [
            ("user_id", "timestamp_x", "timestamp_y"),
              1,        "2018-07-01", "2018-07-02",
              2,        "2018-07-01",  NA,
              3,        "2018-07-02", "2018-07-02",
              4,        "2018-07-01", "2018-07-02",
              4,        "2018-07-04",  NA,
              5,        "2018-07-10", "2018-07-11",
              5,        "2018-07-12",  NA,
              6,        "2018-07-07", "2018-07-10",
              6,        "2018-07-07", "2018-07-11",
              6,        "2018-07-08", "2018-07-10",
              6,        "2018-07-08", "2018-07-11",
              4,         NA ,         "2018-06-10",
              7,         NA ,         "2018-07-07"
        ],

        # first-first ----

        ("inner", "first-first"): [
            ("user_id", "timestamp_x", "timestamp_y"),
              1,        "2018-07-01", "2018-07-02",
              3,        "2018-07-02", "2018-07-02",
              6,        "2018-07-07", "2018-07-10",
              5,        "2018-07-10", "2018-07-11"
        ],

        ("left", "first-first"): [
            ("user_id", "timestamp_x", "timestamp_y"),
              1,        "2018-07-01", "2018-07-02",
              2,        "2018-07-01",  NA,
              4,        "2018-07-01",  NA,
              3,        "2018-07-02", "2018-07-02",
              6,        "2018-07-07", "2018-07-10",
              5,        "2018-07-10", "2018-07-11"
        ],

        ("right", "first-first"): [
            ("user_id", "timestamp_x", "timestamp_y"),
              4,        NA,            "2018-06-10",
              1,        "2018-07-01",  "2018-07-02",
              3,        "2018-07-02",  "2018-07-02",
              7,        NA,            "2018-07-07",
              6,        "2018-07-07",  "2018-07-10",
              5,        "2018-07-10",  "2018-07-11"
        ],

        ("full", "first-first"): [
            ("user_id", "timestamp_x", "timestamp_y"),
              1,        "2018-07-01", "2018-07-02",
              2,        "2018-07-01",  NA,
              4,        "2018-07-01",  NA,
              3,        "2018-07-02", "2018-07-02",
              6,        "2018-07-07", "2018-07-10",
              5,        "2018-07-10", "2018-07-11",
              4,         NA,          "2018-06-10",
              7,         NA,          "2018-07-07"
        ],

        ("semi", "first-first"): [
            ("user_id", "timestamp"),
              1,        "2018-07-01",
              3,        "2018-07-02",
              6,        "2018-07-07",
              5,        "2018-07-10"
        ],

        ("anti", "first-first"): [
            ("user_id", "timestamp"),
              2,        "2018-07-01",
              4,        "2018-07-01"
        ],

}

PARAMS_LIST = list(PARAMS.items())

def test_after_join_firstafter(data):
    landed, registered = data
    q = after_join(landed, registered, by_user = "user_id", by_time = "timestamp", mode = "inner", type = "any-firstafter")
    res = collect(q)


    assert (res.columns == ["user_id", "timestamp_x", "timestamp_y"]).all()
    assert 4 in res["user_id"].array
    assert 1 in res["user_id"].array
    assert 7 not in res["user_id"].array
    assert 2 not in res["user_id"].array
    
    for col in res:
        assert res[col].notna().all()

@pytest.mark.parametrize("args, target", PARAMS_LIST)
def test_after_join_firstafter_mode(args, target, data):
    landed, registered = data
    q = after_join(
            landed,
            registered,
            by_user = "user_id",
            by_time = "timestamp",
            mode = args[0],
            type = args[1],
            )
    
    result = collect(q)
    target = tribble(*target)

    assert_frame_sort_equal(result, target)



#test_that("after_join works with mode = left and type = any-firstafter", {
#
#  res <- after_join(landed, registered, by_user = "user_id", by_time = c("timestamp" = "timestamp"), mode = "left", type = "any-firstafter")
#
#  expect_is(res, "tbl_df")
#  expect_equal(names(res), c("user_id", "timestamp.x", "timestamp.y"))
#  expect_true(all(res$timestamp.y >= res$timestamp.x |
#                    is.na(res$timestamp.y)))
#  expect_gte(nrow(landed), nrow(res))
#  expect_true(4 %in% res$user_id)
#  expect_true(1 %in% res$user_id)
#  expect_true(all(!is.na(res$timestamp.x)))
#  expect_true(any(is.na(res$timestamp.y)))
#  expect_true(all(!is.na(res$user_id)))
#})
