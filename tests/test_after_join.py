import pytest
import pandas as pd
import siuba.sql

from funneljoin import after_join
from siuba import collect

from pathlib import Path
from helpers import backend

here = Path(__file__).parent

landed = pd.read_csv(here / "landed.csv")
registered = pd.read_csv(here / "registered.csv")

@pytest.fixture
def data(backend):
    yield backend.load_df(landed), backend.load_df(registered)


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

