from siuba.sql import LazyTbl
import pytest
import os
from sqlalchemy import create_engine, types

BACKEND_CONFIG = {
        "postgresql": {
            "dialect": "postgresql",
            "dbname": ["SB_TEST_PGDATABASE", "postgres"],
            "port": ["SB_TEST_PGPORT", "5433"],
            "user": ["SB_TEST_PGUSER", "postgres"],
            "password": ["SB_TEST_PGPASSWORD", ""],
            "host": ["SB_TEST_PGHOST", "localhost"],
            },
        "sqlite": {
            "dialect": "sqlite",
            "dbname": ":memory:",
            "port": "0",
            "user": "",
            "password": "",
            "host": ""
            }
        }


class Backend:
    def __init__(self, name):
        self.name = name

    def dispose(self):
        pass

    def load_df(self, df = None, **kwargs):
        if df is None and kwargs:
            df = pd.DataFrame(kwargs)
        elif df is not None and kwargs:
            raise ValueError("Cannot pass kwargs, and a DataFrame")

        return df

    def load_cached_df(self, df):
        return df

    def __repr__(self):
        return "{0}({1})".format(self.__class__.__name__, repr(self.name))

class PandasBackend(Backend):
    pass

class SqlBackend(Backend):
    table_name_indx = 0
    sa_conn_fmt = "{dialect}://{user}:{password}@{host}:{port}/{dbname}"

    def __init__(self, name):
        cnfg = BACKEND_CONFIG[name]
        params = {k: os.environ.get(*v) if isinstance(v, (list)) else v for k,v in cnfg.items()}

        self.name = name
        self.engine = create_engine(self.sa_conn_fmt.format(**params))
        self.cache = {}

    def dispose(self):
        self.engine.dispose()

    @classmethod
    def unique_table_name(cls):
        cls.table_name_indx += 1
        return "siuba_{0:03d}".format(cls.table_name_indx)

    def load_df(self, df = None, **kwargs):
        df = super().load_df(df, **kwargs)
        return copy_to_sql(df, self.unique_table_name(), self.engine)

    def load_cached_df(self, df):
        import hashlib
        from pandas import util
        hash_arr = util.hash_pandas_object(df, index=True).values
        hashed = hashlib.sha256(hash_arr).hexdigest()

        if hashed in self.cache:
            return self.cache[hashed]
        
        res = self.cache[hashed] = self.load_df(df)

        return res

def copy_to_sql(df, name, engine):
    if isinstance(engine, str):
        engine = create_engine(engine)

    df.to_sql(name, engine, dtype = auto_types(df), index = False, if_exists = "replace")
    return LazyTbl(engine, name)


# TODO: don't think auto_types is necessary
PREFIX_TO_TYPE = {
        # for datetime, need to convert to pandas datetime column
        #"dt": types.DateTime,
        "int": types.Integer,
        "float": types.Float,
        "str": types.String
        }

def auto_types(df):
    dtype = {}
    for k in df.columns:
        pref, *_ = k.split('_')
        if pref in PREFIX_TO_TYPE:
            dtype[k] = PREFIX_TO_TYPE[pref]
    return dtype



# fixtures --------------------------------------------------------------------

params_backend = [
    pytest.param(lambda: SqlBackend("postgresql"), id = "postgresql", marks=pytest.mark.postgresql),
    #pytest.param(lambda: SqlBackend("sqlite"), id = "sqlite", marks=pytest.mark.sqlite),
    pytest.param(lambda: PandasBackend("pandas"), id = "pandas", marks=pytest.mark.pandas)
    ]

@pytest.fixture(params = params_backend, scope = "session")
def backend(request):
    return request.param()

@pytest.fixture
def skip_backend(request, backend):
    if request.node.get_closest_marker('skip_backend'):
        mark_args = request.node.get_closest_marker('skip_backend').args
        if backend.name in mark_args:
            pytest.skip('skipped on backend: {}'.format(backend.name)) 

