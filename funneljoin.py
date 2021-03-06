__version__ = "0.0.1"

# Note: can import high-level single dispatch functions, since if a user
#       wants to run funneljoin over SQL data, they'll have imported
#       siuba.sql, which registers all needed single dispatchers
from siuba import mutate, filter, group_by, inner_join, join, semi_join, anti_join, arrange, select, ungroup
from siuba.dply.vector import row_number, desc
from siuba.sql.verbs import _validate_join_arg_on, LazyTbl

from siuba.siu import _
from siuba.dply.verbs import singledispatch2

from pandas import DataFrame

def _get_key_tuple(on):
    mapping = _validate_join_arg_on(on)

    if len(mapping) > 1:
        raise ValueError("by_time and by_user cannot join on multiple columns")

    return next(iter(mapping.items()))

@singledispatch2((DataFrame, LazyTbl))
def distinct_events(tbl, time_col, user_col, type):
    if type not in ["first", "last"]:
        return tbl

    res = (tbl
            >> group_by(_[user_col])
            >> arrange(_[time_col] if type == "first" else -_[time_col])
            >> filter(row_number(_) == 1)
            >> ungroup()
            )

    return res

@singledispatch2((DataFrame, LazyTbl))
def after_join(
        lhs, rhs,
        by_time, by_user,
        mode = "inner",
        type = "first-firstafter",
        max_gap = None,
        min_gap = None,
        gap_col = None,
        suffix = ("_x", "_y")
        ):

    if max_gap is not None or min_gap is not None or gap_col is not None:
        raise NotImplementedError("max_gap, min_gap, gap_col not implemented")

    # Get type of join for both tables, from e.g. "first-firstafter"
    type_lhs, type_rhs = type.split("-")

    # Convert join keys to dictionary form
    by_time_x, by_time_y = _get_key_tuple(by_time)
    by_user_x, by_user_y = _get_key_tuple(by_user)

    # mutate in row_number ----
    lhs_i = (lhs
            >> arrange(_[by_user_x], _[by_time_x])
            >> mutate(__idx = row_number(_))
            >> distinct_events(by_time_x, by_user_x, type_lhs)
            )

    rhs_i = (rhs
            >> arrange(_[by_user_y], _[by_time_y])
            >> mutate(__idy = row_number(_))
            >> distinct_events(by_time_y, by_user_y, type_rhs)
            )

    # Handle when time column is in the other table
    if by_time_x == by_time_y:
        # TODO: don't use implicit join suffix below
        pair_time_x, pair_time_y = by_time_x + "_x", by_time_y + "_y"
    else:
        pair_time_x, pair_time_y = by_time_x, by_time_y

    # Inner join by user, filter by time
    pairs = filter(
            inner_join(lhs_i, rhs_i, by_user),
            _[pair_time_x] <= _[pair_time_y]
            )

    # TODO: firstwithin
    if type_lhs in ["firstwithin", "lastbefore"]:
        raise NotImplementedError("Can't currently handle lhs type %s" % type_lhs)

    # Handle firstafter by subsetting
    if type_rhs == "firstafter":
        pairs = (pairs
                >> arrange(_[pair_time_y])
                >> group_by(_.__idx)
                >> filter(row_number(_) == 1)
                >> ungroup()
                )


    distinct_pairs = select(pairs, _.__idx, _.__idy)


    if mode in ["inner", "left", "right", "full", "outer"]:
        by_dict = dict([(by_user_x, by_user_y), ("__idy", "__idy")])
        res = (lhs_i
                >> join(_, distinct_pairs, on = "__idx", how = mode) 
                # TODO: suffix arg
                >> join(_, rhs_i , on = by_dict, how = mode)#, suffix = suffix)
                >> select(-_["__idx", "__idy"])
                )
    elif mode in ["semi", "anti"]:
        join_func = semi_join if mode == "semi" else anti_join
        res = (lhs_i
                >> join_func(_, distinct_pairs, "__idx")
                >> select(-_["__idx", "__idy"])
                )

    else:
        raise ValueError("mode not recognized: %s" %mode)

    return res

def get_example_data():
    import pkg_resources
    import pandas as pd
    landed = pkg_resources.resource_filename("funneljoin_data", "landed.csv")
    reg = pkg_resources.resource_filename("funneljoin_data", "registered.csv")

    return pd.read_csv(landed), pd.read_csv(reg)
