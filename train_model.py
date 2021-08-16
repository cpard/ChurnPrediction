import argparse
import duckdb
import pandas

from lifetimes.fitters.pareto_nbd_fitter import ParetoNBDFitter
from lifetimes import BetaGeoFitter

con = duckdb.connect(database=':memory:', read_only=False)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.max_columns', None)


def parse_args():
    parser = argparse.ArgumentParser(description='Build a Churn Model')
    parser.add_argument('-p', '--parquet', help="the path to the Parquet file with the time series data.")
    return parser.parse_args()


def create_features(db):
    max_date = con.execute("select max(date) from '" + db + "'").fetchall()[0][0]
    initial_view = "create view filtered as select distinct user_id, date from '" + db + "'"
    first_iteration = "create view first_iteration as select user_id, count(date)-1 as freq, min(date) as first, " \
                      "max(date) as last from filtered " \
                      "group by user_id "

    features = "create view features as select user_id, freq, last - first as recency, DATE '" + str(max_date) + \
               "' - first as T from first_iteration"

    con.execute(initial_view)
    con.execute(first_iteration)
    con.execute(features)

    # we need todo this because for some unknown reason the intervals that come back into the dataframe are all wrong,
    # this is probably a bug with the duckdb implementation. Very ungly hack to turn an interval into a count of days,
    # this is because of the limitations of duckdb in terms of date functions.

    con.execute(
        "select user_id, freq, "
        "extract('year' from recency) as recency_year, "
        "extract('month' from recency) as recency_month, "
        "extract('day' from recency) as recency_day, "
        "extract('year' from t) as t_year, "
        "extract('month' from t) as t_month, "
        "extract('day' from t) as t_day "
        "from features")

    db_result = con.df()

    # calculate number of days from years, months and days. We consider 30.436875 days in a month on an average.
    db_result['t'] = round(
        db_result['t_year'] * 365 + db_result['t_month'] * 30.436875 + db_result['t_day'], 0)
    db_result['recency'] = round(
        db_result['recency_year'] * 365 + db_result['recency_month'] * 30.436875 + db_result['recency_day'], 0)

    db_result = db_result.drop(['recency_year', 'recency_day', 'recency_month', 't_year', 't_month', 't_day'], axis=1)

    filtered_freqs = db_result[db_result['freq'] > 0]
    model = BetaGeoFitter()
    model.fit(filtered_freqs['freq'], filtered_freqs['recency'], filtered_freqs['t'])
    print(model.summary)


if __name__ == '__main__':
    args = parse_args()

    if args.parquet is None:
        print("Where are the Events dude?")
        quit()

    create_features(args.parquet)
