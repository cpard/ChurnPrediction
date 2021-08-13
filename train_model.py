import argparse
import duckdb

con = duckdb.connect(database=':memory:', read_only=False)


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
    con.execute("select * from features")

    l = con.df()
    l['t'] = l['t'].astype('timedelta64[D]')
    l['recency'] = l['recency'].astype('timedelta64[D]')
    print(l)


if __name__ == '__main__':
    args = parse_args()

    if args.parquet is None:
        print("Where are the Events dude?")
        quit()

    create_features(args.parquet)
