import argparse

import snowflake.connector
from lifetimes import BetaGeoFitter


def parse_args():
    parser = argparse.ArgumentParser(
        description='Build a Churn Model with Snowflake doing the heavy lifting of feature creation')

    parser.add_argument('-p', '--password', help="your snowflake password.")
    parser.add_argument('-u', '--username', help='your snowflake username.')
    parser.add_argument('-a', '--account', help='your snowflake account.')
    parser.add_argument('-w', '--warehouse', help='the snowflake warehouse you will be using.')
    parser.add_argument('-d', '--database', help='the snowflake database you will be using.')
    parser.add_argument('-s', '--schema', help='the snowflake schema you will be using.')
    parser.add_argument('-e', '--event', help='the RudderStack event that will be used.')

    return parser.parse_args()


def extract_features(db):
    q = "with filter as( select distinct user_id, timestamp from " + args.event + " where user_id is not null), " \
                                                                                  "max_date as ( " \
                                                                                  "select max(timestamp) as last_date " \
                                                                                  "from filter), first_iteration as (" \
                                                                                  "select user_id, count(timestamp) - " \
                                                                                  "1 as " \
                                                                                  "frequency, min(timestamp) as " \
                                                                                  "first_timestamp, max(timestamp) as " \
                                                                                  "last_timestamp from filter group " \
                                                                                  "by " \
                                                                                  "user_id), merged as ( select * " \
                                                                                  "from first_iteration, max_date), " \
                                                                                  "features as ( select user_id, " \
                                                                                  "frequency, " \
                                                                                  "TIMESTAMPDIFF('day'," \
                                                                                  "first_timestamp, last_timestamp) " \
                                                                                  "as recency, TIMESTAMPDIFF('day'," \
                                                                                  "first_timestamp, " \
                                                                                  "last_date) as t from merged) " \
                                                                                  "select * from features; "
    cur = db.cursor()
    cur.execute(q)

    return cur.fetch_pandas_all()


def train_model(features):

    features_filtered = features[features['FREQUENCY'] > 0]

    model = BetaGeoFitter()
    model.fit(features_filtered['FREQUENCY'], features_filtered['RECENCY'], features_filtered['T'])
    print(model.summary)

    model.save_model('churn_model_snow.pkl')


if __name__ == '__main__':

    args = parse_args()

    if args.password is None:
        print("Password is missing.")
        quit()
    if args.username is None:
        print("Username is missing.")
        quit()
    if args.account is None:
        print("account is missing.")
        quit()
    if args.warehouse is None:
        print("warehouse is missing.")
        quit()
    if args.database is None:
        print("database is missing.")
        quit()
    if args.schema is None:
        print("schema is missing.")
        quit()
    if args.event is None:
        print("event name is missing.")
        quit()

    con = snowflake.connector.connect(user=args.username, password=args.password, account=args.account,
                                      warehouse=args.warehouse, database=args.database, schema=args.schema)

    feats = extract_features(con)
    train_model(feats)
