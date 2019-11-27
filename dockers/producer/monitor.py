import configparser

import psycopg2


def get_pg_conn():
    config = configparser.ConfigParser()
    config.read('config.ini')
    db_cnn_str = config['DATA']['db']

    pg_conn = None

    try:
        pg_conn = psycopg2.connect(db_cnn_str)
    except Exception:
        if pg_conn:
            pg_conn.close()
        raise

    return pg_conn


def get_pg_results(pg_conn, sql, data):
    try:
        cur = pg_conn.cursor()
        if data:
            cur.execute(sql, data)
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    except Exception:
        raise


def get_overall_stats(pg_conn):
    sql_p = "SELECT sum(total) AS produced FROM producer.stats"
    sql_c = "SELECT sum(total) AS produced FROM consumer.stats"

    data = dict()
    rows_p = get_pg_results(pg_conn, sql_p, None)

    if len(rows_p) > 0:
        data['producer'] = rows_p[0][0]

    rows_c = get_pg_results(pg_conn, sql_c, None)

    if len(rows_c) > 0:
        data['consumer'] = rows_c[0][0]

    return data


def get_countries_stats(pg_conn, representation_type, limit=5):
    if representation_type == 'top':
        order_by = 'DESC'
    else:
        order_by = 'ASC'

    sql = """
        SELECT country, sum(total) as total
        FROM consumer.country_stats
        GROUP BY country
        ORDER BY 2 {order_by}
        LIMIT %s
    """.format(order_by=order_by)

    return get_pg_results(pg_conn, sql, (limit,))


def show_stats(pg_conn):
    stats = get_overall_stats(pg_conn=pg_conn)

    print('stats')

    if stats.get('producer'):
        print('\tTotal produced {}'.format(stats.get('producer')))
    else:
        print('\tProduced - 0')

    if stats.get('consumer'):
        print('\tTotal consumed {}'.format(stats.get('consumer')))
    else:
        print('\tConsumed - 0')

    print('............')

    return show_stats


def show_country_stats(pg_conn):
    countries_high = get_countries_stats(pg_conn=pg_conn, representation_type='top', limit=5)
    countries_low = get_countries_stats(pg_conn=pg_conn, representation_type='least', limit=5)

    if len(countries_high) > 0:
        print('Top represented countries')
        for item in countries_high:
            print("\t{} : consumed {} ".format(item[0], item[1]))
    else:
        print('\tNo consumed stats for countries')

    if len(countries_low) > 0:
        print('Least represented countries')
        for item in countries_low:
            print("\t{} : consumed {} ".format(item[0], item[1]))
    else:
        print('\tNo consumed stats for countries')


if __name__ == "__main__":
    pg_conn = get_pg_conn()

    show_stats(pg_conn)
    show_country_stats(pg_conn)

    pg_conn.close()
