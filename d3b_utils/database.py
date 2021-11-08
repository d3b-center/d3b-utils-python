import psycopg2


def test_pg_connection(db_url):
    """Check if it is possible to connect to a postgres database

    :param db_url: database url to attempt to connect to
    :type db_url: str
    :return: outcome of if it is possible to connect to the specified database
    :rtype: bool
    """
    try:
        conn = psycopg2.connect(db_url, connect_timeout=1)
        conn.close()
        return True
    except psycopg2.OperationalError:
        return False
