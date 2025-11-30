from csv import DictReader
from pathlib import Path

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement


def load_csv_to_db(csv_path: str, batch_size: int = 100):
    with open(csv_path) as fp:
        reader = DictReader(fp)

        table_name = Path(csv_path).stem

        cluster = Cluster()
        with cluster.connect() as session:
            keyspace_name = "dados_metereologicos"

            session.execute(
                f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
                """
            )
            session.set_keyspace(keyspace_name)

            session.execute(f"DROP TABLE IF EXISTS {table_name}")
            session.execute(
                f"""
                CREATE TABLE {table_name}(
                    id UUID PRIMARY KEY,
                    ano_2004 float, ano_2005 float, ano_2006 float, ano_2007 float,
                    ano_2008 float, ano_2009 float, ano_2010 float, ano_2011 float,
                    ano_2012 float, ano_2013 float, ano_2014 float, ano_2015 float,
                    ano_2016 float, ano_2017 float, ano_2018 float, ano_2019 float,
                    ano_2020 float, ano_2021 float, ano_2022 float, ano_2023 float,
                    ano_2024 float
                )
                """
            )

            insert_stmt = session.prepare(
                f"""
                INSERT INTO {table_name}(
                    id, ano_2004, ano_2005, ano_2006, ano_2007, ano_2008, ano_2009,
                    ano_2010, ano_2011, ano_2012, ano_2013, ano_2014, ano_2015,
                    ano_2016, ano_2017, ano_2018, ano_2019, ano_2020, ano_2021,
                    ano_2022, ano_2023, ano_2024
                ) VALUES (uuid(), ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """
            )

            batch = BatchStatement()
            count = 0

            for entry in reader:
                values = tuple(float(x or -9999) for x in entry.values())
                batch.add(insert_stmt, values)
                count += 1

                if count >= batch_size:
                    session.execute(batch)
                    batch = BatchStatement()
                    count = 0

            if count > 0:
                session.execute(batch)
