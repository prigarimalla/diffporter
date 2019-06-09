from sqlite3 import connect, Error
from pathlib import Path

class HashCache(object):
    def __init__(self, db_path, db_name='diffporter.db'):
        db_dir = Path(db_path)
        db_dir.mkdir(parents=True, exist_ok=True)
        self.db_conn = connect(str(db_dir.joinpath(db_name)))
        self.bootstrap_db()

    def bootstrap_db(self):
        sql_create_files_table = """
            CREATE TABLE IF NOT EXISTS files (
                md5 text PRIMARY KEY,
                location text
            )
            """

        sql_create_index = """
            CREATE INDEX IF NOT EXISTS file_location ON files(location)
        """
        self.db_conn.execute(sql_create_files_table)
        self.db_conn.execute(sql_create_index)

    def cache(self, file_hash, file_path):
        sql_insert_file = """
            INSERT OR REPLACE INTO files (md5, location)
            VALUES (?, ?);
        """
        self.db_conn.execute(sql_insert_file, (file_hash, file_path))
        self.db_conn.commit()

    def get(self, file_path):
        sql_get_file = """
            SELECT * FROM files WHERE location=(?)
        """
        res = self.db_conn.execute(sql_get_file, (file_path, )).fetchall()
        if len(res) != 1:
            return None
        return res[0]