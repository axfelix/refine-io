import os
import sys
import csv

params = {}

# to generate input for this script,
# openrefine-client --export --output=test.csv "the_fast_and_the_frdr"

class DBInterface:
    def __init__(self, params):
        self.dbtype = params.get('type', 'sqlite')
        self.dbname = params.get('dbname', 'data/globus_oai.db')
        self.host = params.get('host', 'localhost')
        self.schema = params.get('schema', None)
        self.user = params.get('user', None)
        self.password = params.get('pass', None)
        self.connection = None
        self.logger = None

        if self.dbtype == "sqlite":
            self.dblayer = __import__('sqlite3')
            if os.name == "posix":
                try:
                    os.chmod(self.dbname, 0o664)
                except:
                    pass

        elif self.dbtype == "postgres":
            self.dblayer = __import__('psycopg2')

    def getConnection(self):
        if self.connection == None:
            if self.dbtype == "sqlite":
                self.connection = self.dblayer.connect(self.dbname)
            elif self.dbtype == "postgres":
                self.connection = self.dblayer.connect("dbname='%s' user='%s' password='%s' host='%s'" % (
                    self.dbname, self.user, self.password, self.host))
                self.connection.autocommit = True

        return self.connection

    def getCursor(self, con):
        if self.dbtype == "sqlite":
            con.row_factory = self.getRow()
            cur = con.cursor()
        if self.dbtype == "postgres":
            from psycopg2.extras import RealDictCursor
            cur = con.cursor(cursor_factory=RealDictCursor)

        return cur

    def getRow(self):
        return self.dblayer.Row

    def _prep(self, statement):
        if (self.dbtype == "postgres"):
            return statement.replace('?', '%s')
        return statement

with open(sys.argv[1], encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    dbh = DBInterface(params)
    con = dbh.getConnection()
    cur = dbh.getCursor(con)
    for row in reader:
        if row['No match (no equivalent or broader term)'] == 'y' or row['No match (need access to dataset for context)'] == 'y':
            continue
        elif row['Correct auto match to FAST'] == 'y' or row['Manual match to FAST (Within OpenRefine choices)'] == 'y' or row['Manual match to FAST (Need to Look at FAST)'] == 'y' or row['Manual match to FAST (Broader Heading)'] == 'y':
            if len(sys.argv) > 2 and sys.argv[2] == '--dryrun':
                continue
            cur.execute("SELECT tag_id FROM tags WHERE tag=?", (row['Original Keyword'],))
            tag_id = cur.fetchone()
            try:
                cur.execute(dbh._prep("""INSERT INTO reconciliations (tag_id, reconciliation, language) VALUES (?,?,?)"""), (tag_id, row['Reconciliation'], 'en'))
                if row['Reconciliation - Additional Term'] is not None:
                    cur.execute(dbh._prep("""INSERT INTO reconciliations (tag_id, reconciliation, language) VALUES (?,?,?)"""), (tag_id, row['Reconciliation - Additional Term'], 'en'))
            except dbh.dblayer.IntegrityError as e:
                pass
            except dbh.dblayer.InterfaceError as e:
                pass