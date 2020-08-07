import sys
import csv

params = {}

# to generate input for this script,
# openrefine-client --export --output=test.csv "the_fast_and_the_frdr"

class DBInterface:
    def __init__(self, params):
        self.dbtype = params.get('type', None)
        self.dbname = params.get('dbname', None)
        self.host = params.get('host', None)
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

    def _prep(self, statement):
        if (self.dbtype == "postgres"):
            return statement.replace('?', '%s')
        return statement

with open(sys.argv[1], encoding='utf-8') as csvfile:
	reader = csv.DictReader(csvfile)
	for row in reader:
		if row['No match (no equivalent orbroader term)'] == 'y' or row['No match (need access to dataset for context)'] == 'y':
			continue
		elif row['Correct auto match to FAST'] == 'y' or row['Manual match to FAST (Within OpenRefine choices)'] == 'y' or row['Manual match to FAST (Need to Look at FAST)'] == 'y' or row['Manual match to FAST (Broader Heading)'] == 'y':
			con = DBInterface.getConnection()
			with con:
				cur = DBInterface.getCursor(con)
				cur.execute(self._prep("""UPDATE tags SET reconciled = ? WHERE tag = ?"""), ('Reconciliation', 'Original Keyword'))