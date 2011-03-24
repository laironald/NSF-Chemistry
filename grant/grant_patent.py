import sys, re, datetime
sys.path.append("/home/ron/PythonBase")
sys.path.append("/home/ron/PythonBase/tools")
import crawl, SQLite, MySQL

db = "../../patent/usptoPat.s3"
cfg = SQLite.MySQL_cfg(cfg={"host":"localhost", "db":"nsf_chemistry"}, title="MySQL Login")

#FETCH AND STORE PATENTS RELATED TO QUERY
"""
queries = ["govt/(government or interest or rights or certain or support or contract)"]
for query in queries:
    crawl.PattyParse(query=query, fetch=True, patentGrab=True, db=db)
"""

s = SQLite.SQLite(db, table="patent_search")
fetch = s.fetch(field=["key", "html"], fetch=False)

#PARSE HTML FILE FOR GOVERNMENT SECTION -- CURRENTLY SUPPORTS JUST NSF
"""
i = 0
pat_grant = []
patxgrant = []
t0 = datetime.datetime.now()
while True:
    i = i + 1
    html = fetch.fetchone()
    if not html:
        break

    y = re.findall("<HR>.*?GOVERNMENT INTEREST.*?<HR>(.*?)<HR>", html[1], re.I+re.S)
    if len(y)>0:
        y = re.sub("  +", " ", re.sub("<BR>|\n", " ", y[0])).strip()
        sys.stdout.write("{clear}  - {x} {time}".format(clear="\b"*30, x=i, time=datetime.datetime.now()-t0))

        nsf = re.findall(r"\b(NSF|National Science Foundation)\b", y, re.I)
        if len(nsf)>0:
            awards = re.findall(r"[0]?[0-9]{2}[-]?[0-9]{5}", y)
            for award in awards:
                pat_grant.append(["NSF", html[0], int(re.sub("-", "", award))])
            if len(awards)==0:
                patxgrant.append(["NSF", html[0]])

print ""            
s.c.execute("CREATE TABLE IF NOT EXISTS grantxpatent (Agency, Patent)")
s.chgTbl("grantxpatent")
s.index(["Agency"])
s.index(["Agency", "Patent"], unique=True)

s.c.execute("CREATE TABLE IF NOT EXISTS grant_patent (Agency, Patent, Grant)")
s.chgTbl("grant_patent")
s.index(["Agency"])
s.index(["Agency", "Patent"])
s.index(["Agency", "Grant"])
s.index(["Agency", "Patent", "Grant"], unique=True)

s.c.executemany("INSERT OR IGNORE INTO grant_patent VALUES (?, ?, ?)", pat_grant)
s.c.executemany("INSERT OR IGNORE INTO grantxpatent VALUES (?, ?)", patxgrant)
s.conn.commit()
"""

s.chgTbl("grant_patent")
s.mysql_output(cfg=cfg, intList=["GrantNum"])

s.close()

#MySQL Functions
m = MySQL.MySQL(cfg=cfg)
if "grant_patent_chem" not in m.tables():
    m.c.execute("CREATE TABLE grant_patent_chem LIKE grant_patent")
    m.c.execute("""
        INSERT INTO grant_patent_chem
            SELECT  a.*
              FROM  grant_patent AS a
        INNER JOIN  RD.nsf AS b
                ON  a.Agency="NSF" AND a.GrantNum=b.Award_ID AND b.NSF_Organization="CHE"
        """)
