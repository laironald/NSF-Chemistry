import sys
sys.path.append("/home/ron/PythonBase")
import MySQL, SQLite, time, datetime

"""
USAGE: python overflow.py [username] [password]

"""


if len(sys.argv)<3:
    uname = raw_input("username: ")
    pword = raw_input("password: ")
else:
    uname = sys.argv[1]
    pword = sys.argv[2]

cfg={'host':'localhost', 'user':uname, 'passwd':pword, 'db':'nsf_chemistry'}
m = MySQL.MySQL(cfg=cfg)


if not m.tables(lookup="chem"):
    m.c.execute("CREATE TABLE chem LIKE RD.nsf")
    m.c.execute("INSERT INTO chem SELECT * FROM RD.nsf WHERE nsf_organization='CHE'")

    m.c.execute("CREATE TABLE topics2 LIKE dave.topicsindoc_finalv1")
    m.c.execute("INSERT INTO topics2 SELECT * FROM dave.topicsindoc_finalv1 WHERE did LIKE 'NSF%'")
    m.c.execute("UPDATE topics2 SET did=SUBSTR(did, 5);")
    m.c.execute("ALTER TABLE topics2 CHANGE did did INT(11);")

    m.c.execute("ALTER TABLE topics2 ADD COLUMN NSFOrg VARCHAR(256);")
    m.c.execute("ALTER TABLE topics2 ADD COLUMN Date0 VARCHAR(256);")
    m.c.execute("ALTER TABLE topics2 ADD COLUMN Date1 VARCHAR(256);")
    m.c.execute("ALTER TABLE topics2 ADD COLUMN Amount INT(11);")
    m.c.execute("ALTER TABLE topics2 ADD COLUMN tlabel VARCHAR(256);")

    m.c.execute("CREATE INDEX tid ON topics2 (tid);")

    m.c.execute("""
        UPDATE  topics2 a, RD.nsf b
           SET  a.NSFOrg = b.NSF_Organization,
                a.Date0 = b.Project_Start_Date,
                a.Date1 = b.Project_End_Date,
                a.Amount = b.Award_Amount
         WHERE  a.did = b.Award_ID
        """)
    m.c.execute("""
        UPDATE  topics2 a, dave.topics_finalv1 b
           SET  a.tlabel = b.tlabel
         WHERE  a.tid = b.tid
        """)


m.c.execute("SELECT Date0, Date1, prob, tlabel FROM topics2 WHERE NSFOrg='CHE'")
chem = m.c.fetchall()

#MINI TIME TUTORIAL
#t = time.strptime("30-Nov-02", "%d-%b-%y")
#datetime.date(t[0], t[1], t[2]).strftime("%Y%m%d")

def yymm01(t):
    return [(t-1)/12, (t-1)%12+1, 1]


points = {}
for c in chem:
    t0 = time.strptime(c[0], "%d-%b-%y")
    t1 = time.strptime(c[1], "%d-%b-%y")

    d0 = datetime.date(*t0[:3])
    d1 = datetime.date(*t1[:3])

    n0 = t0[0]*12+t0[1]
    n1 = t1[0]*12+t1[1]

    if c[3] not in points:
        points[c[3]] = {}

    for x in range(n0, n1+1):
        cdate = str(datetime.date(*yymm01(x)))
        if cdate not in points[c[3]]:
            points[c[3]][cdate] = 0
        points[c[3]][cdate] = points[c[3]][cdate] + c[2]

output = [["label", "date", "value"]]
for k in points.keys():
    for l in points[k].items():
        output.append([k, l[0], l[1]])

#Output results as CSV
import senAdd
senAdd.csvOutput(output, "nsfchem.csv")
