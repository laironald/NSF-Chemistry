import sys
sys.path.append("/home/ron/PythonBase")
import SQLite, MySQL, senAdd

cfg = SQLite.MySQL_cfg({'host':'localhost', 'db':'RD'})

m = MySQL.MySQL(cfg=cfg, table="nsf")
m.fetch(field=["source_year", "pi_name", "Award_ID", "Award_Title", "Organization_Name"], limit=10, fetch=False, random=True)

#RETRIEVE Chemistry Grants (Chemistry ones)
m.c.execute("""
    CREATE TEMPORARY TABLE grant_chem AS
        SELECT  *
          FROM  RD.nsf
         WHERE  NSF_Organization="CHE"
    """)
# GET first initials to match the block
def initials2(name):
    return "|".join([x.upper()[:2] for x in name.split(", ")])
def jaroPair(name, lastname, firstname):
    name = [x.upper() for x in name.split(", ")]
    return 0.60*senAdd.jarow(name[0], lastname)+0.40*senAdd.jarow(name[1], firstname)
def idx(string, index=0, delimiter="/"):
    try:
        return string.split(delimiter)[index]
    except:
        return ""
  
s = m.sqlite_output(table="grant_chem")
s.conn.create_function("initials2", 1, initials2)
s.conn.create_function("jPair", 3, jaroPair)
s.conn.create_function("jarow", 2, senAdd.jarow)
s.conn.create_function("idx", 1, idx)
s.add("initials")
s.c.execute("UPDATE grant_chem SET initials=initials2(pi_name)")
print s.fetch(field=["pi_name", "initials"], table="grant_chem", limit=5)
s.count()

#MERGE together Grants and Patents (Chemistry ones)
m.c.execute("""
    CREATE TEMPORARY TABLE grant_patent AS
        SELECT  Award_ID, a.Patent, source_year, pi_name, Award_Title, Organization_Name,
                Project_Start_Date
          FROM  nsf_chemistry.grant_patent AS a
    INNER JOIN  RD.nsf AS b
            ON  b.Award_ID=a.GrantNum AND b.NSF_Organization="CHE"
                AND a.Agency="NSF"
      ORDER BY  source_year, Award_ID
    """)

s = m.sqlite_output(table="grant_patent", sLite=s)
s.conn.create_function("dV", 1, senAdd.dateVert)
s.conn.create_function("p8", 1, senAdd.pat8char)
s.add("SDate", table="grant_chem")
s.add("SDate", table="grant_patent")
s.add("FPatent", table="grant_chem")
s.add("FPatent", table="grant_patent")
s.c.executescript("""
    UPDATE  grant_patent
       SET  SDate=dV(Project_Start_Date), FPatent=p8(Patent);
    UPDATE  grant_chem
       SET  SDate=dV(Project_Start_Date);
    """)
s.index(["FPatent"])

#GET and Add Patents based on InvNum
# Figure out the distinct Invnum_N and SDate
s.attach("/home/ron/inputdata/Ron/fullset/invpat.Feb2011.sqlite3")
s.c.execute("SELECT lastname, firstname, block1 FROM invpat LIMIT 10")

# do we need to block?  hrm.. maybe runIt, maybe not
runIt = False
for test in s.c.fetchall():
    if not(test[0][:2]+"|"+test[1][:2]==test[2]):
        runIt = True
        break
if runIt:
    s.c.execute("""
        UPDATE  db.invpat
           SET  Block1=SUBSTR(Lastname, 1,2)||"|"||SUBSTR(Firstname,1,2)
        """)
    s.index(keys=["Block1"], table="invpat", db="db")
    s.index(keys=["Block1", "AppDate"], table="invpat", db="db")
    print "BLOCKIN DA BLOCKS!"

# SERIES of blocking (EXACT)
s.c.executescript("""
    CREATE TABLE grant_fuzzy AS
        SELECT  Award_ID, SDate, Award_Title, Invnum_N, "" AS FPatent, Lastname||", "||Firstname AS Name, Assignee
          FROM  db.invpat AS a
    INNER JOIN  grant_chem AS b
            ON  a.Block1 = b.initials and a.AppDate >= b.SDate
         WHERE  jPair(pi_name, Lastname, Firstname)>=0.90 AND jarow(organization_name, assignee)>0.90
      GROUP BY  Award_ID, Invnum_N;

    CREATE TABLE grant_exact AS
        SELECT  Award_ID, SDate, Award_Title, Invnum_N, FPatent, Lastname||", "||Firstname AS Name, Assignee
          FROM  db.invpat AS a
    INNER JOIN  grant_patent AS b
            ON  a.Patent = b.FPatent;            
    """)
s.index(["Invnum_N", "SDate"], table="grant_exact")
s.c.executescript("""
    CREATE TABLE grant_match_list
        (Award_ID, SDate, Award_Title, Invnum_N, Name, Assignee,
         Patent, AppDate, Class, UNIQUE(Invnum_N, Patent));

    REPLACE INTO grant_match_list  
        SELECT  b.Award_ID, b.SDate, b.Award_Title, b.Invnum_N, Lastname||", "||Firstname, a.Assignee, Patent, AppDate, idx(Class)
          FROM  db.invpat AS a
    INNER JOIN  grant_fuzzy AS b
            ON  a.Invnum_N = b.Invnum_N AND a.AppDate >= b.SDate;

    REPLACE INTO grant_match_list  
        SELECT  b.Award_ID, b.SDate, b.Award_Title, b.Invnum_N, Lastname||", "||Firstname, a.Assignee, Patent, AppDate, idx(Class)
          FROM  db.invpat AS a
    INNER JOIN  grant_exact AS b
            ON  a.Invnum_N = b.Invnum_N AND a.AppDate >= b.SDate;
    CREATE INDEX apat ON grant_match_list (Award_ID, Patent);
    """)

# Don't 100% TRUST my invnum_N results.. going to make it more conservative
#  (F)ULL and (S)ELECTED lists
s.c.executescript("""
    /* DON'T 100% TRUST RESULTS.. LET'S MAKE IT MORE CONSERVATIVE */
    CREATE TABLE name_flist AS
        SELECT  Invnum_N, Name, Assignee, count(*) AS Cnt
          FROM  grant_match_list
      GROUP BY  Invnum_N, Name;
    CREATE TABLE name_slist (Invnum_N, Name, Assignee, UNIQUE(Invnum_N, Name, Assignee));
    INSERT OR IGNORE INTO name_slist SELECT Invnum_N, Name, Assignee FROM grant_exact;
    INSERT OR IGNORE INTO name_slist SELECT Invnum_N, Name, Assignee FROM grant_fuzzy;
    """)

stopList = []
name_flist = s.fetch(table="name_flist")
name_slist = s.fetch(table="name_slist")

#generate dictionary for selected list
name_slistD = {}
for item in name_slist:
    if item[0] not in name_slistD:
        name_slistD[item[0]] = []
    name_slistD[item[0]].append(item[1:])
    
def nameCompare(item1, item2): #Lastname, Firstname
    #item1 is considered the "base"
    nm1 = item1[0].split(", ")[1].split(" ") #GET Names
    nm2 = item2[0].split(", ")[1].split(" ") #GET Names
    if len(nm1)>1 and len(nm2)>1: #Two middle names
        if nm1[-1][0]!=nm2[-1][0]: #If middle initials don't match
            return False
        elif len(nm1[-1])>1 and len(nm2[-1])>1: #Middle names are strings
            return (senAdd.jarow(nm1[-1], nm2[-1])>0.95)
        else:
            return True
    else:
        return (senAdd.jarow(item1[1], item2[1])>0.95)
        
for item in name_flist:
    for sel in name_slistD[item[0]]:
        if nameCompare(sel, item[1:])==False:
            stopList.append(item[1:3])

# DELETE items based on StopList
s.c.executemany("""
    DELETE FROM grant_match_list WHERE Name=? AND Assignee=?
    """, stopList)

s.c.execute("""
    CREATE TABLE grant_match_list2 AS
        SELECT  Award_ID, SDate, Award_Title,
                GROUP_CONCAT(Invnum_N, "|") AS Invnum_N,
                GROUP_CONCAT(Name, "|") AS Name, Patent, AppDate, Class
          FROM  grant_match_list
      GROUP BY  Award_ID, Patent
      ORDER BY  SDate, Award_ID, AppDate;
    """)

s.chgTbl("grant_match_list2")
s.add("source", "INT default 0")
s.c.executemany("""
    UPDATE grant_match_list2 SET source=1 WHERE Award_ID=? AND Patent=?
    """, s.fetch(field=["Award_ID", "FPatent"], table="grant_exact"))

s.chgTbl("name_slist")
s.csv_output("ron1.csv")
s.count()
s.chgTbl("grant_match_list2")

#GET and Add Patent Titles and Abstracts
s.attach("/home/ron/inputdata/Ron/fullset/patdesc.sqlite3")
data = s.c.execute("""
    SELECT  Title, a.Patent
      FROM  db.patdesc AS a
INNER JOIN  grant_match_list2 AS b
        ON  a.Patent = b.Patent
    """).fetchall()
s.add("Title")
s.c.executemany("UPDATE grant_match_list2 SET Title=? WHERE Patent=?", data)

#GET and Add Class Titles
s.attach("/home/ron/inputdata/Ron/fullset/USPTOcls.sqlite3")
s.add("ClassTitle")
s.c.executemany("""
    UPDATE grant_match_list2 SET ClassTitle=? WHERE Class=?
    """, s.fetch(field=["Title", "SS"], table="db.main"))

for x in s.fetch(limit=10):
    print x

s.csv_output("ron2.csv")
s.count()

#GET Cited Patents and Associated Assignees
s.attach("/home/ron/inputdata/Ron/fullset/citation.sqlite3")
s.c.executescript("""
    CREATE TABLE cits (Award_ID, SDate, Patent);
    INSERT INTO cits 
        SELECT  a.Award_ID, a.SDate, b.Patent
          FROM  grant_match_list2 AS a
    INNER JOIN  db.citation AS b
            ON  a.Patent = b.Citation;
    """)
s.chgTbl("cits")
s.index(["Patent"])
s.add("Assignee")
s.add("AsgNum")
s.attach("/home/ron/inputdata/Ron/fullset/invpat.Feb2011.sqlite3")
data = s.c.execute("""
    SELECT  b.Assignee, b.AsgNum, a.Patent
      FROM  cits AS a
INNER JOIN  db.invpat AS b
        ON  a.Patent = b.Patent
    """).fetchall()
s.c.executemany("UPDATE cits SET Assignee=?, AsgNum=? WHERE Patent=?", data)
s.csv_output("ron3.csv")
s.count()

m.close()
