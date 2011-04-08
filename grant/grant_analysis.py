import sys, datetime
sys.path.append("/home/ron/PythonBase")
import SQLite, MySQL, senAdd

cfg = SQLite.MySQL_cfg({'host':'localhost', 'db':'RD'})

m = MySQL.MySQL(cfg=cfg, table="nsf")
m.fetch(field=["source_year", "pi_name", "Award_ID", "Award_Title", "Organization_Name"], limit=10, fetch=False, random=True)

mode = raw_input("Mode -(R)egular or (G)oofy?, default=(R): ")
dx = (mode.upper() in ("", "R") and ["AND a.AppDate >= b.SDate", "DELETE FROM grant_match_full WHERE AppDate < SDate;"] or ["", ""])


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

s = SQLite.SQLite("grant_analysis_{mode}.s3".format(mode=mode.upper()), table="grant_chem")  
s.conn.create_function("initials2", 1, initials2)
s.conn.create_function("jPair", 3, jaroPair)
s.conn.create_function("jarow", 2, senAdd.jarow)
s.conn.create_function("idx", 1, idx)
s.conn.create_function("dV", 1, senAdd.dateVert)
s.conn.create_function("p8", 1, senAdd.pat8char)
if not s.tables(lookup="grant_chem"):
    s = m.sqlite_output(table="grant_chem", sLite=s)
    s.add("initials")
    s.c.execute("UPDATE grant_chem SET initials=initials2(pi_name)")
print s.fetch(field=["pi_name", "initials"], table="grant_chem", limit=5)
s.count()

if not s.tables(lookup="grant_patent"):
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
s.attach("/home/ron/inputdata/Ron/fullset/invpatC.upper.Jan2011.sqlite3")
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
    CREATE TABLE IF NOT EXISTS grant_fuzzy AS
        SELECT  Award_ID, SDate, Award_Title, Invnum_N, "" AS FPatent
          FROM  db.invpat AS a
    INNER JOIN  grant_chem AS b
            ON  a.Block1 = b.initials and a.AppDate >= b.SDate
         WHERE  jPair(pi_name, Lastname, Firstname)>=0.90 AND jarow(organization_name, assignee)>0.90
      GROUP BY  Award_ID, Invnum_N;

    CREATE TABLE IF NOT EXISTS grant_exact AS
        SELECT  Award_ID, SDate, Award_Title, Invnum_N, FPatent
          FROM  db.invpat AS a
    INNER JOIN  grant_patent AS b
            ON  a.Patent = b.FPatent;            
    """)
s.index(["Invnum_N", "SDate"], table="grant_exact")


if not s.tables(lookup="grant_match_list"):
    #SOURCES, 1=exact, 2=related PI, 3=citation
    s.c.executescript("""
        CREATE TABLE grant_match_list
            (Award_ID, SDate, Award_Title, Patent, source, UNIQUE(Patent));
        CREATE INDEX apat ON grant_match_list (Award_ID, Patent);

        CREATE TABLE grant_match_full
            (Award_ID, SDate, Award_Title, Patent, source, Invnum_N, UNIQUE(Patent, Invnum_N));

        /* Directly Cited */
        INSERT OR IGNORE INTO grant_match_list  
            SELECT  Award_ID, SDate, Award_Title, FPatent, 1
              FROM  grant_exact;

        /* Funded PI */
        INSERT OR IGNORE INTO grant_match_list  
            SELECT  b.Award_ID, b.SDate, b.Award_Title, a.Patent, 2
              FROM  db.invpat AS a
        INNER JOIN  grant_exact AS b
                ON  a.Invnum_N = b.Invnum_N {sign};

            /** FUZZY **/
        INSERT OR IGNORE INTO grant_match_list  
            SELECT  b.Award_ID, b.SDate, b.Award_Title, a.Patent, 2
              FROM  db.invpat AS a
        INNER JOIN  grant_fuzzy AS b
                ON  a.Invnum_N = b.Invnum_N {sign};

        /*SELECT  b.Award_ID, b.SDate, b.Award_Title, b.Invnum_N, Lastname||", "||Firstname, a.Assignee, a.AsgNum, Patent, AppDate, idx(Class)*/
        """.format(sign=dx[0]))

    #Add Citation Data
    print "Citation", datetime.datetime.now()
    s.attach("/home/ron/inputdata/Ron/fullset/citation.sqlite3")
    s.c.execute("""
        INSERT OR IGNORE INTO grant_match_list
            SELECT  b.Award_ID, b.SDate, b.Award_Title, a.Patent, 3
              FROM  db.citation AS a
        INNER JOIN  grant_match_list AS b
                ON  a.Citation = b.Patent
        """)
    s.index(["Patent"], table="grant_match_list")
    s.index(["Patent"], table="grant_match_full")

    #FLESH OUT DATA, ADD MORE ATTRIBUTE DATA
    print "Attribute", datetime.datetime.now()
    s.attach("/home/ron/inputdata/Ron/fullset/invpatC.upper.Jan2011.sqlite3")
    s.add(["Name", "Loc", "Assignee", "AsgNum", "AppDate", "Class"], table="grant_match_full")
    s.c.executescript("""
        INSERT OR IGNORE INTO grant_match_full
            SELECT  a.*, b.Invnum_N, b.Lastname||", "||b.Firstname, b.City||" "||b.State||" "||b.Country, b.Assignee, b.AsgNum, b.AppDate, idx(b.Class)
              FROM  grant_match_list AS a
        INNER JOIN  db.invpat AS b
                ON  a.Patent = b.Patent;
        {sign}
        """.format(sign=dx[1]))

    #GET and Add Patent Titles and Abstracts
    print "Descriptives", datetime.datetime.now()
    s.add(["Title", "ClassTitle"], table="grant_match_full")
    s.attach("/home/ron/inputdata/Ron/fullset/patdesc.sqlite3")
    if "Title" not in s.columns(output=False):
        data = s.c.execute("""
            SELECT  a.Title, a.Patent
              FROM  db.patdesc AS a
        INNER JOIN  grant_match_list AS b
                ON  a.Patent = b.Patent
            """).fetchall()
        s.add("Title")
        s.c.executemany("UPDATE grant_match_full SET Title=trim(?) WHERE Patent=?", data)

    #GET and Add Class Titles
    if "ClassTitle" not in s.columns(output=False):
        s.attach("/home/ron/inputdata/Ron/fullset/USPTOcls.sqlite3")
        s.add("ClassTitle")
        s.c.executemany("UPDATE grant_match_full SET ClassTitle=trim(?) WHERE Class=?", s.fetch(field=["Title", "SS"], table="db.main"))

    s.chgTbl("grant_match_full")
    s.csv_output("grant_analysis_invpat.csv")
    s.count()

if not s.tables(lookup="grant_match_sum"):
    s.c.execute("""
        CREATE TABLE IF NOT EXISTS grant_match_sum AS
            SELECT  Award_ID, SDate, Award_Title, count(distinct Invnum_N) AS Inv,
                    Patent, min(source) AS Source, Assignee, AsgNum, AppDate, Class, Title, ClassTitle
              FROM  grant_match_full
          GROUP BY  Award_ID, Patent
          ORDER BY  SDate, Award_ID, AppDate;
        """)
    s.chgTbl("grant_match_sum")
    s.csv_output("grant_analysis_sum.csv")
    s.count()



###-- EXPERIMENTAL --#
##
###Draw networks by Specific Years
##import senGraph, math, igraph
##s.c.execute("""
##    SELECT  substr(SDate,1,4) AS year, invnum_N, count(*) as cnt
##      FROM  grant_match_list
##     WHERE  year = "2000"
##  GROUP BY  year, invnum_N
##    """)
##g = senGraph.senDBSQL()
##Invnum = [x[1] for x in s.c.fetchall()]
##g.graph(vertex_list=g.nhood(Invnum), flag=Invnum,
##        where="AppYearStr>='2000' AND AppYearStr<='2003'")
##sG = senGraph.senGraph(g.tab)
##sG.g.vs["size"] = [math.log(x)*3+4 for x in sG.g.vs["cnt"]]
##sG.vs_color("AsgNum", dbl=True)
##sG.g.vs["layout"] = sG.g.layout("grid_fr")
##
##igraph.plot(sG.g, "ga_2000.png", layout=sG.g.vs["layout"], vertex_label="")
##sG.json(output="ga_2000.json", vBool=["flag"])
##
###------------------#



s.close()
m.close()
