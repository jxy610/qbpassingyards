from datetime import date
from operator import index
import urllib3
import pandas as pd
import numpy as np
import sqlite3
import csv

#pd.set_option('display.max_columns',None)

conn = sqlite3.connect('nfl_db')
c = conn.cursor()

passingURL = 'https://www.pro-football-reference.com/years/2022/passing.htm#'
defenseURL = 'https://www.pro-football-reference.com/years/2022/opp.htm#passing'
passingDFList = pd.read_html(passingURL)
defenseDFList = pd.read_html(defenseURL)

teamAbbr = ['ARI','ATL','BAL','BUF','CAR','CHI','CIN','CLE','DAL','DEN','DET',
'GNB','HOU','IND','JAX','KAN','LVR','LAC','LAR','MIA','MIN','NWE','NOR','NYG',
'NYJ','PHI','PIT','SFO','SEA','TAM','TEN','WAS']

teamName = ['Arizona Cardinals','Atlanta Falcons','Baltimore Ravens',
'Buffalo Bills','Carolina Panthers','Chicago Bears','Cincinnati Bengals',
'Cleveland Browns','Dallas Cowboys','Denver Broncos','Detriot Lions',
'Green Bay Packers','Houston Texans','Indianapolis Colts','Jacksonville Jaguars',
'Kansas City Chiefs','Las Vegas Raiders','Los Angeles Chargers','Los Angeles Rams',
'Miami Dolphins','Minnesota Vikings','New England Patriots','New Oreleans Saints',
'New York Giants','New York Jets','Philadelphia Eagles','Pittsburgh Steelers',
'San Francisco 49ers','Seattle Seahawks','Tampa Bay Buccaneers','Tennessee Titans',
'Washington Commanders']

passingDF = passingDFList[0]
passingDF = passingDF[['Player','Tm','Pos','G','GS','Y/G']] # trim the bullshit
passingDF['Tm'] = passingDF['Tm'].replace(teamAbbr,teamName) # replace team abbr with full name
passingDF = passingDF.loc[passingDF['Pos'] == 'QB'] # keep only QBs
passingDF = passingDF.loc[passingDF['G'] == passingDF['GS']] # keep only QBs with games = games started
passingDF = passingDF.loc[passingDF['GS'] >= '14'] # keep only QBs with 4+ games played
passingDF['Player'] = passingDF['Player'].str.split(" ",n=1).str[-1]
#print(passingDF['Player'])
#passingDF['Player'] = nameSplit[1]
#leagueAvgYardsPerGame
numQB = 0
totalYards = 0
for i in passingDF.index:
    numQB += 1
    #print(f"{passingDF['Y/G'][i]}\n")
    tmp = (float)(passingDF['Y/G'][i])
    totalYards += tmp
    #print(tmp)
leagueAvgYardsPerGame = totalYards/numQB
#print(leagueAvgYardsPerGame)

# add qbMult to passingDF
qbMultArr = []
for i in passingDF.index:
    tmp = (float)(passingDF['Y/G'][i])
    qbMultArr.append(tmp/leagueAvgYardsPerGame)

passingDF['qbMult'] = qbMultArr
passingDF = passingDF.sort_values(by=['Tm'])
#print(passingDF)


#print(defenseDFList[0])

# defense stuff
defenseDF = defenseDFList[0]

# for col in defenseDF.columns:
#     print(col)
defenseDF = defenseDF[[('Unnamed: 1_level_0','Tm'),('Unnamed: 2_level_0','G'),('Passing','Yds')]] # grab team, games played and yards
defColRemap = {
    ('Unnamed: 1_level_0','Tm'): 'Tm',
    ('Unnamed: 2_level_0','G'): 'G',
    ('Passing','Yds'): 'Yds'
}
# make the column names not fucking retarded
defenseDF.rename(defColRemap,axis=1,inplace=True)
defenseDF = defenseDF.drop([32,33,34])
# for col in defenseDF.columns:
#     print(col)


defYardsPerGame = []
for team in defenseDF.index:
    tmpYards = (float)(defenseDF[('Passing','Yds')][team])
    tmpGP = (int)(defenseDF[('Unnamed: 2_level_0','G')][team])
    defYardsPerGame.append(tmpYards/tmpGP)

defenseDF['Yds/G'] = defYardsPerGame
defenseDF = defenseDF.sort_values(by=[('Unnamed: 1_level_0','Tm')])
# drop bottom average rows

df_result = passingDF

# drop tables
qbDrop = '''
DROP TABLE IF EXISTS qbTable
'''

teamDrop = '''
DROP TABLE IF EXISTS teamTable
'''
c.execute(qbDrop)
conn.commit()
c.execute(teamDrop)
conn.commit()

# create SQL database tables
qbTable = '''
CREATE TABLE IF NOT EXISTS QB (
    id INTEGER AUTO INCREMENT,
    Name VARCHAR(100) PRIMARY KEY NOT NULL,
    Tm VARCHAR(100) NOT NULL,
    qbMult DOUBLE NOT NULL
); '''

teamTable = '''
CREATE TABLE IF NOT EXISTS TEAM(
    id INTEGER AUTO INCREMENT,
    Name VARCHAR(100) PRIMARY KEY NOT NULL,
    AvgYards DOUBLE NOT NULL
);'''

c.execute(qbTable)
conn.commit()
c.execute(teamTable)
conn.commit()
#
passingDF.to_sql('qbTable',conn,if_exists='append',index=False)
defenseDF.to_sql('teamTable',conn,if_exists='append',index=False)

#remove bullshit
qbClean = '''
BEGIN TRANSACTION;
CREATE TEMPORARY TABLE qb_backup(Player,Tm,qbMult);
INSERT INTO qb_backup SELECT Player,Tm,qbMult FROM qbTable;
DROP TABLE qbTable;
CREATE TABLE qbTable(Player,Tm,qbMult);
INSERT INTO qbTable SELECT Player,Tm,qbMult FROM qb_backup;
DROP TABLE qb_backup;
COMMIT;
    '''

teamClean = '''
BEGIN TRANSACTION;
CREATE TABLE team_backup(Tm,AvgDefYds);
INSERT INTO team_backup SELECT "('Unnamed: 1_level_0', 'Tm')","('Yds/G', '')" FROM teamTable;
DROP TABLE teamTable;
CREATE TABLE teamTable(Tm,AvgDefYds);
INSERT INTO teamTable SELECT Tm,AvgDefYds FROM team_backup;
DROP TABLE team_backup;
COMMIT;
'''
c.executescript(qbClean)
c.executescript(teamClean)

tableJoin = '''
SELECT *
FROM qbTable
CROSS JOIN teamTable
WHERE qbTable.Tm != teamTable.Tm
'''
c.execute(tableJoin)

#c.execute('select * from teamTable')
#names = list(map(lambda x: x[0],c.description))
#print(names)
#for row in c.fetchall():
#    print(row)

#write big ass table to CSV
dfRes = pd.DataFrame(c.fetchall(), columns=['Player','Team','qbMult','Team','AvgDefYds'])
dfRes['EstYds'] = ''
dfRes['EstYds'] = (dfRes['qbMult'].astype(float)) * (dfRes['AvgDefYds'].astype(float))
print(dfRes)
dfRes.to_csv('results.csv',index=False)

#df_result['Def Yds'] = 

#df_result['Projected Yds'] = (((float)(df_result['qbMult.'])) * ((float)(df_result['Def Yds'])))
#print('qb')
#print(passingDF)
#print('defense')
#print(defenseDF)
#print('result')
#print(df_result)