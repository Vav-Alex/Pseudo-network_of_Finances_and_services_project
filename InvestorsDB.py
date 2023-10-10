import mysql.connector
'''This SQL script creates the database 'InvestorsDB' which stores the historical evaluation of every investors portfolio.
The first 6 tables store those evaluations (a row for every date), while the rest hold informations regarding the investors and
make the connection regarding which portfolio is managed by which investor.
'''
conn = mysql.connector.connect(user='[REDACTED]', password='[REDACTED]', host='[REDACTED]')

cursor = conn.cursor()

# create DB
script = """
DROP DATABASE IF EXISTS InvestorsDB;

CREATE DATABASE InvestorsDB;
USE InvestorsDB;

CREATE TABLE Inv1_P11 (
    C_Date date,
    Eval Float,
    Ch_Eval Float,
    Ch_Eval_per Float,
    PRIMARY KEY(C_Date)
);

CREATE TABLE Inv1_P12(
    C_Date date,
    Eval Float,
    Ch_Eval Float,
    Ch_Eval_per Float,
    PRIMARY KEY(C_Date)
);

CREATE TABLE Inv2_P21 (
    C_Date date,
    Eval Float,
    Ch_Eval Float,
    Ch_Eval_per Float,
    PRIMARY KEY(C_Date)
);

CREATE TABLE Inv2_P22 (
    C_Date date,
    Eval Float,
    Ch_Eval Float,
    Ch_Eval_per Float,
    PRIMARY KEY(C_Date)
);

CREATE TABLE Inv3_P31 (
    C_Date date,
    Eval Float,
    Ch_Eval Float,
    Ch_Eval_per Float,
    PRIMARY KEY(C_Date)
);

CREATE TABLE Inv3_P32 (
    C_Date date,
    Eval Float,
    Ch_Eval Float,
    Ch_Eval_per Float,
    PRIMARY KEY(C_Date)
);

CREATE TABLE Investors (
    Id     INT NOT NULL primary key,
    Name   CHAR(20),
    City   CHAR(20)
);

INSERT INTO Investors(Id, Name, City) VALUES (1, 'John', 'New York');
INSERT INTO Investors(Id, Name, City) VALUES (2, 'Adam', 'Boston');
INSERT INTO Investors(Id, Name, City) VALUES (3, 'James', 'Pittsburgh');

CREATE TABLE Portfolios (
    Id     Int NOT NULL primary key,
    Name   CHAR(20),
    Cumulative BOOLEAN
);

INSERT INTO Portfolios(Id, Name, Cumulative) VALUES (1, 'P11', TRUE);
INSERT INTO Portfolios(Id, Name, Cumulative) VALUES (2, 'P12', TRUE);
INSERT INTO Portfolios(Id, Name, Cumulative) VALUES (3, 'P21', TRUE);
INSERT INTO Portfolios(Id, Name, Cumulative) VALUES (4, 'P22', FALSE);
INSERT INTO Portfolios(Id, Name, Cumulative) VALUES (5, 'P31', FALSE);
INSERT INTO Portfolios(Id, Name, Cumulative) VALUES (6, 'P32', FALSE);

CREATE TABLE Investors_Portfolios (
    iid Int, FOREIGN KEY (iid) REFERENCES Investors(Id),
    pid Int, FOREIGN KEY (pid) REFERENCES Portfolios(Id),
    PRIMARY KEY(iid,pid)
);

INSERT INTO Investors_Portfolios(iid, pid) VALUES (1,1)
INSERT INTO Investors_Portfolios(iid, pid) VALUES (1,2)
INSERT INTO Investors_Portfolios(iid, pid) VALUES (2,1)
INSERT INTO Investors_Portfolios(iid, pid) VALUES (2,2)
INSERT INTO Investors_Portfolios(iid, pid) VALUES (3,1)
INSERT INTO Investors_Portfolios(iid, pid) VALUES (3,2)
"""

cursor.execute(script)
conn.close()