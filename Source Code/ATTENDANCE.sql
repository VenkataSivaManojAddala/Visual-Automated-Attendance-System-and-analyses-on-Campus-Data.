-- Active: 1692852423905@@127.0.0.1@3306@CMS

CREATE TABLE 21MAT301  (STUDENT_ID TEXT, STUDENT_NAME TEXT)

CREATE TABLE 21AIE301  (STUDENT_ID TEXT, STUDENT_NAME TEXT)

CREATE TABLE 21AIE302  (STUDENT_ID TEXT, STUDENT_NAME TEXT)

CREATE TABLE 21AIE303  (STUDENT_ID TEXT, STUDENT_NAME TEXT)

CREATE TABLE 21AIE304  (STUDENT_ID TEXT, STUDENT_NAME TEXT)

load data infile 'C://ProgramData//MySQL//MySQL Server 8.0//Uploads//Students.csv'
into table 21MAT301
fields terminated by ','
lines terminated by '\n' 
ignore 1 lines

load data infile 'C://ProgramData//MySQL//MySQL Server 8.0//Uploads//Students.csv'
into table 21AIE301
fields terminated by ','
lines terminated by '\n' 
ignore 1 lines

load data infile 'C://ProgramData//MySQL//MySQL Server 8.0//Uploads//Students.csv'
into table 21AIE302
fields terminated by ','
lines terminated by '\n' 
ignore 1 lines

load data infile 'C://ProgramData//MySQL//MySQL Server 8.0//Uploads//Students.csv'
into table 21AIE303
fields terminated by ','
lines terminated by '\n' 
ignore 1 lines

load data infile 'C://ProgramData//MySQL//MySQL Server 8.0//Uploads//Students.csv'
into table 21AIE304
fields terminated by ','
lines terminated by '\n' 
ignore 1 lines

-- DROP TABLE 21AIE301

-- DROP TABLE 21AIE302

-- DROP TABLE 21AIE303

-- DROP TABLE 21AIE304

-- DROP TABLE 21MAT301

-- SELECT * FROM 21AIE302



CREATE TABLE TIMETABLE (DAY TEXT, SLOT1 TEXT, SLOT2 TEXT, SLOT3 TEXT)

INSERT INTO TIMETABLE VALUES
("Monday", "21AIE304", "21MAT301", "21AIE303"),
("Tuesday", "21AIE302", "21AIE304", "21AIE301"),
("Wednesday", "21AIE303", "21AIE304", "21MAT301"),
("Thursday", "21MAT301", "21AIE302", "21AIE301"),
("Friday", "21AIE304", "21AIE302", "21MAT301");

DROP TABLE TIMETABLE