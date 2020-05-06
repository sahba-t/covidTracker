/*
University of New Mexico
Authors: Jered Dominguez-Trujillo and Sahba Tashakkori
Date: May 5, 2020
Description: SQL Script that Creates our COVID Database SCHEMA
*/

CREATE TABLE STATE (
  StateAbbreviation VARCHAR2(2),
  StateName VARCHAR(50) NOT NULL,
  PRIMARY KEY(StateAbbreviation)
  );


CREATE TABLE JHUDATA (
  DateRecorded DATE NOT NULL,
  Country VARCHAR(20) CHECK(Country IN 'US') NOT NULL,
  State VARCHAR(20),
  City VARCHAR(50) NOT NULL,
  Confirmed NUMBER,
  Deaths NUMBER,
  Recovered NUMBER,
  Active NUMBER,
  Latitude NUMBER,
  Longitude NUMBER,
  PRIMARY KEY(DateRecorded, Country, State, City),
  FOREIGN KEY(State) REFERENCES STATE(StateAbbreviation)
  );

CREATE TABLE USCASEBYSTATE (
  DateRecorded DATE,
  State VARCHAR(20),
  Positive NUMBER NOT NULL,
  Negative NUMBER NOT NULL,
  Death NUMBER NOT NULL,
  Total NUMBER NOT NULL,
  DataQualityGrade VARCHAR(2) CHECK(DataQualityGrade IN ('F', 'D-', 'D', 'D+', 'C-', 'C', 'C+', 'B-', 'B', 'B+', 'A-', 'A', 'A+')),
  DataGrade VARCHAR(2) CHECK(DataGrade IN ('F', 'D-', 'D', 'D+', 'C-', 'C', 'C+', 'B-', 'B', 'B+', 'A-', 'A', 'A+')),
  PRIMARY KEY(DateRecorded, State),
  FOREIGN KEY(State) REFERENCES STATE(StateAbbreviation)
  );


CREATE TABLE HOSPITALIZED (
  DateRecorded DATE,
  State VARCHAR(20),
  HospitalizedCurrently NUMBER,
  HospitalizedCumulative NUMBER,
  PRIMARY KEY(DateRecorded, State),
  FOREIGN KEY(DateRecorded, State) REFERENCES USCASEBYSTATE(DateRecorded, State)
  );

CREATE TABLE ICU (
  DateRecorded DATE,
  State VARCHAR(20),
  InICUCurrently NUMBER,
  InICUCUmulative NUMBER,
  PRIMARY KEY(DateRecorded, State),
  FOREIGN KEY(DateRecorded, State) REFERENCES USCASEBYSTATE(DateRecorded, State)
  );

CREATE TABLE VENTILATOR (
  DateRecorded DATE,
  State VARCHAR(20),
  OnVentilatorCurrently NUMBER,
  OnVentilatorCUmulative NUMBER,
  PRIMARY KEY(DateRecorded, State),
  FOREIGN KEY(DateRecorded, State) REFERENCES USCASEBYSTATE(DateRecorded, State)
  );

CREATE TABLE RECOVERED (
  DateRecorded DATE,
  State VARCHAR(20),
  Recovered NUMBER,
  PRIMARY KEY(DateRecorded, State),
  FOREIGN KEY(DateRecorded, State) REFERENCES USCASEBYSTATE(DateRecorded, State)
  );


CREATE TABLE USFACILITIES (
  DateRecorded DATE,
  State VARCHAR(20),
  CountyName VARCHAR(50),
  Population Number(10, 0),
  Population20Plus NUMBER(10, 0),
  Population65Plus NUMBER(10, 0),
  StaffedAllBeds NUMBER(10, 0),
  StaffedICUBeds NUMBER(10, 0),
  LicensedAllBeds NUMBER(10, 0),
  PRIMARY KEY(DateRecorded, State, CountyName),
  FOREIGN KEY(State) REFERENCES STATE(StateAbbreviation)
  );

CREATE TABLE USALLBEDOCCUPANCY (
  DateRecorded DATE,
  State VARCHAR(20),
  CountyName VARCHAR(50),
  AllBedOccupancyRate NUMBER(4, 3),
  PRIMARY KEY(DateRecorded, State, CountyName),
  FOREIGN KEY(DateRecorded, State, CountyName) REFERENCES USFACILITIES(DateRecorded, State, CountyName)
  );


CREATE TABLE USICUBEDOCCUPANCY (
  DateRecorded DATE,
  State VARCHAR(20),
  CountyName VARCHAR(50),
  ICUBedOccupancyRate NUMBER(4, 3),
  PRIMARY KEY(DateRecorded, State, CountyName),
  FOREIGN KEY(DateRecorded, State, CountyName) REFERENCES USFACILITIES(DateRecorded, State, CountyName)
  );

CREATE TABLE USTESTS (
  DateRecorded DATE,
  CDCLabs NUMBER,
  USPublicHealthLabs NUMBER,
  PRIMARY KEY(DateRecorded)
  );

CREATE TABLE USMEDAIDAGGREGATE (
  DateRecorded DATE,
  State VARCHAR(20),
  Deliveries NUMBER(6, 0),
  Cost NUMBER(10, 2),
  Weight NUMBER(8, 2),
  PRIMARY KEY(DateRecorded, State),
  FOREIGN KEY(State) REFERENCES STATE(StateAbbreviation)
  );