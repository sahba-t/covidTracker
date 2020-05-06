/*
University of New Mexico
Authors: Jered Dominguez-Trujillo and Sahba Tashakkori
Date: May 5, 2020
Description: SQL Script to Remove Tables and their Data - Clean Up Database
*/

begin
  for i in (select 'drop table '||table_name||' cascade constraints' tbl from user_tables) 
  loop
     execute immediate i.tbl;
  end loop;
end;