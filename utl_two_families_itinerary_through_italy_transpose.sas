Two families itinerary through Italy

   Same results in WPS and SAS

   TWO SOLUTIONS
       1. SQL  (made a slight adjustment for WPS does not support SQL 'eqt' yet?)
       2. HASH (same result in SAS and WPS)


see github
https://tinyurl.com/yalkalzf
https://github.com/rogerjdeangelis/utl_two_families_itinerary_through_italy_transpose

Sas Forum
https://tinyurl.com/yarq9w92
https://communities.sas.com/t5/General-SAS-Programming/How-do-I-do-merging-with-changing-variables/m-p/476274

Freelanace Reinhard profile
https://communities.sas.com/t5/user/viewprofilepage/user-id/32733

INPUT
=====

 Two Italian Families

 WORK.HAVE_A total obs=2

    ID        NAME

  123456    Montague
  134567    Capulet


 The families itinerary through italy

 WORK.HAVE_B total obs=6
                                  TRIP_
   ID      CITY        TRIP_ID    CITY

 123456    Rome         123450    Milan
 123450    Milan        123400    Pisa
 123400    Pisa         123000    Siena

 134567    Naples       134560    Verona
 134560    Verona       134500    Florence
 134500    Florence     134000    Citta

RULES
-----
 When a family arives at a city the last digit of thei ticket is set to zero.

 Consider Montague family

 123456 ** initial ticket
 123450 ** arrive Mian
 123400 ** arrive Pisa
 123000 ** arrive Siena


EXAMPLE OUTPUT HASH
-------------------

The Montague family travel from Milan to Pisa and finally Siena
The Capulet family travel from Verona to Florence and finally Citta

 WORK.WANT total obs=2

                                TRIP_     TRIP_       TRIP_
    ID       FAMILY     START      CITY     CITY1       CITY2    TRIP_ID    TRIP_ID1    TRIP_ID2

  123456    Montague    Rome      Milan     Pisa        Siena    123450      123400      123000
  134567    Capulet     Naples    Verona    Florence    Citta    134560      134500      134000

EXAMPLE OUTPUT SQL
===================

 INITIAL_                            SUBSEQUENT_
  TICKET      FAMILY     CITY          TICKET

  123456     Montague    Rome          123456
  123456     Montague    Milan         123450
  123456     Montague    Pisa          123400

  134567     Capulet     Naples        134567
  134567     Capulet     Verona        134560
  134567     Capulet     Florence      134500


PROCESS
=======

1. SQL (The union remove duplicates)

   proc sql;
     create
          table want as
     select
          l.id    as initial_ticket
         ,l.family
         ,r.city
         ,r.id as subsequent_ticket
     from
         have_a as l left join
         (
          select
               id
              ,city
          from
              have_b
          union
          select
               trip_id
              ,trip_city
          from
              have_b
         ) as r
     on
        substr(l.id,1,3) eqt r.id
     order
         by subsequent_ticket descending
   ;quit;

2. HASH

   /* Perform look-ups */

   data want;
   retain id family start trip_city trip_city1 trip_city2;
   if _n_=1 then do;
     dcl hash h(dataset: 'have_b');
     h.definekey('id');
     h.definedata('trip_id', 'trip_city');
     h.definedone();
     if 0 then set have_b;
   end;
   array pid $6   trip_id1-trip_id2;
   array pn $40 trip_city1-trip_city2;
   set have_a;
   call missing(of trip:);
   rc=h.find();
   p_id=trip_id;
   p_city=trip_city;
   do i=1 to dim(pn) while(h.find(key: trip_id)=0);
     pid[i]=trip_id;
     pn[i]=trip_city;
   end;
   trip_id=p_id;
   trip_city=p_city;
   drop rc i p_: city;
   run;

*                _               _       _
 _ __ ___   __ _| | _____     __| | __ _| |_ __ _
| '_ ` _ \ / _` | |/ / _ \   / _` |/ _` | __/ _` |
| | | | | | (_| |   <  __/  | (_| | (_| | || (_| |
|_| |_| |_|\__,_|_|\_\___|   \__,_|\__,_|\__\__,_|

;

data have_a;
input id$ family$ start$;
cards4;
123456 Montague Rome
134567 Capulet Naples
;;;;
run;quit;

data have_b;

input id$ city$ trip_id$ trip_city$;
cards4;
123456 Rome 123450 Milan
123450 Milan 123400 Pisa
123400 Pisa 123000 Siena
134567 Naples 134560 Verona
134560 Verona 134500 Florence
134500 Florence 134000 Citta
;;;;
run;quit;


*          _       _   _
 ___  ___ | |_   _| |_(_) ___  _ __
/ __|/ _ \| | | | | __| |/ _ \| '_ \
\__ \ (_) | | |_| | |_| | (_) | | | |
|___/\___/|_|\__,_|\__|_|\___/|_| |_|

;

SAS see process

%utl_submit_wps64('
libname wrk sas7bdat "%sysfunc(pathname(work))";
proc sql;
  create
       table wrk.wantwps as
  select
       l.id    as initial_ticket
      ,l.family
      ,r.city
      ,r.id as subsequent_ticket
  from
      wrk.have_a as l left join
      (
       select
            id
           ,city
       from
           wrk.have_b
       union
       select
            trip_id
           ,trip_city
       from
           wrk.have_b
      ) as r
  on
     substr(l.id,1,3) eq substr(r.id,1,3)
  order
      by subsequent_ticket descending
;quit;
');


proc print data=wantwps;
run;quit;


%utl_submit_wps64('
libname wrk sas7bdat "%sysfunc(pathname(work))";
data wrk.wanthash;
retain id family start trip_city trip_city1 trip_city2;
if _n_=1 then do;
  dcl hash h(dataset: "wrk.have_b");
  h.definekey("id");
  h.definedata("trip_id", "trip_city");
  h.definedone();
  if 0 then set wrk.have_b;
end;
array pid $6   trip_id1-trip_id2;
array pn $40 trip_city1-trip_city2;
set wrk.have_a;
call missing(of trip:);
rc=h.find();
p_id=trip_id;
p_city=trip_city;
do i=1 to dim(pn) while(h.find(key: trip_id)=0);
  pid[i]=trip_id;
  pn[i]=trip_city;
end;
trip_id=p_id;
trip_city=p_city;
drop rc i p_: city;
run;
');

proc print data=wrk.wanthash;
run;quit;

