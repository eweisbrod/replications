


****************************Trading Days Macro ***********************************;

/**********************************************************************************************/
/* FILENAME:        tddays.sas                                                    			  */
/* ORIGINAL AUTHOR: Eric Weisbrod						                                      */
/* MODIFIED BY:            													                  */
/* DATE CREATED:    Feb 22, 2021                                                              */
/* LAST MODIFIED:   Feb 22, 2021                                                              */
/* MACRO NAME:      tddays			                                                          */
/* ARGUMENTS:       1) DSETIN: input dataset containing an event date.					      */
/*                  2) DSETOUT: output dataset 									              */
/*                  3) BEGINWIN: first trading day relative to the event to collect 		  */
/*                  4) ENDWIN: last trading day relative to the event to collect              */
/*                  5) DATEVAR: variable name of the date in dsetin to use as the event date  */
/*                																			  */
/*                                        												      */
/* DESCRIPTION:     This macro uses a crsp trading date calendar to look up the event date and*/
/*					output a long (tidy) dataset of trading dates relative to the event date. */
/*					The output dataset can be used to then link returns, etc. The macro will  */
/*					give an error if the collection window includes dates that are beyond the */
/*					last available date of CRSP data available. 							  */
/* EXAMPLE(S):      1) %tddays(dsetin = mydata, dsetout = mydata2, datevar = rdq,			  */	
/*							beginwin=-1,endwin=1);									 	      */       
/*                      ==> Collect [-1,1] trading day windows around compustat quarterly 	  */
/*							earnings announcement date.	Helps to have permno already linked.  */
/**********************************************************************************************/

/* Code for how the trading date calendar is created*/

/*
proc sql;
create table dates as select distinct
date
from mycrsp.dsf
order by date;
quit;


data mycrsp.crspdates;
set dates;
n=_n_;
run;
*/



%macro tddays(dsetin=, dsetout=, datevar=, beginwin=0, endwin=0, calendarname=home.crspdates);

*enter a failsafe for out of range dates;
proc sql noprint;
select max(date)
  into :max_dt trimmed
  from &calendarname
;
quit;

data &dsetout (drop=rc n key);
format date YYMMDDN8.;
*instead of one hash we can do two hashes from the same crspdates set, 
one hash will be to match the eventdate, second hash will iterate through the event window;
declare hash nhash(dataset: "&calendarname", multidata:'no');
nhash.DefineKey("n");
nhash.DefineData("n","date");
nhash.DefineDone();
*see this hash is called datehash but uses same dataset;
declare hash datehash(dataset: "&calendarname", multidata:'no');
datehash.DefineKey("date");
datehash.DefineData("n","date");
datehash.DefineDone();

do until(eof);
	set &dsetin end = eof;
	format &datevar  YYMMDDN8.;
	key = 0;
	date = &datevar;
	td_days = %eval(&beginwin);
	rc=1;
	n_evtdate = .;
	n_days = .;

	*look up the event date;
	if not missing(&datevar) then do;
		do until(rc=0);
			rc=datehash.find();
			*if the date does not fall on a trading day,
			look at the next day, the loop will iterate until the date matches a trading day;
			if rc ^= 0 then do;
				date = INTNX("DAY",date,1);
				if date > &max_dt then do;
					put "Error: Date out of Range";
					stop;
				end;
			end;
			else do;
				n_evtdate = n;

			end;
		end;
	end;



	*now use the index of the event day in the trading calendar to find the begin and end days;
	n_days = ((n_evtdate+%eval(&endwin)) - (n_evtdate+%eval(&beginwin))) + 1;
	*if there is more than one day, output them all;
	if n_days > 1 then do;
		n = .;
		do key= (n_evtdate+%eval(&beginwin)) to (n_evtdate+%eval(&endwin)) by 1;

			n = .;
			n = key;
			date=.;
			rc=nhash.find();
			output;
			td_days +1;
		end;
	end;
	*If there is only one day, just look up that one;
	else do;
		key= (n_evtdate+%eval(&beginwin));
		n = key;
		date=.;
		rc=nhash.find();
		output;
	end;


end;
run;


%mend;


/******************************************************************************************/


**************************** Winsorize Macro ***********************************;

/**********************************************************************************************/
/* FILENAME:        Winsorize_Truncate.sas                                                    */
/* ORIGINAL AUTHOR: Steve Stubben (Stanford University)                                       */
/* MODIFIED BY:     Emmanuel De George and Atif Ellahie (LBS)			                                              */
/* DATE CREATED:    October 3, 2012                                                           */
/* LAST MODIFIED:   October 3, 2012                                                           */
/* MACRO NAME:      winsor			                                                          */
/* ARGUMENTS:       1) DSETIN: input dataset containing variables that will be win/trunc.     */
/*                  2) DSETOUT: output dataset (leave blank to overwrite DSETIN)              */
/*                  3) BYVAR: variable(s) used to form groups (leave blank for total sample)  */
/*                  4) VARS: variable(s) that will be winsorized/truncated                    */
/*                  5) TYPE: = W to winsorize and = T (or anything else) to truncate          */
/*                  6) PCTL = percentile points (in ascending order) to truncate/winsorize    */
/*                            values.  Default is 1st and 99th percentiles.                   */
/* DESCRIPTION:     This macro is capable of both truncating and winsorizing one or multiple  */
/*                  variables.  Truncated values are replaced with a missing observation      */
/*                  rather than deleting the observation.  This gives the user more control   */
/*                  over the resulting dataset.                                               */
/* EXAMPLE(S):      1) %winsor(dsetin = mydata, dsetout = mydata2, byvar = year,  			  */
/*                          vars = assets earnings, type = W, pctl = 0 98)                    */
/*                      ==> Winsorizes by year at 98% and puts resulting dataset into mydata2 */
/**********************************************************************************************/

%macro winsor	(dsetin = , 
			dsetout = , 
			byvar = none, 
			vars = , 
			type = W, 
			pctl = 1 99);
	%if &dsetout = %then
		%let dsetout = &dsetin;
	%let varL=;
	%let varH=;
	%let xn=1;

	%do %until (%scan(&vars,&xn)= );
		%let token = %scan(&vars,&xn);
		%let varL = &varL &token.L;
		%let varH = &varH &token.H;
		%let xn = %EVAL(&xn + 1);
	%end;

	%let xn = %eval(&xn-1);

	data xtemp;
		set &dsetin;
		%let dropvar =;

		%if &byvar = none %then
			%do;

	data xtemp;
		set xtemp;
		xbyvar = 1;
		%let byvar = xbyvar;
		%let dropvar = xbyvar;
			%end;

	proc sort data = xtemp;
		by &byvar;

		/*compute percentage cutoff values*/
	proc univariate data = xtemp noprint;
		by &byvar;
		var &vars;
		output out = xtemp_pctl PCTLPTS = &pctl PCTLPRE = &vars PCTLNAME = L H;

	data &dsetout;
		merge xtemp xtemp_pctl; /*merge percentage cutoff values into main dataset*/
		by &byvar;
		array trimvars{&xn} &vars;
		array trimvarl{&xn} &varL;
		array trimvarh{&xn} &varH;

		do xi = 1 to dim(trimvars);
			/*winsorize variables*/
			%if &type = W %then
				%do;
					if trimvars{xi} ne . then
						do;
							if (trimvars{xi} < trimvarl{xi}) then
								trimvars{xi} = trimvarl{xi};

							if (trimvars{xi} > trimvarh{xi}) then
								trimvars{xi} = trimvarh{xi};
						end;
				%end;

			/*truncate variables*/
			%else
				%do;
					if trimvars{xi} ne . then
						do;
							/*insert .T code if value is truncated*/
							if (trimvars{xi} < trimvarl{xi}) then
								trimvars{xi} = .T;

							if (trimvars{xi} > trimvarh{xi}) then
								trimvars{xi} = .T;
						end;
				%end;
		end;

		drop &varL &varH &dropvar xi;

		/*delete temporary datasets created during macro execution*/
	proc datasets library=work nolist;
		*delete xtemp xtemp_pctl;
	quit;

	run;

%mend winsor;

%put "GOOD JOB! YOU LOADED THE MACROS";

/******************************************************************************************/
