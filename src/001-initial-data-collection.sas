
/*********************************************
SETUP
**********************************************/

*macro variable for the filepath to this code;
*this is useful for creating relative file paths;
%let codepath = %qsysfunc(sysget(SAS_EXECFILEPATH));

%put &codepath;


*using the codepath, we can make a path to the MACROS file;
%let macrofile = &codepath\..\MACROS.sas;

*include the code file for the macros; 
%include "&macrofile";


*library for my dropbox folder;
*MODIFY THIS FOR YOUR DROPBOX FOLDER!;
libname dbox "D:\Dropbox\ACCT 932\data";


/*********************************************
Upload the local macro script to WRDS
**********************************************/

*I often download all of the raw data to my own
machine and work on it locally. In this example,
we will do most of the work remotely on the 
WRDS server in case some students have 
laptops with limited resources;

*Sign on to WRDS;
%let wrds =  wrds-cloud.wharton.upenn.edu 4016;
options comamid=TCP remote=WRDS;
signon username=_prompt_;

*For teaching, you have a remote library on WRDS called home;
*this block of code will show you the directory it refers to;
*you have a little bit of space there but not a huge amount of space;
rsubmit;
%let libpath = %sysfunc(pathname(home)); 
%put &libpath;
endrsubmit;

*If you work with larger files remotely, you should put them in
*KU's temporary scratch space on WRDS;
*This block will set up a library to refer to the scratch space
*and then for fun, print out the path of the library again as above;
rsubmit;
libname ku "/scratch/ukansas";
%let libpath = %sysfunc(pathname(ku)); 
%put &libpath;
endrsubmit;

*now lets upload our local macro file up to WRDS so we can use the macros there;
rsubmit; 
proc upload infile="C:\_git\replications\src\MACROS.sas" outfile="/home/ukansas/eweisbro/MACROS.sas"; run;
endrsubmit;



/***********************************************************
Step 1: Collect a sample of earnings surprises from I/B/E/S
************************************************************/

*I am going to start with the IBES surprise file for a few reasons:

1. It is straightforward. 
2. This is the surprise that IBES users actually "see" on their screens or read in a headline.
3. We covered Compustat a bit in my other example and I assume you get more exposure to that, so I want to expose you to IBES.;

*However, there are some downsides to the surprise file that I can discuss in class. 

*start with quarterly surprises from the unadjusted surprise summary;
rsubmit;
proc sql;
create table data1 as select distinct
ticker as ibes_ticker, oftic,
pyear,pmon,anndats,surpmean,actual,surpstdev, suescore,
/* later for sorting, etc., I want the yearqtr of announcement */
intnx('month',mdy(qtr(anndats)*3,1,year(anndats)),0,'e') as yearqtr format date9.,
/* go ahead and calculate UE */
case when abs(actual) < .25 then .25 else abs(actual) end as denominator,
(actual - surpmean) / (calculated denominator) as ue

from ibes.surpsumu
where measure="EPS" and fiscalp="QTR" 
/* two equivalent ways to write not missing */
and missing(surpmean)=0 and not missing(actual) 
and usfirm=1;
quit;
endrsubmit;
*this data is still on WRDS 

*if we want to take a look at it;
libname rwork slibref=work server=wrds;

*check duplicates;
rsubmit;
proc sql;
create table checkdups as select distinct
ibes_ticker,pyear,pmon, count(surpmean) as n
from data1
group by ibes_ticker,pyear,pmon
order by n desc;
quit;
endrsubmit;
*no dups;

*check another way;
rsubmit;
proc sql;
create table checkdups as select distinct
ibes_ticker,anndats, count(surpmean) as n
from data1
group by ibes_ticker,anndats
order by n desc;
quit;
endrsubmit;
*some fiscal periods seem to be announced on same day;
*let's throw all of these out because they are either errors or confounded;

*note: be careful about this step because i re-use "checkdups" as a dataset name
so you have to be careful that checkdups is the right version of that table when you run this step
this is a little bit sloppy of me but it should be ok;
rsubmit;
proc sql;
create table data2 as select distinct
a.* 
from data1 a, checkdups b
where a.ibes_ticker=b.ibes_ticker and
a.anndats=b.anndats and b.n=1;
quit;

*check again;
proc sql;
create table checkdups as select distinct
ibes_ticker,anndats, count(surpmean) as n
from data2 /*checking data2 now */
group by ibes_ticker,anndats
order by n desc;
quit;
*no dups now;
endrsubmit;

/***********************************************************
Step 2: Link I/B/E/S to CRSP
************************************************************/

*To link IBES and CRSP, we can use iclink;


*run this once to create the link file in your home folder;
*then you don't need to run it again in the future;
rsubmit;
%Include '/wrds/ibes/samples/iclink.sas';
endrsubmit;


*This inevitably leads to duplicates;
*some decisions that I make here are to deal with the duplicates;
*some researchers are more careful about this than others, but I encourage you to be thoughtful about it;


*use the WRDS ICLINK table to link IBES ticker to CRSP Permno;
*Score of 0 is the best matches...I also allow 1 usually;
rsubmit;
proc sql;
create table data3 as select distinct
a.*, b.permno, b.comnam, b.score
from data2 as a, home.iclink as b
where a.ibes_ticker=b.ticker and  b.score in (0,1);
quit;
endrsubmit;
*lots of dups!!;


*Filter to only HEXCDS 1,2,3,4 and SHRCD 10,11;
*HEXCD means the stock exchange header code. 1-4 are the main exchanges, NYSE, NASDAQ, Arca, etc.;
*SHRCD are share codes for the type of stock, 10 and 11 are common stock;
*these are common CRSP filters;
rsubmit;
data stocknames; set crsp.stocknames;
where hexcd in (1,2,3,4) and shrcd in (10,11);
run;
endrsubmit;

*merge to crsp stocknames file using permno and namedt / nameenddt;
rsubmit;
proc sql;
create table data4 as select distinct
a.*, b.ticker as crsp_ticker, b.comnam as crsp_comnam, b.hexcd, b.siccd, b.shrcd, (a.OFTIC=b.ticker) as tickmatch, a.comnam=b.comnam as namematch
from data3 as a, stocknames as b
where a.permno=b.permno and (b.namedt le a.anndats le b.nameenddt)
/* require IBES and CRSP historical tickers to match to reduce duplicates */
and a.oftic=b.ticker;
quit;
endrsubmit;

*check for dups;
rsubmit;
proc sql;
create table checkdups as select distinct
ibes_ticker,anndats,count(permno) as n
from data4
group by ibes_ticker, anndats
order by n desc;
quit;
endrsubmit;
*now we only have a few hundred dups;
*better;

*I am going to leave some dups for now to see if they have valid stock return data;
*the presence of stock price data might help me figure out which matches are good vs bad;

*Nichols and Wahlen filter on price > $1 and MVE >$50 million, so let's do that next;

/***********************************************************
Step 3: Link to Price data
************************************************************/


*Looks like maybe Nichols and Wahlen use prices as of the beginning of the fiscal year;
*this is a fairly arbitrary decision, maybe they took it from lagged compustat annual data?
*I don't know if we will get to Compustat data so let me quickly take price and MVE from crsp
*we will start the graph at day -5, so let me take price from day -6;

*a very important issue in stock market event studies is that trading days are not the same as calendar days;

*For example, if an announcement happens on a monday, we do not want day -1 to be sunday when the market was closed
we must look back to the previous friday, or for example if friday was a holiday it could be the previous Thursday;

*My favorite way to do this is to just create a trading calendar of all the days in crsp and use it to look up dates;

rsubmit;
proc sql;
create table dates as select distinct
date
from crsp.dsf
order by date;
quit;


data home.crspdates;
set dates;
n=_n_;
run;
endrsubmit;

*if we want to take a look at it;
libname rhome slibref=home server=wrds;


*I have written a macro to look up trading windows in this calendar,
let's use it;

rsubmit;
*include the macro file we uploaded earlier;
*only need to do this once per login session;
%Include "/home/ukansas/eweisbro/MACROS.sas";
endrsubmit;

*apply the macro;
rsubmit;
%tddays(dsetin=data4 (keep = permno anndats), 
		dsetout=temp1, 
		datevar=anndats,
		beginwin=-6,
		endwin=-6,
		calendarname = home.crspdates);
endrsubmit;

*next we should merge temp1 with the daily stock file to get price and mve;
*can go ahead and apply filters here;
rsubmit;
proc sql;
create table temp2 as select distinct
a.*, abs(b.prc) as price, (abs(b.prc)*b.shrout)/1000 as MVE 
/* CRSP provides shrout in thousands but Compustat data is in millions so it is helpful to rescale */
from temp1 a, crsp.dsf b
where a.permno=b.permno and a.date=b.date
and abs(b.prc) > 1
and ((abs(b.prc)*b.shrout)/1000) > 50;
quit;
endrsubmit; 

*merge the prices back to the main dataset;
rsubmit;
proc sql;
create table data5 as select distinct
a.*, b.price, b.MVE 
from data4 a, temp2 b
where a.permno=b.permno and a.anndats=b.anndats;
quit;

*check for dups again;
proc sql;
create table checkdups as select distinct
ibes_ticker,anndats,count(permno) as n
from data5
group by ibes_ticker, anndats
order by n desc;
quit;
endrsubmit; 

*alright lets show an example of filtering out dups based on some criteria;
*this is a SEMI thoughtful way to do it. 
* I am not too concerned because at the end of the day we are talking about less that 1% of the sample here;
* I am going to keep a company name match as the best match, followed by the match with the highest MVE;
rsubmit;
proc sort data = data5; by ibes_ticker anndats descending tickmatch descending MVE; run;
proc sort data = data5 out = data6 nodupkey; by ibes_ticker anndats; run;

*I don't want duplicates by permno either;
proc sort data = data6; by permno anndats descending tickmatch descending MVE; run;
proc sort data = data6 out = data7 nodupkey; by permno anndats; run;

*check for dups again;
proc sql;
create table checkdups as select distinct
ibes_ticker,anndats,count(permno) as n
from data7
group by ibes_ticker, anndats
order by n desc;
quit;

proc sql;
create table checkdups as select distinct
permno,anndats,count(permno) as n
from data7
group by permno, anndats
order by n desc;
quit;
endrsubmit;
*ok finally no dups;


/***********************************************************
Sort the earnings announcements into UE deciles
************************************************************/

rsubmit;
proc sort data = data7; by yearqtr; run;
proc rank data = data7 out = data8 groups = 10;
by yearqtr;
var ue;
ranks dec_ue;
run;
endrsubmit;


rsubmit;
proc freq data = data8;
tables yearqtr;
run;
endrsubmit;
*lets cut the sample period to 1993 - 2021;


/***********************************************************
Compute the size-adjusted buy and hold abnormal returns or BHAR

SEE LYON AND BARBER 1997 SECTION 2 
(seriously, please read it)
************************************************************/

*Steps for calculating size adjusted abnormal returns:
1. Match the announcement to its annual size decile
2. Pull the returns to the appropriate size index
3. Pull the firm-specific returns
4. Fill in missing firm returns with the size index return
5. Cumulate each return series
6. Subtract the size index return from the firm return




*Create a list of which permno belongs to which size decile each year;
rsubmit;
proc sql;
create table sizeyears as select distinct
permno, year(date) as year, capn
from crsp.ermport1;
quit;
endrsubmit;

*create a file of daily returns for each size decile;
rsubmit;
proc sql;
create table sizerets as select distinct
date, capn, decret
from crsp.erdport1;
quit;
endrsubmit;

*match the permno to a size decile;
rsubmit;
proc sql;
create table temp1 as select distinct
a.permno, a.anndats, a.dec_ue, b.capn
from data8 (where=(1993 <= year(yearqtr) <= 2021)) as a, sizeyears b
where a.permno=b.permno and year(a.anndats)=b.year;
quit;
endrsubmit;

*apply the tddays macro to get the trading dates we need for the figure;
rsubmit;
%tddays(dsetin=temp1, 
		dsetout=temp2, 
		datevar=anndats,
		beginwin=-5,
		endwin=5,
		calendarname = home.crspdates);
endrsubmit;

*pull the decile returns for each day;
*doing this before firm-level returns ensures full data; 
rsubmit;
proc sql;
create table temp3 as select distinct
a.*,b.decret
from temp2 a, sizerets b
where a.capn=b.capn and
a.date=b.date;
quit;
endrsubmit;

*Now link daily raw return data to the dates in the event window;
*note that if delisting returns are quite important in your study
it may be worthwhile to create a custom dailyreturns file 
that uses custom delisting returns rather than taking the default
crsp daily stock file, feel free to reach out with questions; 
rsubmit;
proc sql;
create table temp4 as select distinct
a.*, b.ret
from temp3 as a left join crsp.dsf as b
on a.permno=b.permno and a.date=b.date
order by permno, anndats,date;
quit;
endrsubmit;


*cumulate the returns as of each day, etc;
rsubmit;
data figdata;
set temp4;
by permno anndats;

*these statements allow for cumulating over a data step;
retain cum_logret;
retain cum_logidx;
retain zeroflag;

*these are placeholders used to check that our lags
are for the same firm-year observation;
lag_permno = lag(permno);
lag_evtdate = lag(anndats);

*assign the firm-specific return to a placeholder variable;
*this allows us to fill in the placeholder, ret1, with the index
*when the firm-specific return is missing;
*this procedure implies that a) on any day with missing data, the abnormal return
*relative to the index was zero, and b) any remaining proceeds after a delisting
*are reinvested in the index;
ret1=ret;

*fill with index return when firm-specific return is missing;
if missing(ret) then ret1 = decret;

*if the first day in the window, start a new cumulation;
if first.anndats then do;
	cum_logret = log(1+ret1);
	cum_logidx = log(1+decret);
	cumret = ret1;
	cumidx = decret;
	*track -100% returns;
	if ret = -1 then zeroflag=1; else zeroflag=0;
end;
*otherwise, check the lags and if they are valid, add to the existing cumulation;
else if (permno=lag_permno and anndats=lag_evtdate) then do;

	*track -100% returns;
	if ret = -1 then zeroflag=1; 

	*if the firm had a -100% return, only cumulate the index return;
	if zeroflag=1 then do;
		cum_logret = log(0);
		cumret = -1;
		*sum the logged returns (price relatives);
		cum_logidx = cum_logidx + log(1+decret);
		*exponentiate the sum of the logged returns, then subtract 1;
		cumidx = exp(cum_logidx)-1;
	end;
	*otherwise cumulate both;
	else do;
		cum_logret = cum_logret + log(1+ret1);
		cumret = exp(cum_logret)-1;
		cum_logidx = cum_logidx + log(1+decret);
		cumidx = exp(cum_logidx)-1;
	end;
end; 

*buy and hold abnormal return is the difference in cumulated returns;
bhar = cumret - cumidx;
*abret is the abnormal return just for that day;
abret = ret1 - decret;

run;
endrsubmit;

*if we don't want to do a big download size, we can go ahead and summarize in WRDS;
rsubmit;
proc sql;
create table figdata2 as select distinct
td_days,dec_ue,mean(bhar) as mean_bhar
from figdata
group by td_days,dec_ue
order by dec_ue,td_days;
quit;
endrsubmit;

*download the data to load into R;
rsubmit; 
proc download data=figdata2 out=dbox.figdata2; run;
endrsubmit;

*if we want a [-1,+1] size-adjusted BHAR to play with;
*then we can subset the dates a bit;
rsubmit;
data m1_p1_bhar;
set temp4 (where=(-1 le td_days le 1));
by permno anndats;

*these statements allow for cumulating over a data step;
retain cum_logret;
retain cum_logidx;
retain zeroflag;

*these are placeholders used to check that our lags
are for the same firm-year observation;
lag_permno = lag(permno);
lag_evtdate = lag(anndats);

*assign the firm-specific return to a placeholder variable;
*this allows us to fill in the placeholder, ret1, with the index
*when the firm-specific return is missing;
*this procedure implies that a) on any day with missing data, the abnormal return
*relative to the index was zero, and b) any remaining proceeds after a delisting
*are reinvested in the index;
ret1=ret;

*fill with index return when firm-specific return is missing;
if missing(ret) then ret1 = decret;

*if the first day in the window, start a new cumulation;
if first.anndats then do;
	cum_logret = log(1+ret1);
	cum_logidx = log(1+decret);
	cumret = ret1;
	cumidx = decret;
	*track -100% returns;
	if ret = -1 then zeroflag=1; else zeroflag=0;
end;
*otherwise, check the lags and if they are valid, add to the existing cumulation;
else if (permno=lag_permno and anndats=lag_evtdate) then do;

	*track -100% returns;
	if ret = -1 then zeroflag=1; 

	*if the firm had a -100% return, only cumulate the index return;
	if zeroflag=1 then do;
		cum_logret = log(0);
		cumret = -1;
		*sum the logged returns (price relatives);
		cum_logidx = cum_logidx + log(1+decret);
		*exponentiate the sum of the logged returns, then subtract 1;
		cumidx = exp(cum_logidx)-1;
	end;
	*otherwise cumulate both;
	else do;
		cum_logret = cum_logret + log(1+ret1);
		cumret = exp(cum_logret)-1;
		cum_logidx = cum_logidx + log(1+decret);
		cumidx = exp(cum_logidx)-1;
	end;
end; 

*buy and hold abnormal return is the difference in cumulated returns;
bhar = cumret - cumidx;
*abret is the abnormal return just for that day;
abret = ret1 - decret;

*only keep final BHAR per observation;
if not last.anndats then delete;

run;
endrsubmit;

*Merge back to data8;
rsubmit;
proc sql;
create table data9 as select distinct
a.*, b.bhar as m1_p1_bhar
from data8 a, m1_p1_bhar b
where a.permno=b.permno and a.anndats=b.anndats;
quit;
endrsubmit;


*show how to winsorize in SAS;
rsubmit;
%winsor(dsetin = data9, dsetout = data10, byvar = yearqtr, vars = ue m1_p1_bhar, type = W, pctl = 1 99);
endrsubmit;

*download the winsorized data;
rsubmit; 
proc download data=data10 out=dbox.beaverdata; run;
endrsubmit;



signoff;





