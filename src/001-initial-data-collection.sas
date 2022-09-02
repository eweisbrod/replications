
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




rsubmit;
proc sort data = data7; by yearqtr; run;
proc rank data = data7 out = data8 groups = 10;
by yearqtr;
var ue;
ranks dec_ue;
run;
endrsubmit;




rsubmit; 
proc download data=ibes.surpsum out=fdp.surpsum; run;
endrsubmit;



signoff;









/*******************************************************************
Obtain recommendation detail file
********************************************************************/

*what makes a unique observation in the detail recommendation file?;
proc sql;
create table checkdups as select distinct
ticker, emaskcd,amaskcd,anndats,anntims,count(ireccd) as n
from ibes.recddet
group by ticker,emaskcd,amaskcd,anndats,anntims
order by n desc;
quit;
*a few hundred dups;

*lets look at one;
data check;
set ibes.recddet;
where ticker = "CN2" and anndats = "26JUN2012"d;
run;
*hmm, it just seems like a data entry error, only system times differ.;

*try getting to unique obs;
*keep the one with the most recent review date;

proc sort data = ibes.recddet out=recddet1; by ticker emaskcd amaskcd anndats anntims descending revdats;
proc sort data = recddet1 out=recddet2 nodupkey; by ticker emaskcd amaskcd anndats anntims;

*common trick for removing dups in SAS, if you sort, and then do nodupkey, it keeps only the first obs;
*if you specify an out= dataset, it will output the filtered data to that dataset instead of modifying the original;



*check again;
proc sql;
create table checkdups as select distinct
ticker, emaskcd,amaskcd,anndats,anntims,count(ireccd) as n
from recddet2
group by ticker,emaskcd,amaskcd,anndats,anntims
order by n desc;
quit;
*no dups;

*link clean recddet to itself to find lags;
*could also sort and then do lag() in a data step;
proc sql;
create table rev1 as select distinct
dhms(a.anndats,0,0,a.anntims) as anndttm format datetime.,
a.*, 
input(a.ireccd,best4.) - input(b.ireccd,best4.) as revision_num,
case when (calculated revision_num) < 0 then "UPGRADE" 
		else case when (calculated revision_num) > 0 then "DOWNGRADE"
			  	else "NO CHANGE"
		end
end as revision, 
b.ireccd as lag_ireccd, b.anndats as lag_anndats,
dhms(b.anndats,0,0,b.anntims) as lag_dttm format datetime.
from recddet2 a, recddet2 b
where a.ticker=b.ticker
and a.emaskcd=b.emaskcd and a.amaskcd=b.amaskcd
and dhms(a.anndats,0,0,a.anntims) > dhms(b.anndats,0,0,b.anntims)
group by a.ticker,a.emaskcd,a.amaskcd,a.anndats,a.anntims
having dhms(b.anndats,0,0,b.anntims) = max(dhms(b.anndats,0,0,b.anntims));
quit;

*check for dups;
proc sql;
create table checkdups as select distinct
emaskcd,amaskcd,ticker,anndttm,count(ireccd) as n
from rev1
group by emaskcd,amaskcd,ticker,anndttm
order by n desc;
quit;
*no dups;

*filter the data to the relevant twitter revision sample;
data rev2;
set rev1;
where (2013 <=year(anndats)<=2020) and revision ne "NO CHANGE" and USFIRM = 1;
run;



/****************************************************************************
Calculate analyst experience
*****************************************************************************/

*general experience, find the first recommendation by this analyst, for any firm;
proc sql;
create table min_analyst as select distinct
emaskcd,amaskcd,min(anndats) as min_date format date9.
from recddet2
where not missing(emaskcd) and not missing(amaskcd) and not missing(anndats)
group by emaskcd,amaskcd;
quit;

*now link it back to the rev file;
proc sql; 
create table rev3 as select distinct
a.*,b.min_date as analyst_min_date,
(a.anndats - b.min_date)/365 as gen_experience
from rev2 a left join min_analyst b
on a.emaskcd=b.emaskcd and a.amaskcd=b.amaskcd;
quit;


*firm specific experience, find the first recommendation by this analyst, for THIS firm;
proc sql;
create table min_analyst_ticker as select distinct
emaskcd,amaskcd,ticker,min(anndats) as min_date format date9.
from recddet2
where not missing(emaskcd) and not missing(amaskcd) and not missing(anndats)
group by emaskcd,amaskcd,ticker;
quit;

*now link it back to the rev file;
proc sql; 
create table rev4 as select distinct
a.*,b.min_date as analyst_ticker_min_date,
(a.anndats - b.min_date)/365 as firm_experience
from rev3 a left join min_analyst_ticker b
on a.emaskcd=b.emaskcd and a.amaskcd=b.amaskcd and a.ticker=b.ticker;
quit;

*check for dups;
proc sql;
create table checkdups as select distinct
emaskcd,amaskcd,ticker,anndttm,count(ireccd) as n
from rev4
group by emaskcd,amaskcd,ticker,anndttm
order by n desc;
quit;
*no dups;



/****************************************************************************
Calculate analyst following
*****************************************************************************/

*first, make a list of unique ticker dates that I need to collect following for;
*following will always be the same for a given ticker-date, so we can simplify to this;
proc sql;
create table ticker_dates as select distinct
ticker,anndats
from rev4;
quit;

*now use max numrec within the past year;
*ticker_dates had 377,604 obs so I will check that the next table has the same nobs;
proc sql;
create table following as select distinct
a.ticker,a.anndats,b.numrec as following
from ticker_dates a, ibes.recdsum b
where a.ticker=b.ticker and a.anndats >= b.statpers
and INTCK("MONTHS",b.statpers,a.anndats) <= 12
group by a.ticker, a.anndats
having b.numrec = max(b.numrec);
quit;


*check for dups;
proc sql;
create table checkdups as select distinct
ticker,anndats,count(following) as n
from following
group by ticker,anndats
order by n desc;
quit;
*no dups;


*a little fewer obs but no dups, that's ok;

*now link it back to the rev file;
proc sql; 
create table rev5 as select distinct
a.*,b.following
from rev4 a left join following b
on a.ticker=b.ticker and a.anndats=b.anndats;
quit;

*check for dups;
proc sql;
create table checkdups as select distinct
emaskcd,amaskcd,ticker,anndttm,count(ireccd) as n
from rev5
group by emaskcd,amaskcd,ticker,anndttm
order by n desc;
quit;
*no dups;



/****************************************************************************
Calculate broker size
*****************************************************************************/

*make a giant file of all the analysts from all the brokers;
proc sql;
create table bsize1 as select distinct 
a.ticker,a.emaskcd,a.amaskcd,a.anndats,a.anntims,b.amaskcd as employee
from rev2 a, recddet2 b 
where a.emaskcd = b.emaskcd
and intck("MONTHS",b.anndats,a.anndats) <= 12;
quit;
*this took 45 min to run on the wrds cloud;

*collapse the giant file back down to get broker size counts;
proc sql;
create table bsize2 as select distinct
ticker,emaskcd,amaskcd,anndats,anntims,count(employee) as broker_size
from bsize1
group by ticker,emaskcd,amaskcd,anndats,anntims;
quit;
*this only takes 7 seconds;

*link it back to the rev file;
proc sql;
create table rev6 as select distinct
a.*, b.broker_size
from rev5 a, bsize2 b
where a.ticker=b.ticker
and a.emaskcd=b.emaskcd
and a.amaskcd=b.amaskcd
and a.anndats=b.anndats
and a.anntims=b.anntims;
quit;
*note that this pt it looks like SAS studio only defaults to showing the first 30 columns;
* so you have to check the little blue box on the left to see broker size;




/****************************************************************************
Save my progress on the ibes variables
*****************************************************************************/
*save a copy to my home folder;
data home.rev;
set rev6;
run;

*save a copy to shared KU folder to share with Matt;
data ku.rev;
set rev6;
run;



*check for dups;
proc sql;
create table checkdups as select distinct
emaskcd,amaskcd,ticker,anndats,anntims,count(ireccd) as n
from home.rev
group by emaskcd,amaskcd,ticker,anndats,anntims
order by n desc;
quit;
*no dups;


*take a step back...I realized the recddet file is both US and international;
proc freq data = home.rev;
tables usfirm;
run;
*only 82,351 obs are for are US firms...this explains a lot;
*this is why so many recs but much fewer matches to twitter data; 

*from here forward I will filter USFIRM=1. We can go back and do it earlier once we clean up the code;

/***************************************************************************************
PULL MOST RECENT ANNUAL ANNOUNCEMENT DATE AND PENDS FROM IBES
****************************************************************************************/

*I am moving this step up here before we link to CRSP and Compustat to avoid duplicates in the matching;

*pulling ibes earnings announcement dates;
data ibes_ea; set ibes.act_epsus;
keep ticker anndats pends;
where USFIRM = 1
and year(anndats)>=2013
and pdicity = "ANN";
run;

*check for dups;
proc sql;
create table checkdups as select distinct
ticker, anndats, pends, count(ticker) as n
from ibes_ea
group by ticker, anndats, pends
order by n desc,ticker,anndats,pends;
quit;
*couple hundred duplicates;

*a data step wasn't the best way to do that because there is no distinct keyword;

*see 2014;
data check;
set ibes_ea;
where ticker = "004M";
run;

data check;
set ibes.act_epsus;
where ticker = "004M";
run;
*in this case it is a Chinese company listed in the US so it has an actual both in US dollars and Chinese Yuan (CNY);



proc sql;
create table ibes_ea as select distinct
ticker,anndats,pends
from ibes.act_epsus
where usfirm=1
and year(anndats >= 2011) /*if we have 2013 announcements they could be linked to 2012 financials...I'll do 2011 just to be extra comprehensive*/
and pdicity = "ANN";
quit;

*check for dups;
proc sql;
create table checkdups as select distinct
ticker, anndats, pends, count(ticker) as n
from ibes_ea
group by ticker, anndats, pends
order by n desc;
quit;
*this has no dups by definition because I used distinct keyword;

*I added the distinct keyword below...I would almost always use that keyword;
*it is hard to think of a situation where you don't want distinct obs;
*let's just do an inner join for now;
*In this case I think I like the having=max way;
*the sorting and deleting should be fine too since we know that both datsets were unique going in;
*this just helps me double check that there is no other source of duplicates;
proc sql;
create table rev7 as select distinct
a.*, b.anndats as ann_ea_date_lag1, b.pends as ann_pends_lag1
from home.rev as a , ibes_ea as b
where  a.ticker = b.ticker
and intck("days",b.pends,a.anndats) <= 400
and a.anndats>=b.anndats
and a.usfirm=1 /*add the usfirm filter here*/
group by a.ticker,a.emaskcd,a.amaskcd,a.anndttm
having b.pends = max(b.pends) ; 
quit; 


*check for dups;
proc sql;
create table checkdups as select distinct
emaskcd,amaskcd,ticker,anndats,anntims,count(ireccd) as n
from rev7
group by emaskcd,amaskcd,ticker,anndats,anntims
order by n desc;
quit;
*no dups once I added the group by and having=max line;

*we didn't lose that many obs linking to act_epsus...so I think this was an ok overall strategy to get pends;


/***************************************************************************************
Link to CRSP 
****************************************************************************************/


*use the WRDS ICLINK table to link IBES ticker to CRSP Permno;
*Score of 0 is the best matches...I also allow 1 usually;
proc sql;
create table permno as select distinct
a.*, b.permno, b.comnam, b.score
from rev7 as a,home.iclink as b
where a.ticker=b.ticker and  b.score in (0,1);
quit;


*Filter to only HEXCDS 1,2,3,4 and SHRCD 10,11;
*HEXCD means the stock exchange header code. 1-4 are the main exchanges, NYSE, NASDAQ, Arca, etc.;
*SHRCD are share codes for the type of stock, 10 and 11 are common stock;
*these are common CRSP filters;
proc sql;
create table data4 as select distinct
b.ticker as crsp_ticker, b.comnam as crsp_comnam, a.*,b.hexcd, b.siccd, b.shrcd,(a.OFTIC=b.ticker) as tickmatch,
a.comnam=b.comnam as namematch
from permno as a, crsp.stocknames as b
where a.permno=b.permno and (b.namedt le a.anndats le b.nameenddt)
and b.hexcd in (1,2,3,4) and b.shrcd in (10,11);
quit;
*now we are at 60k;


*check for dups;
proc sql;
create table checkdups as select distinct
emaskcd,amaskcd,ticker,anndttm,count(permno) as n
from data4
group by emaskcd,amaskcd,ticker,anndttm
order by n desc;
quit;
*now we have about 300 duplicates;

*check what if we require exchange tickers to match between IBES and CRSP?;
proc sql;
create table checkdups as select distinct
emaskcd,amaskcd,ticker,anndttm,count(permno) as n
from data4
where tickmatch=1
group by emaskcd,amaskcd,ticker,anndttm
order by n desc;
quit;
*that gets us down to 82 dups and it is a good idea because we match to twitter on ticker symbol;

*redo the above data4 step to require ticker symbol match;
*leave in both to show Matt;
proc sql;
create table data4 as select distinct
b.ticker as crsp_ticker, b.comnam as crsp_comnam, a.*,b.hexcd, b.siccd, b.shrcd,
a.comnam=b.comnam as namematch
from permno as a, crsp.stocknames as b
where a.permno=b.permno and (b.namedt le a.anndats le b.nameenddt)
and b.hexcd in (1,2,3,4) and b.shrcd in (10,11)
and a.OFTIC=b.ticker;
quit;


*check for dups at the ticker level;
proc sql;
create table checkdups as select distinct
emaskcd,amaskcd,ticker,anndttm,count(permno) as n
from data4
group by emaskcd,amaskcd,ticker,anndttm
order by n desc;
quit;
*79 dups;


*check for dups at the permno level;
proc sql;
create table checkdups as select distinct
emaskcd,amaskcd,ticker,anndttm,permno,count(hexcd) as n
from data4
group by emaskcd,amaskcd,ticker,anndttm, permno
order by n desc;
quit;
*no dups at permno level at least;

*at this point I have to decide what to do. I can leave the dups and hope they drop later due to missing data;
*if one of them is the true match, it will have data available, and the other one might not;
*the risk is that these dups can multiply at the next link. Each dup permno could link to dup gvkeys, and so on;
*I looked ahead and noticed that most of the dups will go away except for a duplicate gvkey match;
*So, for convenience let's leave them in for now, and deal with them later before we link to twitter;



/***************************************************************************************
Create COMPANN Dataset from comp.FUNDA
****************************************************************************************/



*calculating compustat controls;
*I think no reason to do this as a left join if you you are gonna scale everything by the lags; 
*just makes it more difficult to understand where the missing data is coming from;
*I'll also require oancf if we are going to do accruals that way;
*also non-missing mve and ceq > 0;
*negative book value of equity is not something we want to get into;
*at > 0 don't want to divide by zero;
*use same income variable, no need to use ib one place and ni another;
proc sql;
	create view		compann
	as select		*
	from			comp.funda(where = ((indfmt = 'INDL') and (datafmt = 'STD') and (popsrc = 'D') and (consol = 'C') and (curcd = 'USD')));

	create table	compustat_controls
	as select		a.gvkey,a.cik,a.tic,a.datadate,a.conm,a.fyear,a.sich,
					(a.ib - a.oancf)/b.at as accruals "Accruals",  /* I don't think pi is a good scalar here, let's scale by at like the other vars */
					(b.prcc_f * b.csho) as mve "Market Value of Equity",
					(b.prcc_f * b.csho)/b.ceq as mtb "Market to Book",
					log(1+ calculated mve) as mve_size "Size - MVE",
					log(1+ a.at) as at_size "Size - Assets",
					coalesce(a.intan/b.at,0) as intang "Intangibles",
					coalesce(a.ppent/b.at,0) as ppe "Plant, Property and Equipment",
					a.ib/b.at as roa "Return on Assets",
					coalesce(a.capx/b.at,0) as capex "Capital Expenditures",
					coalesce(a.xrd/b.at,0) as rnd "Research and Development Expenditures",
					coalesce(a.xad/b.at,0) as adv "Advertising Expenditures",
					coalesce((a.sale - b.sale)/b.sale,0) as sale_gr "Sales Growth",
					coalesce((a.dltt + a.dlc)/b.at,0) as lev "Leverage",	
					floor(a.sich/100) as sic2 "Two-digit SIC"

	from			compann as a,
					compann as b
	where				a.gvkey = b.gvkey and a.fyear = b.fyear+1 and not missing(b.at) and not missing(a.oancf) and not missing(a.at) and b.ceq > 0 and not missing((b.prcc_f * b.csho)) and not missing(a.ib) and b.at > 0 
	group by		a.gvkey,a.fyear;
quit;


*before merging, good idea to check whether compann is unique in gvkey,datadate;
proc sql;
create table checkdups as select distinct
gvkey,datadate, count(cusip) as n
from compann
group by gvkey,datadate
order by n desc;
quit;
*yes, it is, nice job;

*what is the coverage like on the variables?;
proc means data = compustat_controls n nmiss min p1 p5 p10 p25 mean p50 p75 p90 p95 p99 max;
var accruals mve mtb mve_size at_size intang ppe roa capex rnd sale_gr lev;
run;
*I ran this a few times and kept going back and changing the above code till I was happy with how the distributions and coverage look; 

/****************************************************************************
Link to Compustat
*****************************************************************************/

*Use the WRDS CRSP-Compustat merged (CCM) linkfile;
*link CRSP Permno to Compustat GVKEY;
proc sql;
create table permno3 as select distinct
b.gvkey, a.*
from data4 as a, crsp.ccmxpf_linktable as b
where a.permno=b.lpermno and (b.LINKDT <= a.anndats or b.LINKDT = .B)
and (a.anndats <= b.LINKENDDT or b.LINKENDDT = .E);
quit;



*merge in compustat controls on gvkey and balance sheet date;
*let's make this an inner join for now, let's just work with the dataset that has all the vars;
proc sql;
create table rev8 as select
a.*, b.*
from permno3 as a, compustat_controls as b
where a.gvkey = b.gvkey 
and a.ann_pends_lag1 = b.datadate;
quit;

*check for dups;
proc sql;
create table checkdups as select distinct
ticker, anndats, anntims, emaskcd, amaskcd, count(ticker) as n
from rev8
group by ticker, anndats, anntims, emaskcd, amaskcd
order by n desc;
quit;
*have some dups;



/****************************************************************************
Save my progress so far
*****************************************************************************/
*save a copy to my home folder;
data home.rev8;
set rev8;
run;



/****************************************************************************
Expand the trading windows
*****************************************************************************/



*provide the trading days macro to extend the window around each revision;

*First, create a trading date calendar;
*this will be used to match the days around the event to trading dates in CRSP;
*If an event falls on a Monday, you don't want t-1 to be sunday, you want it to be previous Friday;
*however, there are also holidays like thanksgiving etc. and closures like 9/11;
*so the best way is to just use the list of trading days from CRSP;
*assign each trading day a consecutive number so that you can increment through them;
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




*Now run the macro to get the trading dates around each revision;
*this will only work if you %INCLUDE the macro file above during setup;
%tddays(dsetin=home.rev8  (keep = permno anndats oftic revision) , dsetout=home.rev_m1_p1, datevar=anndats, beginwin=-1, endwin=1, calendarname=home.crspdates);

*the new dataset will have many more rows, and it has two new relevant columns, date and td_days;
*now we can link to tweets using date instead of anndats;


/****************************************************************************
Calculate Abnormal Returns - M1 P1
*****************************************************************************/

*INDEX RETURNS FOR MARKET ADJUSTED; 
*now collect index returns; 
*doing this before firm-level returns ensures full data; 
proc sql;
create table temp1 as select distinct
a.permno, a.anndats, a.date, a.td_days, b.vwretd /* value-weighted return */
from home.rev_m1_p1 as a, crsp.dsi as b /*comes from CRSP daily stock index file */
where a.date=b.date;
quit;

*Now link daily raw return data to the dates in the event window;
proc sql;
create table temp2 as select distinct
a.*, b.ret
from temp1 as a left join crsp.dsf as b
on a.permno=b.permno and a.date=b.date
order by permno, anndats,date;
quit;

*Now cumulate the returns over the event window and subtract the index return from the firm's return;
data m1_p1_bhars;
set temp2;

*subset the data by event, so that only the days within each event will be cumulated;
*this is like group_by in R;
by permno anndats;

*normally a data step looks at one row at a time, but if you tell it to retain a variable, it will keep it from one row to the next;
retain cum_logret; *retain this to store the firm return as we cumulate it;
retain cum_logidx; *retain this to store the index return as we cumulate it;
retain zeroflag; *retain this to deal with -100% returns...I am the only one I know who does this part, but it seems like it can't hurt;
retain n_rets; *retain this to check missing returns;

*these lags are to check to make sure that I don't cumulate across events, i.e. that i don't mix events together;
lag_permno = lag(permno); 
lag_anndats = lag(anndats);

*a temporary placeholder in case I need to fill missing returns with the index;
ret1=ret;



*for market-adjusted fill firm with mkt when missing;
if missing(ret) then ret1 = vwretd;

*if it is the first day in the window, start fresh;
*the formula for cumulating returns is to sum the logs and then exponentiate the sum;
if first.anndats then do;
	n_rets = 0;
	cum_logret = log(1+ret1);
	cum_logidx = log(1+vwretd);
	cumret = ret1;
	cumidx = vwretd;
	*track -100% returns;
	if ret = -1 then zeroflag=1; else zeroflag=0;
	if not missing(ret) then n_rets+1; *increment the counter;
end;

*if it is not the first day in the window, then add to the existing sums;
else if (permno=lag_permno and anndats=lag_anndats) then do;

	*track -100% returns;
	if ret = -1 then zeroflag=1; 

	*SPECIAL CASE;
	*if there was a -100% return, then the overall firm return is -1 from then on, but the index still cumulates;
	if zeroflag=1 then do;
	cum_logret = log(0);
	cumret = -1;
	cum_logidx = cum_logidx + log(1+vwretd);
	cumidx = exp(cum_logidx)-1;
	end;
	
	*this is the normal case, anything besides a -100% return, so we need to cumulate the returns as we go;
	else do;
	cum_logret = cum_logret + log(1+ret1); *add the logged return of the new day to the existing sum;
	cumret = exp(cum_logret)-1; *exponentiate the sum to get the cumulative return to date;
	cum_logidx = cum_logidx + log(1+vwretd); *same logic for index;
	cumidx = exp(cum_logidx)-1;
	
	end;
	
	if not missing(ret) then n_rets+1; *increment the counter;
end; 

* the cumulative abnormal return is the buy-and-hold-return;
bhar = cumret - cumidx;

*keep the last day in each window;
if not last.anndats then delete;
if n_rets < 3 then delete;

*keep only the columns I need;
*keep permno anndats bhar n_rets;

run;

proc freq;
tables n_rets;
run;
*only 20 obs were missing return data, so i went ahead and deleted them in the step above so now it will always be 3;


*link the bhars back to the rev sample;
proc sql;
create table rev9 as select distinct
a.*, b.bhar as m1_p1_bhar
from home.rev8 a, m1_p1_bhars b
where a.permno=b.permno and a.anndats=b.anndats;
quit;

proc sql;
create table check_means as select distinct
revision, mean(m1_p1_bhar) as mean_bhar
from rev9
group by revision;
quit;

/****************************************************************************
Calculate Abnormal Returns - P2 P23
*****************************************************************************/
*this will only work if you %INCLUDE the macro file above during setup;
%tddays(dsetin=home.rev8  (keep = permno anndats oftic revision) , dsetout=rev_p2_p23, datevar=anndats, beginwin=2, endwin=23, calendarname=home.crspdates);


*INDEX RETURNS FOR MARKET ADJUSTED; 
*now collect index returns; 
*doing this before firm-level returns ensures full data; 
proc sql;
create table temp1 as select distinct
a.permno, a.anndats, a.date, a.td_days, b.vwretd /* value-weighted return */
from rev_p2_p23 as a, crsp.dsi as b /*comes from CRSP daily stock index file */
where a.date=b.date;
quit;

*Now link daily raw return data to the dates in the event window;
proc sql;
create table temp2 as select distinct
a.*, b.ret
from temp1 as a left join crsp.dsf as b
on a.permno=b.permno and a.date=b.date
order by permno, anndats,date;
quit;

*Now cumulate the returns over the event window and subtract the index return from the firm's return;
data p2_p23_bhars;
set temp2;

*subset the data by event, so that only the days within each event will be cumulated;
*this is like group_by in R;
by permno anndats;

*normally a data step looks at one row at a time, but if you tell it to retain a variable, it will keep it from one row to the next;
retain cum_logret; *retain this to store the firm return as we cumulate it;
retain cum_logidx; *retain this to store the index return as we cumulate it;
retain zeroflag; *retain this to deal with -100% returns...I am the only one I know who does this part, but it seems like it can't hurt;
retain n_rets; *retain this to check missing returns;

*these lags are to check to make sure that I don't cumulate across events, i.e. that i don't mix events together;
lag_permno = lag(permno); 
lag_anndats = lag(anndats);

*a temporary placeholder in case I need to fill missing returns with the index;
ret1=ret;



*for market-adjusted fill firm with mkt when missing;
if missing(ret) then ret1 = vwretd;

*if it is the first day in the window, start fresh;
*the formula for cumulating returns is to sum the logs and then exponentiate the sum;
if first.anndats then do;
	n_rets = 0;
	cum_logret = log(1+ret1);
	cum_logidx = log(1+vwretd);
	cumret = ret1;
	cumidx = vwretd;
	*track -100% returns;
	if ret = -1 then zeroflag=1; else zeroflag=0;
	if not missing(ret) then n_rets+1; *increment the counter;
end;

*if it is not the first day in the window, then add to the existing sums;
else if (permno=lag_permno and anndats=lag_anndats) then do;

	*track -100% returns;
	if ret = -1 then zeroflag=1; 

	*SPECIAL CASE;
	*if there was a -100% return, then the overall firm return is -1 from then on, but the index still cumulates;
	if zeroflag=1 then do;
	cum_logret = log(0);
	cumret = -1;
	cum_logidx = cum_logidx + log(1+vwretd);
	cumidx = exp(cum_logidx)-1;
	end;
	
	*this is the normal case, anything besides a -100% return, so we need to cumulate the returns as we go;
	else do;
	cum_logret = cum_logret + log(1+ret1); *add the logged return of the new day to the existing sum;
	cumret = exp(cum_logret)-1; *exponentiate the sum to get the cumulative return to date;
	cum_logidx = cum_logidx + log(1+vwretd); *same logic for index;
	cumidx = exp(cum_logidx)-1;
	
	end;
	
	if not missing(ret) then n_rets+1; *increment the counter;
end; 

* the cumulative abnormal return is the buy-and-hold-return;
bhar = cumret - cumidx;

*keep the last day in each window;
if not last.anndats then delete;

*let's say they need to have at least approx. half the data we need..kind of subjective but there are very few missing anyways;
if n_rets < 10 then delete;

*keep only the columns I need;
*keep permno anndats bhar n_rets;

run;

proc freq;
tables n_rets;
run;
*about 600 obs have missing data, I will go back above and just require data for about half the window, if they delist at that point its ok.;


*link the bhars back to the rev sample;
proc sql;
create table rev10 as select distinct
a.*, b.bhar as p2_p23_bhar
from rev9 a, p2_p23_bhars b
where a.permno=b.permno and a.anndats=b.anndats;
quit;

*save to home directory;
data home.rev10;
set rev10;
run;

/****************************************************************************
Calculate Abnormal Returns - P2 P5
*****************************************************************************/
*this will only work if you %INCLUDE the macro file above during setup;
%tddays(dsetin=home.rev8  (keep = permno anndats oftic revision) , dsetout=rev_p2_p5, datevar=anndats, beginwin=2, endwin=5, calendarname=home.crspdates);


*INDEX RETURNS FOR MARKET ADJUSTED; 
*now collect index returns; 
*doing this before firm-level returns ensures full data; 
proc sql;
create table temp1 as select distinct
a.permno, a.anndats, a.date, a.td_days, b.vwretd /* value-weighted return */
from rev_p2_p5 as a, crsp.dsi as b /*comes from CRSP daily stock index file */
where a.date=b.date;
quit;

*Now link daily raw return data to the dates in the event window;
proc sql;
create table temp2 as select distinct
a.*, b.ret
from temp1 as a left join crsp.dsf as b
on a.permno=b.permno and a.date=b.date
order by permno, anndats,date;
quit;

*Now cumulate the returns over the event window and subtract the index return from the firm's return;
data p2_p5_bhars;
set temp2;

*subset the data by event, so that only the days within each event will be cumulated;
*this is like group_by in R;
by permno anndats;

*normally a data step looks at one row at a time, but if you tell it to retain a variable, it will keep it from one row to the next;
retain cum_logret; *retain this to store the firm return as we cumulate it;
retain cum_logidx; *retain this to store the index return as we cumulate it;
retain zeroflag; *retain this to deal with -100% returns...I am the only one I know who does this part, but it seems like it can't hurt;
retain n_rets; *retain this to check missing returns;

*these lags are to check to make sure that I don't cumulate across events, i.e. that i don't mix events together;
lag_permno = lag(permno); 
lag_anndats = lag(anndats);

*a temporary placeholder in case I need to fill missing returns with the index;
ret1=ret;



*for market-adjusted fill firm with mkt when missing;
if missing(ret) then ret1 = vwretd;

*if it is the first day in the window, start fresh;
*the formula for cumulating returns is to sum the logs and then exponentiate the sum;
if first.anndats then do;
	n_rets = 0;
	cum_logret = log(1+ret1);
	cum_logidx = log(1+vwretd);
	cumret = ret1;
	cumidx = vwretd;
	*track -100% returns;
	if ret = -1 then zeroflag=1; else zeroflag=0;
	if not missing(ret) then n_rets+1; *increment the counter;
end;

*if it is not the first day in the window, then add to the existing sums;
else if (permno=lag_permno and anndats=lag_anndats) then do;

	*track -100% returns;
	if ret = -1 then zeroflag=1; 

	*SPECIAL CASE;
	*if there was a -100% return, then the overall firm return is -1 from then on, but the index still cumulates;
	if zeroflag=1 then do;
	cum_logret = log(0);
	cumret = -1;
	cum_logidx = cum_logidx + log(1+vwretd);
	cumidx = exp(cum_logidx)-1;
	end;
	
	*this is the normal case, anything besides a -100% return, so we need to cumulate the returns as we go;
	else do;
	cum_logret = cum_logret + log(1+ret1); *add the logged return of the new day to the existing sum;
	cumret = exp(cum_logret)-1; *exponentiate the sum to get the cumulative return to date;
	cum_logidx = cum_logidx + log(1+vwretd); *same logic for index;
	cumidx = exp(cum_logidx)-1;
	
	end;
	
	if not missing(ret) then n_rets+1; *increment the counter;
end; 

* the cumulative abnormal return is the buy-and-hold-return;
bhar = cumret - cumidx;

*keep the last day in each window;
if not last.anndats then delete;
run;

proc freq;
tables n_rets;
run;
*about 600 obs have missing data, I will go back above and just require data for about half the window, if they delist at that point its ok.;


*link the bhars back to the rev sample;
proc sql;
create table rev11 as select distinct
a.*, b.bhar as p2_p5_bhar
from home.rev10 a, p2_p5_bhars b
where a.permno=b.permno and a.anndats=b.anndats;
quit;

*save to home directory;
data home.rev11;
set rev11;
run;


/****************************************************************************
Calculate 0,5 IPT
*****************************************************************************/

*this will only work if you %INCLUDE the macro file;
%tddays(dsetin=home.rev8  (keep = permno anndats oftic revision) , dsetout=temp0, datevar=anndats, beginwin=0, endwin=5, calendarname=home.crspdates);


*INDEX RETURNS FOR MARKET ADJUSTED; 
*now collect index returns; 
*doing this before firm-level returns ensures full data; 
proc sql;
create table temp1 as select distinct
a.permno, a.anndats, a.date, a.td_days, b.vwretd /* value-weighted return */
from temp0 as a, crsp.dsi as b /*comes from CRSP daily stock index file */
where a.date=b.date;
quit;

*Now link daily raw return data to the dates in the event window;
proc sql;
create table temp2 as select distinct
a.*, b.ret
from temp1 as a left join crsp.dsf as b
on a.permno=b.permno and a.date=b.date
order by permno, anndats,date;
quit;

*Now cumulate the returns over the event window and subtract the index return from the firm's return;
data daily_bhars;
set temp2;

*subset the data by event, so that only the days within each event will be cumulated;
*this is like group_by in R;
by permno anndats;

*normally a data step looks at one row at a time, but if you tell it to retain a variable, it will keep it from one row to the next;
retain cum_logret; *retain this to store the firm return as we cumulate it;
retain cum_logidx; *retain this to store the index return as we cumulate it;
retain zeroflag; *retain this to deal with -100% returns...I am the only one I know who does this part, but it seems like it can't hurt;
retain n_rets; *retain this to check missing returns;

*these lags are to check to make sure that I don't cumulate across events, i.e. that i don't mix events together;
lag_permno = lag(permno); 
lag_anndats = lag(anndats);

*a temporary placeholder in case I need to fill missing returns with the index;
ret1=ret;



*for market-adjusted fill firm with mkt when missing;
if missing(ret) then ret1 = vwretd;

*if it is the first day in the window, start fresh;
*the formula for cumulating returns is to sum the logs and then exponentiate the sum;
if first.anndats then do;
	n_rets = 0;
	cum_logret = log(1+ret1);
	cum_logidx = log(1+vwretd);
	cumret = ret1;
	cumidx = vwretd;
	*track -100% returns;
	if ret = -1 then zeroflag=1; else zeroflag=0;
	if not missing(ret) then n_rets+1; *increment the counter;
end;

*if it is not the first day in the window, then add to the existing sums;
else if (permno=lag_permno and anndats=lag_anndats) then do;

	*track -100% returns;
	if ret = -1 then zeroflag=1; 

	*SPECIAL CASE;
	*if there was a -100% return, then the overall firm return is -1 from then on, but the index still cumulates;
	if zeroflag=1 then do;
	cum_logret = log(0);
	cumret = -1;
	cum_logidx = cum_logidx + log(1+vwretd);
	cumidx = exp(cum_logidx)-1;
	end;
	
	*this is the normal case, anything besides a -100% return, so we need to cumulate the returns as we go;
	else do;
	cum_logret = cum_logret + log(1+ret1); *add the logged return of the new day to the existing sum;
	cumret = exp(cum_logret)-1; *exponentiate the sum to get the cumulative return to date;
	cum_logidx = cum_logidx + log(1+vwretd); *same logic for index;
	cumidx = exp(cum_logidx)-1;
	
	end;
	
	if not missing(ret) then n_rets+1; *increment the counter;
end; 

* the cumulative abnormal return is the buy-and-hold-return;
bhar = cumret - cumidx;

*keep only the columns I need;
*keep permno anndats bhar n_rets;

run;

*calculate the daily ratios;
proc sql;
create table daily_ipt as select distinct
a.*,b.bhar as bhar_0_5,
case when (a.bhar/b.bhar) > 1 then 1 else
	case when (a.bhar/b.bhar) < -1 then -1 else
	(a.bhar/b.bhar) 
	end
end as cum_IPT
from daily_bhars a, daily_bhars (where=(td_days=5 and n_rets=6)) b 
where a.permno=b.permno and a.anndats=b.anndats;
quit;

data home.fig_ipt;
set daily_ipt;
run;

*aggregate to an aggregate area-under-the-curve measure;
proc sql;
create table ipt as select distinct
permno,anndats, sum(cum_IPT)+0.5 as IPT
from daily_ipt
where td_days < 5
group by permno, anndats;
quit;

proc means min max mean median n nmiss;
var IPT;
run;

*merge back to dataset;
proc sql;
create table home.rev12 as select distinct
a.*,b.IPT
from home.rev11 a, ipt b
where a.permno=b.permno and a.anndats = b.anndats;
quit;


/****************************************************************************
Add institutional ownership
*****************************************************************************/

*link to my data;
proc sql;
create table io_permno as select distinct
a.permno ,  a.anndats, b.instown_perc2 as io, b.rdate as io_date
from home.rev12 a left join home.io_wrds_tool_2001_2020 b
ON a.permno=b.permno and  0 < (a.anndats - b.rdate) < 200
group by a.cusip, a.anndats
having b.rdate=max(b.rdate);
quit;


*link in io;
proc sql;
create table home.rev13 as select distinct 
a.*, case when b.io > 1 then 1 else b.io end as io_permno
from home.rev12 a left join io_permno b
ON a.permno=b.permno and a.anndats=b.anndats;
quit;


*link to my data;
proc sql;
create table io_cusip as select distinct
a.cusip ,  a.anndats, b.instown_perc as io, b.rdate as io_date
from home.rev12 a left join home.wtool_output b
ON a.cusip=b.cusip and  0 < (a.anndats - b.rdate) < 200
group by a.cusip, a.anndats
having b.rdate=max(b.rdate);
quit;

*link in io;
proc sql;
create table home.rev14 as select distinct 
a.*, case when b.io > 1 then 1 else b.io end as io_cusip
from home.rev13 a left join io_cusip b
ON a.cusip=b.cusip and a.anndats=b.anndats;
quit;


proc means data = home.rev14 n nmiss min p1 p5 p25 p50 mean p75 p95 p99 max;
var io:;
run;

proc corr data = home.rev14 spearman;
var following mve_size io:;
run;

proc freq data = home.io_wrds_tool_2001_2020;
tables rdate;
run;

proc freq data = home.wtool_output;
tables rdate;
run;

data test;
set crsp.stocknames;
where permno = 10032;
run;

data test2;
set home.rev14;
where permno=10032;
run;

data test3;
set home.rev14;
where cusip = '72913210';
run;

data test3;
set home.io_wrds_tool_2001_2020;
where cusip = '72913210';
run;

data test3;
set home.io_wrds_tool_2001_2020;
where permno=10032;
run;


data test3;
set home.wtool_output;
where cusip = '72913210';
run;

****************************************************************************
Adding dummy if earnings announcement within [-2, +2] around revision
*****************************************************************************/;
proc sql;
create table home.rev15 
as select a.*, b.anndats as ea_anndats
from home.rev14 as a left join ibes.act_epsus as b
on a.oftic = b.oftic
and -2 <= intck("days",a.anndats,b.anndats) <= 2;
quit;

/****************************************************************************
Extra examples - not run
*****************************************************************************/



*example of link to Compustat FUNDQ;
*sometimes this is easier because our data relates to a specific quarter, but here we will just look for the most recent 10q;
proc sql;
create table cstat1 as select distinct
 b.conm, a.*,b.cik, b.datadate, b.rdq, b.fqtr, b.fyearq
from permno3 as a, comp.fundq as b
where a.gvkey=b.gvkey and not missing(b.rdq)
/*10Q should be announced by the revision date,
but if it is more than 6 months old, I don't want it */
and 0 <= a.anndats - b.RDQ < 180 
/*standard Compustat Filters */
and b.indfmt='INDL' and b.datafmt='STD' and b.popsrc='D'
and b.consol='C'
and b.compstq ne "DB";
quit;

*so now I linked to multiple 10qs I should keep the most recent one;
*sort on all the unique IDs, as well as descending datadate and rdq;


proc sort data = cstat1; by ticker emaskcd amaskcd anndats anntims gvkey descending datadate descending rdq;
proc sort data = cstat1 nodupkey; by ticker emaskcd amaskcd anndats anntims gvkey; 
*only keep most recent obs for each gvkey;


