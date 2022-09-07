

# ACCT 932 Replications
This repository guides students through a loose replication of a short-window event study around earnings announcement dates.







<h2> 🛠️ Getting Started / Installation Guide </h2>

* Follow the steps at this link if you need to install R, RStudio and Git: https://github.com/eweisbrod/example-project/

**THIS CODE REQUIRES R 4.2 OR GREATER**



<h2> General Methodology Notes </h2>

Here are some tips and best practices to consider when working with event studies and capital markets data. To the extent that I have time, I will expand on these and provide relevant coding examples.

<h4> Earnings announcement dates </h4>

*  When available, it is ideal to validate earnings announcement dates using Wall St. Horizon (WSH) and/or Ravenpack.
*  Barring these data sources, it is a common best practice to take the earlier of the Compustat and I/B/E/S announcement date (Johnson and So, 2018 JAR).
*  Some recent studies increment the announcement day to the following trading day when earnings are announced after market close on the announcement day. 

<h4> Analyst Forecast Consensus </h4>

* Over the past 20 - 30 years, many studies considered it a best practice to calculate their own forecast consensus using the I/B/E/S detail file. However, there are pros and cons to this approach.
* A benefit of this approach is the ability to exclude stale or outlier forecasts from the consensus. 
* An issue with this approach is that broker entitlements change over time and researchers usually do not have access to all of the individual forecasts that market participants would have seen at the time. This makes custom consensus calculations less replicable and less representative of the "headline" or "as-was" consensus at the time. 
* See Call Hewitt Watkins Yohn 2021 RAST for a detailed discussion. 
* An alternate approach is to use the I/B/E/S "surprise" file (surpsum or surpsumu) to obtain the consensus at the time of an earnings announcement or the I/B/E/S summary history (statsum or statsumu) file to obtain the as-was consensus for a given monthly snapshot date.  These files include the forecasts of all analysts that were contained in the consensus that investors would have seen at the time, even if researchers are not entitled to view all of the forecasts individually. 
* One limitation of the summary history file is that the snapshot is only available as of the third Thursday of each month. 
