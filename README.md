

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
