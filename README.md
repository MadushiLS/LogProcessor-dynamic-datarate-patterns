# log-processor
# EDGAR Log Processor benchmark

A stream processing benchmark based on a HTTP log data set published by Division of Economic and Risk Analysis (DERA) [1]. The data provides details of the usage of
publicly accessible EDGAR company filings in a simple but extensive manner. Each record in the data set consists of 15 different fields hence each event sent to the benchmark had
15 fields (*iij\_timestamp, ip, date, time, zone, cik, accession, code, size, idx, norefer, noagent, find, crawler, and browser*).

## References

[1] https://www.sec.gov/dera/data/edgar-log-file-data-set.html