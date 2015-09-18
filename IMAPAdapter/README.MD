# IMAP Mail Adapter

The standard protocol for all email servers is IMAP. Be it unix based email servers, gmail, even MS Exchange Servers support reading emails via IMAP protocol. 
This adapter connects via the default IMAP port to the specified host (no proxy server being used!) and allows to read the INBOX folder by selecting emails. Or to create remote subscriptions and all new emails are received.
Currently no proper restart logic is implemented as this depends on the exact setup. 

## Install
Two jar files need to be copied into the lib folder of the adapter:
* The javax.mail.jat from [here](https://java.net/projects/javamail/pages/Home)
* The jsoup-1.8.3.jar from [here](http://jsoup.org/download)
