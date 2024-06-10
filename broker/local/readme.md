# local


## Publish from source to message table
source -> publish -> store in database as published `FALSE`


## Publish from message table to subs table
messenger -> process messages -> publish (store messages in db for each sub as processed `FALSE`)


## From subs table to handlers.



