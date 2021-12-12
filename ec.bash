!#/bin/bash
while true
do
{ # try

    output=`./redis-cli -h ezbenchgc0hng9j-m5-large-6-x-001.eka2zi.0001.use1devo.elmo-dev.amazonaws.com set foo bar` &&
    echo ${output}
    sleep 0.1
} || { # catch
    # save log for exception
    echo "Received an error!"
}