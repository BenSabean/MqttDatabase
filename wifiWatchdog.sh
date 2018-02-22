#!/bin/sh

if ! ping -c 1 "www.google.ca" > /dev/null
	then
        echo "Ping Fail - `date`"
	cat .pw | sudo -S reboot
else
	echo "Host Found - `date`"
fi
