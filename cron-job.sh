#!/usr/bin/env bash
# install: bash cron-job.sh
CRON="0 */3 * * * /usr/bin/python3 /home/ec2-user/used-car-scraper/run_all.py >> /var/log/usedcar.log 2>&1"
( crontab -l 2>/dev/null | grep -v 'usedcar.log' ; echo "$CRON" ) | crontab -
echo "cron installed: $CRON"
