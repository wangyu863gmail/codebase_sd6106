import schedule
import time
import subprocess

def run_data_update():
    subprocess.run(['python', 'src\data_daily_update.py'])

# Schedule jobs
schedule.every(1).minutes.do(run_data_update)

while True:
    schedule.run_pending()
    time.sleep(1)
