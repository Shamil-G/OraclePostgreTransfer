VIRTUAL_ENV=c:/Projects/OraclePostgreTransfer
#python3.10 -m venv venv
call $VIRTUAL_ENV/venv/Scripts/activate.bat
rem pip3.10 install --upgrade pip
rem #pip3.10 install flask
rem  pip3.10 install requests
rem pip3.10 install psycopg2-binary
rem pip3.10 install cx_Oracle
rem pip3.10 install xmltodict
rem pip3.10 install psutil
rem pip3.10 install gunicorn
python3.10 main_transfer.py
rem gunicorn
