@echo off
cd C:\Scheduled\Crash-db\src
for /F "usebackq tokens=1,2 delims==" %%i in (`wmic os get LocalDateTime /VALUE 2^>NUL`) do if '.%%i.'=='.LocalDateTime.' set ldt=%%j
set ldt=%ldt:~0,4%-%ldt:~4,2%-%ldt:~6,2% %ldt:~8,2%:%ldt:~10,2%:%ldt:~12,6%
echo "starting update %ldt%" >> C:\Scheduled\crashdb.txt
net use U: location password /USER:user /PERSISTENT:YES
echo "mapped drive" >> C:\Scheduled\crashdb.txt
start /wait C:\Python27\ArcGISx6410.3\python.exe -m dbseeder seed U:/collision stage
echo "finished python script" >> C:\Scheduled\crashdb.txt
net use /delete U:
echo "removed share" >> C:\Scheduled\crashdb.txt
cd C:\Scheduled
