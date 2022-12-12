import os
import sys
import time
import math
import string
import fnmatch
import numpy as np
import subprocess
from glob import glob
from datetime import date, timedelta, datetime

import smtplib

from ftplib import FTP, all_errors

# Import the email modules we'll need
from email.mime.text import MIMEText



print "STEP 0:Create dir for preprocessing"

version     = '210210'
dir_nrt     = "/nfs/GOSAT/AUX/OPERATIONAL/CAMS_GOSAT2/"
dir_gosat2_root   = "/nfs/GOSAT2/FTS/"
dir_ecmwf   = "/nfs/GOSAT/AUX/OPERATIONAL/CAMS_GOSAT1/ECMWF/ECMWF_data/"
now         = datetime.now()# initial processing time
string_now  = now.isoformat()[0:16]


day_step    = [0,1,2]# today, yesterday, the day before yesterday
dates_gosat2 = []
for item in day_step:
    date_p   = date.today()-timedelta(days=item)
#    date_p   = date.today()-timedelta(days=28)
    dates_gosat2.append(date_p)
    year     = date_p.year
    month    = date_p.month
    day      = date_p.day

    dir_ini = dir_nrt + "PREPROCESS_INI/210210/"+str(year)+"/"+str(month).zfill(2)+"/"+str(day).zfill(2)+"/"
#    dir_SPOD_ini = dir_nrt + "PREPROCESS_INI/SPOD/220220/"+str(year)+"/"+str(month).zfill(2)+"/"+str(day).zfill(2)+"/"
    if not os.path.exists(dir_ini):
        os.makedirs(dir_ini)


for ij, date_p0 in enumerate(dates_gosat2):
    files_gosat2 = []
    start_time  = time.time()

    year  = date_p0.year
    month = date_p0.month
    day   = date_p0.day
    date_name = str(year)+str(month).zfill(2)+str(day).zfill(2)
#   date_name_hour = str(year)+str(month).zfill(2)+str(day).zfill(2)+str(now.hour).zfill(2)
    date_name_hour = str(year)+str(month).zfill(2)+str(day).zfill(2)+string_now.replace(':', '')
    print 'Processing L1b at:',date_name
    date_p1      = date_p0+timedelta(days=1)# tormorrow

    year_p1      = date_p1.year
    month_p1     = date_p1.month
    day_p1       = date_p1.day
    date_name_p1 = str(year_p1)+str(month_p1).zfill(2)+str(day_p1).zfill(2)

    # ecmwf data
    dir_ecmwf_ml1 = dir_ecmwf + "ml"+str(date_name)+"_0000.grb"#the day
    dir_ecmwf_ms1 = dir_ecmwf + "ms"+str(date_name)+"_0000.grb"#
    dir_ecmwf_ml2 = dir_ecmwf + "ml"+str(date_name)+"_0600.grb"#
    dir_ecmwf_ms2 = dir_ecmwf + "ms"+str(date_name)+"_0600.grb"#
    dir_ecmwf_ml3 = dir_ecmwf + "ml"+str(date_name)+"_1200.grb"#
    dir_ecmwf_ms3 = dir_ecmwf + "ms"+str(date_name)+"_1200.grb"#
    dir_ecmwf_ml4 = dir_ecmwf + "ml"+str(date_name)+"_1800.grb"#
    dir_ecmwf_ms4 = dir_ecmwf + "ms"+str(date_name)+"_1800.grb"#
    dir_ecmwf_ml5 = dir_ecmwf + "ml"+str(date_name_p1)+"_0000.grb"#day after the day
    dir_ecmwf_ms5 = dir_ecmwf + "ms"+str(date_name_p1)+"_0000.grb"#
    # check if ecmwf avaliable
    ecmwf_flag1 = os.path.exists(dir_ecmwf_ml1) and os.path.exists(dir_ecmwf_ms1)
    ecmwf_flag2 = os.path.exists(dir_ecmwf_ml2) and os.path.exists(dir_ecmwf_ms2)
    ecmwf_flag3 = os.path.exists(dir_ecmwf_ml3) and os.path.exists(dir_ecmwf_ms3)
    ecmwf_flag4 = os.path.exists(dir_ecmwf_ml4) and os.path.exists(dir_ecmwf_ms4)
    ecmwf_flag5 = os.path.exists(dir_ecmwf_ml5) and os.path.exists(dir_ecmwf_ms5)

    # gosat data
    string_leader_1st = version +"/" + str(year)+"/"+str(month).zfill(2)+"/"+str(day).zfill(2)+"/"
    string_leader = 'L1B/SWIR_DAY/'  + string_leader_1st
    dir_gosat2   = dir_gosat2_root   + string_leader
    files_gosat2 = glob(dir_gosat2+"*.h5")

    files_full = files_gosat2 # need to exclude processed files
    files_OBSP = [item.split(dir_gosat2_root+'L1B/SWIR_DAY/')[1] for item in files_full]
    
    if len(files_OBSP) == 0 and date_p0 != date.today():

	#there is a delay in downloading the GOSAT2 data which means that there is no data within 24hrs. We need L1X for NRT
        print 'No GOSAT2 data for the day'
        #msg = MIMEText("CRONJOB ISSUE: No GOSAT2 data for the day. Check "+ dir_nrt)

        ## me == the sender's email address
        ## you == the recipient's email address
        #msg['Subject'] = 'CRONJOB ISSUE: NO GOSAT2 DATA'
        #msg['From'] = 'a.g.barr@sron.nl'
        #msg['To'] = 'a.g.barr@sron.nl'

        ## Send the message via our own SMTP server, but don't include the
        ## envelope header.
        #s = smtplib.SMTP('localhost')
        #s.sendmail('a.g.barr@sron.nl', 'a.g.barr@sron.nl', msg.as_string())
        #s.quit()
        #continue

    # processed gosat data
    dir_processed = dir_nrt+'LOG/processed_'+str(date_name)
    if os.path.exists(dir_processed):
        with open(dir_processed) as f:
            files_processed = f.read().splitlines()
    else:
            files_processed = []
    files_GOSAT2 = [x for x in files_OBSP if x not in files_processed]
    

    if len(files_GOSAT2) == 0:
        ts          = time.time()
        st          = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        files_num   = st + " "+"NUMBER OF PROCESSED L1B FILES = "+str(len(files_processed))+"/"+str(len(files_OBSP))
        with open(dir_nrt+'LOG/GOSAT_'+str(date_name), "a") as logfile:
            logfile.write(files_num+"\n")
        continue


    # Two checks --> One of GOSAT L1B and the other for ECMWF data
    matches   = []
    for file_name in files_GOSAT2:
        file_string  = file_name.split("GOSAT2TFTS2")[1]

        hour = int(file_string[8:10])
        if   hour >=0 and hour < 6:
            ecmwf_flag = ecmwf_flag1 and ecmwf_flag2
        elif hour >=6  and hour < 12:
            ecmwf_flag = ecmwf_flag2 and ecmwf_flag3
        elif hour >=12 and hour < 18:
            ecmwf_flag = ecmwf_flag3 and ecmwf_flag4
        elif hour >=18 and hour < 24:
            ecmwf_flag = ecmwf_flag4 and ecmwf_flag5
        else:
            print "Warning:UNKNOWN TIME for file:",filename
            continue
        if ecmwf_flag:
                matches.append(file_name)

    # If we have no matches, then say something
        if len(matches) ==0 and len(files_GOSAT2)>0:
            ts          = time.time()
            st          = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            files_num = st + " "+"NUMBER OF UNPROCESSED L1B FILES = "+str(len(files_GOSAT2))+"/"+str(len(files_OBSP))
            with open(dir_nrt+'LOG/GOSAT_'+str(date_name), "a") as logfile:
                logfile.write(files_num+"\n")
            continue
        else:

            ts          = time.time()
            st          = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            files_num = st + " "+"NUMBER OF MATCHED L1B FILES = "+str(len(matches))+"/"+str(len(files_OBSP))

        with open(dir_nrt+'LOG/GOSAT_'+str(date_name), "a") as logfile:
            logfile.write(files_num+"\n")
	    


    #STEP 0: Check the arriving time of all L1b files
    last_file = min(matches, key=lambda f:os.path.getctime("{}/{}".format(dir_gosat2_root+'L1B/SWIR_DAY/', f)))
    file_time = os.stat(dir_gosat2_root+'L1B/SWIR_DAY/'+last_file).st_mtime
    file_time_string = datetime.fromtimestamp(file_time).strftime("%Y-%m-%d %H:%M:%S")

    # STEP 1: Creating list (as test)
    print "STEP 1"
    np.savetxt(dir_nrt+'PREPROCESS/INI/preprocess_'+str(date_name_hour),matches, fmt='%s')
    # STEP 3.0: Run preprocessing
    print "STEP 3"
    proc = subprocess.Popen(["./preprocess.sh","1","INI/preprocess_"+str(date_name_hour),str(date_name_hour)],cwd=dir_nrt+'PREPROCESS/')
    proc.wait()

    # STEP 3.1: Store output in subdir
    print "STEP 3.1"
    if not os.path.exists(dir_nrt+'PREPROCESS/CONTRL_OUT/'+str(date_name)):
        proc = subprocess.Popen(["mkdir",date_name],cwd=dir_nrt+'PREPROCESS/CONTRL_OUT/')
        proc.wait()


    # STEP 3.3: Move output preprocessing to subdir
    print "STEP 3.3"
    proc = subprocess.Popen(["mv","preprocess_id0001.out",str(date_name)+"/"+str(date_name_hour)+".out"],cwd=dir_nrt+'PREPROCESS/CONTRL_OUT/')
    proc.wait()

    # STEP 4: Create Input list for NON-SCATTERING
    print "STEP 4"
    if not os.path.exists(dir_nrt+'PREPROCESS/CONTRL_OUT/'+str(date_name)+'/'+str(date_name_hour)+'.out'):
        continue
    # STOP AND continue --> WRITE ERROR MESSAGE and add to list
    proc = subprocess.Popen(["/deos/andreass/anaconda2/bin/python","ascii_to_ascii.py",str(date_name)+'/'+str(date_name_hour)+".out"],cwd = dir_nrt+'PREPROCESS/CONTRL_OUT')
    proc.wait()

    # STEP 4.5: Move proxy list to directory
    print "STEP 4.5"
    proc = subprocess.Popen(["mv","PREPROCESS/CONTRL_OUT/"+str(date_name)+'/'+str(date_name_hour)+".out.db","PROXY/INI/"+str(date_name_hour)],cwd = dir_nrt)
    proc.wait()
    # STEP 5.0: Run NON-SCATTERING
    print "STEP 5.0"
    proc = subprocess.Popen(["./proxy_retrieval.sh",str(date_name_hour),str(date_name_hour)],cwd=dir_nrt+'PROXY/')
    proc.wait()


    # STEP 5.1: Store output in subdir
    print "STEP 5.1"
    if not os.path.exists(dir_nrt+'PROXY/CONTRL_OUT/'+str(date_name)):
        proc = subprocess.Popen(["mkdir",str(date_name)],cwd=dir_nrt+'PROXY/CONTRL_OUT/')
        proc.wait()



    # STEP 5.3: Change output nonscattering to date.out
    print "STEP 5.3"
    proc = subprocess.Popen(["mv","short_id0001.out",str(date_name)+"/"+str(date_name_hour)+".out"],cwd=dir_nrt+'PROXY/CONTRL_OUT/')
    proc.wait()

    # STEP 6: Run RemoTeC to NetCDF script
    print "STEP 6"
    proc = subprocess.Popen(["/deos/andrewb/anaconda3/envs/py2/bin/python","PROXY_netcdf_GOSAT2.py","-d "+str(date_name_hour)],cwd = dir_nrt+'PROXY/')
    proc.wait()

    print "STEP 7"#Save processed L1b lists
    with open(dir_nrt + 'LOG/processed_'+str(date_name), "a") as logfile:
        for l1b_file in matches:
            logfile.write(l1b_file+"\n")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print elapsed_time
