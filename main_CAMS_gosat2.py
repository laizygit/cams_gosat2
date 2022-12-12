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
from email.mime.text import MIMEText



print("Info | STEP 1: Create directories for preprocessing")

version     = '210210'
dir_nrt     = "/nfs/GOSAT/AUX/OPERATIONAL/CAMS_GOSAT2/"
dir_ecmwf   = "/nfs/GOSAT/AUX/OPERATIONAL/CAMS_GOSAT1/ECMWF/ECMWF_data/"
root_path   = "/nfs/GOSAT2/FTS/"

# initial processing time
now         = datetime.now()
string_now  = now.isoformat()[0:16]

# days to process: days are today, yesterday and the day before yesterday
min_step    = [0,-1,-2]

# Create directory for new days that don't yet exist to begin the preprocessing
dates_gosat2 = []
for item in min_step:

    date  = date.today() + timedelta(days=item)
    dates_gosat2.append(date)
    year_rt     = date.year
    month_rt    = date.month
    day_rt      = date.day

    dir_ini = dir_nrt + "PREPROCESS_INI/210210/"+str(year_rt)+"/"+str(month_rt).zfill(2)+"/"+str(day_rt).zfill(2)+"/"
    if not os.path.exists(dir_ini):
        os.makedirs(dir_ini)




# Begin processing looping through dates
for ij, date_d0 in enumerate(dates_gosat2):

    start_time  = time.time()

    year, month, day  = date_d0.year, date_d0.month, date_d0.day
    date_name = str(year)+str(month).zfill(2)+str(day).zfill(2)
    date_name_hour = str(year)+str(month).zfill(2)+str(day).zfill(2)+string_now.replace(':', '')

    print("Info | Begin Processing of L1b at: ", date_name)

    # Define the day after processing as the preprocessing requires ECMWF data from the current and next day
    plus_step = 1
    date_d1      = date_d0+timedelta(days=plus_step)
    year_d1, month_d1, day_d1 = date_d1.year, date_d1.month, date_d1.day
    date_name_d1 = str(year_d1)+str(month_d1).zfill(2)+str(day_d1).zfill(2)

    # define ecmwf data from current day
    ecmwf_ml1 = dir_ecmwf + "ml"+str(date_name)+"_0000.grb"
    ecmwf_ml2 = dir_ecmwf + "ml"+str(date_name)+"_0600.grb"
    ecmwf_ml3 = dir_ecmwf + "ml"+str(date_name)+"_1200.grb"
    ecmwf_ml4 = dir_ecmwf + "ml"+str(date_name)+"_1800.grb"
    ecmwf_ms1 = dir_ecmwf + "ms"+str(date_name)+"_0000.grb"
    ecmwf_ms2 = dir_ecmwf + "ms"+str(date_name)+"_0600.grb"
    ecmwf_ms3 = dir_ecmwf + "ms"+str(date_name)+"_1200.grb"
    ecmwf_ms4 = dir_ecmwf + "ms"+str(date_name)+"_1800.grb"

    # define ecmwf data from next day
    ecmwf_ml5 = dir_ecmwf + "ml"+str(date_name_d1)+"_0000.grb"
    ecmwf_ms5 = dir_ecmwf + "ms"+str(date_name_d1)+"_0000.grb"

    # check if ecmwf avaliable
    ecmwf_flag1 = os.path.exists(ecmwf_ml1) and os.path.exists(ecmwf_ms1)
    ecmwf_flag2 = os.path.exists(ecmwf_ml2) and os.path.exists(ecmwf_ms2)
    ecmwf_flag3 = os.path.exists(ecmwf_ml3) and os.path.exists(ecmwf_ms3)
    ecmwf_flag4 = os.path.exists(ecmwf_ml4) and os.path.exists(ecmwf_ms4)
    ecmwf_flag5 = os.path.exists(ecmwf_ml5) and os.path.exists(ecmwf_ms5)

    # Create list of level 1 file names to be processed
    dir_lv1   = root_path + 'L1B/SWIR_DAY/' + version + "/" + str(year)+"/" +str(month).zfill(2)+"/"+str(day).zfill(2)+"/"
    files_tbp = []  # overwrite file names from previous day
    files_tbp = glob(dir_lv1+"*.h5")
    namelist = [x.split(root_path+'L1B/SWIR_DAY/')[1] for x in files_tbp]

    if len(namelist) == 0 and date_d0 != date.today():
        print("Warning | No GOSAT-2 data for today")
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


    # Keep log of files that have been processed
    procfile = dir_nrt+'LOG/processed_'+str(date_name)
    if os.path.exists(procfile):
        with open(procfile) as f:
            files_processed = f.read().splitlines()
    else:
            files_processed = []
    files_GOSAT2 = [x for x in namelist if x not in files_processed]    # avoid multiple processing of files

    # Check if neccessary ECMWF data avaliable and add to files_processing when true
    files_processing = []
    for i in files_GOSAT2:
        file_string  = i.split("GOSAT2TFTS2")[1]    # parse GOSAT2 level 1 file name
        hour = int(file_string[8:10])               # the hour is the 9th and 10th digit of the file name
        if hour >=0 and hour < 6:
            ecmwf_flag = ecmwf_flag1 and ecmwf_flag2
        elif hour >=6  and hour < 12:
            ecmwf_flag = ecmwf_flag2 and ecmwf_flag3
        elif hour >=12 and hour < 18:
            ecmwf_flag = ecmwf_flag3 and ecmwf_flag4
        elif hour >=18 and hour < 24:
            ecmwf_flag = ecmwf_flag4 and ecmwf_flag5
        else:
            print("Warning | UNKNOWN TIME for file:", filename)
            continue
        if ecmwf_flag:
                files_processing.append(i)

        # Write to log file files_processing: these will be the data that are currently being processed. When they are finished
        # they will be written to the processed log file
        ts = time.time()
        st = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        files_num = st + " "+"NUMBER OF MATCHED L1B FILES = "+str(len(files_processing))+"/"+str(len(namelist))
        with open(dir_nrt+'LOG/GOSAT_'+str(date_name), "a") as logfile:
            logfile.write(files_num+"\n")


    print("Info | STEP 2: Creating list for preprocessing")
    np.savetxt(dir_nrt+'PREPROCESS/INI/preprocess_'+str(date_name_hour),files_processing, fmt='%s')


    print("Info | STEP 3.1: Running preprocessing")
    proc = subprocess.Popen(["./preprocess.sh","1","INI/preprocess_"+str(date_name_hour),str(date_name_hour)],cwd=dir_nrt+'PREPROCESS/')
    proc.wait()


    print("Info | STEP 3.2: Store output in subdirectory")
    if not os.path.exists(dir_nrt+'PREPROCESS/CONTRL_OUT/'+str(date_name)):
        proc = subprocess.Popen(["mkdir",date_name],cwd=dir_nrt+'PREPROCESS/CONTRL_OUT/')
        proc.wait()


    print("Info | STEP 3.3:  Move output preprocessing to subdirectory")
    proc = subprocess.Popen(["mv","preprocess_id0001.out",str(date_name)+"/"+str(date_name_hour)+".out"],cwd=dir_nrt+'PREPROCESS/CONTRL_OUT/')
    proc.wait()


    print("Info | STEP 3.4: Create Input list for proxy retrieval")
    if not os.path.exists(dir_nrt+'PREPROCESS/CONTRL_OUT/'+str(date_name)+'/'+str(date_name_hour)+'.out'):
        continue
    proc = subprocess.Popen(["/deos/andrewb/anaconda3/envs/py2/bin/python","ascii_to_ascii.py",str(date_name)+'/'+str(date_name_hour)+".out"],cwd = dir_nrt+'PREPROCESS/CONTRL_OUT')
    proc.wait()


    print("Info | STEP 3.5: Move proxy list to directory")
    proc = subprocess.Popen(["mv","PREPROCESS/CONTRL_OUT/"+str(date_name)+'/'+str(date_name_hour)+".out.db","PROXY/INI/"+str(date_name_hour)],cwd = dir_nrt)
    proc.wait()


    print("Info | STEP 4.1: Running proxy retrieval")
    proc = subprocess.Popen(["./proxy_retrieval.sh",str(date_name_hour),str(date_name_hour)],cwd=dir_nrt+'PROXY/')
    proc.wait()


    print("Info | STEP 4.2:  Store output in subdir")
    if not os.path.exists(dir_nrt+'PROXY/CONTRL_OUT/'+str(date_name)):
        proc = subprocess.Popen(["mkdir",str(date_name)],cwd=dir_nrt+'PROXY/CONTRL_OUT/')
        proc.wait()


    print("Info | STEP 4.3: Change name of proxy output")
    proc = subprocess.Popen(["mv","short_id0001.out",str(date_name)+"/"+str(date_name_hour)+".out"],cwd=dir_nrt+'PROXY/CONTRL_OUT/')
    proc.wait()


    print("Info | STEP 4.4: Run RemoTeC to NetCDF script")
    proc = subprocess.Popen(["/deos/andrewb/anaconda3/envs/py2/bin/python","PROXY_netcdf_GOSAT2.py","-d "+str(date_name_hour)],cwd = dir_nrt+'PROXY/')
    proc.wait()


    print("Info | STEP 5: Save processed L1b lists")
    with open(dir_nrt + 'LOG/processed_'+str(date_name), "a") as logfile:
        for l1b_file in files_processing:
            logfile.write(l1b_file+"\n")


    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Info | Time ", elapsed_time)
