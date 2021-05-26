from sqlalchemy import create_engine,MetaData, select,update
from sqlalchemy.orm import Session
import pandas as pd 
import numpy
import csv
import math
from scipy.stats import sem
# import pdb; pdb.set_trace()
from sqlalchemy.sql import func
from datetime import datetime,date, timedelta
#from datetime import datetime,timedelta
#from datetime import datetime
from cmath import rect, phase
from math import radians, degrees,sqrt
import statistics
from scipy import special
from sqlalchemy import asc
from sqlalchemy import and_

try:
        engine = create_engine("mysql+pymysql://sarathlal:sarath@123@172.104.173.82/MTM")
        #engine = create_engine("mysql+pymysql://phpmyadmin:distancemonopetri@localhost/MTM")
        metadata =MetaData()
        metadata.reflect(bind = engine)
        conn = engine.connect()
except:
        print("Engine creation failed")
adadata = metadata.tables['ADA_DATA_MTM_bkup1']
#noon_parameter = conn.execute(select([adadata.c.VESSEL_id,adadata.c.SOG,adadata.c.DRAFT_AFT,adadata.c.DRAFT_FORE,adadata.c.STW,adadata.c.RPM,adadata.c.POWER,adadata.c.WIND_DIRECTION,adadata.c.WIND_SPEED,adadata.c.HEADING,adadata.c.ID])).fetchall()#.where(adadata.c.UID == 6).where(adadata.c.UID == 1).where(adadata.c.CAA == 0)


"""valid_param = conn.execute(select( [adadata.c.SOG,adadata.c.STW,adadata.c.WIND_SPEED,adadata.c.TORQUE,adadata.c.POWER,adadata.c.RPM,adadata.c.DRAFT_MEAN,adadata.c.RUDDER_ANGLE,adadata.c.WIND_DIRECTION,adadata.c.HEADING,adadata.c.ID,adadata.c.UTC_REPORT_DATE_TIME]).order_by(asc(adadata.c.UTC_REPORT_DATE_TIME))).fetchall() 
print("valid_param:",valid_param)"""
#utc_param = conn.execute(select([adadata.c.ID,adadata.c.UTC_REPORT_DATE_TIME]).where(and_(adadata.c.validation==None ,adadata.c.outlier==None)).order_by(asc(adadata.c.UTC_REPORT_DATE_TIME))).fetchall()
#utc_param = conn.execute(select([adadata.c.ID,adadata.c.UTC_REPORT_DATE_TIME]).where((adadata.c.UTC_REPORT_DATE_TIME.between(','2019-10-31 09:58:05'))).order_by(asc(adadata.c.UTC_REPORT_DATE_TIME))).fetchall()

#print("utc_param:::::",utc_param)
#max_time = conn.execute(select([func.max(adadata.c.UTC_REPORT_DATE_TIME)])).fetchone()
##print("max_time:",max_time[0])
start='2019-10-31 09:56:05'
end='2019-10-31 09:58:05'

#start = utc_param[0][1]
#end =  start + timedelta(minutes = 10)
#while(start<=max_time[0]):
	
sog_lst=[]
stw_lst=[]
wind_speed_lst=[]
torque_lst=[]
power_lst=[]
rpm_lst=[]
draft_mean_lst=[]
rudder_angle_lst=[]
wind_direction_lst=[]
heading_lst=[]
id_lst=[]
sog_diff_lst=[]
stw_diff_lst=[]
wind_speed_diff_lst=[]
torque_diff_lst=[]
power_diff_lst=[]
draft_diff_lst=[]
rpm_diff_lst=[]
validation_lst=[]
rudder_diff_lst=[]
wind_direction_diff_lst=[]
heading_diff_lst=[]
outlier_lst=[]
valid_param = conn.execute(select( [adadata.c.SOG,adadata.c.STW,adadata.c.WIND_SPEED,adadata.c.TORQUE,adadata.c.POWER,adadata.c.RPM,adadata.c.DRAFT_MEAN,adadata.c.RUDDER_ANGLE,adadata.c.WIND_DIRECTION,adadata.c.HEADING,adadata.c.ID,adadata.c.UTC_REPORT_DATE_TIME]).where(adadata.c.UTC_REPORT_DATE_TIME.between(start,end)).order_by(asc(adadata.c.UTC_REPORT_DATE_TIME))).fetchall() 
print("valid_param:",valid_param)
n=len(valid_param)

	
for rslt_valid_param in valid_param:
	print("start:",start)
	print("end:",end)
	print("rslt_valid_param:",rslt_valid_param[0])
	sog_lst.append(rslt_valid_param[0])
	stw_lst.append(rslt_valid_param[1])
	wind_speed_lst.append(rslt_valid_param[2])
	torque_lst.append(rslt_valid_param[3])
	power_lst.append(rslt_valid_param[4])
	rpm_lst.append(rslt_valid_param[5])
	draft_mean_lst.append(rslt_valid_param[6])
	rudder_angle_lst.append(rslt_valid_param[7])
	wind_direction_lst.append(rslt_valid_param[8])
	heading_lst.append(rslt_valid_param[9])
	id_lst.append(rslt_valid_param[10])
print("id_lst::::::::::::",id_lst)	
print("sog_list:",sog_lst)
print("stw_list:",stw_lst)
print("wind_speed_list:",sog_lst)
print("torque_list:",stw_lst)
print("power_list:",power_lst)
print("rpm_list:",rpm_lst)
print("draft_list:",draft_mean_lst)
print("rudder_angle_list:",stw_lst)
print("wind_direction_lst:",wind_direction_lst)
print("heading_lst:",heading_lst)
sog_mean=statistics.mean(sog_lst)
stw_mean=statistics.mean(stw_lst)
wind_speed_mean=statistics.mean(wind_speed_lst)
torque_mean=statistics.mean(torque_lst)
power_mean=statistics.mean(power_lst)
rpm_mean=statistics.mean(rpm_lst)
draft_mean_mn=statistics.mean(draft_mean_lst)
print("sog_mean:",sog_mean)
print("stw_mean:",stw_mean)
print("wind_speed_mean:",wind_speed_mean)
print("totque_mean:",torque_mean)
print("power_mean:",power_mean)
print("rpm_mean:",rpm_mean)
print("draft_mean:",draft_mean_mn)
"""rudder_angle_lst.append(rslt_valid_param[7])
	wind_direction_lst.append(rslt_valid_param[8])
	heading_lst.append(rslt_valid_param[9])"""
"""sog_mean=1/60*sum(sog_lst)
stw_mean=1/60*sum(stw_lst)
wind_speed_mean=1/60*sum(wind_speed_lst)
torque_mean=1/60*sum(torque_lst)
power_mean=1/60*sum(power_lst)
rpm_mean=1/60*sum(rpm_lst)
draft_mean=1/60*sum(draft_mean_lst)
for sog in sog_lst:
	print("sog:",sog)
	sog_diff=abs(sog-sog_mean)
	print("sog_abs_diff:",sog_diff)
	sog_diff_lst.append(sog_diff)
print("sog_diff_lst:",sog_diff_lst)

for stw in stw_lst:
	stw_diff=abs(stw-stw_mean)
	stw_diff_lst.append(stw_diff)
for wind_speed  in wind_speed_lst:
	wind_speed_diff=wind_speed - wind_speed_mean
	wind_speed_diff_lst.append(wind_speed_diff)

for torque in torque_lst:
	torque_diff= abs(torque -torque_mean)
	torque_diff_lst.append(torque_diff)
for power in power_lst:
	power_diff= abs(power - power_mean)
	power_diff_lst.append(power_diff)

for draft in draft_lst:
	draft_diff= abs(draft - draft_mean)
	draft_diff_lst.append(draft_diff)

sog_std_error=math.sqrt(1/60*(sum(sog_diff_lst*sog_diff_lst)))
print("sog_std_error:",sog_std_error)"""
"""for  in _lst:
	_mean= + _mean
	print("_mean;",_mean)
_mean=1/60*_mean
print("lmean:",_mean)
for rslt_valid_param_ang in valid_param_angles:
	print("rslt_ang:",rslt_valid_param_ang)
	print("valid_ang:",rslt_valid_param_ang[0])"""

for sog in sog_lst:
	print("sog:",sog)
	sog_diff=abs(sog-sog_mean)
	print("sog_abs_diff:",sog_diff)
	sog_diff_lst.append(sog_diff)
print("sog_diff_lst:",sog_diff_lst)

for stw in stw_lst:
	stw_diff=abs(stw-stw_mean)
	stw_diff_lst.append(stw_diff)
print("stw_diff_lst:",stw_diff_lst)

for wind_speed  in wind_speed_lst:
	wind_speed_diff=abs(wind_speed - wind_speed_mean)
	wind_speed_diff_lst.append(wind_speed_diff)
print("wind_speed_diff_lst:",wind_speed_diff_lst)

for torque in torque_lst:
	torque_diff= abs(torque -torque_mean)
	torque_diff_lst.append(torque_diff)
print("torque_diff_lst:",torque_diff_lst)

for power in power_lst:
	power_diff= abs(power - power_mean)
	power_diff_lst.append(power_diff)
print("power_diff_lst:",power_diff_lst)

for rpm in rpm_lst:
	rpm_diff= abs(rpm- rpm_mean)
	rpm_diff_lst.append(rpm_diff)
print("rpm_diff_lst:",rpm_diff_lst)


for draft in draft_mean_lst:
	draft_diff= abs(draft - draft_mean_mn)
	draft_diff_lst.append(draft_diff)
print("draft_diff_lst:",draft_diff_lst)


sog_std_error=statistics.pstdev(sog_lst)
stw_std_error=statistics.pstdev(stw_lst)
wind_speed_std_error=statistics.pstdev(wind_speed_lst)
torque_std_error=statistics.pstdev(torque_lst)
power_std_error=statistics.pstdev(power_lst)
draft_mean_std_error=statistics.pstdev(draft_mean_lst)
rpm_std_error=statistics.pstdev(rpm_lst)
print("sog_std_error:",sog_std_error)
print("stw_std_error:",stw_std_error)
print("wind_speed_std_error:",wind_speed_std_error)
print("torque_std_error:",torque_std_error)
print("power_std_error:",power_std_error)
print("draft_mean_std_error:",draft_mean_std_error)
print("rpm_std_error:",rpm_std_error)



rudder_mean=degrees(phase(sum(rect(1, radians(d)) for d in rudder_angle_lst)/len(rudder_angle_lst)))
print("rudder_mean:",rudder_mean)
for rudder_angle in rudder_angle_lst:
	rudder_diff= abs(rudder_angle - rudder_mean)
	ri=rudder_diff%360
	if(ri>180):
		del_f=360-ri
	else:
		del_f=ri
	rudder_diff_lst.append(del_f)

rudder_std_error=sqrt(1/n *(sum(map(lambda x:x*x,rudder_diff_lst))))
#rudder_std_error=statistics.pstdev(rudder_angle_lst)




wind_direction_mean=degrees(phase(sum(rect(1, radians(d)) for d in wind_direction_lst)/len(wind_direction_lst)))
print("wind_direction_mean:",wind_direction_mean)
for wind_direction in wind_direction_lst:
	wind_direction_diff= abs(wind_direction - wind_direction_mean)
	ri=rudder_diff%360
	if(ri>180):
		del_f=360-ri
	else:
		del_f=ri
	wind_direction_diff_lst.append(del_f)
	
wind_direction_std_error=sqrt(1/n *(sum(map(lambda x:x*x,wind_direction_diff_lst))))

heading_mean=degrees(phase(sum(rect(1, radians(d)) for d in heading_lst)/len(heading_lst)))
print("heading_mean:",heading_mean)
for heading in heading_lst:
	heading_diff= abs(heading- heading_mean)
	ri=rudder_diff%360
	if(ri>180):
		del_f=360-ri
	else:
		del_f=ri
	
	heading_diff_lst.append(del_f)
 
heading_std_error=sqrt(1/n *(sum(map(lambda x:x*x,heading_diff_lst))))

print("rudder_std_error:",rudder_std_error)
print("wind_direction_std_error:",wind_direction_std_error)
print("heading_std_error:",heading_std_error)

print("length of heading_diff_lst:",len(heading_diff_lst))

print(" N at strd_errorrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr:",n)


print("validation")
if(sog_std_error>0.5 or stw_std_error >0.5 or rpm_std_error>3 or rudder_std_error>1):
	x=1
	
else:
	x=0
print("x:",x)	
if(x==1):
	print("strat:",start)
	print("end:",end)
	#data_ids = conn.execute('SELECT ID FROM ADA_DATA_MTM_bkup WHERE  DATE(inserted_time)=DATE("2019-10-24 ") ORDER BY UTC_REPORTED_TIME DESC LIMIT 60 ')
	#data_ids = conn.execute('SELECT ID FROM ADA_DATA_MTM_bkup ')
	for data_id in id_lst:
	#for data_id in data_ids:
		print("validation parametr::::::::::::::::::::::::::::::::::",id_lst)
		print("validation uid::::::::::::::::::::::::::::::::::::::::",data_id)
		update_true = update(adadata).where(adadata.c.ID==data_id)
		update_true=update_true.values(validation='1')
		conn.execute(update_true)
        
	print("block 1")
else:
	print("strat:",start)
	print("end:",end)
	for data_id in id_lst:
		print("validation parametr::::::::::::::::::::::::::::::::::",id_lst)
		print("validation uid::::::::::::::::::::::::::::::::::::::::",data_id)
		update_false = update(adadata).where(adadata.c.ID==data_id)
		update_false=update_false.values(validation='0')
		conn.execute(update_false)
		
	#data_ids = conn.execute('SELECT ID FROM ADA_DATA_MTM_bkup WHERE  DATE(inserted_time)=DATE("2019-10-24") ORDER BY UTC_REPORTED_TIME DESC LIMIT 60 ')
	#data_ids = conn.execute('SELECT ID FROM ADA_DATA_MTM_bkup ')
	#for data_id in data_ids:
		
		
"""if(x==1):
	#data_ids = conn.execute('SELECT ID FROM ADA_DATA_MTM_bkup WHERE  DATE(inserted_time)=DATE("2019-10-24 ") ORDER BY UTC_REPORTED_TIME DESC LIMIT 60 ')
	#data_ids = conn.execute('SELECT ID FROM ADA_DATA_MTM_bkup ')
	for rslt_valid_param in valid_param[start:end]:
	#for data_id in data_ids:
		print("validation parametr::::::::::::::::::::::::::::::::::",rslt_valid_param)
		print("validation uid::::::::::::::::::::::::::::::::::::::::",rslt_valid_param[10])
		update_adadata = update(adadata).where(adadata.c.ID==rslt_valid_param[10])
		update_adadata=update_adadata.values(validation='1')
		conn.execute(update_adadata)
        
	print("block 1")
else:
	for rslt_valid_param in valid_param[start:end]:
	#data_ids = conn.execute('SELECT ID FROM ADA_DATA_MTM_bkup WHERE  DATE(inserted_time)=DATE("2019-10-24") ORDER BY UTC_REPORTED_TIME DESC LIMIT 60 ')
	#data_ids = conn.execute('SELECT ID FROM ADA_DATA_MTM_bkup ')
	#for data_id in data_ids:
		
		update_adadata = update(adadata).where(adadata.c.ID==rslt_valid_param[10])
		update_adadata=update_adadata.values(validation='0')
		conn.execute(update_adadata)"""


"""outlier_lst.append(id_lst)
outlier_lst.append(sog_diff_lst)
outlier_lst.append(stw_diff_lst)
outlier_lst.append(wind_speed_diff_lst)
outlier_lst.append(torque_diff_lst)
outlier_lst.append(power_diff_lst)
outlier_lst.append(rpm_diff_lst)
outlier_lst.append(draft_diff_lst)
outlier_lst.append(rudder_diff_lst)
outlier_lst.append(wind_direction_diff_lst)
outlier_lst.append(heading_diff_lst)
print("outlier_lst:::::::::::::::::::::::::::::;",outlier_lst,len(outlier_lst))
lst1 = [[item[x] for item in outlier_lst] for x in range(len(outlier_lst[0]))]
print("length of item list:::::::::::::::",len(outlier_lst[0]))
print("lst1:::::::::::::::",lst1,len(lst1))
print("first list::::::",lst1[0])
print("first element in list:::::",lst1[0][0])

for row_data in lst1:
	print("row data:",row_data)
	print("lst_nxt0:::",row_data[0])
	if(sog_std_error==0 or stw_std_error==0 or wind_speed_std_error==0 or torque_std_error==0 or power_std_error==0 or  rpm_std_error==0 or draft_mean_std_error==0 or  rudder_std_error==0 or wind_direction_std_error==0 or heading_std_error==0 ):
		print("std error is 00000000000000000000000000000000000000000000000000000000")
		adadata_std_err = update(adadata).where(adadata.c.ID==str(row_data[0]))
		adadata_std_err=adadata_std_err.values(outlier='0')
		conn.execute(adadata_std_err)
	
pfo_sog=special.erfc(sog_lst)
print("type:::::::::",type(pfo_sog[1]))
pfo_stw=special.erfc(stw_lst)
pfo_wind_speed=special.erfc(wind_speed_lst)
pfo_torque=special.erfc(torque_lst)
pfo_power=special.erfc(power_lst)
pfo_rpm=special.erfc(rpm_lst)
pfo_draft_mean=special.erfc(draft_mean_lst)
pfo_rudder=special.erfc(rudder_angle_lst)
pfo_wind_dir=special.erfc(wind_direction_lst)
pfo_heading=special.erfc(heading_lst)
print("pfo_sog:",pfo_sog)
print("pfo_stw:",pfo_stw)
print("pfo_wind_speed:",pfo_wind_speed)
print("pfo_torque:",pfo_torque)
print("pfo_power:",pfo_power)
print("pfo_rpm:",pfo_rpm)
print("pfo_draft_mean:",pfo_draft_mean)
print("pfo_rudder:",pfo_sog)
print("pfo_wind_dir:",pfo_sog)
print("pfo_heading:",pfo_sog)
n=len(valid_param)

for pos in range(n):
	print("postnnnnnnnnnnnnnnn:",pos)
	if((pfo_sog[pos]*n)<0.5 or (pfo_stw[pos]*n)<0.5 or (pfo_wind_speed[pos]*n)<0.5 or (pfo_torque[pos]*n)<0.5 or (pfo_power[pos]*n)<0.5 or (pfo_rpm[pos]*n)<0.5 or (pfo_draft_mean[pos]*n)<0.5 or  (pfo_rudder[pos]*n)<0.5 or (pfo_wind_dir[pos]*n)<0.5 or (pfo_heading[pos]*n)<0.5 ):
		print("yesssssssssssssssssssssssssssssss",str(id_lst[pos]))
		update_adadata_true_out = update(adadata).where(adadata.c.ID==str(id_lst[pos]))
		update_adadata_true_out=update_adadata_true_out.values(outlier='1')
		conn.execute(update_adadata_true_out)

	else:
		print("nooooooooooooooooooooooooooooooooooooooo",str(id_lst[pos]))
		update_false_out = update(adadata).where(adadata.c.ID==str(id_lst[pos]))
		update_false_out=update_false_out.values(outlier='0')
		conn.execute(update_false_out)"""

outlier_lst.append(id_lst)
outlier_lst.append(sog_diff_lst)
outlier_lst.append(stw_diff_lst)
outlier_lst.append(wind_speed_diff_lst)
outlier_lst.append(torque_diff_lst)
outlier_lst.append(power_diff_lst)
outlier_lst.append(rpm_diff_lst)
outlier_lst.append(draft_diff_lst)
outlier_lst.append(rudder_diff_lst)
outlier_lst.append(wind_direction_diff_lst)
outlier_lst.append(heading_diff_lst)
print("outlier_lst:::::::::::::::::::::::::::::;",outlier_lst,len(outlier_lst))
lst1 = [[item[x] for item in outlier_lst] for x in range(len(outlier_lst[0]))]
print("length of item list:::::::::::::::",len(outlier_lst[0]))
print("lst1:::::::::::::::",lst1,len(lst1))
print("first list::::::",lst1[0])
print("first element in list:::::",lst1[0][0])
for row_data in lst1:


	if(sog_std_error==0):
		pfo_sog=1
	else:
		pfo_sog=special.erfc(row_data[1]/(sog_std_error*sqrt(2)))
	if(stw_std_error==0):
		pfo_stw=1
	else:
		pfo_stw=special.erfc(row_data[2]/(stw_std_error*sqrt(2)))
	if(wind_speed_std_error==0):
		pfo_wind_speed=1
	else:
		pfo_wind_speed=special.erfc(row_data[3]/(wind_speed_std_error*sqrt(2)))
	if(torque_std_error==0):
		pfo_torque=1
	else:	
		pfo_torque=special.erfc(row_data[4]/(torque_std_error*sqrt(2)))
	if(power_std_error==0):
		pfo_power=1
	else:
		pfo_power=special.erfc(row_data[5]/(power_std_error*sqrt(2)))
	if(rpm_std_error==0):
		pfo_rpm=1	
	else:
		pfo_rpm=special.erfc(row_data[6]/(rpm_std_error*sqrt(2)))
	if(draft_mean_std_error==0):
		pfo_draft_mean=1
	else:
		pfo_draft_mean=special.erfc(row_data[7]/(draft_mean_std_error*sqrt(2)))
		
	if(rudder_std_error==0):
		pfo_rudder=1
	else:
		pfo_rudder=special.erfc(row_data[8]/(rudder_std_error*sqrt(2)))	
		
	if(wind_direction_std_error==0):
		pfo_wind_dir=1
	else:
		pfo_wind_dir=special.erfc(row_data[9]/(wind_direction_std_error*sqrt(2)))
	if(heading_std_error==0):
		pfo_heading=1
	else:
		pfo_heading=special.erfc(row_data[10]/(heading_std_error*sqrt(2)))
	print("pfo_sog:",pfo_sog)
	print("pfo_stw:",pfo_stw)
	print("pfo_wind_speed:",pfo_wind_speed)
	print("pfo_torque:",pfo_torque)
	print("pfo_power:",pfo_power)
	print("pfo_rpm:",pfo_rpm)
	print("pfo_draft_mean:",pfo_draft_mean)
	print("pfo_rudder:",pfo_sog)
	print("pfo_wind_dir:",pfo_sog)
	print("pfo_heading:",pfo_sog)
	
	print("row data:",row_data)
	print("lst_nxt0:::",row_data[0])
	print(" N at outlier_________rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr:",n)		
	if((pfo_sog*n)<0.5 or (pfo_stw*n)<0.5 or (pfo_wind_speed*n)<0.5 or (pfo_torque*n)<0.5 or (pfo_power*n)<0.5 or (pfo_rpm*n)<0.5 or (pfo_draft_mean*n)<0.5 or  (pfo_rudder*n)<0.5 or (pfo_wind_dir*n)<0.5 or (pfo_heading*n)<0.5  ):
		print("yesssssssssssssssssssssssssssssss",str(row_data[0]))
		update_adadata_true_out = update(adadata).where(adadata.c.ID==str(row_data[0]))
		update_adadata_true_out=update_adadata_true_out.values(outlier='1')
		conn.execute(update_adadata_true_out)

	else:
		print("nooooooooooooooooooooooooooooooooooooooo",str(row_data[0]))
		update_false_out = update(adadata).where(adadata.c.ID==str(row_data[0]))
		update_false_out=update_false_out.values(outlier='0')
		conn.execute(update_false_out)

print("complteeeee")
