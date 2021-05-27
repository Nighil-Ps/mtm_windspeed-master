from sqlalchemy import create_engine,MetaData, select,update
from sqlalchemy.orm import Session

# import numpy
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
        #engine = create_engine("mysql+pymysql://sarathlal:sarath@123@172.104.173.82/MTM")
        engine = create_engine("mysql+pymysql://workfromhome:monitorcoronadementia@172.104.173.82/MTM")
        metadata =MetaData()
        metadata.reflect(bind = engine)
        conn = engine.connect()
except Exception as exception:
	print(exception)
	sys.exit()
adadata = metadata.tables['ADA_DATA_MTM']
adadata_1hr = metadata.tables['ADA_DATA_1HR']
# utc_param = conn.execute(select([adadata.c.ID,adadata.c.UTC_REPORT_DATE_TIME]).order_by(asc(adadata.c.UTC_REPORT_DATE_TIME))).fetchall()
st='2020-03-30 09:32:00'
ed='2020-03-30 10:32:00'
utc_param = conn.execute(select([adadata.c.ID,adadata.c.UTC_REPORT_DATE_TIME]).where(adadata.c.UTC_REPORT_DATE_TIME.between(st,ed)).order_by(asc(adadata.c.UTC_REPORT_DATE_TIME))).fetchall()

# print("utc_time:",utc_param)
max_time = conn.execute(select([func.max(adadata.c.UTC_REPORT_DATE_TIME)])).fetchone()
print("max_time:",max_time)
start = utc_param[0][1]
print("start time:",start)
end =  start + timedelta(minutes = 60)
print("end time:",end)
s=0
e=1
try:
	
		
	sog_lst=[]
	stw_lst=[]
	wind_speed_lst=[]
	torque_lst=[]
	rpm_lst=[]
	draft_mean_lst=[]
	rudder_angle_lst=[]
	wind_direction_lst=[]
	heading_lst=[]
	id_lst=[]
	rprt_datetime_lst=[]
	draft_for_lst=[]
	draft_aft_lst=[]
	pos_lat_lst=[]
	lat_ns_lst=[]
	pos_long_lst=[]
	long_ew_lst=[]
	corrctd_pwr_lst=[]
	ref_speed_lst=[]
	pi_lst=[]
	dct_data_insert={}
	ada_param = conn.execute(select( [adadata.c.SOG,adadata.c.STW,adadata.c.WIND_SPEED,adadata.c.TORQUE,adadata.c.RPM,adadata.c.DRAFT_MEAN,adadata.c.RUDDER_ANGLE,adadata.c.WIND_DIRECTION,adadata.c.HEADING,adadata.c.ID,adadata.c.UTC_REPORT_DATE_TIME,adadata.c.DRAFT_FORE,adadata.c.DRAFT_AFT,adadata.c.POSITION_LATITUDE,adadata.c.LATITUDE_N_S,adadata.c.POSITION_LONGITUDE,adadata.c.LONGITUDE_E_W,adadata.c.Corrected_Power,adadata.c.Ref_speed,adadata.c.P_I,adadata.c.VESSEL_id,adadata.c.validation,adadata.c.outlier]).where(adadata.c.UTC_REPORT_DATE_TIME.between(start,end)).order_by(asc(adadata.c.UTC_REPORT_DATE_TIME))).fetchall() 
	
	# print(ada_param)
	if not ada_param:
		print("nottttttttttttttttttttttttttt")
		start=end
		end=end + timedelta(minutes = 60)
		pass
	else:
		for rslt_param in ada_param:
			print("enterrrrrrrrrrrrrrrrrrrrrrrrrrrrr")
			sog_lst.append(rslt_param[0])
			stw_lst.append(rslt_param[1])
			wind_speed_lst.append(rslt_param[2])
			torque_lst.append(rslt_param[3])
			
			rpm_lst.append(rslt_param[4])
			draft_mean_lst.append(rslt_param[5])
			rudder_angle_lst.append(rslt_param[6])
			wind_direction_lst.append(rslt_param[7])
			heading_lst.append(rslt_param[8])
			rprt_datetime_lst.append(rslt_param[10])
			draft_for_lst.append(rslt_param[11])
			draft_aft_lst.append(rslt_param[12])
			pos_lat_lst.append(rslt_param[13])
			lat_ns_lst.append(rslt_param[14])
			pos_long_lst.append(rslt_param[15])
			long_ew_lst.append(rslt_param[16])
			vessel=rslt_param[20]
			if(rslt_param[21]==0 and rslt_param[22]==0):
				corrctd_pwr_lst.append(rslt_param[17])
				ref_speed_lst.append(rslt_param[18])
				pi_lst.append(rslt_param[19])
			else:
				corrctd_pwr_lst.append(0)
				ref_speed_lst.append(0)
				pi_lst.append(0)

	# print("soglst:",sog_lst)
	# print("stwlst:",stw_lst)
	# print("windspeedlst:",wind_speed_lst)
	# print("torqlst:",torque_lst)
	# print("rpmlst:",rpm_lst)
	# print("draft meanlst:",draft_mean_lst)
	print("rudder anglelst:",rudder_angle_lst)
	# print("headinglst:",heading_lst)
	# print("rprt date timelst:",rprt_datetime_lst)
	# print("draft forlst:",draft_for_lst)
	# print("draft aftlst:",draft_aft_lst)
	# print("pos latitude lst:",pos_lat_lst)
	# print("latitude  ns lst:",lat_ns_lst)
	# print("pos longitude lst:",pos_long_lst)
	# print("longitude  ewlst:",long_ew_lst)
	# print("corrected pwr lst:",corrctd_pwr_lst)
	# print("ref speed lst:",ref_speed_lst)
	# print("pi lst:",pi_lst)
	rprt_datetime_lst_len=len(rprt_datetime_lst)
	pos_lat_lst_len=len(pos_lat_lst)
	lat_ns_lst_len=len(lat_ns_lst)
	pos_long_lst_len=len(pos_long_lst)
	long_ew_lst_len=len(long_ew_lst)
	print("rprt_datetime_lst_len",rprt_datetime_lst_len)
	print("last date:",rprt_datetime_lst[rprt_datetime_lst_len-1])

	last_rprt_date=rprt_datetime_lst[rprt_datetime_lst_len-1]
	last_pos_lat=pos_lat_lst[pos_lat_lst_len-1]
	last_lat_ns=lat_ns_lst[lat_ns_lst_len-1]
	last_pos_long=pos_long_lst[pos_long_lst_len-1]
	last_long_ew=long_ew_lst[long_ew_lst_len-1]

	sog_mean=statistics.mean(sog_lst)
	stw_mean=statistics.mean(stw_lst)
	wind_speed_mean=statistics.mean(wind_speed_lst)
	torque_mean=statistics.mean(torque_lst)
	rpm_mean=statistics.mean(rpm_lst)
	draft_mean_mn=statistics.mean(draft_mean_lst)
	draft_for_mean=statistics.mean(draft_for_lst)
	draft_aft_mean=statistics.mean(draft_aft_lst)
	corrctd_pwr_mean=statistics.mean(corrctd_pwr_lst)
	ref_speed_mean=statistics.mean(ref_speed_lst)
	pi_mean=statistics.mean(pi_lst)
	wind_direction_mean=degrees(phase(sum(rect(1, radians(d)) for d in wind_direction_lst)/len(wind_direction_lst)))
	heading_mean=degrees(phase(sum(rect(1, radians(d)) for d in heading_lst)/len(heading_lst)))
	rudder_mean=degrees(phase(sum(rect(1, radians(d)) for d in rudder_angle_lst)/len(rudder_angle_lst)))
	print("rudder mean:",rudder_mean,type(rudder_mean))
	dct_data_insert['Vessel_Name']=vessel
	dct_data_insert['UTC_REPORT_DATE_TIME']=last_rprt_date
	dct_data_insert['POSITION_LATITUDE']=last_pos_lat
	dct_data_insert['LATITUDE_N_S']=last_lat_ns
	dct_data_insert['POSITION_LONGITUDE']=last_pos_long
	dct_data_insert['LONGITUDE_E_W']=last_long_ew
	dct_data_insert['SOG']=sog_mean
	dct_data_insert['STW']=stw_mean
	dct_data_insert['WIND_SPEED']=wind_speed_mean
	dct_data_insert['TORQUE']=torque_mean
	dct_data_insert['RPM']=rpm_mean
	dct_data_insert['DRAFT_MEAN']=draft_mean_mn
	dct_data_insert['DRAFT_FORE']=draft_for_mean
	dct_data_insert['DRAFT_AFT']=draft_aft_mean
	dct_data_insert['Corrected_power']=corrctd_pwr_mean
	dct_data_insert['Ref_speed']=ref_speed_mean
	dct_data_insert['P_I']=pi_mean
	dct_data_insert['RUDDER_ANGLE']=rudder_mean
	dct_data_insert['WIND_DIRECTION']=wind_direction_mean
	dct_data_insert['HEADING']=heading_mean
	insert_q = adadata_1hr.insert()
	conn.execute(insert_q,dct_data_insert)

except Exception as exception:
	print(exception)
	

finally:
	conn.close()
