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
        engine = create_engine("mysql+pymysql://sarathlal:sarath@123@localhost/MTM")
        # engine = create_engine("mysql+pymysql://workfromhome:monitorcoronadementia@172.104.173.82/MTM")
        metadata =MetaData()
        metadata.reflect(bind = engine)
        conn = engine.connect()
except:
        print("Engine creation failed")
adadata = metadata.tables['ADA_DATA_MTM']
adadata_1hr = metadata.tables['ADA_DATA_1HR']
utc_param = conn.execute(select([adadata.c.ID,adadata.c.UTC_REPORT_DATE_TIME]).where(adadata.c.check_status==None).order_by(asc(adadata.c.UTC_REPORT_DATE_TIME))).fetchall()
# st='2019-09-24 10:06:17'
# ed='2019-09-24 11:06:17'
# utc_param = conn.execute(select([adadata.c.ID,adadata.c.UTC_REPORT_DATE_TIME]).where(adadata.c.UTC_REPORT_DATE_TIME.between(st,ed)).order_by(asc(adadata.c.UTC_REPORT_DATE_TIME))).fetchall()

# print("utc_time:",utc_param)
max_time = conn.execute(select([func.max(adadata.c.UTC_REPORT_DATE_TIME)])).fetchone()
# max_time= datetime.strptime('2019-09-24 11:06:17', '%Y-%m-%d %H:%M:%S')
print("max_time:",max_time)
start = utc_param[0][1]
end =  start + timedelta(minutes = 60)

check_blk=0
try:
	print("start time :",start)
	print("end time:",end)
	while(start<=max_time[0]):
		print("start time while:",start)
		print("end time:while",end)
		check_blk=check_blk+1
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
		power_lst=[]
		pi_lst=[]
		dct_data_insert={}
		ada_param = conn.execute(select( [adadata.c.SOG,adadata.c.STW,adadata.c.WIND_SPEED,adadata.c.TORQUE,adadata.c.RPM,adadata.c.DRAFT_MEAN,adadata.c.RUDDER_ANGLE,adadata.c.WIND_DIRECTION,adadata.c.HEADING,adadata.c.ID,adadata.c.UTC_REPORT_DATE_TIME,adadata.c.DRAFT_FORE,adadata.c.DRAFT_AFT,adadata.c.POSITION_LATITUDE,adadata.c.LATITUDE_N_S,adadata.c.POSITION_LONGITUDE,adadata.c.LONGITUDE_E_W,adadata.c.Corrected_Power,adadata.c.Ref_speed,adadata.c.P_I,adadata.c.VESSEL_id,adadata.c.validation,adadata.c.outlier,adadata.c.POWER]).where(and_(adadata.c.UTC_REPORT_DATE_TIME.between(start,end),adadata.c.validation!=None,adadata.c.outlier!=None)).order_by(asc(adadata.c.UTC_REPORT_DATE_TIME))).fetchall() 
		
		# print(ada_param)
		if not ada_param:
			print("No data")
			start=end
			end=end + timedelta(minutes = 60)
			continue
		else:
			for rslt_param in ada_param:
				
				update_adadata = update(adadata).where(adadata.c.ID==str(rslt_param[9]))
				update_adadata=update_adadata.values(check_status='True')
				conn.execute(update_adadata)
				if(rslt_param[0]!=None):
					sog_lst.append(rslt_param[0])
				if(rslt_param[1]!=None):
					
					stw_lst.append(rslt_param[1])
				if(rslt_param[2]!=None):
					
					wind_speed_lst.append(rslt_param[2])
				if(rslt_param[3]!=None):
					
					torque_lst.append(rslt_param[3])
				if(rslt_param[4]!=None):
					
					rpm_lst.append(rslt_param[4])
				if(rslt_param[5]!=None):
					
					draft_mean_lst.append(rslt_param[5])
				if(rslt_param[6]!=None):
					
					rudder_angle_lst.append(rslt_param[6])
				if(rslt_param[7]!=None):
						
					wind_direction_lst.append(rslt_param[7])
				if(rslt_param[8]!=None):
						
					heading_lst.append(rslt_param[8])

				rprt_datetime_lst.append(rslt_param[10])

				if(rslt_param[11]!=None):
					
					draft_for_lst.append(rslt_param[11])

				if(rslt_param[12]!=None):
					
					draft_aft_lst.append(rslt_param[12])
				if(rslt_param[23]!=None):
					
					power_lst.append(rslt_param[23])

				pos_lat_lst.append(rslt_param[13])
				lat_ns_lst.append(rslt_param[14])
				pos_long_lst.append(rslt_param[15])
				long_ew_lst.append(rslt_param[16])
				vessel=rslt_param[20]
				if(rslt_param[21]==0 and rslt_param[22]==0):
					if(rslt_param[17]!=None):
						# print("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
						corrctd_pwr_lst.append(rslt_param[17])
					
					if(rslt_param[18]!=None):
						ref_speed_lst.append(rslt_param[18])
		
						
					if(rslt_param[19]!=None):
						pi_lst.append(rslt_param[19])
					
						
				
				
				
				


				

		# print("soglst:",sog_lst)
		# print("stwlst:",stw_lst)
		# print("windspeedlst:",wind_speed_lst)
		# print("torqlst:",torque_lst)
		# print("rpmlst:",rpm_lst)
		# print("draft meanlst:",draft_mean_lst)
		# print("rudder anglelst:",rudder_angle_lst)
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
		# print("len:",len(sog_lst))
		# print("corrctd_pwr_lst:",corrctd_pwr_lst)
		rprt_datetime_lst_len=len(rprt_datetime_lst)
		pos_lat_lst_len=len(pos_lat_lst)
		lat_ns_lst_len=len(lat_ns_lst)
		pos_long_lst_len=len(pos_long_lst)
		long_ew_lst_len=len(long_ew_lst)
		# print("rprt_datetime_lst_len",rprt_datetime_lst_len)
		# print("last date:",rprt_datetime_lst[rprt_datetime_lst_len-1])

		last_rprt_date=rprt_datetime_lst[rprt_datetime_lst_len-1]
		last_pos_lat=pos_lat_lst[pos_lat_lst_len-1]
		last_lat_ns=lat_ns_lst[lat_ns_lst_len-1]
		last_pos_long=pos_long_lst[pos_long_lst_len-1]
		last_long_ew=long_ew_lst[long_ew_lst_len-1]
		if(not sog_lst):
			sog_mean=0
		else:
			sog_mean=statistics.mean(sog_lst)
		if(not stw_lst):
			stw_mean=0
		else:
			stw_mean=statistics.mean(stw_lst)
		if(not wind_speed_lst):
			wind_speed_mean=0
		else:
			wind_speed_mean=statistics.mean(wind_speed_lst)
		if(not torque_lst):
			torque_mean=0
		else:
			torque_mean=statistics.mean(torque_lst)
		if(not rpm_lst):
			rpm_mean=0
		else:
			rpm_mean=statistics.mean(rpm_lst)
		if(not draft_mean_lst):
			draft_mean_mn=0
		else:
			draft_mean_mn=statistics.mean(draft_mean_lst)
		if(not draft_for_lst):
			draft_for_mean=0
		else:
			draft_for_mean=statistics.mean(draft_for_lst)
		if(not draft_aft_lst):
			draft_aft_mean=0
		else:
			draft_aft_mean=statistics.mean(draft_aft_lst)
		if(not corrctd_pwr_lst):
			corrctd_pwr_mean=0
		else:
			corrctd_pwr_mean=statistics.mean(corrctd_pwr_lst)
		if(not ref_speed_lst):
			ref_speed_mean=0
		else:
			ref_speed_mean=statistics.mean(ref_speed_lst)
		if(not pi_lst):
			pi_mean=0
		else:
			pi_mean=statistics.mean(pi_lst)
		if(not power_lst):
			power_mean=0
		else:
			power_mean=statistics.mean(power_lst)
		if(not wind_direction_lst):
			wind_direction_mean=0
		else:
			wind_direction_mean=degrees(phase(sum(rect(1, radians(d)) for d in wind_direction_lst)/len(wind_direction_lst)))
		if(not heading_lst):
			heading_mean=0
		else:
			heading_mean=degrees(phase(sum(rect(1, radians(d)) for d in heading_lst)/len(heading_lst)))
		if(not rudder_angle_lst):
			rudder_mean=0
		else:
			rudder_mean=degrees(phase(sum(rect(1, radians(d)) for d in rudder_angle_lst)/len(rudder_angle_lst)))
		# print("rudder mean:",rudder_mean,type(rudder_mean))
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
		dct_data_insert['Corrected_Power']=corrctd_pwr_mean
		dct_data_insert['Ref_speed']=ref_speed_mean
		dct_data_insert['P_I']=pi_mean
		dct_data_insert['POWER']=power_mean
		dct_data_insert['RUDDER_ANGLE']=rudder_mean
		dct_data_insert['WIND_DIRECTION']=wind_direction_mean
		dct_data_insert['HEADING']=heading_mean
		print("dct:",dct_data_insert)
		insert_q = adadata_1hr.insert()
		conn.execute(insert_q,dct_data_insert)
		start=end
		end=end + timedelta(minutes = 60)
		print("last start:",start)
		print("last end:",end)
	print("No of blocks:",check_blk)

except:
        print("Try again. Something wend wrong")
finally:
	conn.close()
