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
import email
import getpass
import imaplib
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

try:
        engine = create_engine("mysql+pymysql://sarathlal:sarath@123@localhost/MTM")
        # engine = create_engine("mysql+pymysql://workfromhome:monitorcoronadementia@172.104.173.82/MTM")
        metadata =MetaData()
        metadata.reflect(bind = engine)
        conn = engine.connect()
except Exception as exception:
	print(exception)
	sys.exit()
adadata = metadata.tables['ADA_DATA_MTM_MVR']
adadata_4hr = metadata.tables['ADA_DATA_4HR_MVR']
def ada_4hr():
	ves_lst=[]
	db_vesls=conn.execute(select([adadata.c.VESSEL_id]).distinct()).fetchall()
	# rslt=db_ves.distinct()
	print("vess:",db_vesls)
	for ves in db_vesls:
		ves_lst.append(ves[0])
	print(ves_lst)
	for ves_id in ves_lst:
		print("vessel id:",ves_id)
		utc_param = conn.execute(select([adadata.c.ID,adadata.c.UTC_REPORT_DATE_TIME,adadata.c.check_status]).where(and_(adadata.c.check_status==None,adadata.c.VESSEL_id==str(ves_id))).order_by(asc(adadata.c.UTC_REPORT_DATE_TIME))).fetchall()
		# st='2019-09-24 10:06:17'
		# ed='2019-09-24 11:06:17'
		# utc_param = conn.execute(select([adadata.c.ID,adadata.c.UTC_REPORT_DATE_TIME]).where(adadata.c.UTC_REPORT_DATE_TIME.between(st,ed)).order_by(asc(adadata.c.UTC_REPORT_DATE_TIME))).fetchall()

		# print("utc_time:",utc_param)
		max_time = conn.execute(select([func.max(adadata.c.UTC_REPORT_DATE_TIME)]).where(adadata.c.VESSEL_id==str(ves_id))).fetchone()
		# max_time= datetime.strptime('2019-09-24 11:06:17', '%Y-%m-%d %H:%M:%S')
		print("max_time:",max_time)
		start = utc_param[0][1]
		end =  start + timedelta(minutes = 240)

		check_blk=0

		print("start time :",start)
		print("end time:",end)
		while(start<=max_time[0]):
			# print("start time while:",start)
			# print("end time:while",end)
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
			draft_pow_lst=[]
			draft_sp_pow_lst=[]
			fo_nor_lst=[]
			dct_data_insert={}
			ada_param = conn.execute(select( [adadata.c.SOG,adadata.c.STW,adadata.c.WIND_SPEED,adadata.c.TORQUE,adadata.c.RPM,adadata.c.DRAFT_MEAN,adadata.c.RUDDER_ANGLE,adadata.c.WIND_DIRECTION,adadata.c.HEADING,adadata.c.ID,adadata.c.UTC_REPORT_DATE_TIME,adadata.c.DRAFT_FORE,adadata.c.DRAFT_AFT,adadata.c.POSITION_LATITUDE,adadata.c.LATITUDE_N_S,adadata.c.POSITION_LONGITUDE,adadata.c.LONGITUDE_E_W,adadata.c.Corrected_Power,adadata.c.Ref_speed,adadata.c.P_I,adadata.c.VESSEL_id,adadata.c.validation,adadata.c.outlier,adadata.c.POWER,adadata.c.check_status,adadata.c.draft_pow,adadata.c.draft_sp_pow,adadata.c.fo_nor]).where(and_(adadata.c.UTC_REPORT_DATE_TIME.between(start,end),adadata.c.validation!=None,adadata.c.outlier!=None,adadata.c.check_status==None,adadata.c.VESSEL_id==str(ves_id))).order_by(asc(adadata.c.UTC_REPORT_DATE_TIME))).fetchall() 
			
			# print("ada_param:",ada_param)
			if not ada_param:
				print("No data")
				start=end
				end=end + timedelta(minutes = 240)
				continue
			else:
				for rslt_param in ada_param:
					# print("uid:",rslt_param[9])
					# print("vessel:",rslt_param[20])
					update_adadata = update(adadata).where(adadata.c.ID==str(rslt_param[9]))
					update_adadata=update_adadata.values(check_status='True')
					conn.execute(update_adadata)
					rprt_datetime_lst.append(rslt_param[10])

					if(rslt_param[21]==0 and rslt_param[22]==0):
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

						
						if(rslt_param[11]!=None):
							
							draft_for_lst.append(rslt_param[11])

						if(rslt_param[12]!=None):
							
							draft_aft_lst.append(rslt_param[12])
						if(rslt_param[23]!=None):
							
							power_lst.append(rslt_param[23])
						if(rslt_param[17]!=None):
							# print("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
							corrctd_pwr_lst.append(rslt_param[17])
						
						if(rslt_param[18]!=None):
							ref_speed_lst.append(rslt_param[18])
			
							
						if(rslt_param[19]!=None):
							pi_lst.append(rslt_param[19])

						if(rslt_param[25]!=None):
							draft_pow_lst.append(rslt_param[25])
						if(rslt_param[26]!=None):
							draft_sp_pow_lst.append(rslt_param[26])
						if(rslt_param[27]!=None):
							fo_nor_lst.append(rslt_param[27])
						
							
						pos_lat_lst.append(rslt_param[13])
						lat_ns_lst.append(rslt_param[14])
						pos_long_lst.append(rslt_param[15])
						long_ew_lst.append(rslt_param[16])
						
				# if(len(sog_lst)!=0 and len(stw_lst)!=0 and len(wind_speed_lst)!=0 and len(torque_lst)!=0 and len(draft_mean_lst)!=0 and len(rudder_angle_lst)!=0 and len(wind_direction_lst)!=0 and len(heading_lst)!=0 and len(draft_for_lst)!=0 and len(draft_aft_lst)!=0 and len(power_lst)!=0 and len(corrctd_pwr_lst)!=0 and len(ref_speed_lst)!=0 and len(pi_lst)!=0 and len(pos_lat_lst)!=0 and len(lat_ns_lst)!=0 and len(pos_long_lst)!=0 and len(long_ew_lst)!=0):
				
				rprt_datetime_lst_len=len(rprt_datetime_lst)

				pos_lat_lst_len=len(pos_lat_lst)
				print("pos_lat_lst_len:",pos_lat_lst_len)

				lat_ns_lst_len=len(lat_ns_lst)
				print("lat_ns_lst_len",lat_ns_lst_len)

				pos_long_lst_len=len(pos_long_lst)
				print("pos_long_lst_len",pos_long_lst_len)

				long_ew_lst_len=len(long_ew_lst)
				print("long_ew_lst_len",long_ew_lst_len)
				# print("rprt_datetime_lst_len",rprt_datetime_lst_len)
				# print("last date:",rprt_datetime_lst[rprt_datetime_lst_len-1])
				last_rprt_date=rprt_datetime_lst[rprt_datetime_lst_len-1]
				if(len(pos_lat_lst)!=0):
					last_pos_lat=pos_lat_lst[pos_lat_lst_len-1]
				else:
					last_pos_lat=0

				if(len(lat_ns_lst)!=0):
					last_lat_ns=lat_ns_lst[lat_ns_lst_len-1]
				else:
					last_lat_ns=0
				if(len(pos_long_lst)!=0):
					last_pos_long=pos_long_lst[pos_long_lst_len-1]
				else:
					last_pos_long=0
				if(len(long_ew_lst)!=0):
					last_long_ew=long_ew_lst[long_ew_lst_len-1]
				else:
					last_long_ew=0
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

				if(not draft_pow_lst):
					draft_pow_mean=0
				else:
					draft_pow_mean=statistics.mean(draft_pow_lst)

				if(not draft_sp_pow_lst):
					draft_sp_pow_mean=0
				else:
					draft_sp_pow_mean=statistics.mean(draft_sp_pow_lst)

				if(not fo_nor_lst):
					fo_nor_mean=0
				else:
					fo_nor_mean=statistics.mean(fo_nor_lst)

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
				dct_data_insert['Vessel_Name']=str(ves_id)
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

				dct_data_insert['draft_pow']=draft_pow_mean
				dct_data_insert['draft_sp_pow']=draft_sp_pow_mean
				dct_data_insert['fo_nor']=fo_nor_mean
				# print("dct:",dct_data_insert)
				insert_q = adadata_4hr.insert()
				conn.execute(insert_q,dct_data_insert)
			start=end
			end=end + timedelta(minutes = 240)
			print("last start23mm3:",start)
			print("last end:",end)
		print("No of blocks:",check_blk)

def status_alert(e):
	body = ''
	fromaddr = "alerts@xship.in"
	toaddrs = "script-status-alerts@xship.in"
	# toaddrs = "lekha@xship.in"

	#toaddrs = "anjali@xship.in"
	#cc = "syamk@xship.in,shyamp@xship.in,sarathlal@xship.in,lekha@xship.in,sriram@xship.in"

	msg = MIMEMultipart()

	msg['From'] = fromaddr
	msg['To'] = toaddrs

	msg['Subject'] = "MTM adadata 4hr calculation status alert"
	body ="MTM adadata 4hr calculation is  not completed due to the error :-\n\n"+str(e)
	msg.attach(MIMEText(body, 'plain'))
	s = smtplib.SMTP('outlook.office365.com', 587)
	s.starttls()
	s.login(fromaddr, "navgathi@12*")
	text = msg.as_string()
	try:
		s.sendmail(fromaddr,toaddrs, text)
	except:
		print("SMTP server connection error")
	    
	s.quit()      

if __name__ == "__main__":
	try:
		ada_4hr()
	except Exception as e:
		status_alert(e)
		print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
	finally:
		conn.close()
