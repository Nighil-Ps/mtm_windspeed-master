from sqlalchemy import create_engine,MetaData, select,update
from sqlalchemy.orm import Session
# import pandas as pd 
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
from decouple import config

try:
        #engine = create_engine("mysql+pymysql:",config('user'),":",config('password'),"@",config('adadata_localhost'),"/",config('database'))
        engine = create_engine("mysql+pymysql://",config('iso_calc_user'),":",config('iso_calc_localhost'),"/",config('database'))
        # engine = create_engine("mysql+pymysql://",config('adadata_user'),":",config('adadata_password'),"@",config('adadata_localhost'),"/",config('database'))
        metadata =MetaData()
        metadata.reflect(bind = engine)
        conn = engine.connect()
except Exception as exception:
	print(exception)
	sys.exit()
adadata = metadata.tables['ADA_DATA_MTM']
#noon_parameter = conn.execute(select([adadata.c.VESSEL_id,adadata.c.SOG,adadata.c.DRAFT_AFT,adadata.c.DRAFT_FORE,adadata.c.STW,adadata.c.RPM,adadata.c.POWER,adadata.c.WIND_DIRECTION,adadata.c.WIND_SPEED,adadata.c.HEADING,adadata.c.ID])).fetchall()#.where(adadata.c.UID == 6).where(adadata.c.UID == 1).where(adadata.c.CAA == 0)


"""valid_param = conn.execute(select( [adadata.c.SOG,adadata.c.STW,adadata.c.WIND_SPEED,adadata.c.TORQUE,adadata.c.POWER,adadata.c.RPM,adadata.c.DRAFT_MEAN,adadata.c.RUDDER_ANGLE,adadata.c.WIND_DIRECTION,adadata.c.HEADING,adadata.c.ID,adadata.c.UTC_REPORT_DATE_TIME]).order_by(asc(adadata.c.UTC_REPORT_DATE_TIME))).fetchall() 
print("valid_param:",valid_param)"""
def validation():
	ves_lst=[]
	now = datetime.now()
	print("runnning time:",now)
	# db_vesls=conn.execute(select([adadata.c.VESSEL_id]).distinct()).fetchall()
	# # rslt=db_ves.distinct()
	# print("vess:",db_vesls)
	# for ves in db_vesls:
	# 	ves_lst.append(ves[0])
	# print("vessel list::",ves_lst)
	ves_lst=['MKW']
	for ves_id in ves_lst:
		print("VESSEL:",ves_id)
		utc_param = conn.execute(select([adadata.c.ID,adadata.c.UTC_REPORT_DATE_TIME,adadata.c.VESSEL_id]).where(and_(adadata.c.VESSEL_id==str(ves_id),adadata.c.validation == None ,adadata.c.outlier == None )).order_by(asc(adadata.c.UTC_REPORT_DATE_TIME))).fetchall()
		# utc_param = conn.execute(select([adadata.c.ID,adadata.c.UTC_REPORT_DATE_TIME,adadata.c.VESSEL_id]).where(and_(adadata.c.ID=='5878235',adadata.c.validation == None ,adadata.c.outlier == None )).order_by(asc(adadata.c.UTC_REPORT_DATE_TIME))).fetchall()
		print("utc param:",utc_param)
		print("nexrtttttttttttttttttttt********************************8")
		# id_lst=[]
		# time_lst=[]
		# for rslt in utc_param:
		# 	id_lst.append(rslt[0])
		# 	time_lst.append(rslt[1])


		max_time = conn.execute(select([func.max(adadata.c.UTC_REPORT_DATE_TIME)]).where(adadata.c.VESSEL_id==str(ves_id))).fetchone()
		print("max_time:",max_time)
		start = utc_param[0][1]
		end =  start + timedelta(minutes = 10)
		print("start tym:",start)
		print("end tym:",end)
		while(start<=max_time[0]):
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
			valid_param = conn.execute(select( [adadata.c.SOG,adadata.c.STW,adadata.c.WIND_SPEED,adadata.c.TORQUE,adadata.c.POWER,adadata.c.RPM,adadata.c.DRAFT_MEAN,adadata.c.RUDDER_ANGLE,adadata.c.WIND_DIRECTION,adadata.c.HEADING,adadata.c.ID,adadata.c.UTC_REPORT_DATE_TIME,adadata.c.DRAFT_FORE,adadata.c.DRAFT_AFT,adadata.c.VESSEL_id]).where(and_(adadata.c.UTC_REPORT_DATE_TIME.between(start,end),adadata.c.validation==None,adadata.c.outlier==None,adadata.c.VESSEL_id==str(ves_id))).order_by(asc(adadata.c.UTC_REPORT_DATE_TIME))).fetchall() 
			print("valid param:",valid_param)
			n=len(valid_param)
			if not valid_param:
				print("not valid parammmmmmmmmm")
				start=end
				end=end + timedelta(minutes = 10)
				continue
			else:

				print("yes valid param")
				for rslt_valid_param in valid_param:
					print("draftfor:",rslt_valid_param[12])
					print("drftaft:",rslt_valid_param[13])
					print("rpm:",rslt_valid_param[5])
					if(rslt_valid_param[12]!=0 and rslt_valid_param[13]!=0 and rslt_valid_param[5]!=None):
						print("yes draftttttttttttttttttttttttttttttttttttttttt")
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
						# print("rpm list:",rpm_lst)
						# if(rslt_valid_param[0]!=None):
						# 	sog_lst.append(rslt_valid_param[0])
						# if(rslt_valid_param[1]!=None):
						# 	stw_lst.append(rslt_valid_param[1])
						# if(rslt_valid_param[2]!=None):
						# 	wind_speed_lst.append(rslt_valid_param[2])
						# if(rslt_valid_param[3]!=None):
						# 	torque_lst.append(rslt_valid_param[3])
						# if(rslt_valid_param[4]!=None):
						# 	power_lst.append(rslt_valid_param[4])
						# if(rslt_valid_param[5]!=None):
						# 	rpm_lst.append(rslt_valid_param[5])
						# else:
						# 	rpm_lst.append(0)
						# if(rslt_valid_param[6]!=None):
						# 	draft_mean_lst.append(rslt_valid_param[6])
						# if(rslt_valid_param[7]!=None):
						# 	rudder_angle_lst.append(rslt_valid_param[7])
						# if(rslt_valid_param[8]!=None):
						# 	wind_direction_lst.append(rslt_valid_param[8])
						# if(rslt_valid_param[9]!=None):
						# 	heading_lst.append(rslt_valid_param[9])
						# if(rslt_valid_param[10]!=None):
						# 	id_lst.append(rslt_valid_param[10])
					else:
						print("no draftttttttttttttttttttttttttttttttttttttt")
						continue
				

					# print("sog list:",sog_lst)
					# print("stw list:",stw_lst)
					# print("wspeed list:",wind_speed_lst)
					# print("torque list:",torque_lst)
					# print("power list:",power_lst)
					# print("rpm list:",rpm_lst)
					# print("draft_mean_lst list:",draft_mean_lst)
					# print("rudder_angle list:",rudder_angle_lst)
					# print("winddir ist:",wind_direction_lst)

					# print("heading list:",heading_lst)
					# print("id list:",id_lst)
					

				# if(rslt_valid_param[12]!=0 and rslt_valid_param[13]!=0):
				if(len(sog_lst)!=0 and len(stw_lst)!=0 and len(wind_speed_lst)!=0 and len(torque_lst)!=0 and len(power_lst)!=0 and len(rpm_lst)!=0 and len(draft_mean_lst)!=0):
					print("list length satisfied................")
					sog_mean=statistics.mean(sog_lst)
					stw_mean=statistics.mean(stw_lst)
					wind_speed_mean=statistics.mean(wind_speed_lst)
					torque_mean=statistics.mean(torque_lst)
					power_mean=statistics.mean(power_lst)
					rpm_mean=statistics.mean(rpm_lst)
					draft_mean_mn=statistics.mean(draft_mean_lst)

					

					for sog in sog_lst:
						sog_diff=abs(sog-sog_mean)
						sog_diff_lst.append(sog_diff)
					
					for stw in stw_lst:
						stw_diff=abs(stw-stw_mean)
						stw_diff_lst.append(stw_diff)
					for wind_speed  in wind_speed_lst:
						wind_speed_diff=abs(wind_speed - wind_speed_mean)
						wind_speed_diff_lst.append(wind_speed_diff)

					for torque in torque_lst:
						torque_diff= abs(torque -torque_mean)
						torque_diff_lst.append(torque_diff)
					for power in power_lst:
						power_diff= abs(power - power_mean)
						power_diff_lst.append(power_diff)
					for rpm in rpm_lst:
						rpm_diff= abs(rpm- rpm_mean)
						rpm_diff_lst.append(rpm_diff)


					for draft in draft_mean_lst:
						draft_diff= abs(draft - draft_mean_mn)
						draft_diff_lst.append(draft_diff)
					

					sog_std_error=statistics.pstdev(sog_lst)
					stw_std_error=statistics.pstdev(stw_lst)
					wind_speed_std_error=statistics.pstdev(wind_speed_lst)
					torque_std_error=statistics.pstdev(torque_lst)
					power_std_error=statistics.pstdev(power_lst)
					draft_mean_std_error=statistics.pstdev(draft_mean_lst)
					rpm_std_error=statistics.pstdev(rpm_lst)
					rudder_mean=degrees(phase(sum(rect(1, radians(d)) for d in rudder_angle_lst)/len(rudder_angle_lst)))
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
					for wind_direction in wind_direction_lst:
						wind_direction_diff= abs(wind_direction - wind_direction_mean)
						ri=wind_direction_diff%360
						if(ri>180):
							del_f=360-ri
						else:
							del_f=ri
						wind_direction_diff_lst.append(del_f)
						
					wind_direction_std_error=sqrt(1/n *(sum(map(lambda x:x*x,wind_direction_diff_lst))))

					heading_mean=degrees(phase(sum(rect(1, radians(d)) for d in heading_lst)/len(heading_lst)))
					for heading in heading_lst:
						heading_diff= abs(heading- heading_mean)
						ri=heading_diff%360
						if(ri>180):
							del_f=360-ri
						else:
							del_f=ri
						
						heading_diff_lst.append(del_f)
					 
					heading_std_error=sqrt(1/n *(sum(map(lambda x:x*x,heading_diff_lst))))
					
					if(sog_std_error>0.5 or stw_std_error >0.5 or rpm_std_error>3 or rudder_std_error>1):
						x=1
						
					else:
						x=0
					print("x:",x)	
					if(x==1):
						
						#data_ids = conn.execute('SELECT ID FROM ADA_DATA_MTM_bkup WHERE  DATE(inserted_time)=DATE("2019-10-24 ") ORDER BY UTC_REPORTED_TIME DESC LIMIT 60 ')
						#data_ids = conn.execute('SELECT ID FROM ADA_DATA_MTM_bkup ')
						for data_id in id_lst:
							update_true = update(adadata).where(adadata.c.ID==data_id)
							update_true=update_true.values(validation='1')
							conn.execute(update_true)
							print("update validation uid1:",data_id)
					        
						
					else:
						for data_id in id_lst:
							update_false = update(adadata).where(adadata.c.ID==data_id)
							update_false=update_false.values(validation='0')
							conn.execute(update_false)
							print("update validation uid0:",data_id)
							
						

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
					# print("*************************************8")
					# print("outlier_lst:",outlier_lst)
					# print("length:",len(outlier_lst[0]))
					# for x in range(len(outlier_lst[0])):

					# 	for item in outlier_lst:
					# 		print("item:",item)
					# 		print("x:",x)
					# 		print("itemxxxxx:",item[x])

					lst1 = [[item[x] for item in outlier_lst] for x in range(len(outlier_lst[0]))]
					# print("lsttttttttt1:",lst1)
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
								
						if((pfo_sog*n)<0.5 or (pfo_stw*n)<0.5 or (pfo_wind_speed*n)<0.5 or (pfo_torque*n)<0.5 or (pfo_power*n)<0.5 or (pfo_rpm*n)<0.5 or (pfo_draft_mean*n)<0.5 or  (pfo_rudder*n)<0.5 or (pfo_wind_dir*n)<0.5 or (pfo_heading*n)<0.5  ):
							update_adadata_true_out = update(adadata).where(adadata.c.ID==str(row_data[0]))
							update_adadata_true_out=update_adadata_true_out.values(outlier='1')
							conn.execute(update_adadata_true_out)
							print("update outlier uid1:",row_data[0])

						else:
							update_false_out = update(adadata).where(adadata.c.ID==str(row_data[0]))
							update_false_out=update_false_out.values(outlier='0')
							conn.execute(update_false_out)
							print("update outlier uid0:",row_data[0])

						sog_stw_diff=abs(row_data[1]-row_data[2])
				
			start=end
			end=end + timedelta(minutes = 10)
			print("complteeeee")
		
def status_alert(e):
	body = ''
	fromaddr = config('frmaddrs')
	toaddrs = config('toaddrs')
	# toaddrs = "lekha@xship.in"
	#toaddrs = "anjali@xship.in"
	#cc = "syamk@xship.in,shyamp@xship.in,sarathlal@xship.in,lekha@xship.in,sriram@xship.in"

	msg = MIMEMultipart()

	msg['From'] = fromaddr
	msg['To'] = toaddrs

	msg['Subject'] = "MTM ADA DATA Validation status check alert"
	body ="MTM ADA DATA Validation is  not completed due to the error :-\n\n"+str(e)
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
	# validation()
	try:
		validation()
	except Exception as e:
		status_alert(e)
		print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
