from sqlalchemy import create_engine,MetaData,select,update,text,and_,desc,func
from sqlalchemy.orm import Session
# import pandas as pd 
# import numpy
import csv
import math
# import pdb; pdb.set_trace()
import email
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from sqlalchemy.sql import text
# from datetime import datetime
import datetime
from decouple import config

def ais_iso_calc():
	
	validation_dct={}
	ves_dtls_vesls_lst=[]
	ves_dtls_vesls=conn_dss.execute(select([vessel_details.c.MMSI])).fetchall()	
	for ves in ves_dtls_vesls:
		ves_dtls_vesls_lst.append(ves[0])
	# print(ves_dtls_vesls_lst)
	mmsi_str= ','.join(ves_dtls_vesls_lst)
	print("format_strings:",mmsi_str)
	# ais_rslt=text("""select MMSI,Last_pos_speed,Last_pos_draught,wind_direction,wind_speed,Last_pos_heading,id,wave_height,wave_direction,swell_height,swell_direction,current_speed,current_direction from SpireSense_API where MMSI in ("""+mmsi_str+""") and Last_pos_time_stamp>='2021-04-23 00:00:00' and R_Wind is NULL""")
	ais_rslt=text("""select MMSI,Last_pos_speed,Last_pos_draught,wind_direction,wind_speed,Last_pos_heading,id,wave_height,wave_direction,swell_height,swell_direction,current_speed,current_direction from SpireSense_API where MMSI in ("""+mmsi_str+""") and Last_pos_time_stamp>='2021-04-23 00:00:00' """)
	# print("query:",ais_rslt)
	ais_parameter=conn_ais.execute(ais_rslt).fetchall()
	
	
	for obj_ais in ais_parameter:
		if(obj_ais[1]!=None):
			if(float(obj_ais[1])>1):
				# print(obj_ais)
				if(obj_ais[0] in ves_dtls_vesls_lst):
					
					print("mmsi......:",obj_ais[0])
					
					dct_isoparams_db = {}
					lst_true_wind_speed = []

					iso_params = conn_dss.execute(select([iso_parameter.c.design_draft,iso_parameter.c.breadth,iso_parameter.c.ref_area,iso_parameter.c.ALV,iso_parameter.c.HC,	iso_parameter.c.CMC,iso_parameter.c.AOD,iso_parameter.c.HBR,iso_parameter.c.AXV,iso_parameter.c.LOA,iso_parameter.c.LBP,iso_parameter.c.B,	iso_parameter.c.SC_draft,iso_parameter.c.anemoht,iso_parameter.c.z_ref,iso_parameter.c.a_b,iso_parameter.c.b_b,iso_parameter.c.a_sc,iso_parameter.c.b_sc,iso_parameter.c.disp_b,iso_parameter.c.disp_sc,iso_parameter.c.LBWL]).where(iso_parameter.c.MMSI == obj_ais[0])).fetchall()
					print("iso_param:",iso_params)
					draft_mean = float(obj_ais[2])
					iso_b = float(iso_params[0][11])
					iso_design_draft = float(iso_params[0][0])
					iso_breadth = float(iso_params[0][1])
					iso_ref_area = float(iso_params[0][2])
					iso_lbp = float(iso_params[0][10])
					iso_anemoht = float(iso_params[0][13])
					iso_z_ref = float(iso_params[0][14])
					iso_loa = float(iso_params[0][9])
					iso_aod = float(iso_params[0][6])
					iso_cmc = float(iso_params[0][5])
					iso_a_b = float(iso_params[0][15])
					iso_b_b = float(iso_params[0][16])
					iso_a_sc = float(iso_params[0][17])
					iso_b_sc = float(iso_params[0][18])
					iso_disp_b = float(iso_params[0][19])
					iso_disp_sc = float(iso_params[0][20])
					air_density = 1.25
					iso_LBWL = float(iso_params[0][21])
					if(obj_ais[1]==None):
						iso_noon_sog=0
					else:
						iso_noon_sog = float(obj_ais[1])

					# iso_noon_power = obj_ais[6]
					# iso_noon_displacement = obj_ais[4]
					iso_deltat = float(iso_params[0][12]) - draft_mean
					if iso_deltat < 0:
						continue
					rad_to_deg = float(3.14/180)
					iso_area = float(iso_params[0][8]) + (iso_deltat*iso_b)


					if iso_params:
						if iso_breadth and iso_breadth and iso_design_draft:
							draft_change = iso_design_draft - float(draft_mean)
							projarea_i = iso_breadth + iso_breadth + float(draft_change)
							
						if iso_params[0][8] and iso_b and iso_params[0][12] and iso_params[0][7] and iso_params[0][3] and iso_params[0][10] and iso_params[0][4]:
							iso_axv = iso_area#float(iso_params[0][8]) + float(iso_b) * (float(iso_params[0][12]) - float(draft_mean))
							iso_hbr = float(iso_params[0][7]) + iso_deltat#(float(iso_params[0][12]) - float(draft_mean))
							iso_alv = float(iso_params[0][3]) + iso_lbp * iso_deltat# (float(iso_params[0][12]) - float(draft_mean))
							iso_hci = (float(iso_params[0][3])*float(iso_params[0][4])+(0.5* iso_lbp * iso_deltat**2))/(float(iso_params[0][3])+(iso_lbp * iso_deltat)) 



						
						
						if(obj_ais[4]!=None and obj_ais[3]!=None and obj_ais[5]!=None and obj_ais[1]!=None and obj_ais[5]!='None'):
							vwtref = true_wind_ref(float(obj_ais[4]),iso_anemoht,iso_z_ref,float(iso_params[0][12]),draft_mean,iso_axv,iso_b)
							# print("vwtref:",vwtref)
							# print("rad_to_deg:",rad_to_deg)
							# print("obj_ais[1]:",obj_ais[1])
							# print("obj_ais[3]:",obj_ais[3])
							# print("obj_ais[5]:",obj_ais[5])

							lst_rel_wind_speed = relativewindspeed(vwtref,float(obj_ais[1]),float(obj_ais[3]),float(obj_ais[5]),rad_to_deg)
							

							rel_win_dir_corr = lst_rel_wind_speed[1]
							rel_win_speed_corr = lst_rel_wind_speed[0]
							# print("rel_win_dir_corr:",rel_win_dir_corr)
							# print("rel_win_speed_corr:",rel_win_speed_corr)
							dct_isoparams_db['Relative_wind_speed'] = rel_win_speed_corr
							dct_isoparams_db['Relative_wind_direction'] = rel_win_dir_corr

							########....Fujiwara method for finding CAA.#####
							isoData_CLF_90_180 = -0.018 + 5.091 * (float(iso_b)/iso_loa) + (-10.367 * (iso_hci/iso_loa)) + (3.011 * (iso_aod/iso_loa**2)) + (0.341 * iso_axv/iso_b	**2)
							isoData_CXLI_90_180 = 1.901 + (-12.727 * iso_alv)/(iso_loa *iso_hbr) + (-24.407 * iso_axv/iso_alv) + (40.310 * (iso_b/iso_loa)) + ((5.481 * iso_axv)/(	iso_b*iso_hbr))
							isoData_CALF_90_180  = 0.314 + ((1.117 * iso_aod) / iso_alv)
							isoData_CLF_0_90 = 0.922 + ((-0.507 * iso_alv)/(iso_loa * iso_b)) + (-1.162 * iso_cmc/iso_loa)
							isoData_CXLI_0_90 = -0.458 + ((-3.245 * iso_alv)/(iso_loa * iso_hbr)) + ((2.313 * iso_axv)/(iso_b * iso_hbr))
							
							isoData_CALF_0_90 = 0.585 + (0.906 * (iso_aod/iso_alv)) + (-3.239 * iso_b/iso_loa)
							#if cond_n to be correct
							if rel_win_dir_corr >= 0 and rel_win_dir_corr < 90:
								isoData_wind_coeff = isoData_CLF_0_90 * math.cos(math.radians(rel_win_dir_corr)) + isoData_CXLI_0_90 * (math.sin(math.radians(rel_win_dir_corr)) -	 (0.5 * math.sin(math.radians(rel_win_dir_corr)) * (math.cos(math.radians(rel_win_dir_corr)))**2)) * math.sin(math.radians(rel_win_dir_corr)) *	 math.cos(math.radians(rel_win_dir_corr))+ isoData_CALF_0_90 * math.sin(math.radians(rel_win_dir_corr)) * (math.cos(math.radians(	rel_win_dir_corr))**3)
							elif rel_win_dir_corr > 90 and rel_win_dir_corr <= 180:
								isoData_wind_coeff = isoData_CLF_90_180 * math.cos(math.radians(rel_win_dir_corr)) + isoData_CXLI_90_180 * (math.sin(math.radians(rel_win_dir_corr	)) - 0.5 * math.sin(math.radians(rel_win_dir_corr)) *(math.cos(math.radians(rel_win_dir_corr)))**2) * math.sin(math.radians(rel_win_dir_corr)) * math.cos(math.radians(rel_win_dir_corr)	) + isoData_CALF_90_180 * math.sin(math.radians(rel_win_dir_corr)) * (math.cos(math.radians(rel_win_dir_corr)))**3
				
							elif rel_win_dir_corr == 90:
								isoData_wind_coeff = (0.5 * (isoData_CLF_0_90 * math.cos(math.radians(80)) + isoData_CXLI_0_90 * (math.sin(math.radians(80)) - 0.5 * math.sin(math	.radians(80)) * (math.cos(math.radians(80)))**2) * math.sin(math.radians(80)) * math.cos(math.radians(80)) + isoData_CALF_0_90 * math.sin(math	.radians(80)) * (math.cos(math.radians(80)))**3 + isoData_CLF_90_180 * math.cos(math.radians(100)) + isoData_CXLI_90_180 * (math.sin(math.	radians(100)) - 0.5 * math.sin(math.radians(100)) * (math.cos(math.radians(100)))**2) * math.sin(math.radians(100)) * math.cos(math.radians(100)) + isoData_CALF_90_180 * math.sin(math.radians(100)) * (math.cos(math.radians(100)))**3))
				
							
							isoData_wind_coeff_heading = (0.922 + (-0.507 *  iso_alv)/( iso_loa *  iso_b) + (-1.162 *  iso_cmc/ iso_loa))*math.cos(math.radians(0))
							# dct_isoparams_db['CAA'] = isoData_wind_coeff
							# print("CAA:",dct_isoparams_db['CAA'])
							##### Corrected Power and Reference Speed #####
							isoData_res_resistance = (.5 * air_density * iso_axv * rel_win_speed_corr**2 * isoData_wind_coeff) - (.5 * air_density * iso_axv * (iso_noon_sog*0.5144)**2 *  isoData_wind_coeff_heading)
							# import pdb 
							# pdb.set_trace()
							if(obj_ais[5]==None):
								COG_ship=0
							else:
								COG_ship=float(obj_ais[5])
							if(obj_ais[7]==None):
								wave_height=0
							else:
								wave_height=float(obj_ais[7])
							if(obj_ais[8]==None):
								wave_direction=0
							else:
								wave_direction=float(obj_ais[8])
							if(obj_ais[9]==None):
								swell_height=0
							else:
								swell_height=float(obj_ais[9])
							if(obj_ais[10]==None):
								swell_direction=0
							else:
								swell_direction=float(obj_ais[10])

							if(obj_ais[11]==None):
								current_speed=0
							else:
								current_speed=float(obj_ais[11])/.5144
							if(obj_ais[12]==None):
								current_direction=0
							else:
								current_direction=float(obj_ais[12])
							Relative_Wave_dir=abs(abs(COG_ship-wave_direction)-180)
							Relative_swell_dir=abs(abs(COG_ship-swell_direction)-180)
							Relative_current_dir=abs(COG_ship-current_direction)
							dct_isoparams_db['R_Wind'] = isoData_res_resistance
							print("R_Wind:",dct_isoparams_db['R_Wind'])
							
							if(Relative_Wave_dir<45 and iso_LBWL!=0):
							   dct_isoparams_db['R_Wave']=1/16*(iso_b*9.81*1025*wave_height*wave_height)*(iso_b/iso_LBWL)**0.5
							   
							else:
							   dct_isoparams_db['R_Wave']=0
							   
							if(Relative_swell_dir<45 and iso_LBWL!=0):
							   dct_isoparams_db['R_Swell']=1/32*(iso_b*9.81*1025*swell_height*swell_height)*(iso_b/iso_LBWL)**0.5
							   
							else:
							   dct_isoparams_db['R_Swell']=0  
							   
							dct_isoparams_db['STW']=(iso_noon_sog**2+current_speed**2+2*iso_noon_sog*current_speed*math.cos(math.radians(Relative_current_dir)))**0.5   
							
							dct_isoparams_db['R_Total']=dct_isoparams_db['R_Wave']+dct_isoparams_db['R_Swell']+dct_isoparams_db['R_Wind']
							try:
								
								
								update_noon_iso = update(AISdata).where(AISdata.c.id == obj_ais[6])
								update_noon_iso = update_noon_iso.values(dct_isoparams_db)
								
								conn_ais.execute(update_noon_iso)
								
							except Exception as e:
								continue
					else:
						pass
						
				
def true_wind_ref(twind,za,zref,tref,t,Aref,B):

	deltat = tref-t
	zaref=10
	Area=Aref+deltat*B
	a = Aref*(zref+deltat)
	b = 0.5*B*deltat**2
	# zref = 4.1
	zref=(Aref*(zref+deltat)+0.5*B*deltat**2)/Area
	z_za = (zref/zaref)
	vwtref = twind*(float(z_za)**float(1/7))
	return vwtref	


def relativewindspeed(vwtref,speed,truewinddir,cog,rad_to_deg):

	lst_result = []
	windspeed=vwtref#*0.5144
	speed=speed*0.5144
	vwrref = math.sqrt(windspeed**2 + speed**2 + (2 * windspeed * speed * math.cos(math.radians (truewinddir-cog))))
	lst_result.append(vwrref)
	cond_n = (windspeed*math.cos(math.radians(truewinddir-cog)))+speed
	numeratorcase = windspeed*math.sin(math.radians(truewinddir-cog))

	relwinddir =math.degrees(math.atan2(numeratorcase,cond_n))
	if relwinddir < 0:
		relwinddir = -relwinddir

	lst_result.append(relwinddir)
	return lst_result

def noon_iso_avg_calc():
	# noon_param=conn_dss.execute(select([NOONDATA.c.UID,NOONDATA.c.Vessel_Name,NOONDATA.c.REPORT_DATE_TIME,NOONDATA.c.TimeZone]).where(and_(NOONDATA.c.REPORT_DATE_TIME>='2021-04-22 00:00:00',NOONDATA.c.ais_R_Wind==None))).fetchall()
	
	noon_param=conn_dss.execute(select([NOONDATA.c.UID,NOONDATA.c.Vessel_Name,NOONDATA.c.REPORT_DATE_TIME,NOONDATA.c.TimeZone]).where(NOONDATA.c.REPORT_DATE_TIME>='2021-04-22 00:00:00')).fetchall()
	# print("noon_param:",noon_param)
	for obj_noon in noon_param:
		print("VESSEL:",obj_noon[1])
		mmsi=conn_dss.execute(select([vessel_details.c.MMSI]).where(vessel_details.c.id==obj_noon[1])).fetchall()
		print("mmsi:",mmsi)
		# print("current_rprt_date:",obj_noon[2])
		# rprt_date_time=conn_dss.execute(select([NOONDATA.c.REPORT_DATE_TIME]).where(NOONDATA.c.Vessel_Name==obj_noon[0]).order_by(desc(NOONDATA.c.REPORT_DATE_TIME)).limit(2)).fetchall()
		pre_rprt_date_time=conn_dss.execute(select([NOONDATA.c.REPORT_DATE_TIME,NOONDATA.c.TimeZone]).where(and_(NOONDATA.c.Vessel_Name==obj_noon[1],NOONDATA.c.REPORT_DATE_TIME<obj_noon[2])).order_by(desc(NOONDATA.c.REPORT_DATE_TIME)).limit(1)).fetchall()
		print("pre REPORT_DATE_TIME:",pre_rprt_date_time)
		# print("pre dte tym:",rprt_date_time)

		if(obj_noon[3] is None or obj_noon[3]==''):
			
			gmt_hr=0
			gmt_min=0
			rprt_time=obj_noon[2] + datetime.timedelta(hours=int(gmt_hr),minutes=int(gmt_min))
			rprt_time=datetime.datetime.strftime(rprt_time,'%Y-%m-%d %H:%M:%S')

			pre_rprt_time=pre_rprt_date_time[0][0] + datetime.timedelta(hours=int(gmt_hr),minutes=int(gmt_min))
			pre_rprt_time=datetime.datetime.strftime(pre_rprt_time,'%Y-%m-%d %H:%M:%S')
			


		else:
			gmt=obj_noon[3][4:].strip(' ')
			# print("gmt:",gmt)
			gmt_split=gmt.split(':')
			# print("gmt_split:",gmt_split)
			sign=gmt_split[0][0]
			# print("gmtsign",sign)
			gmt_hr=gmt_split[0][1:].strip(' ')
			gmt_min=gmt_split[1].strip(' ')
			# print(" current gmt from noon:",gmt)
			# gmt_hr=gmt[1]
			# gmt_min=gmt[3]
			print("current gmt hr:",gmt_hr)
			print("current gmt hr:",gmt_min)
			if(sign=='+'):
				rprt_time=obj_noon[2] - datetime.timedelta(hours=int(gmt_hr),minutes=int(gmt_min))
				rprt_time=datetime.datetime.strftime(rprt_time,'%Y-%m-%d %H:%M:%S')

			elif(sign=='-'):
				rprt_time=obj_noon[2] + datetime.timedelta(hours=int(gmt_hr),minutes=int(gmt_min))
				rprt_time=datetime.datetime.strftime(rprt_time,'%Y-%m-%d %H:%M:%S')

			else:
				pass
		if(pre_rprt_date_time[0][1] is None or pre_rprt_date_time[0][1]==''):
			pre_gmt_hr=0
			pre_gmt_min=0
			pre_rprt_time=pre_rprt_date_time[0][0] + datetime.timedelta(hours=int(pre_gmt_hr),minutes=int(pre_gmt_min))
			pre_rprt_time=datetime.datetime.strftime(pre_rprt_time,'%Y-%m-%d %H:%M:%S')

		else:
			# print(" pre reprt date time from noon:",pre_rprt_date_time)
			pre_gmt=pre_rprt_date_time[0][1][4:]
			pre_sign=pre_rprt_date_time[0][1][0]
			# print("gmt from pre noon:",pre_gmt)
			pre_gmt_split=pre_gmt.split(':')
			# print("gmt_split:",pre_gmt_split)
			pre_sign=pre_gmt_split[0][0]
			# print("gmtsign",pre_sign)
			pre_gmt_hr=pre_gmt_split[0][1:]
			pre_gmt_min=pre_gmt_split[1]
			# print(" pre gmt hr:",pre_gmt_hr)
			# print(" pre gmt min:",pre_gmt_min)

			
			if(pre_sign=='+'):
				pre_rprt_time=pre_rprt_date_time[0][0] - datetime.timedelta(hours=int(pre_gmt_hr),minutes=int(pre_gmt_min))
				pre_rprt_time=datetime.datetime.strftime(pre_rprt_time,'%Y-%m-%d %H:%M:%S')
			elif(pre_sign=='-'):
				pre_rprt_time=pre_rprt_date_time[0][0] + datetime.timedelta(hours=int(pre_gmt_hr),minutes=int(pre_gmt_min))
				pre_rprt_time=datetime.datetime.strftime(pre_rprt_time,'%Y-%m-%d %H:%M:%S')

			else:
				pass
			# print("rprt_time af gmt:",rprt_time,type(rprt_time))
			# print("pre rprt time af gmt :",pre_rprt_time,type(pre_rprt_time))
			
			# ais_iso_rslts=conn_ais.execute(select([func.avg(AISdata.c.Relative_wind_speed),func.avg(AISdata.c.Relative_wind_direction),func.avg(AISdata.c.CAA),func.avg(AISdata.c.RW),func.avg(AISdata.c.Significantheightofcombinedwindwavesandswellmsl),func.avg(AISdata.c.wind_wave_height_cw),func.avg(AISdata.c.swell_wave_height_cw)]).where(and_(AISdata.c.MMSI==mmsi[0][0],AISdata.c.Last_pos_time_stamp.between(pre_rprt_time,rprt_time)))).fetchall()

			qry=text("""select AVG(R_Wind),AVG(R_Wave),AVG(R_Swell),AVG(STW),AVG(R_Total),AVG(wave_height),AVG(swell_height),AVG(Last_pos_speed),AVG(wind_speed),AVG(Relative_wind_speed),AVG(Relative_wind_direction) from SpireSense_API where MMSI="""+mmsi[0][0]+""" and Last_pos_maneuver = 0 and Last_pos_speed>4 and Last_pos_time_stamp between '"""+pre_rprt_time+"""' and '"""+rprt_time+"""' """)
			# print("qry:",qry)
			ais_iso_rslts=conn_ais.execute(qry).fetchall()

			# print("ais data:",ais_iso_rslts)
			insert_dct={}
			for iso_rslt in ais_iso_rslts:
				insert_dct['ais_R_Wind']=iso_rslt[0]
				insert_dct['ais_R_Wave']=iso_rslt[1]
				insert_dct['ais_R_Swell']=iso_rslt[2]
				insert_dct['ais_STW']=iso_rslt[3]
				insert_dct['ais_R_Total']=iso_rslt[4]
				insert_dct['wind_wave_height']=iso_rslt[5]
				insert_dct['swell_wave_height']=iso_rslt[6]
				insert_dct['ais_speed']=iso_rslt[7]
				insert_dct['ais_wind_speed']=iso_rslt[8]
				insert_dct['ais_Relative_wind_speed']=iso_rslt[9]
				insert_dct['ais_Relative_wind_direction']=iso_rslt[10]
				
			print("insert:",insert_dct)
			
			print("uid:",obj_noon[0])
			ais_iso=update(NOONDATA).where(NOONDATA.c.UID==obj_noon[0])
			update_ais_iso=ais_iso.values(insert_dct)
			conn_dss.execute(update_ais_iso)

def weather_iso_calc():
	validation_dct={}
	ves_dtls_vesls_lst=[]
	ves_dtls_vesls=conn_dss.execute(select([vessel_details.c.id])).fetchall()	
	for ves in ves_dtls_vesls:
		ves_dtls_vesls_lst.append(ves[0])
	# print(ves_dtls_vesls_lst)

	# noon_parameter = conn_dss.execute(select([NOONDATA.c.Vessel_Name,NOONDATA.c.SOG,NOONDATA.c.DRAFT_AFT,NOONDATA.c.DRAFT_FWD,NOONDATA.c.DISPLACEMENT,NOONDATA.c.RPM,NOONDATA.c.POWER_kW,NOONDATA.c.Weather_True_Wind_Direction,NOONDATA.c.Weather_True_Wind_Speed,NOONDATA.c.COURSE_AT_SEA,NOONDATA.c.UID,NOONDATA.c.FUEL_M_E_HS,NOONDATA.c.FUEL_M_E_LS,NOONDATA.c.FUEL_M_E_MDO,NOONDATA.c.FUEL_M_E_MGO_HS,NOONDATA.c.FUEL_M_E_MGO_LS,NOONDATA.c.M_E_FUEL_ONLY_STEAMING_TIME,NOONDATA.c.ais_STW,NOONDATA.c.Weather_Wave_Height,NOONDATA.c.Weather_Wave_Direction,NOONDATA.c.ais_R_Total,NOONDATA.c.Power_corrected,NOONDATA.c.FO_per_24Hrs]).where(and_(NOONDATA.c.REPORT_DATE_TIME>='2020-04-22 00:00:00',NOONDATA.c.ais_PI==None))).fetchall()
	noon_parameter = conn_dss.execute(select([NOONDATA.c.Vessel_Name,NOONDATA.c.SOG,NOONDATA.c.DRAFT_AFT,NOONDATA.c.DRAFT_FWD,NOONDATA.c.DISPLACEMENT,NOONDATA.c.RPM,NOONDATA.c.POWER_kW,NOONDATA.c.Weather_True_Wind_Direction,NOONDATA.c.Weather_True_Wind_Speed,NOONDATA.c.COURSE_AT_SEA,NOONDATA.c.UID,NOONDATA.c.FUEL_M_E_HS,NOONDATA.c.FUEL_M_E_LS,NOONDATA.c.FUEL_M_E_MDO,NOONDATA.c.FUEL_M_E_MGO_HS,NOONDATA.c.FUEL_M_E_MGO_LS,NOONDATA.c.M_E_FUEL_ONLY_STEAMING_TIME,NOONDATA.c.ais_STW,NOONDATA.c.Weather_Wave_Height,NOONDATA.c.Weather_Wave_Direction,NOONDATA.c.ais_R_Total,NOONDATA.c.Power_corrected,NOONDATA.c.FO_per_24Hrs]).where(NOONDATA.c.REPORT_DATE_TIME>='2020-04-22 00:00:00')).fetchall()
	print("noon data:",noon_parameter)
	
	for obj_noon in noon_parameter:
		if(obj_noon[0] in ves_dtls_vesls_lst):
			try:
				for chk_noon in obj_noon:
					# print(chk_noon)
					if chk_noon is None:
						raise Exception()
				
			except Exception:
				continue
			
				
		if obj_noon [1] and obj_noon[4] and obj_noon[5] and obj_noon[6] and obj_noon[2] and obj_noon[3]:
			print("if......:",obj_noon[10])
			# if obj_noon[0] not in ves_dtls_vesls_lst:
			# 	print("vessel not found")
			# 	continue
			dct_isoparams_db = {}
			lst_true_wind_speed = []

			iso_params = conn_dss.execute(select([iso_parameter.c.design_draft,iso_parameter.c.breadth,iso_parameter.c.ref_area,iso_parameter.c.ALV,iso_parameter.c.HC,	iso_parameter.c.CMC,iso_parameter.c.AOD,iso_parameter.c.HBR,iso_parameter.c.AXV,iso_parameter.c.LOA,iso_parameter.c.LBP,iso_parameter.c.B,	iso_parameter.c.SC_draft,iso_parameter.c.anemoht,iso_parameter.c.z_ref,iso_parameter.c.a_b,iso_parameter.c.b_b,iso_parameter.c.a_sc,iso_parameter.c.b_sc,iso_parameter.c.disp_b,iso_parameter.c.disp_sc,iso_parameter.c.f1,iso_parameter.c.f2,iso_parameter.c.k,iso_parameter.c.disp_16,iso_parameter.c.speed_nor,iso_parameter.c.draft_nor,iso_parameter.c.Speedcoef]).where(iso_parameter.c.Vessel_id == obj_noon[0])).fetchall()#
			#iso_params = conn_dss.execute(select([ISOPARAMETER.c.design_draft,ISOPARAMETER.c.breadth,ISOPARAMETER.c.ref_area,ISOPARAMETER.c.ALV,ISOPARAMETER.c.HC,ISOPARAMETER.c.CMC,ISOPARAMETER.c.AOD,ISOPARAMETER.c.HBR,ISOPARAMETER.c.AXV,ISOPARAMETER.c.LOA,ISOPARAMETER.c.LBP,ISOPARAMETER.c.B,ISOPARAMETER.c.SC_draft,ISOPARAMETER.c.anemoht,ISOPARAMETER.c.z_ref,ISOPARAMETER.c.a_b,ISOPARAMETER.c.b_b,ISOPARAMETER.c.a_sc,ISOPARAMETER.c.b_sc,ISOPARAMETER.c.disp_b,ISOPARAMETER.c.disp_sc,ISOPARAMETER.c.f1,ISOPARAMETER.c.f2,ISOPARAMETER.c.k,ISOPARAMETER.c.disp_16,ISOPARAMETER.c.speed_nor,ISOPARAMETER.c.draft_nor,ISOPARAMETER.c.Sp_coef,ISOPARAMETER.c.p1,ISOPARAMETER.c.p2,ISOPARAMETER.c.pk]).where(ISOPARAMETER.c.Vessel_id == obj_noon[0])).fetchall()#


			draft_mean = (float(obj_noon[2]) + float(obj_noon[3]))/2
			isoData_res_resistance = float(obj_noon[20])

			iso_b = float(iso_params[0][11])
			iso_design_draft = float(iso_params[0][0])
			iso_breadth = float(iso_params[0][1])
			iso_ref_area = float(iso_params[0][2])
			iso_lbp = float(iso_params[0][10])
			iso_anemoht = float(iso_params[0][13])
			iso_z_ref = float(iso_params[0][14])
			iso_loa = float(iso_params[0][9])
			iso_aod = float(iso_params[0][6])
			iso_cmc = float(iso_params[0][5])
			iso_a_b = float(iso_params[0][15])
			iso_b_b = float(iso_params[0][16])
			iso_a_sc = float(iso_params[0][17])
			iso_b_sc = float(iso_params[0][18])
			iso_disp_b = float(iso_params[0][19])
			iso_disp_sc = float(iso_params[0][20])
			air_density = 1.25
			iso_noon_sog = float(obj_noon[1])
			iso_noon_stw = float(obj_noon[17])
			print("iso_noon_stw:",iso_noon_stw)
			if iso_noon_stw <= 0:
				print("iso_noon_stw <= 0")
				continue

			iso_noon_power = obj_noon[6]
			iso_noon_displacement = obj_noon[4]
			iso_deltat = float(iso_params[0][12]) - draft_mean
			print("iso_deltat:",iso_deltat)
			if iso_deltat < 0:
				print("iso_deltat < 0")

				continue
			rad_to_deg = float(3.14/180)
			iso_area = float(iso_params[0][8]) + (iso_deltat*iso_b)
			iso_fuel_me_hs = float(obj_noon[11])
			
			try:
				iso_fuel_me_ls = float(obj_noon[12])
			except Exception as TypeError:
				iso_fuel_me_ls = 0

			iso_fuel_me_mdo = float(obj_noon[13])
			iso_fuel_me_mgo_hs = float(obj_noon[14])
			iso_fuel_me_mgo_ls = float(obj_noon[15])
			power_corr=float(obj_noon[21])
			iso_f1 = float(iso_params[0][21])
			iso_f2 = float(iso_params[0][22])
			iso_k = float(iso_params[0][23])
			iso_disp_16 = float(iso_params[0][24])
			iso_speed_nor = float(iso_params[0][25])
			iso_draft_nor = float(iso_params[0][26])
			iso_sp_nor = float(iso_params[0][27])
			fo_per_24hrs=float(obj_noon[22])
			# iso_p1 = float(iso_params[0][28])
			# iso_p2 = float(iso_params[0][29])
			# iso_pk = float(iso_params[0][30])
			iso_me_fuel_only_steaming_time = float(obj_noon[16])
			print("iso_me_fuel_only_steaming_time:",iso_me_fuel_only_steaming_time)
			if iso_me_fuel_only_steaming_time == 0:
				print("iso_me_fuel_only_steaming_time==0")
				continue
			# dct_isoparams_db['FO_per_24Hrs'] = (iso_fuel_me_hs + iso_fuel_me_ls + iso_fuel_me_mdo + iso_fuel_me_mgo_hs + iso_fuel_me_mgo_ls)*24/iso_me_fuel_only_steaming_time
			dct_isoparams_db['FO_per_24Hrs'] = fo_per_24hrs
			print("fo per24:",dct_isoparams_db['FO_per_24Hrs'])
			if dct_isoparams_db['FO_per_24Hrs'] <= 0:
				print("FO_per_24Hrs'] <= 0")
				continue
				
			print("restnce:",isoData_res_resistance,type(isoData_res_resistance))
			# dct_isoparams_db['ais_RW'] = isoData_res_resistance
			ref_power = (((1/iso_a_b) * iso_noon_stw)**(1/iso_b_b)) - ((((1/iso_a_b) * iso_noon_stw)**(1/iso_b_b)) - (((1/iso_a_sc) * iso_noon_stw)**(1/iso_b_sc))) * ((iso_noon_displacement-iso_disp_b) / (iso_disp_sc-iso_disp_b))
			isoData_corr_power = iso_noon_power - (isoData_res_resistance * iso_noon_stw*0.5144 /700)
			dct_isoparams_db['ais_Corrected_power'] = isoData_corr_power
			dct_isoparams_db['ais_draft_pow'] = isoData_corr_power+((((1/iso_a_sc) * iso_noon_stw)**(1/iso_b_sc)) - (((1/iso_a_b) * iso_noon_stw)**(1/iso_b_b)))/(iso_disp_sc-iso_disp_b)*(iso_disp_16-iso_noon_displacement)
			draft_sp_pow = dct_isoparams_db['ais_draft_pow']*(iso_speed_nor/iso_noon_stw)**iso_sp_nor
			dct_isoparams_db['ais_fo_nor'] = (iso_f1*draft_sp_pow**2)+(iso_f2*draft_sp_pow)+iso_k
			ref_power_corrected = ref_power - (isoData_res_resistance * iso_noon_stw*0.5144 /700)
			
			ref_FO_corrected =(iso_f1*ref_power_corrected**2)+(iso_f2*ref_power_corrected)+iso_k

			dct_isoparams_db["ais_speed_performance"] = (ref_power_corrected*100)/iso_noon_power
			dct_isoparams_db["ais_Propulsive_eff"] = (ref_FO_corrected*100)/dct_isoparams_db['FO_per_24Hrs']
			
			isoData_refcurv_speed = ((iso_a_b * isoData_corr_power**iso_b_b - ((iso_a_b * isoData_corr_power**iso_b_b) - (iso_a_sc * isoData_corr_power**iso_b_sc)) * ((	iso_noon_displacement-iso_disp_b) / (iso_disp_sc-iso_disp_b))))

			
			print("isoData_refcurv_speed :",isoData_refcurv_speed)
			if isoData_refcurv_speed == 0:
				print("isoData_refcurv_speed == 0:")
				continue
			dct_isoparams_db['ais_Ref_speed'] = isoData_refcurv_speed
			# dct_isoparams_db['ais_RW'] = isoData_res_resistance
			# dct_isoparams_db['Power_ME_Model']=(iso_p1*dct_isoparams_db['FO_per_24Hrs']**2)+(iso_p2*dct_isoparams_db['FO_per_24Hrs'])+iso_pk
			dct_isoparams_db['Power_ME_Model']=power_corr
			isoData_corr_power_chk = dct_isoparams_db['Power_ME_Model'] - (isoData_res_resistance * iso_noon_stw*0.5144 /700)
			ais_draft_pow_chk = isoData_corr_power_chk+((((1/iso_a_sc) * iso_noon_stw)**(1/iso_b_sc)) - (((1/iso_a_b) * iso_noon_stw)**(1/iso_b_b)))/(iso_disp_sc-iso_disp_b)*(iso_disp_16-iso_noon_displacement)
			dct_isoparams_db['NPP_Nor_pow'] = ais_draft_pow_chk*(iso_speed_nor/iso_noon_stw)**iso_sp_nor
			
			try:
				print("UID:",obj_noon[10])
				isoData_perf_value = (iso_noon_stw-isoData_refcurv_speed) * 100/isoData_refcurv_speed
				dct_isoparams_db['ais_PI'] = isoData_perf_value
				print("insertion dct:",dct_isoparams_db)
				update_noon_iso = update(NOONDATA).where(NOONDATA.c.UID == obj_noon[10])
				update_noon_iso = update_noon_iso.values(dct_isoparams_db)
				
				conn_dss.execute(update_noon_iso)
				
			except Exception as e:
				print("except:",e)
				continue
		else:
			print("condn not satisfied")
			continue
def status_alert(e):
	body = ''
	fromaddr = config('frmaddrs')
	toaddrs = config('toaddrs')
	#toaddrs = "lekha@xship.in"
	#cc = "syamk@xship.in,shyamp@xship.in,sarathlal@xship.in,lekha@xship.in,sriram@xship.in"

	msg = MIMEMultipart()

	msg['From'] = fromaddr
	msg['To'] = toaddrs

	msg['Subject'] = "MTM AIS iso status alert"
	body ="MTM iso calculation from ais is  not completed due to the error :-\n\n"+str(e)
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
	engine_dss_new = create_engine("mysql+pymysql://",config('user'),":",config('password'),"@",config('adadata_localhost'),"/",config('database'))
	
	# engine = create_engine("mysql+pymysql://",config('user'),":",config('password'),"@",config('adadata_localhost'),"/",config('pil_database'))
	metadata_dss_new =MetaData()
	metadata_dss_new.reflect(bind = engine_dss_new)
	conn_dss = engine_dss_new.connect()
	engine_ais = create_engine("mysql+pymysql://",config('user'),":",config('password'),"@",config('adadata_localhost'),"/",config('ais_database'))
	metadata_ais =MetaData()
	metadata_ais.reflect(bind = engine_ais)
	conn_ais = engine_ais.connect()
	AISdata = metadata_ais.tables['SpireSense_API']
	NOONDATA=metadata_dss_new.tables['NOONDATA']
	vessel_details=metadata_dss_new.tables['VESSELDETAIL']
	# validation=metadata_dss_new.tables['ISO_VALIDATION']
	iso_parameter= metadata_dss_new.tables['ISOPARAMETER']
	# ais_iso_calc()
	# noon_iso_avg_calc()
	weather_iso_calc()
	# try:
	# 	ais_iso_calc()
	# 	noon_iso_avg_calc()
	# 	weather_iso_calc()
		
		
	# except Exception as e:
	# 	print("exception")
	# 	status_alert(e)
	
