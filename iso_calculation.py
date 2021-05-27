from sqlalchemy import create_engine,MetaData, select,update
from sqlalchemy.orm import Session
import pandas as pd 
import numpy
import csv
import math
# import pdb; pdb.set_trace()


def iso_data_fetch_calc():
	engine = create_engine("mysql+pymysql://phpmyadmin:distancemonopetri@localhost/MTM")
	metadata =MetaData()
	metadata.reflect(bind = engine)
	conn = engine.connect()
	noondata = metadata.tables['NOONDATA']
	noon_parameter = conn.execute(select([noondata.c.Vessel_Name,noondata.c.SOG,noondata.c.DRAFT_AFT,noondata.c.DRAFT_FWD,noondata.c.DISPLACEMENT,noondata.c.RPM,noondata.c.Power_corrected,noondata.c.WindDirection,noondata.c.WindSpeed_kn,noondata.c.COURSE_AT_SEA,noondata.c.UID,noondata.c.FUEL_M_E_HS,noondata.c.FUEL_M_E_LS,noondata.c.FUEL_M_E_MDO,noondata.c.FUEL_M_E_MGO_HS,noondata.c.FUEL_M_E_MGO_LS,noondata.c.M_E_FUEL_ONLY_STEAMING_TIME])).fetchall()#.where(noondata.c.UID == 6).where(noondata.c.UID == 1).where(noondata.c.CAA == 0)
	iso_parameter = metadata.tables['ISOPARAMETER']
	# import pdb; pdb.set_trace()
	print(noon_parameter)
	for obj_noon in noon_parameter:
		try:
			for chk_noon in obj_noon:
				print(chk_noon)
				if chk_noon is None:
					raise Exception()
		except Exception:
			continue

		print("noonobj:",obj_noon[10])
		lst_obj_noon = list(obj_noon)

		print("noonparameter list:",lst_obj_noon)
		if obj_noon[10] == 163815 or obj_noon[10] == 163815 and obj_noon[10] == 83899:
			continue
		iso_ves = ['SAL','SHA','SVE','SEN','SEQ','SPL','SUN','SYN']
		if obj_noon[1] and obj_noon[4] and obj_noon[6]:
			print(obj_noon[0])
			if not obj_noon[0] in iso_ves: 
				continue
			dct_isoparams_db = {}
			lst_true_wind_speed = []
			iso_params = conn.execute(select([iso_parameter.c.design_draft,iso_parameter.c.breadth,iso_parameter.c.ref_area,iso_parameter.c.ALV,iso_parameter.c.HC,	iso_parameter.c.CMC,iso_parameter.c.AOD,iso_parameter.c.HBR,iso_parameter.c.AXV,iso_parameter.c.LOA,iso_parameter.c.LBP,iso_parameter.c.B,	iso_parameter.c.SC_draft,iso_parameter.c.anemoht,iso_parameter.c.z_ref,iso_parameter.c.a_b,iso_parameter.c.b_b,iso_parameter.c.a_sc,iso_parameter.c.	b_sc,iso_parameter.c.disp_b,iso_parameter.c.disp_sc,iso_parameter.c.f1,iso_parameter.c.f2,iso_parameter.c.k,iso_parameter.c.disp_16]).where(iso_parameter.c.Vessel_id ==obj_noon[0] )).fetchall()#
			print("noonparameter list:",lst_obj_noon)
			print("iso_params",iso_params)
	
			draft_mean = (obj_noon[2] + obj_noon[3])/2
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
			iso_noon_sog = obj_noon[1]
			iso_noon_power = float(obj_noon[6])
			iso_noon_displacement = obj_noon[4]
			iso_deltat = float(iso_params[0][12]) - draft_mean
			rad_to_deg = float(3.14/180)
			iso_area = float(iso_params[0][8]) + (iso_deltat*iso_b)
			iso_fuel_me_hs = float(obj_noon[11])
			try:
				iso_fuel_me_ls = float(obj_noon[12])
			except Exception as TypeError:
				iso_fuel_me_ls = 0
			
			iso_fuel_me_mdo = float(obj_noon[13])
			iso_fuel_me_mgo_hs = float(obj_noon[14])
			print("fuel_mgo_ls:",obj_noon[15])
			iso_fuel_me_mgo_ls = float(obj_noon[15])

			iso_f1 = float(iso_params[0][21])
			iso_f2 = float(iso_params[0][22])
			iso_k = float(iso_params[0][23])
			iso_disp_16 = float(iso_params[0][24])
			iso_me_fuel_only_steaming_time = float(obj_noon[16])
			print("fuel_only_steaming time:::::",iso_me_fuel_only_steaming_time)
			if(iso_me_fuel_only_steaming_time!=0 and iso_me_fuel_only_steaming_time < 30 ):
				dct_isoparams_db['FO_per_24Hrs'] = (iso_fuel_me_hs + iso_fuel_me_ls + iso_fuel_me_mdo + iso_fuel_me_mgo_hs + iso_fuel_me_mgo_ls)*24/iso_me_fuel_only_steaming_time
				print("FO_per_24Hrs:",dct_isoparams_db['FO_per_24Hrs'])
				dct_isoparams_db['SFOC'] = (dct_isoparams_db['FO_per_24Hrs']*10**6)/(24*iso_noon_power)
				print("SFOC:",dct_isoparams_db['SFOC'])
				
			
			# import pdb 
			# pdb.set_trace()
			if iso_params:
				if iso_breadth and iso_breadth and iso_design_draft:
					draft_change = iso_design_draft - float(draft_mean)
					projarea_i = iso_breadth + iso_breadth + float(draft_change)
					
				if iso_params[0][8] and iso_b and iso_params[0][12] and iso_params[0][7] and iso_params[0][3] and iso_params[0][10] and iso_params[0][4]:
					iso_axv = iso_area#float(iso_params[0][8]) + float(iso_b) * (float(iso_params[0][12]) - float(draft_mean))
					iso_hbr = float(iso_params[0][7]) + iso_deltat#(float(iso_params[0][12]) - float(draft_mean))
					iso_alv = float(iso_params[0][3]) + iso_lbp * iso_deltat# (float(iso_params[0][12]) - float(draft_mean))
					iso_hci = (float(iso_params[0][3])*float(iso_params[0][4])+(0.5* iso_lbp * iso_deltat**2))/(float(iso_params[0][3])+(iso_lbp * iso_deltat)) 

				
				# print(obj_noon[7])
				if obj_noon[7] is None or obj_noon[1] is None or iso_params[0][12] is None or obj_noon[8] is None or obj_noon[9] is None or obj_noon[7] == '' or iso_me_fuel_only_steaming_time > 30 :
					continue
				lst_true_wind_speed = true_wind_speed_calc(float(obj_noon[8]),float(obj_noon[1]),float(obj_noon[7]),float(obj_noon[9]),rad_to_deg)
				dct_isoparams_db["True_wind_speed"] = lst_true_wind_speed[0]
				print("True_wind_speed:",dct_isoparams_db['True_wind_speed'])
				dct_isoparams_db["True_wind_dir"] = lst_true_wind_speed[1]
				print("True_wind_dir:",dct_isoparams_db['True_wind_dir'])
				dct_isoparams_db["condn"] = lst_true_wind_speed[2]
				print("condn:",dct_isoparams_db['condn'])
				dct_isoparams_db["numeratorcase"] = lst_true_wind_speed[3]
				print("numeratorcase:",dct_isoparams_db['numeratorcase'])
				vwtref = true_wind_ref(lst_true_wind_speed[0],iso_anemoht,iso_z_ref,float(iso_params[0][12]),draft_mean,iso_axv,iso_b)
				
				lst_rel_wind_speed = relativewindspeed(vwtref,obj_noon[1],lst_true_wind_speed[1],obj_noon[9],rad_to_deg)
				
				dct_isoparams_db["ref_power"] = (((1/iso_a_b) * iso_noon_sog)**(1/iso_b_b)) - ((((1/iso_a_b) * iso_noon_sog)**(1/iso_b_b)) - (((1/iso_a_sc) * iso_noon_sog)**(1/iso_b_sc))) * ((iso_noon_displacement-iso_disp_b) / (iso_disp_sc-iso_disp_b))
				print("ref_power:",dct_isoparams_db['ref_power'])
				dct_isoparams_db["ref_fuel"] = (iso_f1*dct_isoparams_db["ref_power"]**2)+(iso_f2*dct_isoparams_db["ref_power"])+iso_k
				print("ref_fuel:",dct_isoparams_db['ref_fuel'])
				if(iso_me_fuel_only_steaming_time!=0 and iso_me_fuel_only_steaming_time < 30 ):
					dct_isoparams_db["fuel_loss"] = (dct_isoparams_db['FO_per_24Hrs']-dct_isoparams_db["ref_fuel"])*100/dct_isoparams_db["ref_fuel"]
					print("fuel_loss:",dct_isoparams_db['fuel_loss'])
				rel_win_dir_corr = lst_rel_wind_speed[1]
				rel_win_speed_corr = lst_rel_wind_speed[0]
				
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
					print("isoData_wind_coeff,0-90::::",isoData_wind_coeff)
				elif rel_win_dir_corr > 90 and rel_win_dir_corr <= 180:				
					lst_rel_wind_speed = relativewindspeed(vwtref,obj_noon[1],lst_true_wind_speed[1],obj_noon[9],rad_to_deg)
				
					dct_isoparams_db["ref_power"] = (((1/iso_a_b) * iso_noon_sog)**(1/iso_b_b)) - ((((1/iso_a_b) * iso_noon_sog)**(1/iso_b_b)) - (((1/iso_a_sc) * iso_noon_sog)**(1/iso_b_sc))) * ((iso_noon_displacement-iso_disp_b) / (iso_disp_sc-iso_disp_b))
					dct_isoparams_db["ref_fuel"] = (iso_f1*dct_isoparams_db["ref_power"]**2)+(iso_f2*dct_isoparams_db["ref_power"])+iso_k
					if(iso_me_fuel_only_steaming_time!=0 and iso_me_fuel_only_steaming_time < 30 ):
						dct_isoparams_db["fuel_loss"] = (dct_isoparams_db['FO_per_24Hrs']-dct_isoparams_db["ref_fuel"])*100/dct_isoparams_db["ref_fuel"]

					rel_win_dir_corr = lst_rel_wind_speed[1]
					rel_win_speed_corr = lst_rel_wind_speed[0]
					dct_isoparams_db['Relative_wind_speed'] = rel_win_speed_corr
					dct_isoparams_db['Relative_wind_direction'] = rel_win_dir_corr
					
					
					########....Fujiwara method for finding CAA.#####
					isoData_wind_coeff = isoData_CLF_90_180 * math.cos(math.radians(rel_win_dir_corr)) + isoData_CXLI_90_180 * (math.sin(math.radians(rel_win_dir_corr	)) - 0.5 * math.sin(math.radians(rel_win_dir_corr)) *(math.cos(math.radians(rel_win_dir_corr)))**2) * math.sin(math.radians(rel_win_dir_corr)) * math.cos(math.radians(rel_win_dir_corr)	) + isoData_CALF_90_180 * math.sin(math.radians(rel_win_dir_corr)) * (math.cos(math.radians(rel_win_dir_corr)))**3
					print("isoData_wind_coeff,90-180::::",isoData_wind_coeff)
				elif rel_win_dir_corr == 90:
					isoData_wind_coeff = (0.5 * (isoData_CLF_0_90 * math.cos(math.radians(80)) + isoData_CXLI_0_90 * (math.sin(math.radians(80)) - 0.5 * math.sin(math.radians(80)) * (math.cos(math.radians(80)))**2) * math.sin(math.radians(80)) * math.cos(math.radians(80)) + isoData_CALF_0_90 * math.sin(math	.radians(80)) * (math.cos(math.radians(80)))**3 + isoData_CLF_90_180 * math.cos(math.radians(100)) + isoData_CXLI_90_180 * (math.sin(math.	radians(100)) - 0.5 * math.sin(math.radians(100)) * (math.cos(math.radians(100)))**2) * math.sin(math.radians(100)) * math.cos(math.radians(100)) + isoData_CALF_90_180 * math.sin(math.radians(100)) * (math.cos(math.radians(100)))**3))
					print("isoData_wind_coeff,90::::",isoData_wind_coeff)
	
				isoData_wind_coeff_heading = (0.922 + (-0.507 *  iso_alv)/( iso_loa *  iso_b) + (-1.162 *  iso_cmc/ iso_loa))*math.cos(math.radians(0))
				dct_isoparams_db['CAA'] = isoData_wind_coeff
	
				##### Corrected Power and Reference Speed #####
				isoData_res_resistance = (.5 * air_density * iso_axv * rel_win_speed_corr**2 * isoData_wind_coeff) - (.5 * air_density * iso_axv * (iso_noon_sog*0.5144)**2 *  isoData_wind_coeff_heading)
				# import pdb 
				# pdb.set_trace()
				dct_isoparams_db['RW'] = isoData_res_resistance
				print("iso_noon_power::::",iso_noon_power)
				print("isoData_res_resistance::::",isoData_res_resistance)
				isoData_corr_power = iso_noon_power - (isoData_res_resistance * iso_noon_sog*0.5144 /700)
				print("isoData_corr_power:::::::",isoData_corr_power)
				dct_isoparams_db['iso_Corrected_Power'] = isoData_corr_power

				dct_isoparams_db["draft_pow"] = isoData_corr_power+((((1/iso_a_sc) * iso_noon_sog)**(1/iso_b_sc)) - (((1/iso_a_b) * iso_noon_sog)**(1/iso_b_b)))/(iso_disp_sc-iso_disp_b)*(iso_disp_16-iso_noon_displacement)

				dct_isoparams_db["draft_sp_pow"] = dct_isoparams_db["draft_pow"]*(12/iso_noon_sog)**3

				dct_isoparams_db['fo_nor'] = (iso_f1*dct_isoparams_db["draft_sp_pow"]**2)+(iso_f2*dct_isoparams_db["draft_sp_pow"])+iso_k
				dct_isoparams_db['fo_model'] = (iso_f1*iso_noon_power**2)+(iso_f2*iso_noon_power)+iso_k
				dct_isoparams_db["sfoc_model"] = (dct_isoparams_db['fo_model']*10**6)/(24*iso_noon_power)
				if(iso_me_fuel_only_steaming_time!=0 and iso_me_fuel_only_steaming_time < 30 ):
					dct_isoparams_db['sfoc_deviation'] = (dct_isoparams_db['SFOC'] - dct_isoparams_db["sfoc_model"])*100/dct_isoparams_db["sfoc_model"]

				dct_isoparams_db["ref_power_corrected"] = dct_isoparams_db["ref_power"] - (isoData_res_resistance * iso_noon_sog*0.5144 /700)

				dct_isoparams_db["speed_performance"] = (dct_isoparams_db["ref_power_corrected"]*100)/iso_noon_power

				print("dct_isoparams_db['ref_power_corrected']",dct_isoparams_db[ref_power_corrected])
				if(isoData_corr_power > 0):
					isoData_refcurv_speed = ((iso_a_b * isoData_corr_power**iso_b_b - ((iso_a_b * isoData_corr_power**iso_b_b) - (iso_a_sc * isoData_corr_power**iso_b_sc)) * ((	iso_noon_displacement-iso_disp_b) / (iso_disp_sc-iso_disp_b))))

					if isoData_refcurv_speed == 0:
						continue
					
					print("isoData_refcurv_speeddddddddddddddddddddddddddddd",isoData_refcurv_speed)
					dct_isoparams_db['Ref_speed'] = isoData_refcurv_speed
					# print(isoData_refcurv_speed)
					# isoData_refcurv_speed = ((iso_a_b * iso_noon_power**iso_b_b) - ((iso_a_b * iso_noon_power**iso_b_b) - (iso_a_sc * iso_noon_power**iso_b_sc) * 	((iso_noon_displacement-iso_disp_b) / (iso_disp_sc-iso_disp_b))))
					
					isoData_perf_value = (iso_noon_sog-isoData_refcurv_speed) * 100/isoData_refcurv_speed
					dct_isoparams_db['P_I'] = isoData_perf_value
		
					print("dictionaryyyyyyyyyyy:",dct_isoparams_db)
					update_noon_iso = update(noondata).where(noondata.c.UID == obj_noon[10])
					update_noon_iso = update_noon_iso.values(dct_isoparams_db)
					conn.execute(update_noon_iso)
			


def true_wind_speed_calc(wind_speed,speed,wind_dir,cog,rad_to_deg):
	# import pdb 
	# pdb.set_trace()
	lst_return = []
	wind_speed = wind_speed*0.5144
	speed = speed*0.5144
	vwt = math.sqrt(wind_speed*wind_speed + speed*speed - (2 * wind_speed * speed * math.cos(math.radians(wind_dir))))
	lst_return.append(vwt)
	print("true wind_speedddddddddddddddddddddddddddddddddddddd",vwt)
	x = wind_speed* math.cos(math.radians(wind_dir+cog))
	y = speed*math.cos(math.radians(cog))
	
	cond_n = (wind_speed* math.cos(math.radians(wind_dir+cog))) -(speed*math.cos(math.radians(cog)))
	cond_n = round(cond_n,4)
	numeratorcase = float((wind_speed*math.sin(math.radians(wind_dir+cog)))-(speed*math.sin(math.radians(cog))))
	truewinddir = math.degrees(math.atan2(cond_n,numeratorcase))
	
	#zero_flg = abs(numeratorcase/cond_n)
	#print("zero_flg1",zero_flg)
	if cond_n >= 0:
		truewinddir = math.degrees(math.atan2(numeratorcase,cond_n))
	else:
		truewinddir = (math.degrees(math.atan2(numeratorcase,cond_n)))+180


	lst_return.append(truewinddir)
	lst_return.append(cond_n)
	lst_return.append(numeratorcase)
	return(lst_return)

def true_wind_ref(twind,za,zref,tref,t,Aref,B):

	# import pdb 
	# pdb.set_trace()
	deltat = tref-t
	zaref=za-t
	Area=Aref+deltat*B
	a = Aref*(zref+deltat)
	b = 0.5*B*deltat**2
	# zref = 4.1
	zref=(Aref*(zref+deltat)+0.5*B*deltat**2)/Area
	z_za = (zref/zaref)
	vwtref = twind*(float(z_za)**float(1/7))
	return vwtref

def relativewindspeed(vwtref,speed,truewinddir,cog,rad_to_deg):
	# import pdb 
	# pdb.set_trace()
	lst_result = []
	windspeed=vwtref#*0.5144
	speed=speed*0.5144
	vwrref = math.sqrt(windspeed**2 + speed**2 + (2 * windspeed * speed * math.cos(math.radians (truewinddir-cog))))
	lst_result.append(vwrref)
	cond_n = (windspeed*math.cos(math.radians(truewinddir-cog)))+speed
	numeratorcase = windspeed*math.sin(math.radians(truewinddir-cog))
	if cond_n>=0:
		relwinddir = math.degrees(math.atan2(cond_n,numeratorcase))
	else:
		relwinddir = (math.degrees(math.atan2(cond_n,numeratorcase)))+180

	lst_result.append(relwinddir)
	return lst_result

if __name__ == "__main__":
	iso_data_fetch_calc()
print("completed...")
