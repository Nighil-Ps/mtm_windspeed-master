from sqlalchemy import create_engine,MetaData, select,update
from sqlalchemy.orm import Session
# import pandas as pd 
# import numpy
import csv
import math
from sqlalchemy import and_
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.sqltypes import String, DateTime, NullType
from sqlalchemy.sql import func,and_
import datetime
import email
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from decouple import config
# import pdb; pdb.set_trace()


def iso_data_fetch_calc():
        print("fn inside")
        # engine = create_engine("mysql+pymysql://",config('iso_calc_user'),":",config('iso_calc_localhost'),"/",config('database'))
        engine = create_engine("mysql+pymysql://",config('user'),":",config('password'),"@",config('localhost'),"/",config('database'))
        # engine = create_engine("mysql+pymysql://",config('user'),":",config('password'),"@",config('adadata_localhost'),"/",config('database'))
        print("add")
        metadata = MetaData()
        metadata.reflect(bind = engine)
        conn = engine.connect()
        validation_dct={}
        noondata = metadata.tables['NOONDATA']

        noondata_fo_nor = metadata.tables['NOONDATA_fo_nor']
        print("bh")
        iso_parameter = metadata.tables['ISOPARAMETER']
        vessel_details=metadata.tables['VESSELDETAIL']
        validation=metadata.tables['ISO_VALIDATION']
        #for previous data also cmnt # if(obj_noon[17]!='NULL' and obj_noon[17]!=0):
                                        # 	iso_noon_stw=float(obj_noon[17])
                                        # else:  at line 139
        # noon_parameter = conn.execute(select([noondata.c.Vessel_Name,noondata.c.SOG,noondata.c.DRAFT_AFT,noondata.c.DRAFT_FWD,noondata.c.DISPLACEMENT,noondata.c.RPM,noondata.c.Power_corrected,noondata.c.WindDirection,noondata.c.WindSpeed_kn,noondata.c.COURSE_AT_SEA,noondata.c.UID,noondata.c.FUEL_M_E_HS,noondata.c.FUEL_M_E_LS,noondata.c.FUEL_M_E_MDO,noondata.c.FUEL_M_E_MGO_HS,noondata.c.FUEL_M_E_MGO_LS,noondata.c.M_E_FUEL_ONLY_STEAMING_TIME]).where(and_(noondata.c.Vessel_Name=='MNP',noondata.c.CAA==0)).fetchall()
        #for new data having stw
        ins_lst=[]
        noon_parameter = conn.execute(select([noondata.c.Vessel_Name,noondata.c.SOG,noondata.c.DRAFT_AFT,noondata.c.DRAFT_FWD,noondata.c.DISPLACEMENT,noondata.c.RPM,noondata.c.Power_corrected,noondata.c.WindDirection,noondata.c.WindSpeed_kn,noondata.c.COURSE_AT_SEA,noondata.c.UID,noondata.c.FUEL_M_E_HS,noondata.c.FUEL_M_E_LS,noondata.c.FUEL_M_E_MDO,noondata.c.FUEL_M_E_MGO_HS,noondata.c.FUEL_M_E_MGO_LS,noondata.c.M_E_FUEL_ONLY_STEAMING_TIME,noondata.c.STW,noondata.c.REPORT_DATE_TIME]).where(noondata.c.REPORT_DATE_TIME>='2020-01-01 00:00:00')).fetchall()

        # noon_parameter = conn.execute(select([noondata.c.Vessel_Name,noondata.c.SOG,noondata.c.DRAFT_AFT,noondata.c.DRAFT_FWD,noondata.c.DISPLACEMENT,noondata.c.RPM,noondata.c.Power_corrected,noondata.c.WindDirection,noondata.c.WindSpeed_kn,noondata.c.COURSE_AT_SEA,noondata.c.UID,noondata.c.FUEL_M_E_HS,noondata.c.FUEL_M_E_LS,noondata.c.FUEL_M_E_MDO,noondata.c.FUEL_M_E_MGO_HS,noondata.c.FUEL_M_E_MGO_LS,noondata.c.M_E_FUEL_ONLY_STEAMING_TIME,noondata.c.STW]).where(and_(noondata.c.Vessel_Name=='PCO',noondata.c.CAA==0))).fetchall()
        
        # noon_parameter = conn.execute(select([noondata.c.Vessel_Name,noondata.c.SOG,noondata.c.DRAFT_AFT,noondata.c.DRAFT_FWD,noondata.c.DISPLACEMENT,noondata.c.RPM,noondata.c.Power_corrected,noondata.c.WindDirection,noondata.c.WindSpeed_kn,noondata.c.COURSE_AT_SEA,noondata.c.UID,noondata.c.FUEL_M_E_HS,noondata.c.FUEL_M_E_LS,noondata.c.FUEL_M_E_MDO,noondata.c.FUEL_M_E_MGO_HS,noondata.c.FUEL_M_E_MGO_LS,noondata.c.M_E_FUEL_ONLY_STEAMING_TIME,noondata.c.STW]).where(noondata.c.Vessel_Name=='PHU')).fetchall()
        # noon_parameter = conn.execute(select([noondata.c.Vessel_Name,noondata.c.SOG,noondata.c.DRAFT_AFT,noondata.c.DRAFT_FWD,noondata.c.DISPLACEMENT,noondata.c.RPM,noondata.c.Power_corrected,noondata.c.WindDirection,noondata.c.WindSpeed_kn,noondata.c.COURSE_AT_SEA,noondata.c.UID,noondata.c.FUEL_M_E_HS,noondata.c.FUEL_M_E_LS,noondata.c.FUEL_M_E_MDO,noondata.c.FUEL_M_E_MGO_HS,noondata.c.FUEL_M_E_MGO_LS,noondata.c.M_E_FUEL_ONLY_STEAMING_TIME]).where(noondata.c.UID==83982)).fetchall()
        # noon_parameter = conn.execute(select([noondata.c.Vessel_Name,noondata.c.SOG,noondata.c.DRAFT_AFT,noondata.c.DRAFT_FWD,noondata.c.DISPLACEMENT,noondata.c.RPM,noondata.c.Power_corrected,noondata.c.WindDirection,noondata.c.WindSpeed_kn,noondata.c.COURSE_AT_SEA,noondata.c.UID,noondata.c.FUEL_M_E_HS,noondata.c.FUEL_M_E_LS,noondata.c.FUEL_M_E_MDO,noondata.c.FUEL_M_E_MGO_HS,noondata.c.FUEL_M_E_MGO_LS,noondata.c.M_E_FUEL_ONLY_STEAMING_TIME]).where(and_(noondata.c.mail_date < datetime.date(2019,8,23),noondata.c.Vessel_Name=='PHU'))).fetchall()
        # print("noondata:",noon_parameter)
        # ves_mis=['PSL','PMA','PMU','PYN']
        ves_dtls_lst =[]
        validation_parameters = conn.execute(select([validation.c.Parameter,validation.c.Minimum_value,validation.c.Maximum_value])).fetchall()
        ves_dtls = conn.execute(select([vessel_details.c.id]).distinct())
        # print("vessel_details:",ves_dtls)
        for ves in ves_dtls:
                ves_dtls_lst.append(ves[0])
        # print("ves_dtls_lst:",ves_dtls_lst)
        
        for validation_param in validation_parameters:
                validation_dct[validation_param[0]+"_min"]=validation_param[1]
                validation_dct[validation_param[0]+"_max"]=validation_param[2]

        for obj_noon in noon_parameter:
                # print("obj_noon:",obj_noon)

                obj_noon_chk=list(obj_noon)
                obj_noon_chk=obj_noon_chk[:-1]
                # print("rslt:obj",obj_noon_chk)
                # if(obj_noon[0] not in ves_dtls_lst):
                # 	continue
                # if obj_noon[0] in ves_mis:
                # 	continue

                if obj_noon[0] not in ves_dtls_lst:
                        print("vessel not in ves_dtls_lst")
                        continue

                try:
                        for chk_noon in obj_noon_chk:
                                # print("check noon:",chk_noon)
                                # print("uidd:",chk_noon[10])
                                if chk_noon is None:
                                        print("check noon is none....")
                                        raise Exception()
                except Exception as e:
                        print("exception:",e)
                        continue

                lst_obj_noon = list(obj_noon)
                
                
                mcr=conn.execute(select([vessel_details.c.MCR ]).where(vessel_details.c.id==obj_noon[0])).fetchall()
                
                power_min=float(validation_dct["Power_min"])*float(mcr[0][0])
                power_max=float(validation_dct["Power_max"])*float(mcr[0][0])
                # print("UID:",obj_noon[10])
                # print("vessel_name:",obj_noon[0])

                if( ( (obj_noon[2]>=float(validation_dct["Draft_min"]))&(obj_noon[2]<=float(validation_dct["Draft_max"]))) &( (obj_noon[3]>=float(validation_dct["Draft_min"]))&(obj_noon[3]<=float(validation_dct["Draft_max"])))   &( (obj_noon[6]>=float(power_min))&(obj_noon[6]<=float(power_max))) &( (obj_noon[7]>=float(validation_dct["WindDirection_min"]))&(obj_noon[7]<=float(validation_dct["WindDirection_max"])))  &( (obj_noon[8]>=float(validation_dct["WindSpeed_kn_min"]))&(obj_noon[8]<=float(validation_dct["WindSpeed_kn_max"]))) &( (obj_noon[9]>=float(validation_dct["COURSE_AT_SEA_min"]))&(obj_noon[9]<=float(validation_dct["COURSE_AT_SEA_max"]))) &((obj_noon[16]>=float(validation_dct["M_E_FUEL_ONLY_STEAMING_TIME_min"]))&(obj_noon[16]<=float(validation_dct["M_E_FUEL_ONLY_STEAMING_TIME_max"]))) ) :
                        #iso_parameter=metadata.tables['ISOPARAMETERnew']
                        iso_vesls = conn.execute(select([iso_parameter.c.Vessel_id])).fetchall()
                        
                        #print("vessels in iso:",iso_vesls)
                        print("after validation")
                        if obj_noon[1] and obj_noon[4] and obj_noon[6]:
                                for iso_ves in iso_vesls:
                                        if not obj_noon[0] in iso_ves[0]: 
                                                continue
                                        dct_isoparams_db = {}
                                        lst_true_wind_speed = []
                                        iso_params = conn.execute(select([iso_parameter.c.design_draft,iso_parameter.c.breadth,iso_parameter.c.ref_area,iso_parameter.c.ALV,iso_parameter.c.HC,	iso_parameter.c.CMC,iso_parameter.c.AOD,iso_parameter.c.HBR,iso_parameter.c.AXV,iso_parameter.c.LOA,iso_parameter.c.LBP,iso_parameter.c.B,	iso_parameter.c.SC_draft,iso_parameter.c.anemoht,iso_parameter.c.z_ref,iso_parameter.c.a_b,iso_parameter.c.b_b,iso_parameter.c.a_sc,iso_parameter.c.	b_sc,iso_parameter.c.disp_b,iso_parameter.c.disp_sc,iso_parameter.c.f1,iso_parameter.c.f2,iso_parameter.c.k,iso_parameter.c.disp_16,iso_parameter.c.Speedcoef]).where(iso_parameter.c.Vessel_id ==obj_noon[0] )).fetchall()
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
                                        # print("draft_mean:",draft_mean)
                                        # print("iso_deltat:",iso_deltat)
                                        rad_to_deg = float(3.14/180)
                                        iso_area = float(iso_params[0][8]) + (iso_deltat*iso_b)
                                        # print("iso_area:",iso_area)
                                        iso_fuel_me_hs = float(obj_noon[11])

                                        try:
                                                iso_fuel_me_ls = float(obj_noon[12])
                                        except Exception as TypeError:
                                                iso_fuel_me_ls = 0
                                        
                                        iso_fuel_me_mdo = float(obj_noon[13])
                                        iso_fuel_me_mgo_hs = float(obj_noon[14])
                                        iso_fuel_me_mgo_ls = float(obj_noon[15])
                                        iso_f1 = float(iso_params[0][21])
                                        iso_f2 = float(iso_params[0][22])
                                        iso_k = float(iso_params[0][23])
                                        iso_disp_16 = float(iso_params[0][24])
                                        iso_speedcoef = float(iso_params[0][25])
                                        iso_me_fuel_only_steaming_time = float(obj_noon[16])
                                        if(obj_noon[17]!='NULL' and obj_noon[17]!=0 and obj_noon[17] is not None ):
                                                # print("obj_noon[17]:",obj_noon[17],type(obj_noon[17]))
                                                iso_noon_stw=float(obj_noon[17])
                                        else:
                                                iso_noon_stw=iso_noon_sog
                                        if(iso_me_fuel_only_steaming_time!=0 and iso_me_fuel_only_steaming_time < 30 ):
                                                dct_isoparams_db['FO_per_24Hrs'] = (iso_fuel_me_hs + iso_fuel_me_ls + iso_fuel_me_mdo + iso_fuel_me_mgo_hs + iso_fuel_me_mgo_ls)*24/iso_me_fuel_only_steaming_time
                                                dct_isoparams_db['SFOC'] = (dct_isoparams_db['FO_per_24Hrs']*10**6)/(24*iso_noon_power)
                                                
                                                
                                        # import pdb 
                                        # pdb.set_trace()
                                        if iso_params:
                                                if iso_breadth and iso_breadth and iso_design_draft:
                                                        draft_change = iso_design_draft - float(draft_mean)
                                                        projarea_i = iso_breadth + iso_breadth + float(draft_change)
                                                        
                                                if iso_params[0][8] and iso_b and iso_params[0][12] and iso_params[0][7] and iso_params[0][3] and iso_params[0][10] and iso_params[0][4]:
                                                        iso_axv = iso_area
                                                        # print("iso_axv:",iso_axv)
                                                        iso_hbr = float(iso_params[0][7]) + iso_deltat
                                                        iso_alv = float(iso_params[0][3]) + iso_lbp * iso_deltat
                                                        iso_hci = (float(iso_params[0][3])*float(iso_params[0][4])+(0.5* iso_lbp * iso_deltat**2))/(float(iso_params[0][3])+(iso_lbp * iso_deltat)) 

                                                if obj_noon[7] is None or obj_noon[1] is None or iso_params[0][12] is None or obj_noon[8] is None or obj_noon[9] is None or obj_noon[7] == ''  :
                                                        continue
                                                lst_true_wind_speed = true_wind_speed_calc(float(obj_noon[8]),float(obj_noon[1]),float(obj_noon[7]),float(obj_noon[9]),rad_to_deg)
                                                dct_isoparams_db["True_wind_speed"] = lst_true_wind_speed[0]
                                                dct_isoparams_db["True_wind_dir"] = lst_true_wind_speed[1]
                                                dct_isoparams_db["condn"] = lst_true_wind_speed[2]
                                                dct_isoparams_db["numeratorcase"] = lst_true_wind_speed[3]
                                                vwtref = true_wind_ref(lst_true_wind_speed[0],iso_anemoht,iso_z_ref,float(iso_params[0][12]),draft_mean,iso_axv,iso_b)
                                                lst_rel_wind_speed = relativewindspeed(vwtref,obj_noon[1],lst_true_wind_speed[1],obj_noon[9],rad_to_deg)
                                                dct_isoparams_db["ref_power"] = (((1/iso_a_b) * iso_noon_stw)**(1/iso_b_b)) - ((((1/iso_a_b) * iso_noon_stw)**(1/iso_b_b)) - (((1/iso_a_sc) * iso_noon_stw)**(1/iso_b_sc))) * ((iso_noon_displacement-iso_disp_b) / (iso_disp_sc-iso_disp_b))
                                                dct_isoparams_db["ref_fuel"] = (iso_f1*dct_isoparams_db["ref_power"]**2)+(iso_f2*dct_isoparams_db["ref_power"])+iso_k
                                                if(iso_me_fuel_only_steaming_time!=0 and iso_me_fuel_only_steaming_time < 30 ):
                                                        dct_isoparams_db["fuel_loss"] = (dct_isoparams_db['FO_per_24Hrs']-dct_isoparams_db["ref_fuel"])*100/dct_isoparams_db["ref_fuel"]
                                                        
                                                rel_win_dir_corr = lst_rel_wind_speed[1]
                                                # print("rel_win_dir_corr:",rel_win_dir_corr)
                                                rel_win_speed_corr = lst_rel_wind_speed[0]
                                                # print("rel_win_speed_corr:",rel_win_speed_corr)
                                                dct_isoparams_db['Relative_wind_speed'] = rel_win_speed_corr
                                                dct_isoparams_db['Relative_wind_direction'] = rel_win_dir_corr
                                                
                                                
                                                ########....Fujiwara method for finding CAA.#####
                                                isoData_CLF_90_180 = -0.018 + 5.091 * (float(iso_b)/iso_loa) + (-10.367 * (iso_hci/iso_loa)) + (3.011 * (iso_aod/iso_loa**2)) + (0.341 * iso_axv/iso_b	**2)
                                                isoData_CXLI_90_180 = 1.901 + (-12.727 * iso_alv)/(iso_loa *iso_hbr) + (-24.407 * iso_axv/iso_alv) + (40.310 * (iso_b/iso_loa)) + ((5.481 * iso_axv)/(	iso_b*iso_hbr))
                                                isoData_CALF_90_180  = 0.314 + ((1.117 * iso_aod) / iso_alv)
                                                isoData_CLF_0_90 = 0.922 + ((-0.507 * iso_alv)/(iso_loa * iso_b)) + (-1.162 * iso_cmc/iso_loa)
                                                isoData_CXLI_0_90 = -0.458 + (-3.245 *((iso_alv)/(iso_loa * iso_hbr))) + (2.313 * (iso_axv)/(iso_b * iso_hbr))
                                                isoData_CALF_0_90 = 0.585 + (0.906 * (iso_aod/iso_alv)) + (-3.239 * iso_b/iso_loa)
                                                
                                                #if cond_n to be correct
                                                if rel_win_dir_corr >= 0 and rel_win_dir_corr < 90:
                                                        isoData_wind_coeff = isoData_CLF_0_90 * math.cos(math.radians(rel_win_dir_corr)) + isoData_CXLI_0_90 *(math.sin(math.radians(rel_win_dir_corr)) - (0.5 * math.sin(math.radians(rel_win_dir_corr)) * (math.cos(math.radians(rel_win_dir_corr)))**2)) * math.sin(math.radians(rel_win_dir_corr)) *	 math.cos(math.radians(rel_win_dir_corr))+ isoData_CALF_0_90 * math.sin(math.radians(rel_win_dir_corr)) * (math.cos(math.radians(rel_win_dir_corr))**3)
                                                        # print("isoData_wind_coeff:",isoData_wind_coeff)
                                                        
                                                elif rel_win_dir_corr > 90 and rel_win_dir_corr <= 180:				
                                                        lst_rel_wind_speed = relativewindspeed(vwtref,obj_noon[1],lst_true_wind_speed[1],obj_noon[9],rad_to_deg)
                                                
                                                        dct_isoparams_db["ref_power"] = (((1/iso_a_b) * iso_noon_stw)**(1/iso_b_b)) - ((((1/iso_a_b) * iso_noon_stw)**(1/iso_b_b)) - (((1/iso_a_sc) * iso_noon_stw)**(1/iso_b_sc))) * ((iso_noon_displacement-iso_disp_b) / (iso_disp_sc-iso_disp_b))
                                                        dct_isoparams_db["ref_fuel"] = (iso_f1*dct_isoparams_db["ref_power"]**2)+(iso_f2*dct_isoparams_db["ref_power"])+iso_k
                                                        if(iso_me_fuel_only_steaming_time!=0 and iso_me_fuel_only_steaming_time < 30 ):
                                                                dct_isoparams_db["fuel_loss"] = (dct_isoparams_db['FO_per_24Hrs']-dct_isoparams_db["ref_fuel"])*100/dct_isoparams_db["ref_fuel"]

                                                        rel_win_dir_corr = lst_rel_wind_speed[1]
                                                        rel_win_speed_corr = lst_rel_wind_speed[0]
                                                        dct_isoparams_db['Relative_wind_speed'] = rel_win_speed_corr
                                                        dct_isoparams_db['Relative_wind_direction'] = rel_win_dir_corr
                                                        
                                                        
                                                        ########....Fujiwara method for finding CAA.#####
                                                        isoData_wind_coeff = isoData_CLF_90_180 * math.cos(math.radians(rel_win_dir_corr)) + isoData_CXLI_90_180 * (math.sin(math.radians(rel_win_dir_corr	)) - (0.5 * math.sin(math.radians(rel_win_dir_corr)) *(math.cos(math.radians(rel_win_dir_corr)))**2)) * math.sin(math.radians(rel_win_dir_corr)) * math.cos(math.radians(rel_win_dir_corr)) + isoData_CALF_90_180 * math.sin(math.radians(rel_win_dir_corr)) * (math.cos(math.radians(rel_win_dir_corr)))**3
                                                        
                                                elif rel_win_dir_corr == 90:
                                                        isoData_wind_coeff = (0.5 * (isoData_CLF_0_90 * math.cos(math.radians(80)) + isoData_CXLI_0_90 * (math.sin(math.radians(80)) - 0.5 * math.sin(math	.radians(80)) * (math.cos(math.radians(80)))**2) * math.sin(math.radians(80)) * math.cos(math.radians(80)) + isoData_CALF_0_90 * math.sin(math	.radians(80)) * (math.cos(math.radians(80)))**3 + isoData_CLF_90_180 * math.cos(math.radians(100)) + isoData_CXLI_90_180 * (math.sin(math.	radians(100)) - 0.5 * math.sin(math.radians(100)) * (math.cos(math.radians(100)))**2) * math.sin(math.radians(100)) * math.cos(math.radians(100)) + isoData_CALF_90_180 * math.sin(math.radians(100)) * (math.cos(math.radians(100)))**3))
                                                        
                        
                                                isoData_wind_coeff_heading = (0.922 + (-0.507 *  iso_alv)/( iso_loa *  iso_b) + (-1.162 *  iso_cmc/ iso_loa))*math.cos(math.radians(0))
                                                dct_isoparams_db['CAA'] = isoData_wind_coeff
                                                # print("isoData_wind_coeff_heading:",isoData_wind_coeff_heading)
                        
                                                ##### Corrected Power and Reference Speed #####
                                                isoData_res_resistance = (.5 * air_density * iso_axv * rel_win_speed_corr**2 * isoData_wind_coeff) - (.5 * air_density * iso_axv * (iso_noon_sog*0.5144)**2 *  isoData_wind_coeff_heading)
                                                # print("isoData_res_resistance:",isoData_res_resistance)
                                                dct_isoparams_db['RW'] = isoData_res_resistance
                                                isoData_corr_power = iso_noon_power - (isoData_res_resistance * iso_noon_stw*0.5144 /700)
                                                dct_isoparams_db['Corrected_Power'] = isoData_corr_power
                                                dct_isoparams_db["draft_pow"] = isoData_corr_power+((((1/iso_a_sc) * iso_noon_stw)**(1/iso_b_sc)) - (((1/iso_a_b) * iso_noon_stw)**(1/iso_b_b)))/(iso_disp_sc-iso_disp_b)*(iso_disp_16-iso_noon_displacement)
                                                
                                                # print("csv columns:",csv_columns)
                                                fo_nor_dct={} 
                                                val_lst=[11,11.5,12,12.5,13,13.5,14,14.5,15]
                                                fo_nor_dct['REPORT_DATE_TIME']=obj_noon[18]
                                                fo_nor_dct['Vessel_Name']=obj_noon[0]
                                                fo_nor_dct['SOG']=iso_noon_sog
                                                fo_nor_dct['draft_pow']=dct_isoparams_db["draft_pow"]
                                                fo_nor_dct['STW']=obj_noon[17]
                                                fo_nor_dct['WindSpeed_kn'] = obj_noon[8]
                                                for val in val_lst:
                                                        dct_isoparams_db["draft_sp_pow"] = dct_isoparams_db["draft_pow"]*(val/iso_noon_stw)**iso_speedcoef
                                                        fo_nor_dct['draft_sp_pow'+str(val)]=dct_isoparams_db["draft_sp_pow"]

                                                        dct_isoparams_db['fo_nor'] = (iso_f1*dct_isoparams_db["draft_sp_pow"]**2)+(iso_f2*dct_isoparams_db["draft_sp_pow"])+iso_k
                                                        fo_nor_dct['fo_nor'+str(val)] = dct_isoparams_db['fo_nor']
                                                ins_lst.append(fo_nor_dct)
                                                # print("lst:",lst)
                                                # print("fo_nor_dct:",fo_nor_dct)
                                                # print("fo_nor_lst:",lst)
        try:
                insert_q = noondata_fo_nor.insert()
                conn.execute(insert_q,ins_lst)
        except Exception as e:
                print(e)

                                                
                                                
                                                        

        # csv_columns = fo_nor_dct.keys()
        # print("csv_columns:",csv_columns)
        # try:
        # 	with open('/home/lekha/Documents/mtm_windspeed/ISO/test.csv', 'w') as csvfile:
        # 		writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        # 		writer.writeheader()
        # 		for data in lst:
        # 			writer.writerow(data)
        # except IOError:
        # 	print("I/O error")
def true_wind_speed_calc(wind_speed,speed,wind_dir,cog,rad_to_deg):
        # import pdb 
        # pdb.set_trace()
        lst_return = []
        wind_speed = wind_speed*0.5144
        speed = speed*0.5144
        vwt = math.sqrt(wind_speed*wind_speed + speed*speed - (2 * wind_speed * speed * math.cos(math.radians(wind_dir))))
        lst_return.append(vwt)
        x = wind_speed* math.cos(math.radians(wind_dir+cog))
        y = speed*math.cos(math.radians(cog))
        cond_n = (wind_speed* math.cos(math.radians(wind_dir+cog))) -(speed*math.cos(math.radians(cog)))
        cond_n = round(cond_n,4)
        numeratorcase = float((wind_speed*math.sin(math.radians(wind_dir+cog)))-(speed*math.sin(math.radians(cog))))
        wind_dir = math.degrees(math.atan2(numeratorcase,cond_n))
        truewinddir = abs(math.degrees(math.atan2(numeratorcase,cond_n)))
        
        #zero_flg = abs(numeratorcase/cond_n)
        #print("zero_flg1",zero_flg)
        """if cond_n >= 0:
                truewinddir = math.degrees(math.atan2(numeratorcase,cond_n))
        else:
                truewinddir = (math.degrees(math.atan2(numeratorcase,cond_n)))+180"""
        lst_return.append(truewinddir)
        lst_return.append(cond_n)
        lst_return.append(numeratorcase)
        return(lst_return)


def alert_mail(str_error):

        
        msg = MIMEMultipart()
        # storing the senders email address
        fromaddr = config('frmaddrs')
        msg['From'] = fromaddr
        # storing the receivers email address   script-status-alerts@xship.in
        toaddrs = config('toaddrs')
        msg['To'] = toaddrs
        # storing the subject
        msg['Subject'] = "MTM  NOONDATA iso script status alert"
        body = ""
        body = "MTM  NOONDATA ISO calculation not completed due to the error :-\n\n"+str(str_error)
        

        msg.attach(MIMEText(body, 'plain'))
        # creates SMTP session
        s = smtplib.SMTP('outlook.office365.com', 587)
        # start TLS for security
        s.starttls()
        # Authentication
        s.login(fromaddr, "navgathi@12*")
        # Converts the Multipart msg into a string
        text = msg.as_string()

        s.sendmail(fromaddr,[toaddrs], text)
        s.quit()


def true_wind_ref(twind,za,zref,tref,t,Aref,B):

        # import pdb 
        # pdb.set_trace()
        deltat = tref-t
        zaref=za-t
        Area=Aref+deltat*B
        a = Aref*(zref+deltat)
        b = 0.5*B*deltat**2
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
        relwinddir = abs(math.degrees(math.atan2(numeratorcase,cond_n)))
        """if cond_n>=0:
                relwinddir = math.degrees(math.atan2(cond_n,numeratorcase))
        else:
                relwinddir = (math.degrees(math.atan2(cond_n,numeratorcase)))+180"""

        lst_result.append(relwinddir)
        return lst_result

if __name__ == "__main__":
        # iso_data_fetch_calc()
        try:
                iso_data_fetch_calc()
        except Exception as e:
                alert_mail(e)
                raise
                


