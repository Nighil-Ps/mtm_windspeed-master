from sqlalchemy import create_engine,MetaData, select,update,and_,desc
from sqlalchemy.orm import Session

import csv
import math
# import pdb; pdb.set_trace()
from sqlalchemy.sql import func
from datetime import datetime
import email
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
def draft_from_noon():
        try:
                engine = create_engine("mysql+pymysql://phpmyadmin:distancemonopetri@localhost/MTM")
                # engine = create_engine("mysql+pymysql://sarathlal:sarath@123@172.104.173.82/MTM")
                metadata =MetaData()
                metadata.reflect(bind = engine)
                conn = engine.connect()
        except:
                print("Engine creation failed")
        adadata = metadata.tables['ADA_DATA_MTM']
        noondata=metadata.tables['NOONDATA']
        noon_param=conn.execute(select([noondata.c.UID,noondata.c.Vessel_Name,noondata.c.REPORT_DATE_TIME,noondata.c.DRAFT,noondata.c.DRAFT_AFT,noondata.c.DRAFT_FWD]).where(and_(noondata.c.REPORT_DATE_TIME>='2020-10-17 00:00:00',noondata.c.Vessel_Name=='MKW'))).fetchall()
        for obj_noon in noon_param:
                rprt_datetime=obj_noon[2]
                pre_rprt_date_time=conn.execute(select([noondata.c.REPORT_DATE_TIME]).where(and_(noondata.c.Vessel_Name=='MKW',noondata.c.REPORT_DATE_TIME<obj_noon[2])).order_by(desc(noondata.c.REPORT_DATE_TIME)).limit(1)).fetchall()
                pre_rprt_datetime=pre_rprt_date_time[0][0]
                print("rprt_datetime:",rprt_datetime)
                print("pre_rprt_date_time:",pre_rprt_datetime)
                ada_param=conn.execute(select([adadata.c.ID,adadata.c.VESSEL_id]).where(and_(adadata.c.UTC_REPORT_DATE_TIME.between(pre_rprt_datetime,rprt_datetime),adadata.c.VESSEL_NAME=='MTM KEY WEST'))).fetchall()
                print("ada :",ada_param)
                for ada_data in ada_param:
                        print("IDDD:",ada_data[0])
                        update_dct={}
                        update_dct['DRAFT_MEAN']=obj_noon[3]
                        update_dct['DRAFT_AFT']=obj_noon[4]
                        update_dct['DRAFT_FORE']=obj_noon[5]
                        print("dct:",update_dct)
                        draft=update(adadata).where(adadata.c.ID==ada_data[0])
                        update_draft=draft.values(update_dct)
                        conn.execute(update_draft)


def iso_data_fetch_calc():
        today = datetime.now()
        print("running time:", today)
        engine = create_engine("mysql+pymysql://phpmyadmin:distancemonopetri@localhost/MTM")
        #engine = create_engine("mysql+pymysql://sarathlal:sarath@123@172.104.173.82/MTM")
        metadata =MetaData()
        metadata.reflect(bind = engine)
        conn = engine.connect()
        adadata = metadata.tables['ADA_DATA_MTM_MVR']
        #validation=metadata.tables['ISO_VALIDATION_ADA']
        noon_parameter = conn.execute(select([adadata.c.VESSEL_id,adadata.c.SOG,adadata.c.DRAFT_AFT,adadata.c.DRAFT_FORE,adadata.c.STW,adadata.c.RPM,adadata.c.POWER,adadata.c.WIND_DIRECTION,adadata.c.WIND_SPEED,adadata.c.HEADING,adadata.c.ID,adadata.c.validation,adadata.c.outlier]).where(adadata.c.draft_pow == None)).fetchall()
        # noon_parameter = conn.execute(select([adadata.c.VESSEL_id,adadata.c.SOG,adadata.c.DRAFT_AFT,adadata.c.DRAFT_FORE,adadata.c.STW,adadata.c.RPM,adadata.c.POWER,adadata.c.WIND_DIRECTION,adadata.c.WIND_SPEED,adadata.c.HEADING,adadata.c.ID,adadata.c.validation,adadata.c.outlier])).fetchall()
        iso_parameter = metadata.tables['ISOPARAMETER']
        iso_ves_lst=[]
        
        iso_vesls = conn.execute(select([iso_parameter.c.Vessel_id])).fetchall()
        for ves in iso_vesls:
                
                iso_ves_lst.append(ves[0])
        """validation_dct={}
        validation_parameters = conn.execute(select([validation.c.Parameter,validation.c.Minimum_value,validation.c.Maximum_value])).fetchall()
        for validation_param in validation_parameters:
                validation_dct[validation_param[0]+"_min"]=validation_param[1]
                validation_dct[validation_param[0]+"_max"]=validation_param[2]"""
        # import pdb; pdb.set_trace()
        
        for obj_noon in noon_parameter:
                try:
                        
                        for chk_noon in obj_noon:
                                
                                if chk_noon is None:
                                        
                                        raise Exception()
                except Exception:
                        continue
                lst_obj_noon = list(obj_noon)
                print("obj_noon",obj_noon)
                #mcr=conn.execute(select([vessel_details.c.MCR ]).where(vessel_details.c.idvessel==obj_noon[0])).fetchall()
                #power_min=float(validation_dct["Power_min"])*mcr[0][0]
                #power_max=float(validation_dct["Power_max"])*mcr[0][0]
                """sog_stw_sum = obj_noon[1] + obj_noon[4]
                if(obj_noon[12]==0 and obj_noon[13]==0):
                        if( ( (obj_noon[1]>=float(validation_dct["SOG_min"]))&(obj_noon[1]<=float(validation_dct["SOG_max"])) ) &( (obj_noon[2]>=float(validation_dct["Draft_min"]))&(obj_noon[2]<=float(validation_dct["Draft_max"]))) &( (obj_noon[3]>=float(validation_dct["Draft_min"]))&(obj_noon[3]<=float(validation_dct["Draft_max"]))) &( (obj_noon[4]>=float(validation_dct["STW_min"]))&(obj_noon[4]<=float(validation_dct["STW_max"])))   &( (obj_noon[6]>=float(power_min))&(obj_noon[6]<=float(power_max))) &( (obj_noon[7]>=float(validation_dct["WindDirection_min"]))&(obj_noon[7]<=float(validation_dct["WindDirection_max"])))  &( (obj_noon[8]>=float(validation_dct["WindSpeed_kn_min"]))&(obj_noon[8]<=float(validation_dct["WindSpeed_kn_max"]))) &( (obj_noon[9]>=float(validation_dct["HEADING_min"]))&(obj_noon[9]<=float(validation_dct["HEADING_max"]))) & sog_stw_sum > 0.5 ) :"""

                """if obj_noon[10] == 163815 or obj_noon[10] == 163815 and obj_noon[10] == 83899:
                        continue"""
                #iso_ves = ['SAL','SHA','SVE','SEN','SEQ','SPL','SUN','SYN']
                

                
                if(obj_noon[11]==0 and obj_noon[12]==0):
                        if obj_noon[1] > 0 and obj_noon[4] >0 and obj_noon[6] >0:
                                
                                
                                if obj_noon[0] not in iso_ves_lst: 
                                        
                                        
                                        continue
                                dct_isoparams_db = {}
                                lst_true_wind_speed = []
                                iso_params = conn.execute(select([iso_parameter.c.design_draft,iso_parameter.c.breadth,iso_parameter.c.ref_area,iso_parameter.c.ALV,iso_parameter.c.HC,	iso_parameter.c.CMC,iso_parameter.c.AOD,iso_parameter.c.HBR,iso_parameter.c.AXV,iso_parameter.c.LOA,iso_parameter.c.LBP,iso_parameter.c.B,	iso_parameter.c.SC_draft,iso_parameter.c.anemoht,iso_parameter.c.z_ref,iso_parameter.c.a_b,iso_parameter.c.b_b,iso_parameter.c.a_sc,iso_parameter.c.	b_sc,iso_parameter.c.disp_b,iso_parameter.c.disp_sc,iso_parameter.c.f1,iso_parameter.c.f2,iso_parameter.c.k,iso_parameter.c.ballast_draft,iso_parameter.c.draft_nor,iso_parameter.c.Speedcoef,iso_parameter.c.speed_nor]).where(iso_parameter.c.Vessel_id ==obj_noon[0] )).fetchall()#
                                
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
                                iso_noon_stw = obj_noon[4]
                                iso_noon_power = float(obj_noon[6])
                                iso_deltat = float(iso_params[0][12]) - float(draft_mean)
                                rad_to_deg = float(3.14/180)
                                iso_area = float(iso_params[0][8]) + (iso_deltat*iso_b)
                                iso_f1 = float(iso_params[0][21])
                                iso_f2 = float(iso_params[0][22])
                                iso_k = float(iso_params[0][23])
                                iso_ballast_draft = float(iso_params[0][24])
                                iso_sc_draft = float(iso_params[0][12])
                                iso_draft_nor= float(iso_params[0][25])
                                iso_sp_nor= float(iso_params[0][26])
                                iso_speed_nor = float(iso_params[0][27])
                                # import pdb 
                                # pdb.set_trace()
                                if iso_params:
                                        if iso_breadth and iso_breadth and iso_design_draft:
                                                draft_change = iso_design_draft - float(draft_mean)
                                                projarea_i = iso_breadth + iso_breadth + float(draft_change)
                                                
                                        if iso_params[0][8] and iso_b and iso_params[0][12] and iso_params[0][7] and iso_params[0][3] and iso_params[0][10] and iso_params[0][4]:
                                                iso_axv = iso_area
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
                                        dct_isoparams_db["ref_power"] = (((1/iso_a_b) * iso_noon_stw)**(1/iso_b_b)) - ((((1/iso_a_b) * iso_noon_stw)**(1/iso_b_b)) - (((1/iso_a_sc) * iso_noon_stw)**(1/iso_b_sc))) * ((draft_mean-iso_ballast_draft) / (iso_sc_draft-iso_ballast_draft))
                                        rel_win_dir_corr = lst_rel_wind_speed[1]
                                        rel_win_speed_corr = lst_rel_wind_speed[0]
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
                                                isoData_wind_coeff = isoData_CLF_0_90 * math.cos(math.radians(rel_win_dir_corr)) + isoData_CXLI_0_90 *(math.sin(math.radians(rel_win_dir_corr)) - (0.5 * math.sin(math.radians(rel_win_dir_corr)) * (math.cos(math.radians(rel_win_dir_corr)))**2)) * math.sin(math.radians(rel_win_dir_corr)) *	 math.cos(math.radians(rel_win_dir_corr))+ isoData_CALF_0_90 * math.sin(math.radians(rel_win_dir_corr)) * (math.cos(math.radians(	rel_win_dir_corr))**3)
                                                print("isoData_wind_coeff/CAA,0-90::::",isoData_wind_coeff)
                                        elif rel_win_dir_corr > 90 and rel_win_dir_corr <= 180:				
                                                lst_rel_wind_speed = relativewindspeed(vwtref,obj_noon[1],lst_true_wind_speed[1],obj_noon[9],rad_to_deg)
                                        
                                                #dct_isoparams_db["ref_power"] = (((1/iso_a_b) * iso_noon_stw)**(1/iso_b_b)) - ((((1/iso_a_b) * iso_noon_stw)**(1/iso_b_b)) - (((1/iso_a_sc) * iso_noon_stw)**(1/iso_b_sc))) * ((draft_mean-iso_ballast_draft) / (iso_sc_draft-iso_ballast_draft))
                                                #dct_isoparams_db["ref_fuel"] = (iso_f1*dct_isoparams_db["ref_power"]**2)+(iso_f2*dct_isoparams_db["ref_power"])+iso_k
                                                """if(iso_me_fuel_only_steaming_time!=0 and iso_me_fuel_only_steaming_time < 30 ):
                                                        dct_isoparams_db["fuel_loss"] = (dct_isoparams_db['FO_per_24Hrs']-dct_isoparams_db["ref_fuel"])*100/dct_isoparams_db["ref_fuel"]"""

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
                
                                        ##### Corrected Power and Reference Speed #####
                                        isoData_res_resistance = (.5 * air_density * iso_axv * rel_win_speed_corr**2 * isoData_wind_coeff) - (.5 * air_density * iso_axv * (iso_noon_sog*0.5144)**2 *  isoData_wind_coeff_heading)
                                        # import pdb 
                                        # pdb.set_trace()
                                        dct_isoparams_db['RW'] = isoData_res_resistance
                                        isoData_corr_power = iso_noon_power - (isoData_res_resistance * iso_noon_stw*0.5144 /700)
                                        dct_isoparams_db['Corrected_Power'] = isoData_corr_power
                                        dct_isoparams_db["ref_power_corrected"] = dct_isoparams_db["ref_power"] - (isoData_res_resistance * iso_noon_stw*0.5144 /700)

                                        dct_isoparams_db["speed_performance"] = (dct_isoparams_db["ref_power_corrected"]*100)/iso_noon_power
                                        isoData_refcurv_speed = ((iso_a_b * isoData_corr_power**iso_b_b - ((iso_a_b * isoData_corr_power**iso_b_b) - (iso_a_sc * isoData_corr_power**iso_b_sc)) * ((	draft_mean-iso_ballast_draft) / (iso_sc_draft-iso_ballast_draft))))
                                        dct_isoparams_db["draft_pow"] = isoData_corr_power+((((1/iso_a_sc) * iso_noon_stw)**(1/iso_b_sc)) - (((1/iso_a_b) * iso_noon_stw)**(1/iso_b_b)))/(iso_sc_draft-iso_ballast_draft)*(iso_draft_nor-draft_mean)
                                        dct_isoparams_db["draft_sp_pow"] = dct_isoparams_db["draft_pow"]*(iso_speed_nor/iso_noon_stw)**iso_sp_nor
                                        dct_isoparams_db['fo_nor'] = (iso_f1*dct_isoparams_db["draft_sp_pow"]**2)+(iso_f2*dct_isoparams_db["draft_sp_pow"])+iso_k
                                        
                                        print("corrctd power:",isoData_corr_power)
                                        if(isoData_corr_power > 0):
                                                isoData_refcurv_speed = ((iso_a_b * isoData_corr_power**iso_b_b - ((iso_a_b * isoData_corr_power**iso_b_b) - (iso_a_sc * isoData_corr_power**iso_b_sc)) * ((	draft_mean-iso_ballast_draft) / (iso_sc_draft-iso_ballast_draft))))
                                                if isoData_refcurv_speed == 0:
                                                        continue
                                                dct_isoparams_db['Ref_speed'] = isoData_refcurv_speed
                                                isoData_perf_value = (iso_noon_stw-isoData_refcurv_speed) * 100/isoData_refcurv_speed
                                                dct_isoparams_db['P_I'] = isoData_perf_value
                                                update_noon_iso = update(adadata).where(adadata.c.ID == obj_noon[10])
                                                update_noon_iso = update_noon_iso.values(dct_isoparams_db)
                                                conn.execute(update_noon_iso)
                                        
                                
def alert_mail(str_error):

        
        msg = MIMEMultipart()
        # storing the senders email address
        fromaddr = "alerts@xship.in"
        msg['From'] = fromaddr
        # storing the receivers email address   script-status-alerts@xship.in
        toaddrs = "script-status-alerts@xship.in"
        msg['To'] = toaddrs
        # storing the subject
        msg['Subject'] = "MTM  ADA DATA iso script status alert"
        body = ""
        body = "MTM  ADA DATA ISO calculation not completed due to the error :-\n\n"+str(str_error)
        

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

def true_wind_speed_calc(wind_speed,speed,wind_dir,cog,rad_to_deg):
        # import pdb 
        # pdb.set_trace()
        lst_return = []
        wind_speed = wind_speed
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
        speed = speed
        cog=float(cog)
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
        draft_from_noon()
        # iso_data_fetch_calc()
        # try:
        # 	iso_data_fetch_calc()
        # 	today = datetime.now()
        # except Exception as e:
        # 	alert_mail(e)
        
        
