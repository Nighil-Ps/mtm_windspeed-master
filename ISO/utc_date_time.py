from sqlalchemy import create_engine,MetaData, select,update
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
import datetime
from decouple import config
try:
        #engine = create_engine("mysql+pymysql://",config('user'),":",config('password'),"@",config('adadata_localhost'),"/",config('database'))
        engine = create_engine("mysql+pymysql://",config('iso_calc_user'),":",config('iso_calc_localhost'),"/",config('database'))
        metadata =MetaData()
        metadata.reflect(bind = engine)
        conn = engine.connect()
except Exception as exception:
	print(exception)
	sys.exit()
adadata = metadata.tables['ADA_DATA_MTM']
utc_param = conn.execute(select( [adadata.c.UTC_REPORTED_DATE,adadata.c.UTC_REPORTED_TIME,adadata.c.ID]).where(adadata.c.UTC_REPORT_DATE_TIME == None)).fetchall()
print("utc_param:",utc_param)
for date_time in utc_param:
	utc_date = date_time[0]
	print("utc_date:",utc_date)
	utc_time = date_time[1]
	print("utc_time:",utc_time)
	#datetime.datetime.combine(datetime.date(utc_date), datetime.time(utc_time))
	utc_date_time = datetime.datetime.strptime("{}, {}".format(utc_date, utc_time), "%Y-%m-%d, %H:%M:%S")
	#utc_date_time = utc_date +" "+ utc_time
	print("date_time:",utc_date_time)
	update_adadata = update(adadata).where(adadata.c.ID==str(date_time[2]))
	update_adadata=update_adadata.values(UTC_REPORT_DATE_TIME=utc_date_time)
	conn.execute(update_adadata)
