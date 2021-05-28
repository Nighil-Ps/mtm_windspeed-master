from dbhandler.dbhandler import executeQ
# q1="SELECT UID FROM NOONDATA"
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

def calc_wind_speed():
	# q1="SELECT UID FROM NOONDATA WHERE DATE(mail_date) = DATE(NOW())"
	q1="SELECT UID FROM NOONDATA"
	vessels=executeQ(q1)
	# print("vessels::",vessels)
	for vessel_uid in vessels:
		# print("uid:",vessel_uid[0])
		q3="SELECT WindForce FROM NOONDATA WHERE UID=\'"+str(vessel_uid[0])+"\'"
		wind_force=executeQ(q3)
		print("wind force",wind_force)
		q4="SELECT Mean_wind_speed FROM Beaufort_Wind WHERE Beaufort_wind_scale="+str(wind_force[0][0])+""
		mean_wind=executeQ(q4)
		# print("mean_wind::::",mean_wind)
		# print("mean_wind[0][0]::::",mean_wind[0][0])
		if(mean_wind):
			q5="UPDATE NOONDATA SET WindSpeed_kn=\'"+str(mean_wind[0][0])+"\' WHERE UID=\'"+str(vessel_uid[0])+"\'"
			res=executeQ(q5)
		else:
			print("no windforce")
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

	msg['Subject'] = "MTM WindSpeed_kn calculation Alert status"
	body = "MTM NOONDATA WindSpeed_kn calculation not completed due to the error :-\n\n"+str(e)
		

	msg.attach(MIMEText(body, 'plain'))
	s = smtplib.SMTP('outlook.office365.com', 587)
	s.starttls()
	s.login(fromaddr, "navgathi@12*")
	text = msg.as_string()
	try:
		s.sendmail(fromaddr,toaddrs, text)
		#s.sendmail(fromaddr,[toaddrs], text)
	except:
		print("SMTP server connection error")
	s.quit()
if __name__ == "__main__":
	try:
		calc_wind_speed()
	except Exception as e:
		status_alert(e)
