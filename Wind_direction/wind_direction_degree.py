import pymysql
import pymysql.cursors
mydb = pymysql.connect(
  host="localhost",
  user="lekha",
  passwd="lekha@123",
  database="PIL"
)
mycursor=mydb.cursor()
def wind_dir_calc():
	q1=mycursor.execute("SELECT UID,WindDirection FROM NOONDATA WHERE WindDirection IS NOT NULL ")
	wind_dirs=mycursor.fetchall()
	print("wind_directions",wind_dirs)
	for wind_dir in wind_dirs:
		print("wind_dir",wind_dir)
		print("wind_dir0",wind_dir[0])
		q2=mycursor.execute("SELECT degree FROM wind_direction WHERE direction='"+str(wind_dir[1])+"' ")
		degree=mycursor.fetchall()
		print("degree",degree)
		q3=mycursor.execute("UPDATE NOONDATA SET WindDirection_Degree='"+str(degree[0][0])+"' WHERE UID='"+str(wind_dir[0])+"'")
		print(mycursor._last_executed)
wind_dir_calc()
