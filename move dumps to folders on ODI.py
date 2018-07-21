mport os, sys, os.path
import fnmatch

sourceConnection=odiRef.getJDBCConnection( "DEST" )
sqlstring=sourceConnection.createStatement()

task_name='#PRJ_FILE.V_TASK_NAME' 
#'CLT_SAM_NETWORK_TICKETS'

file_id= #PRJ_FILE.V_FILE_ID 



sql_query=("SELECT TASK_KEY, TASK_NAME, INCOMMING_DIR, PROCESSING_DIR, ERROR_DIR, ARCHIVE_DIR, "
                    " WILDCARD, IS_ENABLE, INSERT_DT, UPDATE_DT, START_DATE, END_DATE,DEST_FILE_NAME"
                    " FROM CTL.FILE_TASKS WHERE IS_ENABLE='YES' AND TASK_NAME = '"+task_name+"'")
result=sqlstring.executeQuery(sql_query)

while (result.next()):
  task_key=result.getString("TASK_KEY")
  incomming_dir=result.getString("INCOMMING_DIR")
  processing_dir=result.getString("PROCESSING_DIR")
  error_dir=result.getString("ERROR_DIR")
  archive_dir=result.getString("ARCHIVE_DIR")
  wildcard=result.getString("WILDCARD")
  dest_file_name=result.getString("DEST_FILE_NAME")
  



sql_query=("SELECT  FILE_ID, TASK_KEY, TASK_NAME, ODI_SESSION_ID, FILE_PATH, FILE_NAME, "
                      "   LOAD_START_DT, LOAD_END_DT, STATUS, CREATE_DT"
                      " FROM CTL.FILE_LOAD_DTLS"
                      " WHERE FILE_ID=" + str(file_id )+ " AND TASK_NAME='" + task_name +"'")
result=sqlstring.executeQuery(sql_query)

while (result.next()):
  file_name=result.getString("FILE_NAME")
  file_dir=result.getString("FILE_PATH")

  
#f = open(error_dir+"/"+dest_file_name+"_"+str(file_id), 'w')
#f.write(file_name+"\n"+incomming_dir+"/"+file_name+"\n"+ processing_dir+"/"+dest_file_name+"_"+str(file_id))  
#f.close()
  
file_status='#PRJ_FILE.V_FILE_STATUS'
  
if  file_status=='SUCCESS':
  os.rename(processing_dir+"/"+dest_file_name+"_"+str(file_id),archive_dir+"/"+file_name+"FID"+str(file_id))
else:
  os.rename(processing_dir+"/"+dest_file_name+"_"+str(file_id),error_dir+"/"+file_name+"FID"+str(file_id) )

sql_query=("UPDATE CTL.FILE_LOAD_DTLS"
                      " SET STATUS='"+file_status+"'"
                      " WHERE FILE_ID="+str(file_id) + " AND TASK_NAME='" + task_name + "'")
sqlstring.execute(sql_query)