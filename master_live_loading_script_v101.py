#!/usr/local/bin/python3.6
import sys
import os
import os.path, time
import csv
import glob
import configparser as conf
import cx_Oracle
from datetime import datetime,timedelta
import multiprocessing
import gzip
from pathlib import Path
import math

credentialsFile = "/edataocdmtemp1/script/ms_script_important/python_scripts_data/password_file.txt"

def data_chunker(data, size):
	return [data[i * size:(i+1)*size] for i in range((len(data) + size -1) // size)]
def get_query_output_from_DB(query,reType):
	config=conf.ConfigParser()
	config.read(credentialsFile)
	password=config.get("ETLADM","password")
	constr='ETLADM'+'/'+password+'@raxdw-scan:1628/raxdw'
	conn=cx_Oracle.connect(constr)
	curs=conn.cursor()
	curs.execute(query)
	if reType == 'DICT':
		colNames = [i[0] for i in curs.description]
		queryOutput = []
		for row in curs:
			queryOutput.append(dict(zip(colNames,row)))
		conn.commit()
		curs.close()
		conn.close()
		return queryOutput
	elif reType == 'LIST':
		query_output = [row for row in curs.fetchall()]
		conn.commit()
		curs.close()
		conn.close()
		return query_output
def truncate_raw_table_data():
	config=conf.ConfigParser()
	config.read(credentialsFile)
	password=config.get("ETLADM","password")
	constr='ETLADM'+'/'+password+'@raxdw-scan:1628/raxdw'
	try:
		conn=cx_Oracle.connect(constr)
		curs=conn.cursor()
		curs.arraysize=50
		curs.execute(truncate_raw_tbl)
	except Exception as e:
		quit()
	conn.commit()
	curs.close()
	conn.close()
def insert_filenames_to_log_table(file_list):
	config=conf.ConfigParser()
	config.read(credentialsFile)
	password=config.get("ETLADM","password")
	constr='ETLADM'+'/'+password+'@raxdw-scan:1628/raxdw'
	conn=cx_Oracle.connect(constr)
	curs=conn.cursor()
	for fname in file_list:
		tfname = Path(raw_source_dir+fname)
		mtime = datetime.fromtimestamp(tfname.stat().st_mtime)
		curs.execute(file_process_log_insertion,[fname,mtime])
	conn.commit()
	conn.close()
def write_processing_log_to_tbl(log_data):
        config=conf.ConfigParser()
        config.read(credentialsFile)
        password=config.get("ETLADM","password")
        constr='ETLADM'+'/'+password+'@raxdw-scan:1628/raxdw'
        conn=cx_Oracle.connect(constr)
        curs=conn.cursor()
        for cdata in log_data:
                curs.execute("insert into TBL_LIVE_LOADING_LOG_PY values(:1,:2,:3,:4,:5)",cdata)
        conn.commit()
        conn.close()
def process_data_and_load(file_list,divider,rem,raw_backup_dir):
	config=conf.ConfigParser()
	config.read(credentialsFile)
	password=config.get("ETLADM","password")
	constr='ETLADM'+'/'+password+'@raxdw-scan:1628/raxdw'
	try:
		conn=cx_Oracle.connect(constr)
		curs=conn.cursor()
		curs.arraysize=50
	except Exception as e:
		with open(process_log_file,'a') as el_wfh:
			el_wfh.write(str(datetime.now()) + ':Fatal Error: DB Conn Failed for Parallel Process Divider:' + str(divider) 
				    + ' Reminder:' + str(rem) + ' Error Message:' + str(e) + '\n')
			quit()
	processing_list = []
	for seq in range(len(file_list)):
		if seq % divider == rem:
			processing_list.append(file_list[seq])
	for fl in processing_list:
		fname = fl.split('/')[-1]
		data_set = []
		try:
			data_reader = csv.reader(open(fl,'r'),delimiter=field_delimiter)
		except Exception as e:
			write_processing_log_to_tbl([[datetime.now(),processing_data,'File Cant be open:'+fname,'BAD FILE',datetime.now()]])
		for rec in data_reader:
			rec.insert(0,fname)
			data_set.append(rec)
		for chunk_data in data_chunker(data_set,30000):
			curs.executemany(insertion_query,chunk_data,batcherrors=True)
			for error in curs.getbatcherrors():
				br_file_name = chunk_data[error.offset][0]
				write_processing_log_to_tbl([[datetime.now(),processing_data,'Bad Records Error Msg:'+error.message + ':Row Offset:' \
						            + str(error.offset),'BR',datetime.now()]])
				with open(bad_record_dir + 'badrecords_' + br_file_name ,'a') as br_wfh:
					br_wfh.write(str(chunk_data[error.offset]) + '\n')
		conn.commit()
	curs.close()
	conn.close()
def load_data_to_edw(PROCEDURE,ptype):
	config=conf.ConfigParser()
	config.read(credentialsFile)
	password=config.get("ETLADM","password")
	constr='ETLADM'+'/'+password+'@raxdw-scan:1628/raxdw'
	conn=cx_Oracle.connect(constr)
	curs=conn.cursor()
	curs.arraysize=50
	try:
		if ptype == 'PROC':
			curs.callproc(PROCEDURE)
		elif ptype == 'DPROC':
			curs.execute(PROCEDURE)
	except Exception as e:
		write_processing_log_to_tbl([[datetime.now(),processing_data,'Proc Failed. Removing Flag File and QUITing.' + str(e),'QUIT',datetime.now()]])
		Path(process_running_flag_file).unlink()
		curs.close()
		conn.close()
		quit()
	curs.close()
	conn.close()

def run_parallel_processing():
	truncate_raw_table_data()
	raw_tbl_rec_count = get_query_output_from_DB(get_row_count_from_raw_tbl,'LIST')
	if raw_tbl_rec_count[0][0] == 0:
		write_processing_log_to_tbl([[datetime.now(),processing_data,'RAW Intermediate TBL truncation done. Raw TBL record cnt:' + \
				str(raw_tbl_rec_count[0][0]),'TBL_TRUNCATE',startD]])
	else:
		write_processing_log_to_tbl([[datetime.now(),processing_data,'RAW Intermeidate TBL Truncate failed. Raw TBL record cnt:' + \
				str(raw_tbl_rec_count[0][0]),'QUIT',startD]])
		Path(process_running_flag_file).unlink()
		quit()
	table_file_list_dic = get_query_output_from_DB(get_table_file_list,'LIST')
	table_file_list = [fname[0] for fname in table_file_list_dic]

	raw_dir_file_list_abs = glob.glob(raw_source_dir + raw_filename_pattern)
	raw_dir_file_list = [fname.split('/')[-1] for fname in raw_dir_file_list_abs]

	unprocessed_file_list = set(raw_dir_file_list) - set(table_file_list)
	insert_filenames_to_log_table(unprocessed_file_list)
	write_processing_log_to_tbl([[datetime.now(),processing_data,'New File List Insertion to TBL done. Elapsed Time:' + \
		str(datetime.now() - startD),'FILE_LIST_INSERTION',datetime.now()]])
	processing_file_list = get_query_output_from_DB(get_processing_file_list,'LIST')
	unprocessed_file_list_abs = [raw_source_dir + fname[0] for fname in processing_file_list]
	final_unprocessed_file_list_abs =list(set(unprocessed_file_list_abs) & set(raw_dir_file_list_abs))

	parallel_process = int(math.log(len(final_unprocessed_file_list_abs),2)*4)
	if parallel_process <= 0: 
		parallel_process = 2
	write_processing_log_to_tbl([[datetime.now(),processing_data,'Parallel Insertion Starts. Processing File Count:' + \
		str(len(final_unprocessed_file_list_abs)) + ' Parallel Session:' + str(parallel_process),'P_INSERTION',startD]])
	subprocess_list = []
	for rem in range(parallel_process):
		proc = multiprocessing.Process(target=process_data_and_load,args=(final_unprocessed_file_list_abs,parallel_process,rem,raw_backup_dir,))
		proc.start()
		subprocess_list.append(proc)
	for prc in subprocess_list:
		prc.join()
	write_processing_log_to_tbl([[datetime.now(),processing_data,'Insertion to TBL done. Elapsed Time:' + str(datetime.now() - startD),\
									'P_INSERTION_DONE',datetime.now()]])
	#Run Live Load Procedure to load data to Target Schema
	if proc_raw_to_mediation == '':
		load_data_to_edw(proc_raw_to_mediation,'PROC')
	else:
		load_data_to_edw(procedure_to_load_data_to_mediation,'DPROC')
def getSet_variables_data(processingData):
	query = '''select * 
			from TBL_DATA_LIVE_LOAD_CONFIG_DATA
			where PROCESSING_DATA = \''''+processingData+'''\'
		'''
	global processing_data
	global process_log_file
	global bad_records_dir
	global bad_files_dir
	global raw_backup_dir
	global raw_source_dir
	global process_running_flag_file
	global raw_tbl_creation_query
	global live_load_procedure
	global file_process_log_tbl
	global raw_data_tbl
	global raw_filename_pattern
	global raw_to_trans_view
	global proc_raw_to_mediation
	global target_live_load_tbl
	global process_log_update_col
	global raw_tbl_column_no
	global field_delimiter
	variableData = get_query_output_from_DB(query,'DICT')
	if len(variableData) == 1:
		processing_data = variableData[0]['PROCESSING_DATA']
		process_log_file = variableData[0]['PROCESS_LOG_FILE']
		bad_records_dir = variableData[0]['BAD_RECORDS_DIR']
		bad_files_dir = variableData[0]['BAD_FILES_DIR']
		raw_backup_dir = variableData[0]['RAW_BACKUP_DIR']
		raw_source_dir = variableData[0]['RAW_SOURCE_DIR']
		process_running_flag_file = variableData[0]['PROCESS_RUNNING_FLAG_FILE']
		raw_tbl_creation_query = variableData[0]['RAW_TBL_CREATION_QUERY']
		live_load_procedure = variableData[0]['LIVE_LOAD_PROCEDURE']
		file_process_log_tbl = variableData[0]['FILE_PROCESS_LOG_TBL']
		raw_data_tbl = variableData[0]['RAW_DATA_TBL']
		raw_filename_pattern = variableData[0]['RAW_FILE_PATTERN']
		raw_to_trans_view = variableData[0]['RAW_TO_TRANS_VIEW']
		proc_raw_to_mediation = variableData[0]['PROC_RAW_TO_MEDIATION']
		target_live_load_tbl = variableData[0]['TARGET_LIVE_LOAD_TBL']
		process_log_update_col = variableData[0]['PROCESS_LOG_UPD_COL']
		raw_tbl_column_no = variableData[0]['RAW_TBL_COLUMN_NO']
		field_delimiter = variableData[0]['FIELD_DELIMITER']
	else:
		write_processing_log_to_tbl([[datetime.now(),processing_data,'Getting More Dimensional Data','QUIT',startD]])
		quit()
def set_query_variables_data():
	global get_table_file_list
	global get_processing_file_list
	global get_row_count_from_raw_tbl
	global truncate_raw_tbl
	global insertion_query
	global procedure_to_load_data_to_mediation
	global file_process_log_insertion
	get_table_file_list = '''select file_name
				    from '''+file_process_log_tbl+'''
				    where file_type = \''''+processing_data+'''\'
			      '''
	get_processing_file_list = '''select file_name 
					from '''+file_process_log_tbl+'''
					where file_type = \''''+processing_data+'''\' and processed = 'N'
			       '''
	get_row_count_from_raw_tbl = '''select count(*)
					   from '''+raw_data_tbl+'''
				     '''
	truncate_raw_tbl = '''truncate table '''+raw_data_tbl+''' drop storage'''
	file_process_log_insertion = '''insert into '''+file_process_log_tbl+''' values(\''''+processing_data+'''\',:1,'N',:2,sysdate,NULL,NULL)'''
	column_bind_string = ''
	cols = []
	for i in range(raw_tbl_column_no):
		cols.append(':'+str(i+1))
	column_bind_string = ','.join(cols)
	insertion_query = '''insert into '''+raw_data_tbl+''' values('''+column_bind_string+''')'''

	procedure_to_load_data_to_mediation = '''
	DECLARE
	BEGIN
		execute immediate 'alter session enable parallel dml';
		execute immediate 'alter table '''+target_live_load_tbl+''' parallel 64';
		execute immediate 'alter table '''+raw_data_tbl+''' parallel 64';

		execute immediate 'INSERT INTO '''+target_live_load_tbl+'''
				  SELECT --+PARALLEL(V,32)
				  V.*,SYSDATE AS INSERT_TIME FROM '''+raw_to_trans_view+''' V';

		execute immediate 'UPDATE '''+file_process_log_tbl+''' 
						    set processed= ''Y'',loading_date=sysdate 
						    where file_type = \'\''''+processing_data+'''\'\' and processed = ''N'' 
							    and file_name in (select --+parallel(a,32)  
												DISTINCT '''+process_log_update_col+''' 
												from '''+raw_data_tbl+''' a)'; 
		COMMIT;                   
		EXCEPTION
		    WHEN OTHERS THEN
		    pkg_exception.log_error_dev('RND GGSN Live Loading Process Failed. Table Name:'''+target_live_load_tbl+''''); 
		    raise;
	END;
	'''
if __name__=="__main__":
	startD = datetime.now()	
	try:
		process_parameter = sys.argv[1]
		if process_parameter == '':
			write_processing_log_to_tbl([[datetime.now(),processing_data,'Parameter Not Correct','QUIT',startD]])
			quit()
	except Exception as e:
		write_processing_log_to_tbl([[datetime.now(),processing_data,'Cant Parse Parameter','QUIT',startD]])
		quit()
	getSet_variables_data(process_parameter)
	flag_live_loading_start = Path(process_running_flag_file)
	if not flag_live_loading_start.is_file():
		Path(process_running_flag_file).touch()
		write_processing_log_to_tbl([[datetime.now(),processing_data,'Live load started','START',startD]])
		set_query_variables_data()
		run_parallel_processing()
		write_processing_log_to_tbl([[datetime.now(),processing_data,'Total Elapsed Time:' + str(datetime.now() - startD),'END',datetime.now()]])
		Path(process_running_flag_file).unlink()
	else:
		write_processing_log_to_tbl([[datetime.now(),processing_data,'Previous Script is running. Process will exit','QUIT',datetime.now()]])
