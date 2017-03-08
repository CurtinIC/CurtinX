import os
import json
import multiprocessing

from datetime import datetime
from multiprocessing import Pool

import gzip as gz
import numpy as np
import pandas as pd

import curtinSettings as setting


def trim_fraction(text):

	"""
	Help function to trim text
	"""

	if '.0' in text:
		return text[:text.rfind('.0')]
	return text




def log_cleaner(logs):

	"""
	Clean up the (df) logs file
	Commented code not mandatory to produce the csv file
	"""

	# keep only certain columns
	# if outcommented, keep all fields of the original .gz file
	'''
	logs = logs.loc[:, ["username", "context.course_id", "context.user_id", "time", "name", "event_source", "event_type", "event.user_id"]]
	'''


	# remove weirdly encoded timestamps
	'''
	logs = logs[logs.loc[:, "time"].str.len() == 32]
	logs.loc[:, "time"] = logs.loc[:, "time"].str.replace('T', '-').str.slice(stop=-13)
	'''


	# fill empty "name" with "event_type"
	'''
	logs["name"].fillna(logs["event_type"], inplace=True)
	'''

	# filter out not needed/strange/unsupported events
	'''
	logs = logs[~((logs.loc[:, "name"].str.contains("cohorts")) | (logs.loc[:, "name"].str.contains("undefined")) | (
            logs.loc[:, "name"].str.contains("arrows-")) | (logs.loc[:, "name"].str.contains('/"https://')) | (
            logs.loc[:, "name"].str.endswith("getSnoozing.do")) | (logs.loc[:, "name"].str.contains("edxnotes")) | (
            logs.loc[:, "name"].str.contains("jump_to")) | (logs.loc[:, "name"].str.contains("api")) | (
            logs.loc[:, "name"].str.contains("generate_user_cert")) | (logs.loc[:, "name"].str.contains("moment.js")) | (
            logs.loc[:, "name"].str.contains("courseware")) | (logs.loc[:, "name"].str.contains("@")) | (
            logs.loc[:, "name"] == "edx.user.settings.changed") | (
            logs.loc[:, "name"] == "edx.course.home.resume_course.clicked") | (
            logs.loc[:, "name"] == "edx.course.home.upgrade_verified.clicked") | (
            logs.loc[:, "name"] == "edx.course.home.course_update.toggled"))]
	logs = logs[~((logs.loc[:, "event_source"] == "server") & ((logs.loc[:, "name"].str.contains("problem")) | (logs.loc[:, "name"] == "showanswer")))]
	'''

	# filter out certificates events
	'''
	logs = logs[~(logs.loc[:, "name"].isin(setting.tools_cert))]
	'''

	# users enrollment by professors: fill "context.user_id" with userId, give a dumb "username" to avoid it being dropped
	logs.loc[((logs["name"] == "edx.course.enrollment.activated") | (logs["name"] == "edx.course.enrollment.deactivated")) & (logs["context.user_id"] != logs["event.user_id"]), "username"] = "empty username"
	logs.loc[((logs["name"] == "edx.course.enrollment.activated") | (logs["name"] == "edx.course.enrollment.deactivated")) & (logs["context.user_id"] != logs["event.user_id"]), "context.user_id"] = logs["event.user_id"]

	# fill nan "context.user_id" with the right "event.user_id"
	logs["context.user_id"].fillna(logs["event.user_id"], inplace=True)

	res = list()
	for mooc in results:

		tlogs = logs[logs.loc[:, "context.course_id"] == mooc.log_name]

		# merge interaction and substitute user ids with the obfuscated ones
		tlogs = pd.merge(mooc.mapped_users, tlogs, how='right', left_on='id', right_on='context.user_id')
		tlogs.drop('id', inplace = True, axis=1)

		# convert "time" column to datetime
		'''
		tlogs.time = pd.to_datetime(tlogs.time, errors='coerce')
		'''

		# keep enrollment action that took place anywhere between enroll and end date
		# keep any other type of interaction, only if it happened between start and end date
		'''
		tlogs = tlogs[(tlogs.loc[:, 'time'] >= mooc.enroll_date) & (tlogs.loc[:, 'time'] <= mooc.end_date)]
		tlogs_enrollment = tlogs[tlogs.loc[:, 'name'].str.contains('edx.course.enrollment')]
		
		tlogs = tlogs[tlogs.loc[:, 'time'] >= mooc.start_date]
		tlogs = tlogs.append(tlogs_enrollment, ignore_index=True)
		'''

		# keep only the neeeded columns
		'''
		tlogs = tlogs.loc[:, ["context.user_id", 'hash_id', "time", "name", "event_source"]]
		'''

		res.append(tlogs)

	return res




def paral_processer(name):

	"""
	Process a .gz file into a pandas df.
	Return the cleaned df file representing the .gz
	"""

	setting.dbg("Analyze of file {0}".format(name))
	data = gz.open(name).readlines() # open the gzipped file and read it lines by lines into data
	data = map(lambda x: x.strip(), data) # remove any carriage return from the string
	data_json_str = "[" + ','.join(data) + "]" # transform the string into parsable "json-string"
	data_j = json.loads(data_json_str) # transform the parsable "json-string" into json
	df = pd.DataFrame(data=pd.io.json.json_normalize(data_j)) # normalize json into nice pandas dataframe
	df = log_cleaner(df) # call to log_cleaner to clean it up
	return df




def event_reader(arg, dirname, names):

	"""
	Lunch "nJobs" parallels jobs to process each zipped event file
	"""

	generalStart = min([ arg[i].enroll_date for i in range(len(arg)) ]) # find the initial date across all courses

	zippedValid = list() # zippedValid contains only the zipped file to process (those whose date >= generalStart)
	zipped = [ os.path.join(dirname, name) for name in names if name.endswith(".gz") ]
	for zi in zipped:
		liso = [int(i) for i in list(zi.split("events-")[1].split(".")[0].split("-"))]
		temp = datetime(int(liso[0]), int(liso[1]), int(liso[2]))
	        if temp >= generalStart:
	            zippedValid.append(zi)


	setting.dbg("Start reading event file!")
	nJobs = multiprocessing.cpu_count() - 2
	pool = Pool(processes=nJobs)
	res = pool.map(paral_processer, [ name for name in zippedValid ] )
	setting.dbg("Done reading event file!")

	for m in range(len(arg)):

		setting.dbg("Creating file for {0}!".format(arg[m].name))

		logs = pd.concat([ res[i][m] for i in range(len(res)) ], ignore_index = True) # concat each course df in a single file

		# add class: 1 for passed, 0 for failed
		# as default: "audit_passing", "downloadable", "generating", "regenerating" mean course completed
		os.chdir(setting.folder_sql)
		cert = pd.read_csv(arg[m].sql_certificate, delimiter="\t")
		cert.rename(columns={"user_id":"context.user_id"}, inplace=True)
		cert = cert[cert.loc[:, "status"].isin(["audit_passing", "downloadable", "generating", "regenerating"])]
		cert = cert.loc[:, ["status", "context.user_id"]]
		logs = pd.merge(logs, cert, on="context.user_id", how="left")
		logs = logs.replace(to_replace={"status": {"[a-z]": 1}}, regex=True)
		logs = logs.replace(to_replace={"status": {np.nan: 0}}, regex=True)
		logs.reset_index(0, len(logs), inplace=True)


		# print infos
		comp = logs[logs.loc[:, 'status'] == 1]
		dis = logs[logs.loc[:, 'status'] == 0]
		setting.dbg('Total Learner: {0}'.format(len(logs['context.user_id'].unique())))
		setting.dbg('Total Completers: {0}'.format(len(comp['context.user_id'].unique())))
		setting.dbg('Total Dropouts: {0}'.format(len(dis['context.user_id'].unique())))

		mapper(arg[m], logs) # do a mapping to have a more indicative field "Map"




def mapper(mooc, df):

	"""
	Translate the "Map" field of the df to a better explanatory name using the eventMapper of "curtinSettings.py".
	The commented part of code is a not mandatory step to produce the final csv file. 
	"""

	setting.dbg("Start Mapping for {0}".format(mooc.name))

	'''
	df["Map"] = df["name"].replace(setting.eventMapper)

	# Home page clicks
	df.loc[df["name"].str.endswith("info"), "Map"] = "MainPageLink_Home"
	df.loc[df["name"].str.endswith("info/"), "Map"] = "MainPageLink_Home"
	df.loc[df["name"].str.endswith(mooc.log_name), "Map"] = "MainPageLink_Home"
	df.loc[df["name"].str.endswith(mooc.log_name + "/"), "Map"] = "MainPageLink_Home"

	# Progress clicks
	df.loc[df["name"].str.endswith("progress"), "Map"] = "MainPageLink_Progress"
	df.loc[df["name"].str.endswith("progress/"), "Map"] = "MainPageLink_Progress"
	df.loc[(df["name"].str.contains("/progress/")) & ~(
	df["name"].str.endswith("progress")), "Map"] = "MainPageLink_ProgressUser"

	# Course Wiki clicks
	df.loc[df["name"].str.endswith("course_wiki"), "Map"] = "MainPageLink_Wiki"
	df.loc[df["name"].str.endswith("course_wiki/"), "Map"] = "MainPageLink_Wiki"

	# About clicks
	df.loc[df["name"].str.endswith("about"), "Map"] = "MainPageLink_About"
	df.loc[df["name"].str.endswith("about/"), "Map"] = "MainPageLink_About"

	# Teams clicks
	df.loc[df["name"].str.endswith("teams"), "Map"] = "MainPageLink_Teams"
	df.loc[df["name"].str.endswith("teams/"), "Map"] = "MainPageLink_Teams"

	# Instructors clicks
	df.loc[df["name"].str.endswith("instructor"), "Map"] = "MainPageLink_Instructor"
	df.loc[df["name"].str.endswith("instructor/"), "Map"] = "MainPageLink_Instructor"

	# Forum interactions
	df.loc[df["name"].str.endswith("users"), "Map"] = "ForumVisualizeAllUsers"
	df.loc[df["name"].str.endswith("users/"), "Map"] = "ForumVisualizeAllUsers"
	df.loc[(df["name"].str.contains("/forum/users/")) & ~(
	df["name"].str.endswith("followed")), "Map"] = "ForumVisualizeSingleUser"
	df.loc[(df["name"].str.contains("/forum/users/")) & (df["name"].str.endswith("followed")), "Map"] = "ForumFollowUser"
	df.loc[df["name"].str.endswith("discussion/forum"), "Map"] = "ForumVisualizeMainPage"
	df.loc[df["name"].str.endswith("discussion/forum/"), "Map"] = "ForumVisualizeMainPage"

	df.loc[(df["name"].str.endswith("threads/create")), "Map"] = "ForumThreadCreate"
	df.loc[(df["name"].str.contains("discussion/threads/")) & (
	df["name"].str.endswith("upvote")), "Map"] = "ForumThreadUpvote"
	df.loc[(df["name"].str.contains("discussion/threads/")) & (
	df["name"].str.endswith("reply")), "Map"] = "ForumThreadReply"
	df.loc[(df["name"].str.contains("discussion/threads/")) & (
	df["name"].str.endswith("follow")), "Map"] = "ForumThreadFollow"
	df.loc[(df["name"].str.contains("discussion/threads/")) & (
	df["name"].str.endswith("unfollow")), "Map"] = "ForumThreadUnfollow"
	df.loc[(df["name"].str.contains("discussion/threads/")) & (
	df["name"].str.endswith("/unvote")), "Map"] = "ForumThreadUnvote"
	df.loc[(df["name"].str.contains("discussion/threads/")) & (
	df["name"].str.endswith("/update")), "Map"] = "ForumThreadUpdate"
	df.loc[(df["name"].str.contains("discussion/threads/")) & (
	df["name"].str.endswith("/flagAbuse")), "Map"] = "ForumThreadFlagAbuse"
	df.loc[(df["name"].str.contains("discussion/threads/")) & (
	df["name"].str.endswith("/unFlagAbuse")), "Map"] = "ForumThreadUnflagAbuse"
	df.loc[(df["name"].str.contains("discussion/threads/")) & (
	df["name"].str.endswith("/delete")), "Map"] = "ForumThreadDelete"
	df.loc[(df["name"].str.contains("discussion/threads/")) & (df["name"].str.endswith("/pin")), "Map"] = "ForumThreadPin"
	df.loc[(df["name"].str.contains("/discussion/forum/course/threads/")), "Map"] = "ForumThreadVisualize"
	df.loc[(df["name"].str.contains("discussion/forum/")) & (
	df["name"].str.contains("/threads/")), "Map"] = "ForumThreadVisualize"

	df.loc[(df["name"].str.contains("discussion/comments/")) & (
	df["name"].str.endswith("upvote")), "Map"] = "ForumCommentUpvote"
	df.loc[(df["name"].str.contains("discussion/comments/")) & (
	df["name"].str.endswith("reply")), "Map"] = "ForumCommentReply"
	df.loc[(df["name"].str.contains("discussion/comments/")) & (
	df["name"].str.endswith("update")), "Map"] = "ForumCommentUpdate"
	df.loc[(df["name"].str.contains("discussion/comments/")) & (
	df["name"].str.endswith("delete")), "Map"] = "ForumCommentDelete"
	df.loc[(df["name"].str.contains("discussion/comments/")) & (
	df["name"].str.endswith("unvote")), "Map"] = "ForumCommentUnvote"
	df.loc[(df["name"].str.contains("discussion/comments/")) & (
	df["name"].str.endswith("/flagAbuse")), "Map"] = "ForumCommentFlagAbuse"
	df.loc[(df["name"].str.contains("discussion/comments/")) & (
	df["name"].str.endswith("/unFlagAbuse")), "Map"] = "ForumCommentUnflagAbuse"
	df.loc[(df["name"].str.contains("discussion/comments/")) & (
	df.loc[(df["name"].str.endswith("discussion/upload")), "Map"] = "ForumDiscussionUpload"
	df.loc[(df["name"].str.endswith("forum/search")), "Map"] = "ForumSearch"

	df.loc[(df["name"].str.endswith("discussion/users")), "Map"] = "ForumDiscussionUsers" # Unclear what does this refer to
	df["name"].str.endswith("endorse")), "Map"] = "ForumCommentEndorse" # Unclear what does this refer to
	df.loc[(df["name"].str.endswith("/inline")), "Map"] = "ForumThreadInline" # Unclear what does this refer to


	os.chdir(setting.folder_sql) # handle the redirects using the "-course_structure-prod-analytics.json" file

	js = pd.read_json(mooc.sql_course_struct).T
	js = pd.DataFrame(data=pd.io.json.json_normalize(js[js.category == "course"].metadata))
	js = pd.DataFrame.from_dict(js.iloc[0].tabs).dropna(subset=["url_slug"])
	if not js.empty:
		stat = js["name"].unique().tolist()
		for st in stat:
			toSet = [j.capitalize() for j in st.split(" ")]
			to = ""
			for t in toSet:
				to = to + t
			df.loc[df["name"].str.contains(js[js.loc[:, "name"] == st].iloc[0]["url_slug"]), "Map"] = "MainPageLink_" + to


	# This are extra steps to shrink the output coloumns and drop nan values
	#df.rename(columns={"hash_id": "User", "time": "Timestamp", "status": "Class", 'event_source': 'Source'}, inplace=True)
	#df = df.loc[:, ["User", "Timestamp", "Class", "Map", 'Source']]
	#df.dropna(inplace = True)
	'''


	os.chdir(setting.folder_preprocessed_mooc)
	df.to_csv(mooc.name+'.csv') # write the csv for the course

	setting.dbg("Mapping done for {0}".format(mooc.name))




def paral_dict_creator(mClass):

	"""
	Create a dictionary to map each userId to its obfuscatedId and save it within the "mapped_users" field of mClass.
	"""

	df_auth = pd.read_csv(mClass.sql_auth_users, delimiter="\t")
	df_staff = df_auth[df_auth.loc[:, "email"].isin(mClass.staff)]
	df_users = df_auth[~(df_auth.loc[:, "email"].isin(mClass.staff))]

	df_obf = pd.read_csv(mClass.sql_global_obfuscated, delimiter="\t")
	df_users = pd.merge(df_users, df_obf, how='left', left_on='id', right_on='id')
	df_users = df_users.loc[:, ['id', 'hash_id']]

	mClass.mapped_users = df_users
	del df_auth, df_obf



def moocsAnalyzer(mClass):

	"""
	Create e return the right class for the given course name
	"""

	paral_dict_creator(mClass)
	return mClass




if __name__ == "__main__":

	"""
	Start of the script.
	Creates the specific class for each course as in "curtinSettings.py" and call "event_reader" to process the event files
	"""

	os.chdir(setting.folder_sql)
	pool = Pool()
	results = pool.map(moocsAnalyzer, [ setting.get(mooc) for mooc in setting.moocs_available ])

	os.chdir(setting.folder_logs)
	[ os.path.walk(os.path.dirname(os.path.realpath('__file__')), event_reader, results) ]
