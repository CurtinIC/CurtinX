import os
import datetime
import pandas as pd
import timing
import warnings
warnings.filterwarnings("ignore")


# paths to specific needed folder
folder_base = os.path.dirname(os.path.realpath("__file__"))
folder_preprocessed_mooc = folder_base + "/ObfuscatedCSV"
if os.path.isdir(folder_preprocessed_mooc) == False: # if no "ObfuscatedCSV" folder, create it
	os.makedirs(folder_preprocessed_mooc)
folder_sql = folder_base + "/curtinx-2016-06-26" # "folder_sql" points to the folder with all sql files
folder_logs = folder_base + "/EventData" # "folder_logs" points at the folder with the event files to analyze


# The list of moocs for which an obfuscation has to be done
# The name of each entry must be the the name that appears in the logs
moocs_available = ["course-v1:CurtinX+MKT1x+1T2016", "course-v1:CurtinX+DBAEx+3T2015"]


def get(course):

	"""
	Return the right class for course (based on the name in the log)
	"""

	if course in ["course-v1:CurtinX+MKT1x+1T2016", "MKT1x+1T2016.csv"]:
		ret = MKT1T1T2016(course)
	elif course in ["course-v1:CurtinX+MKT2x+2T2016", "MKT2x+2T2016.csv"]:
		ret = MKT2T2T2016(course)
	elif course in ["course-v1:CurtinX+DBAEx+3T2015", "DBAEx+3T2015.csv"]:
		ret = DBAE3T2015(course)
	elif course in ["course-v1:CurtinX+TBOMx+2T2015", "TBOMx+2T2015.csv"]:
		ret = TBOM2T2015(course)
	else:
		ret = None

	return ret


class MKT1T1T2016:

	"""
	Class for MKT1T12016
	"""
	def __init__(self, name):

		# SQL Table Pointers
		self.sql_prefix = name.split(":")[1].replace("+", "-")
		self.sql_auth_users = self.sql_prefix + "-auth_user-prod-analytics.sql"
		self.sql_auth_users_profile = self.sql_prefix + "-auth_userprofile-prod-analytics.sql"
		self.sql_certificate = self.sql_prefix + "-certificates_generatedcertificate-prod-analytics.sql"
		self.sql_course_struct = self.sql_prefix + "-course_structure-prod-analytics.json"
		self.sql_course_obfuscated = self.sql_prefix + "-student_courseenrollment-prod-analytics.sql"
		self.sql_global_obfuscated = self.sql_prefix + "-user_id_map-prod-analytics.sql"
		self.sql_language = self.sql_prefix + "-student_languageproficiency-prod-analytics.sql"

		# File Pointers
		self.log_name = name
		self.name = name.split(":")[1].split("CurtinX+")[1]
		self.nice_name = "Missing name!"
		self.plot_name = self.name.split("x")[0]
		self.clean = self.name + "_Clean.csv"
		self.markov = self.name + "_Markov.csv"
		self.chain = self.name + "_Chain.txt"
		self.chainDirty = self.name + "_ChainDirty.txt"

		# Object use in the obfuscation process
		self.dictio = dict()
		self.mapped_users = pd.DataFrame()
		self.staff = pd.DataFrame()
		self.tools = list()
		self.staff = list(['mallen@edx.org', 'J.Forsyth@cbs.curtin.edu.au', 'sonia.dickinson@cbs.curtin.edu.au', 'alison.barker@cbs.curtin.edu.au', 'vanessa.chang@curtin.edu.au', 'brian.murphy1@curtin.edu.au', 'jacqui.kelly@curtin.edu.au', 'jacqui.kelly@curtin.edu.au', 'h.mcnair@curtin.edu.au', 'simon.huband@curtin.edu.au', 'marketing@curtin.edu.au', 'J.Schrape@curtin.edu.au', 'patrice.williams@curtin.edu.au', 'hipd0@slipry.net', 'catherine.martella@curtin.edu.au', 'himky@slipry.net', 'dexter.retamar@gmail.com', 'kstypsianou@gmail.com', 'burrelau@kean.edu', 'kat@themagichappens.com', 'rejanaraju@outlook.com', 'shirubi@yahoo.com'])

		# Date for the Mooc (not used)
		self.enroll_date = datetime.datetime(2016, 01, 22, 00, 00, 00)
		self.start_date = datetime.datetime(2016, 04, 04, 00, 00, 00)
		self.end_date = datetime.datetime(2016, 05, 13, 23, 59, 59)
		self.total_days = (self.end_date - self.start_date).days
		self.weeks = self.total_days // 7 + 1


class MKT2T2T2016:

	"""
	Class for MKT2T2T2016
	"""

	def __init__(self, name):

		# SQL Table Pointers
		self.sql_prefix = name.split(":")[1].replace("+", "-")
		self.sql_auth_users = self.sql_prefix + "-auth_user-prod-analytics.sql"
		self.sql_auth_users_profile = self.sql_prefix + "-auth_userprofile-prod-analytics.sql"
		self.sql_certificate = self.sql_prefix + "-certificates_generatedcertificate-prod-analytics.sql"
		self.sql_course_struct = self.sql_prefix + "-course_structure-prod-analytics.json"
		self.sql_course_obfuscated = self.sql_prefix + "-student_courseenrollment-prod-analytics.sql"
		self.sql_global_obfuscated = self.sql_prefix + "-user_id_map-prod-analytics.sql"
		self.sql_language = self.sql_prefix + "-student_languageproficiency-prod-analytics.sql"

		# File Pointers
		self.log_name = name
		self.name = name.split(":")[1].split("CurtinX+")[1]
		self.nice_name = "Missing name!"
		self.plot_name = self.name.split("x")[0]
		self.clean = self.name + "_Clean.csv"
		self.markov = self.name + "_Markov.csv"
		self.chain = self.name + "_Chain.txt"
		self.chainDirty = self.name + "_ChainDirty.txt"

		# Object use in the obfuscation process
		self.dictio = dict()
		self.mapped_users = pd.DataFrame()
		self.staff = pd.DataFrame()
		self.tools = list()
		self.staff = list(['jjorge@edx.org', 'Bridget.tombleson@curtin.edu.au', 'Lydia.Gallant@cbs.curtin.edu.au', 'k.wolf@curtin.edu.au', 'vanessa.chang@curtin.edu.au', 'brian.murphy1@curtin.edu.au', 'simon.huband@curtin.edu.au', 'kduffy@edx.org', 'marketing@curtin.edu.au', 'J.Forsyth@cbs.curtin.edu.au', 'jjorge@edx.org', 'J.Schrape@curtin.edu.au', 'patrice.williams@curtin.edu.au', 'n.david@curtin.edu.au', 'h.mcnair@curtin.edu.au', 'james.holloway@curtin.edu.au', 'curtinx@curtin.edu.au'])

		# Date for the Mooc (not used)
		self.enroll_date = datetime.datetime(2016, 02, 29, 00, 00, 00)
		self.start_date = datetime.datetime(2016, 05, 30, 00, 00, 00)
		self.end_date = datetime.datetime(2016, 07, 23, 00, 00, 00)
		self.total_days = (self.end_date - self.start_date).days
		self.weeks = self.total_days // 7 + 1


class DBAE3T2015:

	"""
	Class for DBAE3T2015
	"""

	def __init__(self, name):
		# SQL Table Pointers
		self.sql_prefix = name.split(":")[1].replace("+", "-")
		self.sql_auth_users = self.sql_prefix + "-auth_user-prod-analytics.sql"
		self.sql_auth_users_profile = self.sql_prefix + "-auth_userprofile-prod-analytics.sql"
		self.sql_certificate = self.sql_prefix + "-certificates_generatedcertificate-prod-analytics.sql"
		self.sql_course_struct = self.sql_prefix + "-course_structure-prod-analytics.json"
		self.sql_course_obfuscated = self.sql_prefix + "-student_courseenrollment-prod-analytics.sql"
		self.sql_global_obfuscated = self.sql_prefix + "-user_id_map-prod-analytics.sql"
		self.sql_language = self.sql_prefix + "-student_languageproficiency-prod-analytics.sql"

		# File Pointers
		self.log_name = name
		self.name = name.split(":")[1].split("CurtinX+")[1]
		self.nice_name = "Missing name!"
		self.plot_name = self.name.split("x")[0]
		self.clean = self.name + "_Clean.csv"
		self.markov = self.name + "_Markov.csv"
		self.chain = self.name + "_Chain.txt"
		self.chainDirty = self.name + "_ChainDirty.txt"

		# Object use in the obfuscation process
		self.dictio = dict()
		self.mapped_users = pd.DataFrame()
		self.staff = pd.DataFrame()
		self.tools = list()
		self.staff = list(['emily@edx.org', 'J.Schrape@curtin.edu.au', 'h.mcnair@curtin.edu.au', 'patrice.williams@curtin.edu.au', 'sonia.dickinson@cbs.curtin.edu.au', 'jjorge@edx.org', 'vanessa.chang@curtin.edu.au', 'james.holloway@curtin.edu.au', 'J.Forsyth@cbs.curtin.edu.au', 'brian.murphy1@curtin.edu.au', 'n.david@curtin.edu.au', 'simon.huband@curtin.edu.au', 'marketing@curtin.edu.au', 'massimo.vitiello@curtin.edu.au', 'jacqui.kelly@curtin.edu.au', 'curtinx@curtin.edu.au', 'd.taylor@curtin.edu.au', 'brian.murphy1@curtin.edu.au', 'Lydia.Gallant@cbs.curtin.edu.au', 'Bridget.tombleson@curtin.edu.au', 'k.wolf@curtin.edu.au', 'catherine.martella@curtin.edu.au', 'h.mcnair@curtin.edu.au', 'james.holloway@curtin.edu.au'])

		self.enroll_date = datetime.datetime(2015, 7, 8, 00, 00, 00)
		self.start_date = datetime.datetime(2015, 11, 02, 00, 00, 00)
		self.end_date = datetime.datetime(2015, 12, 22, 23, 59, 59)
		self.total_days = (self.enroll_date - self.start_date).days
		self.weeks = self.total_days // 7 + 1


class TBOM2T2015:

	"""
	Class for TBOM2T2015
	"""

	def __init__(self, name):


		# SQL Table Pointers
		self.sql_prefix = name.split(":")[1].replace("+", "-")
		self.sql_auth_users = self.sql_prefix + "-auth_user-prod-analytics.sql"
		self.sql_auth_users_profile = self.sql_prefix + "-auth_userprofile-prod-analytics.sql"
		self.sql_certificate = self.sql_prefix + "-certificates_generatedcertificate-prod-analytics.sql"
		self.sql_course_struct = self.sql_prefix + "-course_structure-prod-analytics.json"
		self.sql_course_obfuscated = self.sql_prefix + "-student_courseenrollment-prod-analytics.sql"
		self.sql_global_obfuscated = self.sql_prefix + "-user_id_map-prod-analytics.sql"
		self.sql_language = self.sql_prefix + "-student_languageproficiency-prod-analytics.sql"

		# File Pointers
		self.log_name = name
		self.name = name.split(":")[1].split("CurtinX+")[1]
		self.nice_name = "Missing name!"
		self.plot_name = self.name.split("x")[0]
		self.clean = self.name + "_Clean.csv"
		self.markov = self.name + "_Markov.csv"
		self.chain = self.name + "_Chain.txt"
		self.chainDirty = self.name + "_ChainDirty.txt"

		# Object use in the obfuscation process
		self.dictio = dict()
		self.mapped_users = pd.DataFrame()
		self.staff = pd.DataFrame()
		self.tools = list()

		# Date for the Mooc (not used)
		self.enroll_date = datetime.datetime(2016, 05, 25, 00, 00, 00)
		self.start_date = datetime.datetime(2016, 07, 01, 00, 00, 00)
		self.end_date = datetime.datetime(2016, 12, 19, 00, 00, 00)
		self.total_days = (self.end_date - self.start_date).days
		self.weeks = self.total_days // 7 + 1



def dbg(string, pr=True):

	"""
	Print string as debug
	"""

	if pr:
		print "  \x1b[35m-ModelPredictor-\x1b[00m [\x1b[33m{}\x1b[00m] \x1b[36m{}\x1b[00m".format(datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S"), string)


# (not mandatory) Dictionary to do the mapping
eventMapper = dict()
eventMapper["edx.course.enrollment.activated"] = "EnrollmentActivated"
eventMapper["edx.course.enrollment.deactivated"] = "EnrollmentDeactivated"
eventMapper["edx.course.enrollment.mode_changed"] = "EnrollmentModeChanged"
eventMapper["edx.course.enrollment.upgrade_clicked"] = "EnrollmentUpgradeClicked"
eventMapper["edx.course.enrollment.upgrade.succeeded"] = "EnrollmentUpgradeSucceeded"

eventMapper["edx.ui.lms.link_clicked"] = "LMSLinkClicked"
eventMapper["edx.ui.lms.outline.selected"] = "LMSOutlineSelected"
eventMapper["edx.ui.lms.sequence.next_selected"] = "LMSNextSelected"
eventMapper["seq_next"] = "LMSNextSelected"
eventMapper["edx.ui.lms.sequence.previous_selected"] = "LMSPreviousSelected"
eventMapper["seq_prev"] = "LMSPreviousSelected"
eventMapper["edx.ui.lms.sequence.tab_selected"] = "LMSTabSelected"
eventMapper["seq_goto"] = "LMSTabSelected"
eventMapper["page_close"] = "PageClose"

eventMapper["hide_transcript"] = "VideoTranscriptHidden"
eventMapper["edx.video.transcript.hidden"] = "VideoTranscriptHiddenMobile"
eventMapper["edx.video.closed_captions.hidden"] = "VideoCaptionHidden"
eventMapper["edx.video.closed_captions.shown"] = "VidepCaptionShown"
eventMapper["edx.video.loaded"] = "VideoLoadedMobile"
eventMapper["load_video"] = "VideoLoaded"
eventMapper["edx.video.paused"] = "VideoPausedMobile"
eventMapper["pause_video"] = "VideoPaused"
eventMapper["edx.video.played"] = "VideoPlayedMobile"
eventMapper["play_video"] = "VideoPlayed"
eventMapper["edx.video.position.changed"] = "VideoPositionChangedMobile"
eventMapper["seek_video"] = "VideoPositionChanged"
eventMapper["show_transcript"] = "VideoTranscriptShown"
eventMapper["edx.video.transcript.shown"] = "VideoTranscriptShownMobile"
eventMapper["speed_change_video"] = "VideoSpeedChanged"
eventMapper["edx.video.stopped"] = "VideoStoppedMobile"
eventMapper["stop_video"] = "VideoStopped"
eventMapper["edx.video.language_menu.hidden"] = "VideoLanguageMenuHiddenMobile"
eventMapper["video_hide_cc_menu"] = "VideoLanguageMenuHidden"
eventMapper["edx.video.language_menu.shown"] = "VideoLanguageMenuShownMobile"
eventMapper["video_show_cc_menu"] = "VideoLanguageMenuShown"

eventMapper["edx.video.bumper.dismissed"] = "PreRollVideoDismissed"
eventMapper["edx.video.bumper.loaded"] = "PreRollVideoLoaded"
eventMapper["edx.video.bumper.played"] = "PreRollVideoPlayed"

eventMapper["edx.video.bumper.skipped"] = "PreRollVideoSkipped"
eventMapper["edx.video.bumper.stopped"] = "PreRollVideoStopped"
eventMapper["edx.video.bumper.transcript.hidden"] = "PreRollVideoTranscriptHidden"
eventMapper["edx.video.bumper.transcript.menu.hidden"] = "PreRollVideoTranscriptMenuHidden"
eventMapper["edx.video.bumper.transcript.menu.shown"] = "PreRollVideoTranscriptMenuShown"
eventMapper["edx.video.bumper.transcript.shown"] = "PreRollVideoTranscriptShown"

eventMapper["book"] = "TextbookBook"
eventMapper["textbook.pdf.thumbnails.toggled"] = "TextbookThumbnailsToggled"
eventMapper["textbook.pdf.thumbnail.navigated"] = "TextbookThumbnailNavigated"
eventMapper["textbook.pdf.outline.toggled"] = "TextbookOutlineToogled"
eventMapper["textbook.pdf.chapter.navigated"] = "TextbookChapterNavigated"
eventMapper["textbook.pdf.page.navigated"] = "TextbookPageNavigated"
eventMapper["textbook.pdf.zoom.buttons.changed"] = "TextbookZoomButtonsChanged"
eventMapper["textbook.pdf.zoom.menu.changed"] = "TextbookZoomMenuChanged"
eventMapper["textbook.pdf.display.scaled"] = "TextbookDisplayScaled"
eventMapper["textbook.pdf.page.scrolled"] = "TextbookPageScrolled"
eventMapper["textbook.pdf.search.executed"] = "TextbookSearchExecuted"
eventMapper["textbook.pdf.search.navigatednext"] = "TextbookSearchNavigatedNext"
eventMapper["textbook.pdf.search.highlight.toggled"] = "TextbookSearchHighlightToggled"
eventMapper["textbook.pdf.searchcasesensitivity.toggled"] = "TextbookSearchCaseSensitivityToggled"

eventMapper["edx.problem.hint.demandhint_displayed"] = "ProblemDemandHintDisplayed"
eventMapper["edx.problem.hint.feedback_displayed"] = "ProblemFeedbackHintDisplayed"
eventMapper["problem_check"] = "ProblemCheck"
eventMapper["problem_check_fail"] = "ProblemCheckFail"
eventMapper["problem_graded"] = "ProblemGraded"
eventMapper["problem_rescore"] = "ProblemRescore"
eventMapper["problem_rescore_fail"] = "ProblemRescoreFail"
eventMapper["problem_reset"] = "ProblemReset"
eventMapper["problem_save"] = "ProblemSave"
eventMapper["problem_show"] = "ProblemShow"
eventMapper["reset_problem"] = "ProblemReset"
eventMapper["reset_problem_fail"] = "ProblemResetFail"
eventMapper["save_problem_fail"] = "ProblemSaveFail"
eventMapper["save_problem_success"] = "ProblemSaveSuccess"
eventMapper["showanswer"] = "ProblemShowAnswer"

eventMapper["edx.bookmark.accessed"] = "BookmarkAccessed"
eventMapper["edx.bookmark.added"] = "BookmarkAdded"
eventMapper["edx.bookmark.listed"] = "BookmarkListed"
eventMapper["edx.bookmark.removed"] = "BookmarkRemoved"

eventMapper["edx.course.student_notes.added"] = "NotesAdded"
eventMapper["edx.course.student_notes.deleted"] = "NotesDeleted"
eventMapper["edx.course.student_notes.edited"] = "NotesEdited"
eventMapper["edx.course.student_notes.notes_page_viewed"] = "NotesPageViewed"
eventMapper["edx.course.student_notes.searched"] = "NotesSearched"
eventMapper["edx.course.student_notes.used_unit_link"] = "NotesUsedUnitLink"
eventMapper["edx.course.student_notes.viewed"] = "NotesViewed"

eventMapper["edx.forum.comment.created"] = "ForumCommentCreated"
eventMapper["edx.forum.response.created"] = "ForumResponseCreated"
eventMapper["edx.forum.response.voted"] = "ForumResponseVoted"
eventMapper["edx.forum.searched"] = "ForumSearched"
eventMapper["edx.forum.thread.created"] = "ForumThreadCreated"
eventMapper["edx.forum.thread.voted"] = "ForumThreadVoted"

eventMapper["xblock.poll.submitted"] = "PollSubmitted"
eventMapper["xblock.poll.view_results"] = "PollViewResults"
eventMapper["xblock.survey.submitted"] = "SurveySubmitted"
eventMapper["xblock.survey.view_results"] = "SurveyViewResults"

eventMapper["edx.certificate.created"] = "CertificateCreated"
eventMapper["edx.certificate.shared"] = "CertificateShared"
eventMapper["edx.certificate.evidence_visited"] = "CertificateEvidenceVisited"

tools_cert = set(["CertificateCreated", "CertificateShared", "CertificateEvidenceVisited"])
