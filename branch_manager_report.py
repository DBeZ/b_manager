#########################################
# Report for she codes; branch managers #
# By: Dr. Doreen Ben-Zvi                #
# Aug 2020                              #
#########################################
import pandas as pd
import numpy as np
import datetime as dt
import dateutil.rrule as rrule
import dateutil.relativedelta as relativedelta
import mysql.connector  # Install mysql-connector-python
import matplotlib.pyplot as plt
import yagmail
import logging
import os
import boto3
import sys
import getopt
import json
import urllib3

# Set global variables
help_text = '''
usage: branch_manager_report [option] 
Options:
-h   : Print this help message and exit
-m   : Test mode, specify report recipient Email address. 
        If no branch ID is specified - sends all reports to the specified Email address.
-b   : Test mode, specify branch report to send
        Cannot be used without specifying report recipient Email address. 
-a   : About this report script
'''

about_text = '''
Branch Manager Report v1.0 Aug 20 Doreen Ben-Zvi

1. SQL query retrieves data from sheconnect database for a specific dates range.
2. Branch overview table of how many participants and staff studied each track during each week in the date range.
3. Graph of overview table from (1) including overall student trend.
4. User specific table displaying maximal lesson studied by each user in each track during each week in the date range.
5. Branch emails retrieved.
6. 2-4 are sent to the appropriate branch
7. 2-4 are copied to AWS as archive (Backup limit is set to #TODO add limit)

v1.0  Includes: loading from config file, command line options, error log, output save to separate folder
'''

# Load Config file
config_file_name = "config.json"
with open(config_file_name) as config_file:
    config_data = json.load(config_file)

# Set global variables
log_filename = config_data["output"]["log_filename"]
log_template = "%s %s failed at function %s"
log_template_with_branch = "%s %s failed at function %s during branch %s report"
logging.basicConfig(filename=log_filename, level=logging.ERROR)

sql_details = config_data["db_login"]
username_key = "User"
password_key = "Pass"
database_key = "Database"
host_key = "Host"
port_key = "Port"

email_details = config_data["email_login"]
query_fields_details = config_data["query_fields"]
team_role_details = config_data["team_values"]["values"]
s3_details = config_data["s3_login"]

branch_data_url = config_data["output"]["branch_data_url"]
output_folder_name = config_data["output"]["folder_name"]

email_title = 'דו"ח שבועי למנהלות סניף ' + "%s"
email_content = '''
<p style = "direction: rtl;" >שלום לכן,</p>
<p style = "direction: rtl;" >מצורף הדו&quot;ח השבועי של הסניף שלכן, המכיל מידע בנוגע לפעילות משתתפות וחברות צוות בתאריכים %s.</p>
<p style = "direction: rtl;" >המידע בנוגע לשייכות לסניף נכון לעת שליפת הנתונים ולאו דוקא אחורה בזמן. כלומר, מידע של משתתפות שעברו סניף יופיע בסניף אליו הן הצטרפו.</p>
<p style = "direction: rtl;" >בברכה,</p>
<p style = "direction: rtl;" >;she codes צוות Data</p>
'''


def load_contents_from_txt(filename):
    try:
        f = open(filename, "r")
        if f.mode == 'r':
            contents = f.readlines()
        f.close()
        return contents
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Reading from text file",
            "load_contents_from_txt"))


# Load variables from text files to list
# In text file first line is sql table name and is ignored
def load_txt_as_list(contents):
    try:
        value_list = []
        for k, line in enumerate(contents):
            if k != 1:
                value_list.append(line.strip())
        return value_list
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Loading from text file to list",
            "load_txt_as_list"))


# Load variables from text files to dict
# In text file- key: value
def load_txt_as_dict(contents):
    try:
        login_dict = {}
        for line in contents:
            splitted = line.split(":")
            key_name = splitted[0].strip()
            value = splitted[-1].strip()
            login_dict[key_name] = value
        return login_dict
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Loading from text file to dictionary",
            "load_txt_as_dict"))


# Load variables from text files
# If text file contains key value pairs separated by colon,
# they will be loaded as a dictionary.
# If text file begins with table name, all other lines will
# be loaded as list
def fields_from_text_loader(filename, to_dict_flag=False):
    contents = load_contents_from_txt(filename)
    if to_dict_flag is True:
        login_dict = load_txt_as_dict(contents)
        return login_dict
    else:
        value_list = load_txt_as_list(contents)
        return value_list


# Saves table as csv to current folder
def save_as_csv(file_name, table):
    try:
        table.to_csv(file_name, index=True, encoding='utf-8-sig')
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Saving table as CSV file",
            "save_as_csv"))


# Connects to Gmail using oath2.0
def connect_to_gmail(email_login_dict):
    try:
        yag_connection = yagmail.SMTP(user=email_login_dict[username_key],
                                      oauth2_file=os.path.join(os.getcwd(),
                                                               email_details["Oauth_2.0_file"]))  # client_secret_local
        return yag_connection
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Connecting to SMTP email server",
            "connect_to_gmail"))


# Generates and sends email including all attachments created
def send_email(branch_email, email_title, email_content, attachments_list):
    yag_connection = connect_to_gmail(email_details)
    attachments_list = [os.path.join(os.getcwd(), entry) for entry in attachments_list]
    try:
        yag_connection.send(to=branch_email, subject=email_title, contents=email_content,
                            attachments=attachments_list)
        yag_connection.close()
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Sending Email",
            "send_email"))


# Transfer file to S3 AWS as archive
def transfer_to_aws(file_list, s3_login_dict):
    try:
        session = boto3.Session(
            aws_access_key_id=s3_login_dict['aws_access_key_id'],
            aws_secret_access_key=s3_login_dict['aws_secret_access_key'],
            region_name=s3_login_dict['region_name']
        )
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Connecting to s3",
            "transfer_to_aws"))

    for single_file in file_list:
        try:
            timestamped_filename = str(dt.datetime.now().date()) + " " + single_file
            s3_resource = session.resource(s3_login_dict['service_name']).Bucket(s3_login_dict['bucket_name'])
            s3_resource.upload_file(single_file, timestamped_filename)
        except:
            logging.error(log_template % (
                str(dt.datetime.now()), "Uploading to s3",
                "transfer_to_aws") + ". Failed on file " + str(single_file))

    # Print all file names in bucket
    # for file in s3_resource.objects.all():
    #     print(file.key)


# creates connection to sql database
def connect_to_database(sql_details):
    try:
        cnx = mysql.connector.connect(
            user=sql_details[username_key],
            password=sql_details[password_key],
            database=sql_details[database_key],
            host=sql_details[host_key],
            port=sql_details[port_key]
        )
        return cnx
    except:
        str(logging.error(log_template % (dt.datetime.now()), "Connection to SQL database", "connect_to_database", ))


# Creates SQL query
def branch_manager_report_query(date_four_months_ago, date_now, fields_dict):
    # Dates needs to be strings in format 2016-08-07
    try:
        final_columns = '''
            userID, 
            firstname_eng, 
            lastname_eng, 
            email, 
            dateJoined, 
            track, 
            lessonDate, 
            lessonNo, 
            branchID, 
            branchName, 
            role_ID, 
            roleName
            '''
        all_columns = '''
            userID,
            firstname_eng,
            lastname_eng,
            email,
            dateJoined,
            track,
            lessonDate,
            serial_number AS lessonNo,
            branchID,
            branchName
            '''
        lesson_followup_columns = '''
            userid_connect,
            track_id_connect AS trackID,
            FROM_UNIXTIME(TIMESTAMP, '%Y-%m-%d') AS lessonDate,
            lesson_id_connect
            '''
        lesson_followup_table = "shecodes_monster_2_0.lessons_followup"
        new_users_columns = '''
            userid_connect AS userID,
            firstname_eng,
            lastname_eng,
            email,
            FROM_UNIXTIME(date_joined_lms, '%Y-%m-%d') AS dateJoined,
            branch_ID AS BranchID
            '''
        new_users_table = "shecodes_monster_2_0.users_new"
        lesson_mapping_table = "shecodes_monster_2_0.lessons_mapping"
        branch_type_select = '''
            id AS branch_ID,
            short_name AS branchName,
            branchTypeID,
            branchTypes AS branchType,
            active
            '''
        branch_type_columns = '''
            id AS branchTypeID,
            branch_type AS branchTypes
            '''
        branch_type_table = "shecodes_monster_2_0.branch_types"
        branch_table = "shecodes_monster_2_0.branch"
        track_name_columns = '''
            ID,
            track_name AS track
            '''
        track_name_table = "shecodes_monster_2_0.track_name"
        where_clause = '''
            track_category NOT IN(0, 8) AND
            track_type NOT IN(0, 3) AND
            branchTypeID NOT IN(1, 9) AND
            track_id_connect NOT IN(14) AND
            active IS NOT NULL AND
            serial_number IS NOT NULL
            '''
        order_clause = "lessonNo"
        group_clause = '''
            userID,
            lessondate
            '''
        user_max_type_no_assignment_date_select = '''
            user_ID, maxRoleID AS role_ID, shortname AS roleName
            '''
        user_max_type_select = '''
            user_ID, MAX(replaced_role_ids) as maxRoleID, assignRoleDate
            '''
        user_type_select = '''
            userid as user_ID , 
            FROM_UNIXTIME(timemodified, '%Y-%m-%d') AS assignRoleDate
            ''' + ","
        cases_clause = ''' 
        WHEN roleid = 28 THEN 0 
        WHEN roleid = 25 THEN 0 
        WHEN roleid = 24 THEN 0 
        ELSE roleid 
        '''
        roles_table = "shecodes_shecodes.mdl_role_assignments"
        group_clause2 = 'user_ID'
        where_clause2 = "user_ID IS NOT NULL"

        query_text = (
            f'SELECT {final_columns} '
            f'FROM (SELECT * '
            f'FROM (SELECT * '
            f'FROM(SELECT {all_columns} '
            f'FROM(SELECT {lesson_followup_columns} '
            f'FROM {lesson_followup_table} '
            f') AS lessonDateTable '
            f'LEFT JOIN (SELECT {new_users_columns} '
            f'FROM {new_users_table} '
            f') AS usersSubset '
            f'ON userid_connect = userID '
            f'LEFT JOIN {lesson_mapping_table} '
            f'ON lessons_mapping.lesson_id_connect = lessonDateTable.lesson_id_connect '
            f'LEFT JOIN (SELECT {branch_type_select} '
            f'FROM(SELECT {branch_type_columns} '
            f'FROM {branch_type_table} '
            f') AS Branch_Types  '
            f'RIGHT JOIN {branch_table} '
            f'ON branch.branch_type = Branch_Types.branchTypeID '
            f') AS Branches '
            f'ON branch_ID = BranchID '
            f'LEFT JOIN shecodes_monster_2_0.tracks '
            f'ON tracks.track_id_connect = lessonDateTable.trackID '
            f'LEFT JOIN (SELECT {track_name_columns} '
            f'FROM {track_name_table} '
            f') AS '
            f'track ON track.ID = tracks.track_name '
            f'WHERE {where_clause} AND '
            f"UNIX_TIMESTAMP(lessonDate) BETWEEN UNIX_TIMESTAMP(\'{date_four_months_ago}\') AND UNIX_TIMESTAMP(\'{date_now}\') "
            f'ORDER BY {order_clause} DESC '
            f') AS t '
            f'GROUP BY {group_clause} '
            f')AS t3 '
            f')AS user_lessons_branch_table '
            f'LEFT JOIN (SELECT {user_max_type_no_assignment_date_select} '
            f'FROM (SELECT * '
            f'FROM (SELECT {user_max_type_select} '
            f'FROM (SELECT {user_type_select} '
            f'CASE {cases_clause} '
            f'END AS replaced_role_ids '
            f'FROM {roles_table} '
            f') AS userRoleSubset '
            f'GROUP BY {group_clause2} '
            f') AS data_table '
            f'LEFT JOIN mdl_role '
            f'ON data_table.maxRoleID=mdl_role.id '
            f'WHERE {where_clause2} '
            f') AS role_table '
            f') AS highest_roles '
            f'ON user_lessons_branch_table.userID=highest_roles.user_ID '
        )
        return query_text, fields_dict
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "SQL query composition failed", "branch_manager_report_query"))


# Retrieve data from sql database using a pre generated query
def query_database(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        columns = cursor.column_names
        query_data_df = pd.DataFrame(cursor.fetchall())
        query_data_df.columns = list(columns)
        connection.close()
        cursor.close()
        return query_data_df
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Querying database failed", "query_database"))


# Builds query and retrieves data from database
def retrieve_branch_manager_report_data(date_begin, date_end):
    try:
        if not isinstance(date_begin, str):
            date_begin = str(date_begin)
        if not isinstance(date_end, str):
            date_end = str(date_end)
        connection = connect_to_database(sql_details)
        query_text, fields_dict = branch_manager_report_query(date_begin, date_end, query_fields_details)
        data_df = query_database(connection, query_text)
        return data_df, fields_dict
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Querying database failed", "retrieve_branch_manager_report_data"))


# Get date 15 weeks ago
def last_15_weeks_range():
    try:
        today_date = dt.date.today()
        date_15_weeks_ago = today_date - dt.timedelta(weeks=15)
        return date_15_weeks_ago, today_date
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Generating date range",
            "last_15_weeks_range"))


## Get start and end dates for each week in date range
# Input - any two dates. Output - all week start and week ends in this range.
# Week begins on Sunday
def week_date_start_end(start_limit, end_limit):
    try:
        # convert to a single value
        if isinstance(start_limit, list) or isinstance(start_limit, pd.Series):
            start_limit = start_limit[0]
        if isinstance(end_limit, list) or isinstance(end_limit, pd.Series):
            end_limit = end_limit[0]
        # convert to datetime
        if not isinstance(start_limit, dt.datetime) and not isinstance(start_limit, dt.date):
            start_limit = dt.datetime(start_limit, 1, 1)
        if not isinstance(end_limit, dt.datetime) and not isinstance(end_limit, dt.date):
            end_limit = dt.datetime(end_limit, 12, 31)
        if isinstance(start_limit, dt.date):
            start_limit = dt.datetime(start_limit.year, start_limit.month, start_limit.day)
        if isinstance(end_limit, dt.date):
            end_limit = dt.datetime(end_limit.year, end_limit.month, end_limit.day)
        rule_sunday = rrule.rrule(rrule.WEEKLY, byweekday=relativedelta.SU, dtstart=start_limit)
        sundays = rule_sunday.between(start_limit, end_limit, inc=True)
        saturdays = [d + dt.timedelta(days=6) for d in sundays]
        # start_week_dates=[dt.datetime.strftime(d, '%Y-%m-%d') for d in sundays]
        # end_week_dates=[dt.datetime.strftime(d, '%Y-%m-%d') for d in saturdays]
        return sundays, saturdays
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Date range conversion",
            "week_date_start_end"))


# Converts registration date to month-year
def convert_track_opening(df, registered_col):
    try:
        registered_series_datetime = pd.to_datetime(df[registered_col])
        month = registered_series_datetime.dt.strftime("%b %y")
        df[registered_col] = month
        return df
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Converting track opening date",
            "convert_track_opening"))


## Count active users in the given date range
def activity_total(data, activity_col, userid_col, week_start_list, week_end_list, result_name):
    try:
        data[activity_col] = data[activity_col].apply(pd.to_datetime)
        cols = []
        results = []
        for i in range(len(week_start_list)):
            mask = (week_start_list[i] <= data[activity_col]) & (data[activity_col] <= week_end_list[i])
            active_in_range = data[userid_col][mask]
            col_name = str(dt.datetime.strftime(week_start_list[i], '%d/%m/%Y')) + "-" + str(
                dt.datetime.strftime(week_end_list[i], '%d/%m/%Y'))
            results.append(active_in_range.unique().size)
            cols.append(col_name)
        result_df = pd.DataFrame(results)
        result_df.index = cols
        result_df.rename(columns={0: result_name}, inplace=True)
        result_df = result_df.transpose()
        return result_df
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Counting all users active in date range",
            "activity_total"))


## Count active users by track in the given date range
def activity_by_track(data, track_col, activity_col, userid_col, week_start_list, week_end_list):
    try:
        result_df = pd.DataFrame()
        tracks = data[track_col].unique()
        tracks = np.sort(tracks)
        for track in tracks:
            data[track_col]
            mask1 = (data[track_col] == track)
            track_data = data.loc[mask1].copy(deep=True)
            result_name = track
            df = activity_total(data=track_data, activity_col=activity_col, week_start_list=week_start_list,
                                userid_col=userid_col, week_end_list=week_end_list, result_name=result_name)
            result_df = pd.concat([result_df, df.transpose()], axis=1)
        return result_df.transpose()
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Counting all users active in date range by track",
            "activity_by_track"))


# Convert activity date to week
# Adds week label as date range in a new column
def activity_by_user(data, activity_col, userid_col, week_start_list, week_end_list):
    try:
        data[activity_col] = data[activity_col].apply(pd.to_datetime)
        data["lesson_week"] = ""
        all_week_names = []
        for i in range(len(week_start_list)):
            mask = (week_start_list[i] <= data[activity_col]) & (data[activity_col] <= week_end_list[i])
            date_range_name = str(dt.datetime.strftime(week_start_list[i], '%d/%m/%Y')) + "-" + str(
                dt.datetime.strftime(week_end_list[i], '%d/%m/%Y'))
            all_week_names.append(date_range_name)
            data.loc[mask, "lesson_week"] = date_range_name
        return data, all_week_names
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Converting activity date to week range",
            "activity_by_user"))


# Converts role name to Yes/No staff
def convert_role(data_df, fields_dict, team_role_names_list):
    try:
        dummy_table = pd.get_dummies(data_df[fields_dict["role"]])  # Turn role category into binary attribute column
        data_df["team_member"] = False
        for col in dummy_table.columns:
            if col in team_role_names_list:
                data_df["team_member"] = data_df["team_member"] | dummy_table[col].astype('bool')
        data_df["team_member"] = data_df["team_member"].astype('str')
        data_df = data_df.replace({"team_member": {'True': "Yes", 'False': "No"}})
        role_data = data_df.drop([fields_dict["role"], fields_dict["role_ID"]], axis=1)
        return role_data
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Converting role to Yes/No staff",
            "convert_role"))


# Creates table to monitor learning progress for each user
def progress_table_generator(df_branch_user, fields_dict, all_week_names):
    try:
        # concat values for pivot
        df_branch_user["full_details"] = df_branch_user[fields_dict["user_first_name"]].str.title() + " " + \
                                         df_branch_user[fields_dict["user_last_name"]].str.title() + "__" + \
                                         df_branch_user["team_member"].astype(str) + "__" + df_branch_user[
                                             fields_dict["email"]] + "__" + df_branch_user[fields_dict["enroll"]]
        # Pivot
        progress_table = pd.pivot_table(df_branch_user, values='lessonNo', index=["userID", "track", "full_details"],
                                        columns=['lesson_week'], aggfunc=np.max)
        # Editing for display
        progress_table.reset_index(inplace=True)
        # Create summation columes based on pivot
        attendance = progress_table.iloc[:, 3:].count(axis=1)
        maxLesson = progress_table.iloc[:, 3:].max(axis=1).astype('str')
        progress_table.fillna('', inplace=True)
        # separate concatenated values for final display
        details = progress_table["full_details"].str.split("__", expand=True)
        progress_table.insert(1, "full name", details[0], allow_duplicates=False)
        progress_table.insert(2, "staff", details[1], allow_duplicates=False)
        progress_table.insert(3, "email", details[2], allow_duplicates=False)
        progress_table.insert(4, "joined", details[3], allow_duplicates=False)
        progress_table.insert(6, "attendance in last 15 weeks", attendance, allow_duplicates=False)
        progress_table.insert(7, "Max lesson entered", maxLesson, allow_duplicates=False)
        progress_table = progress_table.drop(columns=["userID", "full_details"], axis=1)
        cols = progress_table.columns[0:7].values.tolist() + all_week_names  # sort week ranges by order

        if dt.date.today().isoweekday() != 7:
            progress_table = progress_table.drop(columns=[""],
                                                 axis=1)  # If report is not run on sunday sql date range in query is larger then week range. This removes attendance out of the date range.
        else:
            cols = cols[
                   0:-2]  # If run on sunday Prevents range from including week that has just begun and has no data yet
        progress_table = progress_table[cols]
        return progress_table
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Generating progress table",
            "progress_table_generator"))


# Generate branch summary graph
def branch_summary_graph(table_totals, figure_name):
    try:
        # Generate graph
        plt.rcParams.update({'font.size': 18})
        table_totals.iloc[1:-1, :].transpose().plot(kind='bar', figsize=(40, 10))
        current_ax = plt.gca()

        x = table_totals.transpose().reset_index().reset_index()["level_0"]
        y = table_totals.transpose().reset_index().reset_index()["Total participants+staff"]

        table_totals.iloc[0, :].transpose().plot(ax=current_ax, color='r')
        for i, j in zip(x, y):
            current_ax.annotate(str(j), xy=(i, j))

        plt.grid(True, which='both', axis='y')
        plt.xticks(rotation=90)
        plt.rcParams.update({'font.size': 18})

        z = np.polyfit(x, y, 1)
        p = np.poly1d(z)
        plt.plot(x, p(x), "r--")
        current_ax.legend(
            [table_totals.index.values.tolist()[0], "Trendline (Total P+S)"] + table_totals.index.values.tolist()[1:-1],
            loc='center left', bbox_to_anchor=(1, 0.5))

        plt.tight_layout()
        plt.savefig(figure_name)
        plt.close()

    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Generating graph",
            "branch_summary_graph"))


# Creates dataframe with data specific for a single branch
def branch_specific_data(activity_df, branch_code, fields_dict):
    mask = (activity_df[fields_dict["branch_id"]] == branch_code)
    df_branch = activity_df.loc[mask, :].copy(deep=True)
    return df_branch


# Branch summary table - generation and save as csv
def branch_summary_table(all_participants_df, by_track_all_participants_df, branch_name):
    table_totals = pd.concat([all_participants_df, by_track_all_participants_df], axis=0)
    table_totals_save_name = "Branch report for " + branch_name + ".csv"
    save_as_csv(table_totals_save_name, table_totals)
    return table_totals, table_totals_save_name


# Participant progress table  - generation and save as csv
def user_progress_table(df_branch_user, all_week_names, branch_name, fields_dict):
    progress_table_save_name = branch_name + " branch member activity.csv"
    progress_table = progress_table_generator(df_branch_user, fields_dict, all_week_names)
    save_as_csv(progress_table_save_name, progress_table)
    return progress_table, progress_table_save_name


# Generate the three output files for each branch, then sends them
# to the branch email address and copies it to AWS as backup
def branch_report_generator(activity_df, start_week_dates, end_week_dates, fields_dict, output_folder_name,
                            email_content, email_title,
                            branch_code, branch_email):
    try:
        df_branch = branch_specific_data(activity_df, branch_code, fields_dict)
        branch_name = str(df_branch[fields_dict["branch"]].iloc[0])
        # Total participants accessing each track each week
        all_participants_df = activity_total(data=df_branch, activity_col="lessonDate", userid_col="userID",
                                             week_start_list=start_week_dates, week_end_list=end_week_dates,
                                             result_name="Total participants+staff")
        # Participants no accessing each track each week
        by_track_all_participants_df = activity_by_track(data=df_branch, track_col="track", activity_col="lessonDate",
                                                         userid_col="userID", week_start_list=start_week_dates,
                                                         week_end_list=end_week_dates)
        # User specific progress each week in each track she accessed
        [df_branch_user, all_week_names] = activity_by_user(data=df_branch, activity_col="lessonDate",
                                                            userid_col="userID",
                                                            week_start_list=start_week_dates,
                                                            week_end_list=end_week_dates)

        home_dir = os.getcwd()
        if not os.path.exists(output_folder_name):
            os.makedirs(output_folder_name)
        os.chdir(output_folder_name)

        # Branch summary table - generation and save as csv
        [table_totals, table_totals_save_name] = branch_summary_table(all_participants_df, by_track_all_participants_df,
                                                                      branch_name)

        # Branch summary figure - generation and save as png
        figure_name = branch_name + " branch summary graph"
        branch_summary_graph(table_totals, figure_name)

        # Participant progress table  - generation and save as csv
        [_, progress_table_save_name] = user_progress_table(df_branch_user, all_week_names, branch_name, fields_dict)

        file_list = [table_totals_save_name, figure_name + ".png", progress_table_save_name]
        send_email(branch_email, email_title, email_content, file_list)
        os.chdir(home_dir)
        transfer_to_aws(file_list, s3_details)

    except:
        logging.error(log_template_with_branch % (
            str(dt.datetime.now()), "Generating data",
            "branch_report_generator", str(branch_code)))


# Get branches and emails from JSON
def retrieve_all_branch_codes_and_emails(url):
    try:
        http = urllib3.PoolManager()
        response = http.request('GET', url)
        data = json.loads(response.data.decode('utf-8'))
        return data
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Getting branches and emails from JSON",
            "retrieve_all_branch_codes_and_emails"))


# Loop over all branches to generate all their reports and send to each branch email
def generate_report_for_all_branches(activity_df, start_week_dates, end_week_dates, fields_dict, email_title,
                                     email_content, email=""):
    branch_data_list = retrieve_all_branch_codes_and_emails(branch_data_url)

    # TODO: remove next two lines when live
    for dict_branch in branch_data_list:
        dict_branch["branch_email"] = "doreen@she-codes.org"

    branch_data = pd.DataFrame(branch_data_list)
    [col_name.replace(":", "").strip() for col_name in branch_data.columns]
    mask = (branch_data["branch_type"] == 9) | (branch_data["branch_type"] == 1) | (
            branch_data["branch_type"] == 7)  # NG branches and Haredi branches which study offline
    branch_data = branch_data.loc[~mask, ['id', "branch_name", "branch_email"]]
    branch_data.dropna(inplace=True)
    try:
        if email == '':
            for inx, row in branch_data.iterrows():
                branch_report_generator(activity_df, start_week_dates, end_week_dates, fields_dict, output_folder_name,
                                        email_content=email_content, email_title=email_title % row["branch_name"],
                                        branch_code=int(row["id"]), branch_email=row["branch_email"])
        else:
            for inx, row in branch_data.iterrows():
                branch_report_generator(activity_df, start_week_dates, end_week_dates, fields_dict, output_folder_name,
                                        email_content=email_content,
                                        email_title=email_title % row["branch_name"],
                                        branch_code=int(row["id"]), branch_email=email)
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Looping over all branches", 'generate_report_for_all_branches'))


# Date range setup - true for all branches each week
def retriev_data_from_last_four_months(date_four_months_ago, date_now):
    try:
        [start_week_dates, end_week_dates] = week_date_start_end(date_four_months_ago, date_now)
        [activity_df, fields_dict] = retrieve_branch_manager_report_data(date_four_months_ago, date_now)
        return activity_df, fields_dict, start_week_dates, end_week_dates
    except:
        logging.error(log_template % (
            str(dt.datetime.now()), "Retrieving data", 'retriev_data_from_last_four_months'))


def main(argv, email_title, email_content):
    test_email = ''
    branch_code = ''
    try:
        options, args = getopt.getopt(argv, "m:b:ha", ["test_email=", "branch_id="])
    except getopt.GetoptError:
        print(
            "Test options requires input \n Use the form: branch_manager_report -t xxx@yyyy.zzz \n or Use the form: branch_manager_report -t xxx@yyyy.zzz -b #no")
        sys.exit(2)

    for opt, arg in options:
        if opt == "-a":
            print(about_text)
            sys.exit()
        elif opt == "-h":
            print(help_text)
            sys.exit()

    [date_four_months_ago, date_now] = last_15_weeks_range()
    [activity_df, fields_dict, start_week_dates, end_week_dates] = retriev_data_from_last_four_months(
        date_four_months_ago, date_now)

    date_four_months_ago = date_four_months_ago.strftime("%d.%m.%Y")
    date_now = date_now.strftime("%d.%m.%Y")
    email_content = email_content % (str(date_now) + " - " + str(date_four_months_ago))

    # Data cleanup - true for all branches each week
    activity_df = convert_role(activity_df, fields_dict, team_role_details)
    activity_df = convert_track_opening(activity_df, "dateJoined")

    for opt, arg in options:
        if opt in ("-b"):
            branch_code = arg
        if opt in ("-m"):
            test_email = arg

    if (branch_code == "") and (test_email == ""):
        generate_report_for_all_branches(activity_df, start_week_dates, end_week_dates, fields_dict, email_title,
                                         email_content)
        sys.exit()
    elif branch_code == "":
        generate_report_for_all_branches(activity_df, start_week_dates, end_week_dates, fields_dict, email_title,
                                         email_content, email=test_email)
        sys.exit()
    elif test_email == "":
        print("Branch ID cannot be input without report recipient Email address")
    elif (not branch_code == "") and (not test_email == ""):
        branch_report_generator(activity_df, start_week_dates, end_week_dates, fields_dict, output_folder_name,
                                email_content, email_title=email_title % branch_code,
                                branch_code=int(branch_code), branch_email=test_email)
        sys.exit()
    else:
        print(help_text)


if __name__ == "__main__":
    main(sys.argv[1:], email_title, email_content)
