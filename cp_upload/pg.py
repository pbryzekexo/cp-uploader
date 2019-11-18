import os
import sys
import psycopg2
import csv
import string
import consts
from datetime import datetime
from counties import Counties

# 2006 c00411041 a q2  p 25970755004         15  ind ooms   caspar a   san francisco ca  941143018 tamarac capital llc management  5092005 500   c24 181927      4072220051059599373
# 2018
# c00416693 n m2  p 201702029042408827  15  ind ames   patrick m  oakland       ca  94607     corelogic svp of operations for vsg 1052017 500   sa11ai.4353 1147203     4020820171370029025
# (cmte_id text, AMNDT_IND text, RPT_TP text, TRANSACTION_PGI text, IMAGE_NUM text, TRANSACTION_TP text, ENTITY_TP text, last_name text, first_name text, city text, state text, zip_code text, employer text, OCCUPATION text, TRANSACTION_DT Date, TRANSACTION_AMT numeric, OTHER_ID text, TRAN_ID text, FILE_NUM numeric, MEMO_CD text, MEMO_TEXT text, SUB_ID numeric);
# 94110|GARCIA|ANA|GRACIELA| |    |F|653 CAPP ST|SAN FRANCISCO|CA|(415)641-1802||11/25/28|DEMOCRATIC
# zip numeric, last_name text, first_name text, middle_name text, prefix text, suffix text, gender text, address text, city text, state text, phone text, email_address text, birth_date text, party_name text

# C00369926|N|Q1||23990740119|15C|CAN|STATON, CECIL P. DR. JR.|ROME|GA|30161|SMYTH & HELWYS PUBLISHING CO.|PRESIDENT|03012003|14000|H2GA11131|SA11D.7240|82127|||4042320031031003165
voter_fhs = {}
donation_fhs = {}
unique_voter_fhs = {}

unique_donors = {}
unique_voters = {}

voter_file_table_dict = {}
unique_voter_file_table_dict = {}
donor_file_table_dict = {}

states_allowed = {'ca': True}

class PG:
    def __init__(self):
        print("Construction PG")

    def pg_load_donor_table(self, file_path, table_name):
        '''
        This function upload csv to a target table
        '''
        try:
            conn = psycopg2.connect(dbname=consts.dbname, host=consts.host,
                                    port=consts.port, user=consts.user, password=consts.pwd)
            print("Connecting to Database")
            cur = conn.cursor()
            f = open(file_path, "r")
            cur.copy_from(f, table_name, columns=('cmte_id', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI', 'IMAGE_NUM', 'TRANSACTION_TP', 'ENTITY_TP', 'last_name', 'first_name',
                                                  'city', 'state', 'zip', 'employer', 'occupation', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID', 'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT', 'SUB_ID'), sep=",")
            conn.commit()
            conn.close()
            print("DB connection closed.")
        except Exception as e:
            print("Error: {}".format(str(e)))
            sys.exit(1)

    def pg_load_voter_table_from_donations(self, file_path, table_name):
        '''
        This function upload csv to a target table
        '''
        try:
            conn = psycopg2.connect(dbname=consts.dbname, host=consts.host, port=consts.port, user=consts.user, password=consts.pwd)
            print("Connecting to Database")
            cur = conn.cursor()
            f = open(file_path, "r")
            cur.copy_from(f, table_name, columns=('last_name','first_name','city','state','zip','employer','occupation'), sep=",")
            conn.commit()
            conn.close()
            print("Load Voter from Donations DB connection closed.")
        except Exception as e:
            print("Error: {}".format(str(e)))
            sys.exit(1)

    def pg_load_voter_table(self, file_path, table_name):
        '''
        This function upload csv to a target table
        '''
        try:
            conn = psycopg2.connect(
                dbname=consts.dbname, host=consts.host, port=consts.port, user=consts.user, password=consts.pwd)
            print("Connecting to Database")
            cur = conn.cursor()
            f = open(file_path, "r")
            cur.copy_from(f, table_name, columns=('zip', 'last_name', 'first_name', 'middle_name', 'prefix', 'suffix',
                                                  'gender', 'address', 'city', 'state', 'phone', 'email_address', 'birth_date', 'party_name'), sep=",")
            conn.commit()
            conn.close()
            print("Load Voter DB connection closed.")
        except Exception as e:
            print("Error: {}".format(str(e)))
            sys.exit(1)

    def pg_load_table(self, file_path, table_name):
        '''
        This function upload csv to a target table
        '''
        try:
            conn = psycopg2.connect(
                dbname=consts.dbname, host=consts.host, port=consts.port, user=consts.user, password=consts.pwd)
            print("Connecting to Database")
            cur = conn.cursor()
            f = open(file_path, "r")
            # Truncate the table first
            cur.execute("Truncate {} Cascade;".format(table_name))
            print("Truncated {}".format(table_name))
            # Load table from the file with header
            cur.copy_expert("copy {} from STDIN CSV HEADER QUOTE '\"'".format(table_name), f)
            cur.execute("commit;")
            print("Loaded data into {}".format(table_name))
            conn.close()
            print("DB connection closed.")
        except Exception as e:
            print("Error: {}".format(str(e)))
            sys.exit(1)

    def pg_create_table(self, create_sql):
        '''
        This function will create the table if it does not exist
        '''
        try:
            conn = psycopg2.connect(dbname=consts.dbname, host=consts.host, port=consts.port, user=consts.user, password=consts.pwd)
            print("Connecting to Database")
            cur = conn.cursor()
            cur.execute(create_sql)
            conn.commit()
            conn.close()
            print("DB connection closed.")
        except Exception as e:
            print("Error: {}".format(str(e)))
            sys.exit(1)

    def clean_string(self, orig_str):
        clean_str = orig_str.lower().strip()
        clean_str = ' '.join(clean_str.split())
        clean_str = ''.join(clean_str.split(","))
        clean_str = ''.join(clean_str.split("\\"))
        final_clean_str = "".join(filter(lambda char: char in string.printable, clean_str))
        if ',' in final_clean_str:
            print ('Clean String comma found')
        return final_clean_str

    def write_voter_sql(self, file_path, state):
        sql_file_path = file_path + '/'  + state + '/votersql.txt'
        fh_sql = open(sql_file_path, 'a')

        voter_table_name = 'cpvoter_' + state
        voter_sql_create_table = 'CREATE TABLE IF NOT EXISTS ' + voter_table_name + \
            ' (id serial, zip numeric, last_name text, first_name text, middle_name text, prefix text, suffix text, gender text, address text, city text, state text, phone text, email_address text, birth_date text default null, party_name text default null, UNIQUE(zip, last_name, first_name, middle_name, prefix, suffix, gender, address, city, state, phone, email_address, birth_date, party_name));'
        voter_trigger_name = 'modified_' + voter_table_name + '_trg'
        voter_sql_trigger = 'CREATE TRIGGER ' + voter_trigger_name + ' AFTER INSERT OR UPDATE OR DELETE ON ' + \
            voter_table_name + ' FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();'

        self.write_sql_line(fh_sql, voter_sql_create_table)
        self.write_sql_line(fh_sql, voter_sql_trigger)
        fh_sql.close()

        d = dict()
        d['voter_table'] = voter_table_name
        d['voter_trigger'] = voter_sql_trigger
        d['voter_create_table'] = voter_sql_create_table
        return d

    def write_sql_line(self, fh_sql, sql_line):
        fh_sql.write(sql_line)
        fh_sql.write("\n")
        fh_sql.write("\n")

    def write_unique_voter_from_donor_sql(self, file_path, state):
        sql_file_path = file_path + '/sql.txt'
        fh_sql = open(sql_file_path, 'a')

        voter_table_name = 'cpvoter_' + state
        voter_sql_create_table = 'CREATE TABLE IF NOT EXISTS ' + voter_table_name + \
	      ' (id serial, last_name text, first_name text, city text, state text, zip text, employer text, occupation text, UNIQUE(last_name, first_name, city, state, zip, employer, occupation));'
        voter_trigger_name = 'modified_' + voter_table_name + '_trg'
        voter_sql_trigger = 'CREATE TRIGGER ' + voter_trigger_name + ' AFTER INSERT OR UPDATE OR DELETE ON ' + \
	    voter_table_name + ' FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();'
        
        self.write_sql_line(fh_sql, voter_sql_create_table)
        self.write_sql_line(fh_sql, voter_sql_trigger)
        
        d = dict()
        d['voter_table'] = voter_table_name
        d['voter_trigger'] = voter_sql_trigger
        d['voter_create_table'] = voter_sql_create_table
        return d

    def write_donor_sql(self, file_path, state):
        sql_file_path = file_path + '/sql.txt'
        fh_sql = open(sql_file_path, 'a')

        donor_table_name = 'cpdonor_' + state
        donor_sql_create_table = 'CREATE TABLE IF NOT EXISTS ' + donor_table_name + \
            ' (id serial, cmte_id text, AMNDT_IND text, RPT_TP text, TRANSACTION_PGI text, IMAGE_NUM text, TRANSACTION_TP text, ENTITY_TP text, last_name text, first_name text, city text, state text, zip text, employer text, OCCUPATION text, TRANSACTION_DT text, TRANSACTION_AMT numeric, OTHER_ID text, TRAN_ID text, FILE_NUM text, MEMO_CD text, MEMO_TEXT text, SUB_ID numeric, UNIQUE(cmte_id, AMNDT_IND, RPT_TP, TRANSACTION_PGI, IMAGE_NUM, TRANSACTION_TP, ENTITY_TP, last_name, first_name, city, state, zip, employer, OCCUPATION, TRANSACTION_DT, TRANSACTION_AMT, OTHER_ID, TRAN_ID, FILE_NUM, MEMO_CD, MEMO_TEXT, SUB_ID));'
        donor_trigger_name = 'modified_' + donor_table_name + '_trg'
        donor_sql_trigger = 'CREATE TRIGGER ' + donor_trigger_name + ' AFTER INSERT OR UPDATE OR DELETE ON ' + \
            donor_table_name + ' FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();'

        self.write_sql_line(fh_sql, donor_sql_create_table)
        self.write_sql_line(fh_sql, donor_sql_trigger)
        fh_sql.close()
        
        d = dict()
        d['donor_table'] = donor_table_name
        d['donor_trigger'] = donor_sql_trigger
        d['donor_create_table'] = donor_sql_create_table
        return d

    def get_sm_pieces(self, pieces):
        if not pieces:
            print ('pieces not found skipping')
            return None
        if len(pieces) < 14:
            print ('skipping since not enough data')
            return None
        return pieces

    def get_sf_pieces(self, pieces):
        # 94110|GARCIA|ANA|GRACIELA| |    |F|653 CAPP ST|SAN FRANCISCO|CA|(415)641-1802||11/25/28|DEMOCRATIC
        if not pieces:
            print ('pieces not found skipping')
            return None
        city = pieces[8]
        if not city:
            print ('voter city not found skipping')
            return None
        if len(pieces) < 14:
            print ('skipping since not enough data')
            return None
        return pieces
        # TODO fix.
        # for x in range(len(pieces), 14):
        #  pieces.append("")

    def get_nv_pieces(self, clean_line, county):
        pieces = clean_line.split('|')
        if not pieces:
            print ('nv continue skipping over pieces')
            return None

    def get_ca_pieces(self, clean_line, county):
        pieces = clean_line.split('|')
        if not pieces:
            print ('nv continue skipping over pieces')
            return None

        if county == 'san francisco':
            return self.get_sf_pieces(pieces)
        elif county == 'san mateo':
            return self.get_sm_pieces(pieces)
        else:
            print('Unsupported county ' + county)
            return None

    def get_state_csvline(self, voter_file, orig_line, state, county):
        clean_line = self.clean_string(orig_line)
        if not clean_line:
            print ('skip voter line')
            return None

        pieces = []
        if state == 'ca':
            pieces = self.get_ca_pieces(clean_line, county)
        elif state == 'nv':
            pieces = self.get_nv_pieces(clean_line, county)
        else:
            print('Unknown State ' + state)
            return None

        if not pieces:
            return None
        voter_csv_line = ','.join(str(x) for x in pieces)
        return voter_csv_line

    # Parse all voters
    def upload_voters(self):
        supported_states = states_allowed.keys()
        for supported_state in supported_states:
            file_path = consts.voter_directory + supported_state
            voter_file_path = file_path + '/voters.csv'
            if not os.path.exists(file_path):
                print('New State ' + file_path)
                os.system('mkdir ' + file_path)
                
            voter_fh = open(voter_file_path, 'a+')
            if voter_file_path in voter_fhs:
                print('This State is duplicated! skipping' + voter_file_path)
                continue

            voter_fhs[voter_file_path] = voter_fh
            for root, dirs, files in os.walk(consts.voter_directory):
                for county_file in files:
                    if not county_file.startswith("voters_"):
                        print("Skipping file = " + county_file)
                        continue

                    voter_file = consts.voter_directory + county_file
                    county = county_file.split('_')[1]
                    print("voter_file" + voter_file)
                    print ('for county= ' + county)

                    if voter_file and not voter_file in voter_file_table_dict:
                        voter_table_name = 'cpvoter_' + supported_state
                        voter_file_table_dict[voter_file] = voter_table_name

                    with open(voter_file, "r") as fh:
                        for line in fh:
                            voter_csv_line = self.get_state_csvline(voter_file, line, supported_state, county)
                            if voter_csv_line:
                                voter_fh.write(voter_csv_line)
                                voter_fh.write("\n")

            # Create the voter table if it doesn't exist
            d = self.write_voter_sql(consts.voter_directory, supported_state)
            voter_create_table = d['voter_create_table']
            print ("creating Voter Table")
            self.pg_create_table(voter_create_table)

        # Close all the file handles
        vals = voter_fhs.values()
        for i in range(len(vals)):
            vals[i].close()

        # Load the state specific values into the DB.
        for supported_state in supported_states:
            voter_file_path = consts.voter_directory + supported_state + '/voters.csv'
            voter_table_name = 'cpvoter_' + supported_state
            print ('voter_file_path ' + voter_file_path)
            print ('voter_table_name ' + voter_table_name)
            self.pg_load_voter_table(voter_file_path, voter_table_name)

    # Parse all donations
    def upload_donations(self):
        counties = Counties()

        for root, dirs, files in os.walk(consts.donor_directory):
            for donation_file in files:
                # For now just limiting to a single file for DB constraints.
                if not donation_file.endswith(".txt"):
                    continue

                if donation_file.startswith('itcont_18') or donation_file.startswith('itcont_20'):
                # if donation_file.endswith(".txt") and (donation_file.startswith('itcont_18') or donation_file.startswith('itcont_16')
                #     or donation_file.startswith('itcont_14') or donation_file.startswith('itcont_12')):
                    print(donation_file)
                    file_path = consts.donor_directory + donation_file
                    f = open(file_path, 'r')
                    for line in f:
                        clean_string = self.clean_string(line)
                        if not clean_string:
                            print ('skip donor line 1')
                            continue

                        pieces = clean_string.split('|')
                        if not pieces or len(pieces) < 10:
                            print ('skip donor line 2')
                            continue
                        elif len(pieces) != 21:
                            # 22 cols but they have name as 1 field.
                            # Some of the format on the excel sheet is incorrect.
                            print ('len= ' + str(len(pieces)))
                            continue

                        state = pieces[9]
                        if (not state) or not (state in states_allowed):
                            continue

                        city = pieces[8]
                        if not city:
                            continue
                        elif state == 'ca' and not counties.is_bay_area(city):
                            continue
                        elif state == 'nv' and city != 'reno':
                            continue

                        zip_code = pieces[10].strip()
                        if not zip_code:
                            zip_code = '0'

                        employer = pieces[11]
                        occupation = pieces[12]
                        if pieces[13]:
                            try:
                                date_str = datetime.strptime(pieces[13], '%m%d%Y').strftime('%Y-%m-%d')
                                pieces[13] = date_str
                            except:
                                print ('date issue ' + pieces[13])
                                continue

                        name_str = pieces[7]
                        name_pieces = name_str.split(' ', 1)

                        last_name = ''
                        first_name = ''
                        if len(name_pieces) >= 1:
                            last_name = name_pieces[0]

                        if len(name_pieces) >= 2:
                            first_name = name_pieces[1]

                        pieces[7] = last_name
                        pieces.insert(8, first_name)
                        csv_line = ','.join(str(x) for x in pieces)

                        file_path = consts.donor_directory + state
                        if not os.path.exists(file_path):
                            print('New State ' + state)
                            os.system('mkdir ' + file_path)
                            d = self.write_donor_sql(file_path, state)
                            donor_table_name = d['donor_table']
                            create_donor_table = d['donor_create_table']
                            self.pg_create_table(create_donor_table)

                        if not state in donation_fhs:
                            donor_file_path = file_path + '/donations.csv'
                            fh_state = open(donor_file_path, 'a')
                            donation_fhs[state] = fh_state
                            if donor_file_path and not donor_file_path in donor_file_table_dict:
                                donor_table_name = 'cpdonor_' + state
                                donor_file_table_dict[donor_file_path] = donor_table_name

                            voter_file_path = file_path + '/unique_voter_donations.csv'
                            voter_fh = open(voter_file_path, 'a')
                            unique_voter_fhs[state] = voter_fh

                            if voter_file_path and not voter_file_path in unique_voter_file_table_dict:
                                voter_table_name = 'cpvoter_' + state
                                unique_voter_file_table_dict[voter_file_path] = voter_table_name

                        voter_pieces = []
                        voter_pieces.append(last_name)
                        voter_pieces.append(first_name)
                        voter_pieces.append(city)
                        voter_pieces.append(state)
                        voter_pieces.append(zip_code)
                        voter_pieces.append(employer)
                        voter_pieces.append(occupation)
                        voter_csv_line = ','.join(str(x) for x in pieces)

                        # To avoid duplicate donors
                        if not csv_line in unique_donors:
                            fh_write = donation_fhs[state]
                            fh_write.write(csv_line)
                            fh_write.write("\n")
                            unique_donors[csv_line] = True

                        # To avoid duplicate voters
                        if not voter_csv_line in unique_voters:
                            voter_fh_write = unique_voter_fhs[state]
                            voter_fh_write.write(voter_csv_line)
                            voter_fh_write.write("\n")
                            unique_voters[voter_csv_line] = True

                    f.close()

        vals = donation_fhs.values()
        for i in range(len(vals)):
            vals[i].close()

        donor_file_paths = donor_file_table_dict.keys()
        for i in range(len(donor_file_paths)):
            donor_file_path = donor_file_paths[i]
            donor_table_name = donor_file_table_dict[donor_file_path]
            print ('donor_file_path ' + donor_file_path)
            print ('donor_table_name ' + donor_table_name)
            self.pg_load_donor_table(donor_file_path, donor_table_name)

        '''
	  # Unique Voters Derived from Donations File
	  voter_vals = unique_voter_fhs.values()
	  for i in range(len(voter_vals)):
	      voter_vals[i].close()

	  # This is for unique Voters derived from the donations
	  voter_file_paths = unique_voter_file_table_dict.keys()
	  for i in range(len(voter_file_paths)):
	      voter_file_path = voter_file_paths[i]
	      voter_table_name = unique_voter_file_table_dict[voter_file_path]
	      print ('voter_file_path ' + voter_file_path)
	      print ('voter_table_name ' + voter_table_name)
	      pg_load_voter_table(voter_file_path, voter_table_name)
	  '''
