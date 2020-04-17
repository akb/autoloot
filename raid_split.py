from __future__ import print_function
import datetime
import time
import pickle
import os.path
import random
import string
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

UNIQUE_TAG = ' ' + str(datetime.date.today()) + ' ' + ''.join(random.choice(string.ascii_uppercase) for _ in range(6))


RAID = 'ZG'
#RAID = 'ONY'

SPREADSHEET_ID = '1TCScsxoXeSHoCeMo53ncz_1Vu10t0wtqEmMvjK7ZNpk'

TO_SPLIT_TAB = 'to split!A2:A'
RAIDERS_TAB = 'raiders!A2:H'

if RAID == 'ONY':
    REQUIRED_TAB = 'ony required!A2:B'
elif RAID == 'ZG':
    REQUIRED_TAB = 'zg required!A2:B'
else:
    raise Exception('Raid not specified.')
    

POOLS = {"Tank": ['bear', 'wtank'], 
        "Range": ['mage', 'warlock', 'hunter', 'spriest'], 
        "Melee": ['cat', 'rogue', 'wdps', 'rpaladin'],
        "Heal": ['hpaladin', 'hpriest', 'hdruid']}

HIGHLIGHTS = {
    "rogue": (252.0/256.0, 233.0/256.0, 15.0/256.0, 0.75),
    "hunter": (70.0/256.0, 189.0/256.0, 66.0/256.0, 0.75),
    "paladin": (227.0/256.0, 118.0/256.0, 180.0/256.0, 0.75),
    "druid": (254.0/256.0, 141.0/256.0, 10.0/256.0, 0.75),
    "mage": (38.0/256.0, 184.0/256.0, 252.0/256.0, 0.75),
    "warlock": (161.0/256.0, 77.0/256.0, 235.0/256.0, 0.75),
    "priest": (181.0/256.0, 177.0/256.0, 181.0/256.0, 0.75),
    "warrior": (138.0/256.0, 85.0/256.0, 39.0/256.0, 0.75),
}


        
NUM_RANDOM_SPLITS = 20000
NUM_SPLITS_TO_WRITE = 5

def authenticate():
    creds = None
    if os.path.exists('token_raid_split.pickle'):
        with open('token_raid_split.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token_raid_split.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds

def read_to_split(sheet): 
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,range=TO_SPLIT_TAB).execute()                        
    names = []                        
    for row in result.get('values', []):
        names.append(row[0])
    return names

def read_raiders(sheet): 
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,range=RAIDERS_TAB).execute()                        
    raiders = {}                        
    for row in result.get('values', []):
        name = row[0]
        role = row[1]
        attributes = set(row[2].split(';'))
        main = ""
        if len(row) >= 4:
            main = row[3]
        partner = ""
        if len(row) >= 5:
            partner = row[4]
        social_pref = set()
        if len(row) >= 6 and row[5]:
            social_pref = set(row[5].split(";"))
        loot = set()
        if len(row) >= 7 and row[6] and RAID == 'ONY':
            loot = set(row[6].split(";"))
        if len(row) >= 8 and row[7] and RAID == 'ZG':
            loot = set(row[7].split(";"))
        raiders[name] = {"name":name, "role":role, "attributes":attributes, "main":main, "partner":partner, "social_pref":social_pref, "loot":loot}
    return raiders

def read_required_constraints(sheet): 
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,range=REQUIRED_TAB).execute()                        
    constraints = []                        
    for row in result.get('values', []):
        constraint = row[0]
        min_req = int(row[1])
        constraints.append((constraint, min_req))
    print(constraints)
    return constraints

def read_to_split(sheet): 
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,range=TO_SPLIT_TAB).execute()                        
    names = []                        
    for row in result.get('values', []):
        names.append(row[0])
    return names

def is_alt(name, raiders):
    return 'alt' in raiders[name]['attributes']

def add_alts_to_raiders(raiders):
    for name in raiders.keys():
        if is_alt(name, raiders):
            main = raiders[name]['main']
            alts = raiders[main].get('alts', set())
            alts.add(name)
            raiders[main]['alts'] = alts
            
def make_split(raiders_to_split, raiders):
    raid1 = []
    raid2 = []
    alts = []
    for pool_roles in POOLS.values():
        pool = filter(lambda x: raiders[x]['role'] in pool_roles, raiders_to_split)
        random.shuffle(pool)
        while pool:
            if pool and len(raid1) <= len(raid2):
                raider = pool.pop()
                if is_alt(raider, raiders):
                    alts.append(raider)
                else:
                    raid1.append(raider)
            if pool:
                raider = pool.pop()
                if is_alt(raider, raiders):
                    alts.append(raider)
                else:
                    raid2.append(raider)
    
    for alt in alts:
        if raiders[alt]["main"] in raid1:
            raid2.append(alt)
        elif raiders[alt]["main"] in raid2:
            raid1.append(alt)
        else:
            if len(raid1) < len(raid2):
                raid1.append(alt)
            else:
                raid2.append(alt)
    return raid1, raid2

def check_constraints(raid, constraints, raiders):
    for constraint in constraints:
        count = 0
        for name in raid:
            if constraint[0] in raiders[name]["attributes"]:
                count+=1
        if count < constraint[1]:
            return False
    return True
            
def score_balance(raid1, raid2, raiders, annotations):
    raid_size_score = (len(raid1) - len(raid2))**2
    total_score = raid_size_score
    annotations['info'].append("Raid size difference %s (%s vs %s)" % (abs(len(raid1)-len(raid2)), len(raid1), len(raid2)))
    for pool_name, pool_roles in POOLS.items():
        raid1_count = len(filter(lambda x: raiders[x]['role'] in pool_roles, raid1))
        raid2_count = len(filter(lambda x: raiders[x]['role'] in pool_roles, raid2))
        score = (raid1_count - raid2_count)**2
        total_score += score
        annotations['info'].append("%s size difference %s (%s vs %s)" % (pool_name, abs(raid1_count-raid2_count), raid1_count, raid2_count))
    return total_score


def is_in_raid(name, raid, raiders):
    return len(filter(lambda x: x == name or x in raiders[name].get('alts', set()), raid)) > 0
    
def in_different_raids(name, partner, raid1, raid2, raiders):
    return (is_in_raid(name, raid1, raiders) and not is_in_raid(name, raid2, raiders) and is_in_raid(partner, raid2, raiders) and not is_in_raid(partner, raid1, raiders)) or (is_in_raid(name, raid2, raiders) and not is_in_raid(name, raid1, raiders) and is_in_raid(partner, raid1, raiders) and not is_in_raid(partner, raid2, raiders))
    
            
def score_social(raid1, raid2, raiders, annotations):
    partner_count=0
    pref_count=0
    for name in raid1 + raid2:
        for social_pref in raiders[name]['social_pref']:
            if in_different_raids(name, social_pref, raid1, raid2, raiders):
                annotations['warning'].append("Social Preference %s and %s not in same raid." % (name, social_pref))
                pref_count+=1
                                
        partner = raiders[name]["partner"]
        if not partner:
            continue
        if in_different_raids(name, partner, raid1, raid2, raiders):
            annotations['warning'].append("Partners %s and %s not in same raid." % (name, partner))
            partner_count+=1

    return 12*partner_count + pref_count

def score_loot(raid1, raid2, raiders, annotations):
    loot_count = 0
    for name1 in raid1:
        for name2 in raid1:
            if name1 == name2:
                continue
            loot_intersection = raiders[name1]['loot'].intersection(raiders[name2]['loot'])
            if len(loot_intersection) > 0:
                annotations['warning'].append("Loot Conflict %s and %s in same raid for item %s." % (name1, name2, loot_intersection))
            loot_count+=len(loot_intersection)

    for name1 in raid2:
        for name2 in raid2:
            if name1 == name2:
                continue
            loot_intersection = raiders[name1]['loot'].intersection(raiders[name2]['loot'])
            if len(loot_intersection) > 0:
                annotations['warning'].append("Loot Conflict %s and %s in same raid for item %s." % (name1, name2, loot_intersection))
            loot_count+=len(loot_intersection)
    return loot_count
    
def combine_scores(balance_score, social_score, loot_score):
    return (balance_score + social_score + loot_score) / 3
        
def run_splits(raiders_to_split, required_constraints, raiders):
    successful_splits = []
    balance_total = 0.0
    social_total = 0.0
    loot_total = 0.0
    for i in range(NUM_RANDOM_SPLITS):
        raid1, raid2 = make_split(raiders_to_split, raiders)

        if not check_constraints(raid1, required_constraints, raiders):
            continue
        if not check_constraints(raid2, required_constraints, raiders):
            continue
        annotations = {'info':[], 'warning':[]}
        balance_score = score_balance(raid1, raid2, raiders, annotations)
        social_score = score_social(raid1, raid2, raiders, annotations)
        loot_score = score_loot(raid1, raid2, raiders, annotations)
        
        balance_total += balance_score
        social_total += social_score
        loot_total += loot_score
    
        successful_splits.append((raid1, raid2, balance_score, social_score, loot_score, annotations, i))
    
    splits_formated = map(lambda x: {'raid1': x[0], 'raid2': x[1], 'balance_norm': 1 - x[2]/balance_total, 'social_norm': 1 - x[3]/social_total, 'loot_norm': 1 - x[4]/loot_total, 'balance_raw': x[2], 'social_raw': x[3], 'loot_raw': x[4], 'score': combine_scores(1 - x[2]/balance_total, 1 - x[3]/social_total, 1 - x[4]/loot_total), 'annotations': x[5], 'index': x[6]}, successful_splits)
    
    
    return sorted(splits_formated, key=lambda x:x['score'], reverse=True)   
        

def format_split(split):
    col_1 = ['Team North']
    col_2 = ['Team Light']
    col_1 += split['raid1']
    col_2 += split['raid2']
    
    col_3 = ['']
    col_3.append('Overall Score: %.4f' % split['score'])
    col_3.append('Balance Score: %.4f (Raw: %i)' % (split['balance_norm'], split['balance_raw']))
    col_3.append('Social Score: %.4f (Raw: %i)' % (split['social_norm'], split['social_raw']))
    col_3.append('Loot Score: %.4f (Raw: %i)' % (split['loot_norm'], split['loot_raw']))
    col_3.append('')
    col_3.append('Info')
    col_3 += split['annotations']['info']
    col_3.append('')
    col_3.append('Warnings')
    col_3 += split['annotations']['warning']
    
    return [col_1, col_2, col_3]
    

def add_tab(sheet, tab_name):
    body = {
        'requests':[{
            'addSheet': {
                'properties': {
                    'title': tab_name
                 },
             }
         }]
     }
    response = sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID,body=body).execute()

def write_tab(sheet, tab_name, values, majorDimension='COLUMNS'):
    add_tab(sheet, tab_name)
    time.sleep(1)
    resp = sheet.values().update(spreadsheetId=SPREADSHEET_ID, range=tab_name, valueInputOption='RAW', body={'values':values, 'majorDimension':majorDimension}).execute()
    print("Write tab %s successful" % tab_name)    

def resize_columns(start_column, end_column, tab_name, spreadsheet, requests, column_size=150):
    for spreadsheet in spreadsheet.get('sheets'):
        if spreadsheet.get('properties').get('title') != tab_name:
            continue
        sheet_id = spreadsheet.get('properties').get('sheetId')
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": start_column,
                    "endIndex": end_column+1,
                },
                "properties": {
                    "pixelSize": column_size,
                },
                "fields": "pixelSize"
            }
        })

    

def add_format_request(row, col, rgba, spreadsheet, tab_name, requests, font_size=12, bold=True):
    for spreadsheet in spreadsheet.get('sheets'):
        if spreadsheet.get('properties').get('title') != tab_name:
            continue
        sheet_id = spreadsheet.get('properties').get('sheetId')
        requests.append({
            "updateCells": {
                "rows": [{
                    "values": [{
                        "userEnteredFormat": {
                            "backgroundColor": {
                                "red": rgba[0],
                                "green": rgba[1],
                                "blue": rgba[2],
                                "alpha":rgba[3]
                            },
                            "textFormat": {
                              "fontSize": font_size,
                              "bold": 'true' if bold else 'false',
                            }
                        }
                    }]
                }],
                "fields": 'userEnteredFormat',
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row,
                    "endRowIndex": row+1,
                    "startColumnIndex": col,
                    "endColumnIndex": col+1
                }
            }
        })

def get_rgba(raider, raiders):
    if raider in raiders:
        intersection = list(raiders[raider]['attributes'].intersection(HIGHLIGHTS.keys()))
        if not intersection:
            return None
        rgba = HIGHLIGHTS[intersection[0]]
        return rgba

def add_highlighting(formatted_split, tab_name, raiders, spreadsheet, requests):
    for col in range(len(formatted_split)):
        for row in  range(len(formatted_split[col])):
            rgba = get_rgba(formatted_split[col][row], raiders)
            if rgba != None:
                add_format_request(row, col, rgba, spreadsheet, tab_name, requests)
    add_format_request(0, 0, (1,1,1,1), spreadsheet, tab_name, requests, font_size=18)
    add_format_request(0, 1, (1,1,1,1), spreadsheet, tab_name, requests, font_size=18)
            

def format_tab(split, tab_name, raiders, spreadsheet, requests):
    add_highlighting(split, tab_name, raiders, spreadsheet, requests)
    resize_columns(0,1, tab_name, spreadsheet, requests, column_size=200)
    resize_columns(2,2, tab_name, spreadsheet, requests, column_size=400)
    
    
    
    
def main():
    creds = authenticate()
    sheet = build('sheets', 'v4', credentials=creds).spreadsheets()
    
    
    raiders_to_split = read_to_split(sheet)
    raiders = read_raiders(sheet)
    add_alts_to_raiders(raiders)
    required_constraints = read_required_constraints(sheet)
    
    splits = run_splits(raiders_to_split, required_constraints, raiders)
    print("There were %s successful splits!" % len(splits))
    
    formatted_splits = map(lambda x: format_split(x), splits)
    tab_names = []
    for i in range(NUM_SPLITS_TO_WRITE):
        tab_names.append(UNIQUE_TAG+' Split %i' %(i+1))
        write_tab(sheet, tab_names[i], formatted_splits[i])
        
    spreadsheet = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
    requests = []
    for i in range(len(tab_names)):
        format_tab(formatted_splits[i], tab_names[i], raiders, spreadsheet, requests)
    
    sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={'requests':requests}).execute()    
    
    
if __name__ == '__main__':
    main()