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

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '1vgM-OwsR3GJ19zv1CVfr6rIJn25yuCWYXKj_0r_X95E'
LC_DUMP_LOCATION = 'loot_dump_2_2_2020.csv'
MC_PRIORITY_TAB = 'mc!A2:C'
BWL_PRIORITY_TAB = 'bwl!A2:C'
RAIDERS_TAB = 'raiders!A2:E'
TIER_TAGS = ['mage', 'warlock', 'hpriest', 'hdruid', 'rogue', 'hunter', 'wtank', 'hpaladin']
QUEUE_GROUP_MAP = {
    'CASTER':set(['mage', 'warlock', 'spriest']),
    'MELEE':set(['wdps', 'rogue', 'rpaladin', 'cat']),
    'HEALER':set(['hpaladin', 'hpriest', 'hdruid']),
    'HUNTER':set(['hunter']),
    'TANK':set(['wtank', 'bear'])
}

RESPONSE_TO_QUEUE_MAP = {
    'Mainspec/Need':0,
    'Mainspec (major upgrade)':0,
    'Minor Upgrade':1,
    'Mainspec (minor upgrade)':1,
    'Teir Bonus/Completion':2,
    'Offspec':2,
    'Offspec/Greed':2,
    'Offline or RCLootCouncil not installed':2,
    'Pass':2,
}

def authenticate():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
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
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds

def parse_timestamp(date, time="0:0:0"):
    datesplit = date.split('/')
    timesplit = time.split(':')
    if len(datesplit) !=3 or len(timesplit) != 3:
        return None
    dt = datetime.datetime(2000+int(datesplit[2]), int(datesplit[0]), int(datesplit[1]), int(timesplit[0]), int(timesplit[1]))
    timestamp = int((dt - datetime.datetime(1970, 1, 1, 0, 0, 0)).total_seconds())
    return timestamp

def read_raiders(sheet): 
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,range=RAIDERS_TAB).execute()                        
    raiders = {}                        
    for row in result.get('values', []):
        name = row[0]
        tags=[]
        loot = []
        join_timestamp = 0
        rank = 0
        if len(row) > 1:
            tags = row[1].split(';')
        if len(row) > 2:
            loot = row[2].split(';')
        if len(row) > 3:
            join_timestamp = parse_timestamp(row[3])
        if len(row) > 4:
            rank = int(row[4])
        raiders[name] = {"tags":tags, "loot":loot, "join_timestamp":join_timestamp, "rank":rank}
    return raiders
              
def read_item_priority(sheet, tab):
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=tab).execute()
    items = {}                        
    for row in result.get('values', []):
        name = row[0]
        constraints=[]
        priority = []
        if len(row) > 1:
            constraints = row[1]
        if len(row) > 2:
            priority = int(row[2])
        items[name] = {"constraints":constraints, "priority":priority}
    return items
    
def parse_constraint(constraint):
    return_list = []
    for tier in constraint.split('>'):
        return_list.append(set(tier.split('=')))
    return return_list

def match_item(item_name, con_list, raiders, queue):
    return_list = []
    for tag_set in con_list:
        for name in queue:
            if item_name not in raiders[name]["loot"] and tag_set.intersection(raiders[name]["tags"]) and name not in return_list:
                return_list.append(name)
    return return_list

def match_all_items(items, raiders, queue):
    return_list = []
    for item_name in items.keys():
        con_list = parse_constraint(items[item_name]["constraints"])
        return_list.append([item_name] + match_item(item_name, con_list, raiders, queue))
    return_list.sort(key=lambda x:x[0])
    return return_list

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
        
def read_lc_dump(items):
    file = open(LC_DUMP_LOCATION)
    loot_history = []
    for line in file:
        if not line or line == '\n':
            break
        spt = line.split(",")
        name = spt[0][:-6]
        timestamp = parse_timestamp(spt[1],spt[2])
        item_name = spt[4][1:-1]
        response = spt[7]
        if not timestamp or response not in RESPONSE_TO_QUEUE_MAP:
            continue
        if item_name not in items:
            continue
        loot_history.append((name, timestamp, item_name, RESPONSE_TO_QUEUE_MAP[response]))
    loot_history.sort(key=lambda x:x[1])
    return loot_history

def update_raider_loot(raiders, loot_history):
    for event in loot_history:
        name = event[0]
        item_name = event[2]
        if name not in raiders:
            continue
        if item_name not in raiders[name]["loot"]:
            raiders[name]["loot"].append(item_name)
           
def update_queues(raiders, loot_history, items):
    raiders_join = sorted(list(map(lambda x: (raiders[x]["join_timestamp"], x, raiders[x]["rank"]), raiders.keys())), key=lambda x:x[0])
    join_index = 0
    queues = [[],[],[]]
    for event in loot_history:
        while (join_index < len(raiders_join) and raiders_join[join_index][0] < event[1]):
            for i in range(raiders_join[join_index][2], len(queues)):
                queues[i].append(raiders_join[join_index][1])
            join_index +=1
        player_name = event[0]
        priority = max(event[3], items[event[2]]["priority"])
        for queue in queues[priority:]:
            if player_name not in queue:
                continue
            queue.remove(player_name)
            queue.append(player_name)
    while (join_index < len(raiders_join)):
        for i in range(raiders_join[join_index][2], len(queues)):
            queues[i].append(raiders_join[join_index][1])
        join_index +=1
    return queues

def create_queue(tags, priority, queues, raiders):
    return_queue = []
    for player_name in queues[priority]:
        if tags.intersection(raiders[player_name]["tags"]):
            return_queue.append(player_name)
    return return_queue

def create_tier_queues(queues, raiders):
    tier_queues = []
    for tier_tag in TIER_TAGS:
        tier_queue = create_queue(set([tier_tag]), 1, queues, raiders)
        tier_queue.insert(0, tier_tag)
        tier_queues.append(tier_queue)
    return tier_queues

def format_priority_queues(priority, queues, raiders):
    output_queues = []
    for queue_name in sorted(QUEUE_GROUP_MAP.keys()):
        queue = create_queue(QUEUE_GROUP_MAP[queue_name], priority, queues, raiders)
        queue.insert(0, queue_name)
        output_queues.append(queue)
    return output_queues                

def write_tab(sheet, tab_name, values, majorDimension='ROWS'):
    add_tab(sheet, tab_name)
    time.sleep(1)
    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range=tab_name, valueInputOption='RAW', body={'values':values, 'majorDimension':majorDimension}).execute()
    print("Write tab %s successful" % tab_name)
            
def main():
    creds = authenticate()
    sheet = build('sheets', 'v4', credentials=creds).spreadsheets()
    
    raiders = read_raiders(sheet)
    bwl_items = read_item_priority(sheet, BWL_PRIORITY_TAB)
    mc_items = read_item_priority(sheet, MC_PRIORITY_TAB)
    all_items = dict(bwl_items.items() + mc_items.items())

    loot_history = read_lc_dump(all_items)
    update_raider_loot(raiders, loot_history)
    
    queues = update_queues(raiders, loot_history, all_items)
    
    mc_matches = match_all_items(mc_items, raiders, queues[0])
    bwl_matches = match_all_items(bwl_items, raiders, queues[0])
    
    tier_queues = create_tier_queues(queues, raiders)
    major_queues = format_priority_queues(0, queues, raiders)
    minor_queues = format_priority_queues(1, queues, raiders)
    offspec_queues = format_priority_queues(2, queues, raiders)

    write_tab(sheet, 'mc matches' + UNIQUE_TAG, mc_matches)
    write_tab(sheet, 'bwl matches' + UNIQUE_TAG, bwl_matches)
    write_tab(sheet, 'master queues' + UNIQUE_TAG, queues, 'COLUMNS')
    write_tab(sheet, 'tier queues' + UNIQUE_TAG, tier_queues, 'COLUMNS')
    write_tab(sheet, 'major upgrade queues' + UNIQUE_TAG, major_queues, 'COLUMNS')
    write_tab(sheet, 'minor queues' + UNIQUE_TAG, minor_queues, 'COLUMNS')
    write_tab(sheet, 'offspec queues' + UNIQUE_TAG, offspec_queues, 'COLUMNS')
    
if __name__ == '__main__':
    main()