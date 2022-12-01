from dotenv import load_dotenv
load_dotenv()
import os
import json
from datetime import date
import numpy as np
import pandas as pd
import pyodbc
import requests
import subprocess

def makeWrikeTask (title = "New Pricing Task", description = "No Description Provided", status = "Active", assignees = "KUAAY4PZ", folderid = "IEAAJKV3I4JBAOZD"):
    url = "https://www.wrike.com/api/v4/folders/" + folderid + "/tasks"
    querystring = {
        'title':title,
        'description':description,
        'status':status,
        'responsibles':assignees
        } 
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
        }        
    response = requests.request("POST", url, headers=headers, params=querystring)
    return response

def attachWrikeTask (attachmentpath, taskid):
    url = "https://www.wrike.com/api/v4/tasks/" + taskid + "/attachments"
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
    }

    files = {
        'X-File-Name': (attachmentpath, open(attachmentpath, 'rb')),
    }

    response = requests.post(url, headers=headers, files=files)
    print(response)
    return response       

if __name__ == '__main__':

    #Connection String
    conn_str = os.environ.get(r"sage_conn_str").replace("UID=;","UID=" + os.environ.get(r"sage_login") + ";").replace("PWD=;","PWD=" + os.environ.get(r"sage_pw") + ";") 
    #Establish sage connection
    print('Connecting to Sage')
    cnxn = pyodbc.connect(conn_str, autocommit=True)  
    
    #SQL Sage data into dataframe
    sql = """
        SELECT 
            CI_Item.ItemCode, CI_Item.UDF_REPLACEMENT_ITEM, CI_Item.InactiveItem
        FROM
            CI_Item CI_Item
    """

    df = pd.read_sql(sql,cnxn)
    df = df.replace(to_replace='None', value=np.nan)

    #LegitItemCodes
    legitItemsList = list(set(df['ItemCode']))
    #Items that 'should' be replacements
    replaceList = list(set(df['UDF_REPLACEMENT_ITEM'].dropna()))
    replaceList = [x for x in replaceList if x is not None]
    #Make a list of the bad replacements
    badReplacementList = list(set(replaceList) - set(legitItemsList))
    #Remove bad replacements from both lists

    if not badReplacementList:
        print('no bad replacements :)')
    else:
        legitItemsList = list(set(legitItemsList) - set(badReplacementList))
        replaceList = list(set(replaceList) - set(badReplacementList))
        #Make a data frame and wrike task
        ReplacementsToFixdf = df[df['UDF_REPLACEMENT_ITEM'].isin(badReplacementList)]
        ReplacementsToFixdf.to_excel(r'\\FOT00WEB\Alt Team\Andrew\Andrews_Code\ReplacementsToFixdf.xlsx')

        assignees = '[KUAAZAC4,KUACOUUA,KUAEL7RV,KUAAY4PZ,KUALCDZR]'
        folderid = 'IEAAJKV3I4JEW3BI' #Web Requests
        description = "Someone loaded up some bogus Replacements! Let's find em and get em!!!\n\n\n\n\n\nFIXED!\n) "
        wriketitle = date.today().strftime('%m-%d-%Y')+ " - Bad Replacements Found! (" + str(ReplacementsToFixdf.shape[0]) +")"
        response = makeWrikeTask(title = wriketitle, description = description, assignees = assignees, folderid = folderid)
        response_dict = json.loads(response.text)
        print(response_dict)
        taskid = response_dict['data'][0]['id']
        print('Attaching file to ', taskid)
        attachWrikeTask(attachmentpath = r'\\FOT00WEB\Alt Team\Andrew\Andrews_Code\ReplacementsToFixdf.xlsx', taskid = taskid)
        print('File attached!')

    #Make DF of legit codes to parse through
    fullLegitdf = df[df['ItemCode'].isin(legitItemsList)]   
    fullLegitdf['AlphaReplacement'] = fullLegitdf['UDF_REPLACEMENT_ITEM'] 

    #Stuff for while loop
    go_on = True
    count = 0
    parseReplacementDF_checker = pd.DataFrame(data=None, index = ['UDF_REPLACEMENT_ITEM'], columns = ['AlphaReplacement'])  

    while go_on:
        count += 1
        print(count)

        parseReplacementDF = fullLegitdf[fullLegitdf['ItemCode'].isin(replaceList)]
        print(parseReplacementDF)

        parseReplacementDF = parseReplacementDF.drop(columns = ['AlphaReplacement'])
        parseReplacementDF = parseReplacementDF.set_index('ItemCode')
        parseReplacementDF = parseReplacementDF.rename(columns={'UDF_REPLACEMENT_ITEM':'AlphaReplacement'})   

        if parseReplacementDF.shape[0] == 0:
            go_on = False
            print("stopping")
        else:  
            print("not empty")
            if parseReplacementDF_checker.equals(parseReplacementDF):
                print("no more drill down")
                go_on= False 
            else:
                parseReplacementDF_checker = parseReplacementDF
                replaceList = list(set(parseReplacementDF['AlphaReplacement'].dropna())) 
                fullLegitdf = fullLegitdf.set_index('UDF_REPLACEMENT_ITEM')
                fullLegitdf.index.name = None
                fullLegitdf.update(parseReplacementDF.dropna())
                fullLegitdf.index.name = 'UDF_REPLACEMENT_ITEM'
                fullLegitdf = fullLegitdf.reset_index()

    #Only Things needing a change
    reReplacementsDf = fullLegitdf.dropna(subset=['UDF_REPLACEMENT_ITEM'])   
    reReplacementsDf = reReplacementsDf.loc[(reReplacementsDf['AlphaReplacement'] != reReplacementsDf['UDF_REPLACEMENT_ITEM'])]

    #Only VI stuff if we have stuff to inactivate
    if reReplacementsDf.shape[0] > 0:
        reReplacementsDf.to_excel(r'\\FOT00WEB\Alt Team\Andrew\Andrews_Code\reReplacementsDf.xlsx')   
        reReplacementsDf.to_csv(r'\\FOT00WEB\Alt Team\Qarl\Automatic VI Jobs\Maintenance\CSVs\AA_REPLACEFIXES_VIWI7T.csv', header=False, sep=',', index = False, columns=['ItemCode','AlphaReplacement'])   
        print('fixing ' + str(reReplacementsDf.shape[0]) + 'bad replacements')
        #Auto VI .... 
        print('VIing Replacement Inception Fixes')
        p = subprocess.Popen('Auto_REPLACEFIXES_VIWI7T.bat', cwd=r"Y:\Qarl\Automatic VI Jobs\Maintenance", shell=True)
        stdout, stderr = p.communicate()
        p.wait()
        print('Sage VI Complete!')