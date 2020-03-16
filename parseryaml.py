import yaml 
import logging
import csv
import pandas as pd
import argparse

logging.basicConfig(filemode='w', filename='app.log', level=logging.INFO)

#! get user imputs as args
argeParser = argparse.ArgumentParser(description='arguments for Pulse Util')
argeParser.add_argument('--s', type=str, help='sink file name (suffix)', required=True)
argeParser.add_argument('--i', type=str, help='column name to use', required=True)
argeParser.add_argument('--c', type=str, help='mongodb collection name', required=True)
args = argeParser.parse_args()

fileNameSuffix=args.s
columnName=args.i
collectionName=args.c

logging.info('filename {}'.format(fileNameSuffix))
logging.info('columnName  {}'.format(columnName))
logging.info('collection name {}'.format(collectionName))


#! open config yaml file
#! need to use file's absolute path to read this file

def readYAMLFile():
    #! returns the dict after loading the yaml 
    with open('config.yaml', 'r') as file:
        try:
            YAMLDICT = yaml.safe_load(file) 
        except yaml.YAMLError as e:
            logging.exception(e)
    return YAMLDICT

def readCSVFile():
    try:
        structure = pd.read_csv('structure.csv')
        return structure
    except Exception as e:
        logging.exception(e)
    
    # print(structure[structure['project.blacklist'] == 1]['JiraSourceField'].to_list())

def createProjectSinkProperties():
    filePrefix = "mongo-sink-{}.properties"
    fileName = filePrefix.format(fileNameSuffix)
    
    try:
        yamldict = readYAMLFile()
        structure = readCSVFile()

        #! collection name from user input
        blacklistColumn = structure[structure[columnName] == 0]['SourceField'].to_list()
        renameField = structure[structure[columnName] == 1]['field'].to_list()
        currentField = structure[structure[columnName] == 1]['SourceField'].to_list()
        logging.info('rename fields')
        logging.info(renameField)
        logging.info('current fields')
        logging.info(currentField)

        all_lines = []

        Line1 = 'name={}project\n'.format(yamldict['name'])
        all_lines.append(Line1)

        Line2 = 'connector.class={}\n'.format(yamldict['connector.class'])
        all_lines.append(Line2)

        Line = 'topics={}\n'.format(yamldict['topics'][0])
        all_lines.append(Line)

        Line3 = 'tasks.max={}\n'.format(yamldict['tasks.max'])
        all_lines.append(Line3)

        Line4 = 'connection.uri={}\n'.format(yamldict['connection.uri'])
        all_lines.append(Line4)

        Line5 = 'database={}\n'.format(yamldict['database'])
        all_lines.append(Line5)

        Line6 = 'collection={}\n'.format(collectionName)
        all_lines.append(Line6)

        all_lines.append('\n')

        Line7 = 'key.converter={}\n'.format(yamldict['key.converter']['key.converter'])
        all_lines.append(Line7)

        Line8 = 'key.converter.schemas.enable={}\n'.format("true")
        all_lines.append(Line8)

        Line9 = 'key.converter.schema.registry.url={}\n'.format(yamldict['key.converter']['key.converter.schema.registry.url'])
        all_lines.append(Line9)

        Line = 'key.converter.basic.auth.credentials.source={}\n'.format("USER_INFO")
        all_lines.append(Line)

        Line10 = 'key.converter.schema.registry.basic.auth.user.info={}\n'.format(yamldict['key.converter']['key.converter.schema.registry.basic.auth.user.info'])
        all_lines.append(Line10)

        all_lines.append('\n')

        Line11 = 'value.converter={}\n'.format(yamldict['value.converter']['value.converter'])
        all_lines.append(Line11)

        Line12 = 'value.converter.schemas.enable={}\n'.format("true")
        all_lines.append(Line12)

        Line13= 'value.converter.schema.registry.url={}\n'.format(yamldict['value.converter']['value.converter.schema.registry.url'])
        all_lines.append(Line13)

        Line = 'value.converter.basic.auth.credentials.source={}\n'.format("USER_INFO")
        all_lines.append(Line)

        Line14 = 'value.converter.schema.registry.basic.auth.user.info={}\n'.format(yamldict['value.converter']['value.converter.schema.registry.basic.auth.user.info'])
        all_lines.append(Line14)

        all_lines.append('\n')

        Line15 = 'transforms={}\n'.format(yamldict['transforms']['transforms'])
        all_lines.append(Line15)

        Line16 = 'transforms.dropColumns.type={}\n'.format(yamldict['transforms']['transforms.dropColumns.type'])
        all_lines.append(Line16)

        Line17 = 'transforms.dropColumns.blacklist='
        all_lines.append(Line17)

        start = "["
        for l in range(0, len(blacklistColumn)):
            start = start+blacklistColumn[l]+","
        end = "]"

        Line18 = start+end
        logging.info(Line18)
        all_lines.append(str(Line18))
        all_lines.append('\n')

        Line19 = 'transforms.renameField.type={}\n'.format(yamldict['transforms']['transforms.renameField.type'])
        all_lines.append(Line19)

        start = ""
        for i in range(0,len(renameField)):
            if i == (len(renameField) - 1):
                start = start + currentField[i]+":"+renameField[i]
            else:
                start = start + currentField[i]+":"+renameField[i]+","
            
        logging.info(start)

        Line20 = 'transforms.renameField.renames={}\n'.format(start)
        all_lines.append(Line20)

        Line21 = 'transforms.hoistField.type={}\n'.format(yamldict['transforms']['transforms.hoistField.type'])
        all_lines.append(Line21)

        Line22 = 'transforms.hoistField.field={}\n'.format(fileNameSuffix)
        all_lines.append(Line22)

        projectPropertiesFile = open(fileName, 'a')

        #! change the pointer to line 1
        projectPropertiesFile.seek(0)

        #! delete the entire content
        projectPropertiesFile.truncate()

        #! write the lines
        projectPropertiesFile.writelines(all_lines)
        #! close the file
        projectPropertiesFile.close()

        print('successfully created file : {}'.format(fileName))
    except Exception as e:
        logging.exception(e)
    
def createTaskSinkProperties():
    pass

def createIssuetrackSinkProperties():
    pass

#! function -- takes YAMLDICT and CSV file obejct and generate .properties file
def compileYAMLandCSV():
    pass
# compileYAMLandCSV()
createProjectSinkProperties()