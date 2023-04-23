# Now I'm going to implement all the code, connect them amd use them for parsing storing and overall execution.
from logger import log
import logging
from configparser import RawConfigParser
from code1 import *

config=None

def load_config():
    global config
    try:
        log.info("Starting to load the configuration file")
        config=RawConfigParser()
        config.read("new_names.cfg")
        log.info("Config file loaded successfully")
    except Exception:
        log.error(f"Error while loading the file(new_names.cfg): {str(Exception)}.")
    return config

def main():
    try:
        log.info("Extracting the source xml file url")
        url=config.get("sourcefile","xml_url")
        csv_path = os.path.join(os.getcwd(), config.get("csv", "csv_path"))
        
        log.info("Extracting xml file download path")
        download_path=os.path.join(
            os.getcwd(),config.get("download","download_path")
        )
        log.info("Extracting the bucket details")
        bucket_name = config.get("aws", "bucket_name")
        aws_access_key_id = config.get("aws", "aws_access_key")
        aws_secret_access_key = config.get("aws", "aws_secret_access_key")
        region_name = config.get("aws", "region")
        
        xml_file=download(url, download_path,"sourcefile.xml")
        
        if not xml_file:
            log.error("Failed!!")
            return
        
        fmd=parse_xml(xml_file)
        if not fmd:
            log.info("Parsing failed")
            return
        
        filename,file_download_link=fmd
        
        log.info("Download function is called")
        
        xml_zip_file=download(file_download_link,download_path,filename)
        
        xml_file=os.path.join(download_path,filename.split(".")[0]+".xml")
        csv_file=csv_file_creation(xml_file,csv_path)
        
        if not csv_file:
            print("Unable to convert xml file to csv file")
            return
        
        status=aws_s3(csv_file,region_name,aws_access_key_id,aws_secret_access_key,bucket_name)
        if not status:
            print("File upload to AWS S3 failed")
            return
        
        return True
    except Exception:
        log.error(f"Error in loading the cgf file:{str(Exception)}.")
        
        
#Executing the main function
if __name__=="__main__":
    if not load_config():
        print("Error occurred while loading config file")
        exit(1)
        
    print("Execution begins")
    if main():
        print("Execution Successful")
    else:
        print("Execution failed!!!")
     