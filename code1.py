import os
import requests
import zipfile
import logging
from xml.etree import ElementTree as ET
import pandas as pd
import boto3 #for aws bucket S3
from boto3.session import Session
import botocore.session
import botocore.client
from botocore.docs.docstring import WaiterDocstring
from botocore.docs.service import ServiceDocumenter
from botocore.docs.client import ClientDocumenter, ClientExceptionsDocumenter
from botocore.docs.example import ResponseExampleDocumenter 
from botocore.utils import is_json_value_header
import botocore.httpsession


log=logging.getLogger()

def parse_xml(xmlfile):
    try:
        log.info("XML file is leading")
        xmlparse=ET.parse(xmlfile)
        
        log.info("XML file is being parsed")
        root=xmlparse.getroot()[1]
        doc=root.findall("doc")
        
        for i in doc:
            log.info("File extraction begins")
            file_type=i.find(".//str[@name='file_type']").text
            
            if file_type.text=="DLTINS":
                log.info("DLTINS File is found and extraction begins")
                file_name=i.find(".//str[@name='file_name']").text
                
                log.info("Extraction of download link begins")
                download_link=doc.find(".//str[@name='download_link']").text
                
                break
        else:
            log.info("No DLTINS named file found")
            return
        return file_name, download_link
    except Exception:
        log.error(f"An error has occurred-{str(Exception)}")
  
def download(url,download_path,filename):
    log.info("Download begins for xml file...")
    file=""
    try:
        r=requests.get(url)
        mext=["xml","html"]
        if(filename.split(".")[-1] in mext & filename.split(".")[-1] not in r.text):
            return file
        if r.ok:
            if not os.path.exists(download_path):
                os.mkdir(download_path)
            file=os.path.join(download_path,filename)
            with open(file,"wb") as f:
                f.write(r.context)
                log.info("XML File is successfuly downloaded.")
        else:
            log.error("error loading the xml file")
    except Exception:
        log.error(f"Error occurred {str(Exception)}")
    
    return file
        
def csv_file_creation(xml_file,csv_path):
    try:
        if not os.path.exists(csv_path):
            log.info("CSV file fath created")
            os.mkdir(csv_path)
        
        csv_file_name=xml_file.split(os.sep)[-1].split(".")[0]+".csv"
        csv_file=os.path.join(csv_path,csv_file_name)
        
        log.info("Csv file path is being created")
        iter_xml=ET.iterparse(xml_file,events=("start",))
        
        csv_cols=[
            "FinInstrmGnlAttrbts.Id",
            "FinInstrmGnlAttrbts.FullNm",
            "FinInstrmGnlAttrbts.ClssfctnTp",
            "FinInstrmGnlAttrbts.CmmdtyDerivInd",
            "FinInstrmGnlAttrbts.NtnlCcy",
            "Issr"
        ]
        
        df=pd.DataFrame(columns=csv_cols)
        
        data=[]
        
        log.info("Parsing the XML file and extracting data....it would take a bit of time")
        
        for i, element in iter_xml:
            if i=="start":
                if "TermntRcrd" in element.tag:
                    data_dict={}
                    
                    read_elements=[
                        (elem.tag, elem)
                        for elem in element
                        if "FinInstrmGnlAttrbts" in elem.tag or "Issr" in elem.tag
                    ]
                    for tag,elem in read_elements:
                        if "FinInstrmGnlAttrbts" in tag:
                            for child in elem:
                                if "Id" in child.tag:
                                    data_dict[csv_cols[0]]=child.text
                                elif "FullNm" in child.tag:
                                    data_dict[csv_cols[1]]=child.text
                                elif "ClssfctnTp" in child.tag:
                                    data_dict[csv_cols[2]] = child.text
                                elif "CmmdtyDerivInd" in child.tag:
                                    data_dict[csv_cols[3]] = child.text
                                elif "NtnlCcy" in child.tag:
                                    data_dict[csv_cols[4]] = child.text
                        else:
                            data_dict[csv_cols[5]]=child.text
                    data.append(data_dict)
        log.info("Data is successfully extracted from  the file(xml)")
        
        df=df.append(data,ignore_index=True)
        df.dropna(inplace=True)
        df.to_csv(csv_file,index=False)        
        
        return csv_file
    except Exception:
        log.error(f"While extracting an error had occured - {str(Exception)}")

def aws_s3(file,region,aws_access_key,aws_secret_access_key,bucket_name):
    try:
        filename_s3=file.split(os.sep)[-1]
        log.info("Creating bucket S3")
        
        S3=boto3.resource(
            service_name="s3",
            region_name=region,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_access_key,
        )
        
        log.info("Uploading the file")
        S3.bucket(bucket_name).upload_file(Filename=file,Key=filename_s3)
        log.info("File is successfully uploaded")
        
        return True
    except Exception:
        log.error(f"Error has occurred-{str(Exception)}")