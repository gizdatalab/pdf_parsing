import configparser
import os
import logging
from requests import post, get
import json
import pandas as pd
import fitz
from ast import literal_eval
from io import StringIO
import logging
from typing import Callable, Dict, List, Optional, Text, Tuple, Union
import glob
import numpy as np
server_config='../axaserver/defaultConfig.json'

def get_config(configfile_path:str)->object:
    """
    configfile_path: file path of .cfg file

    """

    config = configparser.ConfigParser()

    try:
        config.read_file(open(configfile_path))
        return config
    except:
        logging.warning("config file not found")


def check_if_imagepdf(file_path:str)->bool:  
    """
    check for if the pdf file is normal pdf or scanned/image pdf
    """
    text = ""
    try:
        doc = fitz.open(file_path)
        for page in doc:
            text += page.get_text()
        if text == "":
            return True
        else:return False
    except Exception as e:
        logging.error(e)
        return False


def check_input_file(file_path:str)->bool:
    """
    check for if file type supported
    """
    supported_filetype = ['.pdf', '.jpg', '.jpeg', '.png', '.tiff',
                '.tif', '.docx', '.xml', '.eml', '.json']
    
    document_name, extension_type = os.path.splitext(os.path.basename(file_path))
    if extension_type in supported_filetype:
        if (extension_type == '.pdf') & check_if_imagepdf(file_path):
            logging.warning(f"""{file_path} is of type imagepdf and using inbuilt 
                    tesseract-ocr (eng). Suggest to use the useOCR module instead""")
        return True
    else:
        logging.error(f"{file_path} filetype not supported, should be of {supported_filetype}") 
        return False


def send_doc(url="http://localhost:3001", 
            file_path :str ="", 
            server_config:str=server_config, 
            authfile:str = "")->dict:

    """
    Make the post request to axaserver listening port:3001

    Params
    -------------
    url: either the localhost or huggingface hosted server acceptible right now
    file_path: file to be sent to server
    server_config:filepath to configs for the axaserver
    authfile: private server on huggingface need auth-token

    Return
    --------------
    filename: name of file sent to server
    config: config file used by server to process the input file
    status_code: status of the post request made to server
    server_response: request-id(unique) of the document processing request made to server

    """
    # we need it if using the private server on HF
    if url != "http://localhost:3001":
        configs = get_config(configfile_path= authfile)
        try:
            url = configs.get("axaserver","api")
            token = configs.get("axaserver","token")
            headers = {
                        "Authorization": f"Bearer {token}"}
        except Exception as e:
            logging.warning(e)
    else:
        headers = None
    if check_input_file(file_path):
        try:
            packet = {
            'file': (file_path, open(file_path, 'rb'), 'application/pdf'),
            'config': (server_config, open(server_config, 'rb'), 'application/json')
            }
            r = post(url + '/api/v1/document', headers=headers, files=packet)
            return {
            'filename': os.path.basename(file_path),
            'config': server_config,
            'status_code': r.status_code,
            'server_response': r.text}
        except Exception as e:
            logging.error(e)


def send_documents_batch(batch: str,
            url="http://localhost:3001", 
            server_config:str=server_config, 
            authfile:str = "",
           ) -> list:
        """
        Send all the files inside a folder

        Params
        -------------
        url: either the localhost or huggingface hosted server acceptible right now
        folder: filder to be sent to server
        server_config:filepath to configs for the axaserver to be sued for this 
                      batch of files
        authfile: private server on huggingface need auth-token

        """
        if url != "http://localhost:3001":
            configs = get_config(configfile_path= authfile)
            try:
                url = configs.get("axaserver","api")
                token = configs.get("axaserver","token")
                headers = {
                            "Authorization": f"Bearer {token}"}
            except Exception as e:
                logging.warning(e)
                return
        else:
            headers = None

        responses = []
        # files = glob.glob(folder + "*")
        # if len(files)  == 0:
        #     logging.error("folder {} is empty".format(folder))
        #     return
            
        # filemask = [check_input_file(file) for file in files]
        # files = list(np.array(files)[filemask])

        for file in batch:
            packet = {
                'file': (file, open(file, 'rb'), 'application/pdf'),
                'config': (server_config, open(server_config, 'rb'), 'application/json'),
            }
            r = post(url + '/api/v1/document', headers=headers, files=packet)
            responses.append({
            'filename': os.path.basename(file),
            'config': server_config,
            'status_code': r.status_code,
            'server_response': r.text})
        return responses


def send_documents_folder(folder: str,
            url="http://localhost:3001", 
            server_config:str=server_config, 
            authfile:str = "",
           ) -> list:
        """
        Send all the files inside a folder

        Params
        -------------
        url: either the localhost or huggingface hosted server acceptible right now
        folder: filder to be sent to server
        server_config:filepath to configs for the axaserver to be sued for this 
                      batch of files
        authfile: private server on huggingface need auth-token

        """
        if url != "http://localhost:3001":
            configs = get_config(configfile_path= authfile)
            try:
                url = configs.get("axaserver","api")
                token = configs.get("axaserver","token")
                headers = {
                            "Authorization": f"Bearer {token}"}
            except Exception as e:
                logging.warning(e)
                return
        else:
            headers = None

        responses = []
        files = glob.glob(folder+"*")
        if len(files)  == 0:
            logging.error("folder {} is empty".format(folder))
            return
            
        filemask = [check_input_file(file) for file in files]
        files = list(np.array(files)[filemask])

        for file in files:
            packet = {
                'file': (file, open(file, 'rb'), 'application/pdf'),
                'config': (server_config, open(server_config, 'rb'), 'application/json'),
            }
            r = post(url + '/api/v1/document', headers=headers, files=packet)
            responses.append({
            'filename': os.path.basename(file),
            'config': server_config,
            'status_code': r.status_code,
            'server_response': r.text})
        return responses


def get_status(url: str = "http://localhost:3001",
               request_id: str = "", authfile:str = "")->dict:
    """Get the status of a particular request using its ID

    - request_id: The ID of the request to be queried with the server
    - server: The server address where the query is to be made

    """
    if url != "http://localhost:3001":
        configs = get_config(configfile_path= authfile)
        try:
            url = configs.get("axaserver","api")
            token = configs.get("axaserver","token")
            headers = {
                        "Authorization": f"Bearer {token}"}
        except Exception as e:
            logging.warning(e) 
    else:
        headers = None
    
    if request_id == "":
        raise Exception('No request ID provided')

    r = get('{}/api/v1/queue/{}'.format(url, request_id), headers=headers)

    return r
    # if r.text == "":
    #     logging.error(r)
    # else:
    #     return {
    #         'request_id': request_id,
    #         'server_response': json.loads(
    #             r.text)}


def get_json(url: str = "http://localhost:3001",
               request_id: str = "", authfile:str = "")->dict:
    """Get the status of a particular request using its ID

    - request_id: The ID of the request to be queried with the server
    - server: The server address where the query is to be made

    """
    if url != "http://localhost:3001":
        configs = get_config(configfile_path= authfile)
        try:
            url = configs.get("axaserver","api")
            token = configs.get("axaserver","token")
            headers = {
                        "Authorization": f"Bearer {token}"}
        except Exception as e:
            logging.warning(e) 
    else:
        headers = None
    
    if request_id == "":
        raise Exception('No request ID provided')
    
    r = get('{}/api/v1/json/{}'.format(url, request_id), headers=headers)
    if r.text != "":
        return r.json()
    else:
        return {'request_id': request_id, 'server_response': r.json()}


def get_simplejson(url: str = "http://localhost:3001",
               request_id: str = "", authfile:str = "")->dict:
    """Get the status of a particular request using its ID

    - request_id: The ID of the request to be queried with the server
    - server: The server address where the query is to be made

    """
    if url != "http://localhost:3001":
        configs = get_config(configfile_path= authfile)
        try:
            url = configs.get("axaserver","api")
            token = configs.get("axaserver","token")
            headers = {
                        "Authorization": f"Bearer {token}"}
        except Exception as e:
            logging.warning(e) 
    else:
        headers = None
    
    if request_id == "":
        raise Exception('No request ID provided')
    
    r = get('{}/api/v1/simple-json/{}'.format(url, request_id), headers=headers)
    if r.text != "":
        return r.json()
    else:
        return {'request_id': request_id, 'server_response': r.json()}


def get_markdown(url: str = "http://localhost:3001",
               request_id: str = "", authfile:str = "")->dict:
    """Get the status of a particular request using its ID

    - request_id: The ID of the request to be queried with the server
    - server: The server address where the query is to be made

    """
    if url != "http://localhost:3001":
        configs = get_config(configfile_path= authfile)
        try:
            url = configs.get("axaserver","api")
            token = configs.get("axaserver","token")
            headers = {
                        "Authorization": f"Bearer {token}"}
        except Exception as e:
            logging.warning(e) 
    else:
        headers = None
    
    if request_id == "":
        raise Exception('No request ID provided')
    
    r = get('{}/api/v1/markdown/{}'.format(url, request_id), headers=headers)
    if r.text != "":
        return r.text
    else:
        return {'request_id': request_id, 'server_response': r.text}
    

def get_text(url: str = "http://localhost:3001",
               request_id: str = "", authfile:str = "")->dict:
    """Get the status of a particular request using its ID

    - request_id: The ID of the request to be queried with the server
    - server: The server address where the query is to be made

    """
    if url != "http://localhost:3001":
        configs = get_config(configfile_path= authfile)
        try:
            url = configs.get("axaserver","api")
            token = configs.get("axaserver","token")
            headers = {
                        "Authorization": f"Bearer {token}"}
        except Exception as e:
            logging.warning(e) 
    else:
        headers = None
    
    if request_id == "":
        raise Exception('No request ID provided')
    
    r = get('{}/api/v1/text/{}'.format(url, request_id), headers=headers)
    if r.text != "":
        return r.text
    else:
        return {'request_id': request_id, 'server_response': r.text}
    

def get_tables_list(url: str = "http://localhost:3001",
               request_id: str = "", authfile:str = "")->dict:
    """Get the status of a particular request using its ID

    - request_id: The ID of the request to be queried with the server
    - server: The server address where the query is to be made

    """
    if url != "http://localhost:3001":
        configs = get_config(configfile_path= authfile)
        try:
            url = configs.get("axaserver","api")
            token = configs.get("axaserver","token")
            headers = {
                        "Authorization": f"Bearer {token}"}
        except Exception as e:
            logging.warning(e) 
    else:
        headers = None
    
    if request_id == "":
        raise Exception('No request ID provided')
    
    r = get('{}/api/v1/csv/{}'.format(url, request_id), headers=headers)
    
    if r.text != "":
        return [(table.rsplit('/')[-2], table.rsplit('/')[-1])
                for table in literal_eval(r.text)]
    else:
        return {'request_id': request_id, 'server_response': r.text}


def get_table(url: str = "http://localhost:3001",
               request_id: str = "", authfile:str = "",
               page=None,table=None,seperator=";",
               column_names: list = None):
    """Get a particular table from a processed document.

    - request_id: The request to be queried to get a document.
    - page: The page number on which the queried table exists.
    - table: The table number to be fetched.
    - seperator: The seperator to be used between table cells (default ';')
    - server: The server address which is to be queried.
    - column_names: The headings of the table searched (column titles)
    """
    if url != "http://localhost:3001":
        configs = get_config(configfile_path= authfile)
        try:
            url = configs.get("axaserver","api")
            token = configs.get("axaserver","token")
            headers = {
                        "Authorization": f"Bearer {token}"}
        except Exception as e:
            logging.warning(e) 
    else:
        headers = None
    
    if request_id == "":
        raise Exception('No request ID provided')
    
    if page is None or table is None:
        raise Exception('No Page or Table number provided')
    else:
        r = get('{}/api/v1/csv/{}/{}/{}'.format(url,request_id,page,table))

    if r.text != "":
        try:
            df = pd.read_csv(
                StringIO(
                    r.text),
                sep=seperator,
                names=column_names)
            df.loc[:, ~df.columns.str.match('Unnamed')]
            df = df.where((pd.notnull(df)), " ")
            return df
        except Exception:
            return r.text
    else:
        return r.text


def download_files(request_id, folder_location, filename):
    if get_status(request_id=request_id).status_code == 201:
        if not os.path.exists(folder_location + f"{request_id}/"):
            os.makedirs(folder_location + f"{request_id}/")
            new_path = folder_location + f"{request_id}/"

            r_markdown = get_markdown(request_id=request_id)
            with open(new_path + f'{filename}.md', 'w') as file:
                file.write(r_markdown)
            
            r_text = get_text(request_id=request_id)
            with open(new_path+f'{filename}.txt', 'w') as file:
                file.write(r_text)
            
            r_json = get_json(request_id=request_id)
            with open(new_path + f'{filename}.json', 'w') as file:
                json.dump(r_json, file)  

            r_simplejson = get_simplejson(request_id=request_id)
            with open(new_path+f'{filename}.simple.json', 'w') as file:
                json.dump(r_simplejson, file)

            tables_list = get_tables_list(request_id=request_id)
            os.makedirs(folder_location + f"{request_id}/tables/")
            new_path_table = folder_location + f"{request_id}/tables/"
            for val in tables_list:
                df = get_table(request_id=request_id, page=val[0], table=val[1])
                df.to_csv(new_path_table+f"{val[0]}_{val[1]}.csv")

            return new_path
        
        else: logging.warning("folder already exists")

# def get_processedfiles(url: str = "http://localhost:3001",
#                request_id: str = "", authfile:str = ""):
#     r_json = get_json(url = url, request_id=request_id,authfile=authfile)
#     pages = {}
#     file_output = ParsrOutputInterpreter(r_json)
#     for i in range(0,file_output.page_count):
#         text_data = file_output.get_sorted_text(page_number=i, text_elements=[""])


class ParsrOutputInterpreter(object):
    """Functions to interpret Parsr's resultant JSON file, enabling
    access to the underlying document content
    """

    def __init__(self, object=None):
        """Constructor for the class

        - object: the Parsr JSON file to be loaded
        """
        logging.basicConfig(level=logging.DEBUG,
                            format='%(name)s - %(levelname)s - %(message)s')
        self.object = None
        self.page_count = None
        if object is not None:
            self.load_object(object)
        self.page_count = len(self.object['pages'])
        self.get_text_elements_list = ['word', 'line', 'character', 'paragraph', 'heading']

    
    def load_object(self, object):
        self.object = object


    def get_page_raw(self, page_number: int):
        """Get a particular page raw json in a document
        
        Params
        ---------
        page_number: The page number to be searched

        Return
        -------------
        raw json file corresponding to the page number
        """
        for p in self.object['pages']:
            if p['pageNumber'] == page_number:
                return p
        logging.error("Page {} not found".format(page_number))
        return None


    def __get_text_objects(self, page_number:int=None, text_elements:list=["paragraph"]):
        """
        Get the specified text elements from the page

        Params
        -------------
        page_number: the page from which text elements need to be searched
        text_elements: list of text_elements which need to be extracted

        Return
        ----------
        list of text elements
        
        """
        texts = []
        if page_number is not None:
            page = self.get_page_raw(page_number)
            if page is None:
                logging.error(
                    "Cannot get text elements for the requested page; Page {} not found".format(page_number))
                return None
            else:
                for element in page['elements']:
                    if element['type'] in text_elements:
                        texts.append(element)
        else:
            texts = self.__text_objects_none_page(text_elements)

        return texts


    def __text_objects_none_page(self, text_elements):
        texts = []
        for page in self.object['pages']:
            for element in page['elements']:
                if element['type'] in text_elements:
                    texts.append(element)
        return texts


    def __text_from_text_object(self, text_object: dict) -> str:
        """
        Get the text from text_element
        """
        result = ""
        if (text_object['type'] in ['paragraph', 'heading']) or (
                text_object['type'] in ['line']):
            for i in text_object['content']:
                result += self.__text_from_text_object(i)
        elif text_object['type'] in ['word']:
            if isinstance(text_object['content'], list):
                for i in text_object['content']:
                    result += self.__text_from_text_object(i)
            else:
                result += text_object['content']
                result += ' '
        elif text_object['type'] in ['character']:
            result += text_object['content']
        return result


    def get_text_elements(self, page_number: int = None, 
                          text_elements:list = ["paragraph"]) -> dict:
        """Get the entire text from a particular page.

        Params
        ------------
        page_number: page from which text elements need to be extracted
        text_elements: list of text_elments which need to be extracted


        Return
        ---------------
        text_list: dictionary with key=element type (only first order), and value = list 
                    of those element types.



        """
        text_list = {}
        for text_element in text_elements:
            text_element_list = []
            for text_obj in self.__get_text_objects(page_number=page_number,text_elements=[text_element]):
                final_text = ""
                final_text += self.__text_from_text_object(text_obj)
                if text_element == "heading":
                    text_element_list.append((final_text, text_obj["level"]))
                else:
                    text_element_list.append(final_text)
            text_list[text_element]= text_element_list
            
        return text_list
    
    def get_sorted_text(self, page_number: int = None, text_elements:list = ["paragraph"]) -> list:
        """Get the entire text from a particular page

        - page_number: The page number from which all the text is to be
        extracted
        """
        text_list = []
        # for text_element in text_elements:
        #     text_element_list = []
        for text_obj in self.__get_text_objects(page_number=page_number,text_elements=text_elements):
            final_text = ""
            final_text += self.__text_from_text_object(text_obj)
            text_list.append((text_obj["type"], final_text))
        return text_list

    
    



    




