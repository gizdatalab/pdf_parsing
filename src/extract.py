# script to extract data from directory and write content from PDFs into machine readable format

import os
from langchain.document_loaders import PyMuPDFLoader

def extract(extract_path = '../documents/'):
    
    docs = []
    for pdf_document in os.listdir(extract_path):
        try:
            docs.append(PyMuPDFLoader(os.path.join(extract_path, pdf_document)).load())
        except Exception as e:
            print("Exception: ", e)

    return docs

