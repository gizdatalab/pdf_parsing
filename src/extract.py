# script to extract data from directory and write content from PDFs into machine readable format

# could also use this function to return images for OCR?

import os
# langchain PyMuPDF
from langchain.document_loaders import PyMuPDFLoader
# llama-index simple reader & Nougat 
from llama_index.core import SimpleDirectoryReader
from llama_index.readers.nougat_ocr import PDFNougatOCR
from llama_index.readers.file import UnstructuredReader

# haystack 2.0

def lc_extract_pdf(extract_path = '../documents/'):
    
    if extract_path == '../documents/':

        print("data path optional - default is '../documents/'")

    docs = []
    for pdf_document in os.listdir(extract_path):
        try:
            docs.append(PyMuPDFLoader(os.path.join(extract_path, pdf_document)).load())
        except Exception as e:
            print("Exception: ", e)

    print("Langchain Doc Extraction successful!")

    return docs

def lli_extract_pdf(extract_path = '../documents/'):

    if extract_path == '../documents/':

        print("data path optional - default is '../documents/'")

    
