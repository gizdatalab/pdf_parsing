# script to process and transform the data - includes chunking, cleaning and embedding

import os
from langchain.text_splitter import RecursiveCharacterTextSplitter, SentenceTransformersTokenTextSplitter
from transformers import AutoTokenizer
from llama_index.core import Document
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.extractors import TitleExtractor
from llama_index.core.ingestion import IngestionPipeline, IngestionCache

def lama_pipeline_transform(docs, splitter = RecursiveCharacterTextSplitter, 
              chunk_size = 256, 
              tokenizer_model = "BAAI/bge-small-en-v1.5",
              chunk_overlap_denominator = 10,
              add_start_index=True,
              strip_whitespace=True,
              separators=["\n\n", "\n", ".", " ", ""]
              ):

    default_splitter = splitter.from_huggingface_tokenizer(
        AutoTokenizer.from_pretrained(tokenizer_model),
        chunk_size=chunk_size,
        chunk_overlap=int(chunk_size / chunk_overlap_denominator),
        add_start_index=add_start_index,
        strip_whitespace=strip_whitespace,
        separators=separators,
    )

    transformed_docs = default_splitter.core.get_nodes_from_documents(docs)

    return transformed_docs