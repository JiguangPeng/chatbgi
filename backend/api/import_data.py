from pdfminer.high_level import extract_text
import re
import sys
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams
from qdrant_client.http.models import PointStruct, PointIdsList, Filter, FieldCondition, MatchValue
from text2vec import SentenceModel, EncoderType

import os
import tqdm

def process(mode, input,suffix):
    info(f"mode:{mode}  process {input}")
    if mode == 'tr':
        if input.endswith(".pdf"):
            file = pdf_parser(input,w2v_model,write=True)
        elif input.endswith("/"):
            info(f"read input:{input}")
            for root, dirs, files in os.walk(input):
                for file in tqdm.tqdm(files):
                    if file.endswith(".pdf"):
                        file_path = os.path.join(root, file)
                        file = pdf_parser(file_path,w2v_model,write=True)
    elif mode == 'db':
        qd_client = qdrant_client(collection_name, size=768, port=6333)
        qd_client.recreate()
        if input.endswith("/"):
            qdrant_from_dir(qd_client, input, suffix, port=6333)
        else:
            qdrant_from_file(qd_client,input , suffix, port=6333)


class file_parser():
    def __init__(self) -> None:
        self.content = self.split_message()
        if self.write:
            self.write_content()
        self.embedding_lst=self.to_embeddings(self.content)

    def to_embeddings(file_path: str, text: str):
        sentences = text.split("\n")
        embedding_lst = list(zip([file_path]*len(sentences),
                             sentences, w2v_model.encode(sentences).tolist()))
        return embedding_lst

    def write_content(self):
        new_file = os.path.splitext(self.file_path)[0]+".txt"
        with open(new_file, "w") as f:
            f.write(self.content)

    def split_m(self,sentence, count=0):
        last_b = 0
        for idx, m in enumerate(sentence):
            if m in ["。", "."]:
                last_b = idx
            if idx - count > 300 and last_b - count >10:
                count = last_b
                yield last_b+1

    def split_message(self):
        sentences = self.content.split("\n")
        filter_sentence = self.filter_short(sentences)
        split_messege = []
        for idn, sentence in enumerate(filter_sentence):
            if len(sentence) > 300:
                message = sorted(list(set(self.split_m(sentence))))
                if len(message) > 2 and len(sentence) - message[-1] < 100:
                    a = [0] + message[:-1] + [len(sentence)]
                else:
                    a = [0] + message + [len(sentence)]
                split_i = []
                for idx, i in enumerate(a[:-1]):
                    split_i += [sentence[a[idx]:a[idx+1]]]
                print(f"idn:{idn} len:{len(sentence)} ==> part:{len(split_i)} {a}")
                split_messege += ["\n".join(split_i)]
            else:
                split_messege += [sentence]
        return "\n".join(split_messege)

    def filter_short(self,sentences):
        filter_sentence=[ i for i in sentences if len(i)>20 ]
        return filter_sentence




class pdf_parser(file_parser):
    def __init__(self,file_path, model, write) -> None:
        self.file_path=file_path
        self.model=model
        self.write=write
        self.content=self.extract_pdf()
        super().__init__()

    def extract_pdf(self, write=True):
        text = extract_text(self.file_path)
        text = re.sub(r'\D[。.][\n\t ]{2,}', '<br>', text)
        text = re.sub(r'\x0c', '', text)
        text = re.sub(r'[\n\t ]+', '', text)
        # text = re.sub(r'[\t ]{2,}', ' ', text)
        text = re.sub(r'<br>', '\n', text)
        # 增加一步过长内容分割
        return text

class txt_parser(file_parser):
    def __init__(self, file_path, model, write) -> None:
        self.file_path=file_path
        self.model=model
        self.write=write
        self.content=self.extract_txt()
        super().__init__()

    def extract_txt(self):
        text=open(self.file_path).read()
        return text

class word_parser():
    def __init__(self) -> None:
        pass

def choose_parser(suffix):
    if suffix =="pdf":
        return pdf_parser
    elif suffix =="txt":
        return txt_parser



class qdrant_client():
    def __init__(self, collection_name, size=768, port=6333):
        self.client = QdrantClient("127.0.0.1", port=port)
        # 创建collection,use COSINE distance
        self.collection_name = collection_name
        self.size=size

    def recreate(self):
        self.client.recreate_collection(
            collection_name=self.collection_name,
            vectors_config=VectorParams(size=self.size, distance=Distance.COSINE),
        )

    def upsert(self, point_ID, vector, payload):
        self.client.upsert(
            collection_name=self.collection_name,
            wait=True,
            points=[
                PointStruct(id=point_ID, vector=vector,
                            payload=payload),
            ],
        )

    def search(self, query_vector, top_k=3):
        search_result = self.client.search(
            collection_name=self.collection_name,
            query_vector=query_vector,
            limit=top_k,
            # search_params={"exact": False, "hnsw_ef": 128}
        )
        return search_result

    def delete_point(self, point_ID):
        self.client.delete(
            collection_name=self.collection_name,
            points_selector=PointIdsList(
                points=point_ID,
            ),)

    def retrieve(self, point_ID):
        retrieve_result = self.client.retrieve(
            collection_name=self.collection_name,
            point_id=point_ID,
        )
        return retrieve_result

    def scroll(self, key, value):
        scroll_result = self.client.scroll(collection_name=self.collection_name,
                                           scroll_filter=Filter(must=[
                                               FieldCondition(
                                                   key=key, match=MatchValue(value=value)),
                                           ]
                                           ), limit=1,
                                           with_payload=True,
                                           with_vector=False,
                                           )
        return scroll_result
    def delete_collection(self):
        self.client.delete_collection(collection_name=self.collection_name)

    def info(self):
        return self.client.get_collection(collection_name=self.collection_name)


def qdrant_from_dir(client, workdir, suffix, port=6333):
    count = 0
    parser=choose_parser(suffix)
    for root, dirs, files in os.walk(workdir):
        for file in tqdm.tqdm(files):
            file_path = os.path.join(root, file)
            if file.endswith(suffix):
                info(f"import {suffix} file: {file} ")
                # file 读取
                paserclass = parser(file_path, w2v_model, write=False)
                # contend 为pdf的内容，str类型
                embedding_lst = paserclass.embedding_lst
                # debug(len(embedding_lst))
                for i in embedding_lst:
                    client.upsert(point_ID=count, vector=i[2], payload={
                                  "file_path": file, "text": i[1]})
                    count += 1
            else:
                info(f"ignore file type: {file} ")


def qdrant_from_file(client, file_path, suffix, port=6333):
    count = 0
    parser=choose_parser(suffix)
    if file_path.endswith(suffix):
        info(f"import {suffix} file: {file_path} ")
        # file 读取
        paserclass = parser(file_path, w2v_model, write=False)
        # contend 为pdf的内容，str类型
        embedding_lst = paserclass.embedding_lst
        # debug(len(embedding_lst))
        for i in embedding_lst:
            client.upsert(point_ID=count, vector=i[2], payload={
                          "file_path": file_path, "text": i[1]})
            count += 1
    else:
        info(f"ignore file type: {file_path} ")





def server_init(collection_name):
    qd_client = qdrant_client(collection_name, size=768, port=6333)
    return qd_client


if __name__ == "__main__":
    from mylog import info, debug
    from mylog import logging_init
    w2v_model = SentenceModel("shibing624/text2vec-base-chinese",
                              encoder_type=EncoderType.FIRST_LAST_AVG)
    collection_name = "data_collection"
    logging_init("info")
    process(sys.argv[1], sys.argv[2],sys.argv[3])
