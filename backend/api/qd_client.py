import sys
import jieba
import jieba.analyse
from collections import defaultdict
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams
from qdrant_client.http.models import PointStruct, PointIdsList, Filter, FieldCondition, MatchValue,MatchAny,MatchText

def keyword_search(user_input,exact_dict):
    # jieba.analyse.set_idf_path("/home/aigi/project/ChatBGI/document_ai/chatBGI/userdict.txt")
    keywords=jieba.analyse.extract_tags(user_input,topK=20, withWeight=False)
    print(f'user_input keyword:{keywords}')
    key_in_exact=[]
    member_lst=[]
    for keyword in keywords:
        if 'member' in exact_dict[keyword]:
            print(f"member_name {keyword}")
            member_lst+=[keyword]
        key_in_exact+=exact_dict[keyword]
    return key_in_exact,member_lst

def load_exact_dict(datadir):
    exact_dict=defaultdict(list)
    for group in ["member","product","history","strategy"]:
        with open(f'{datadir}/{group}.txt', 'r') as f:
            member_lst = f.read().splitlines()
            for i in member_lst:
                exact_dict[i]+=[group]
    return exact_dict


class qdrant_client():
    def __init__(self, size=768, port=6333):
        self.client = QdrantClient("127.0.0.1", port=port)
        # 创建collection,use COSINE distance
        self.collection_name = {"exact": "exact_collection","corpus":"corpus_collection"}
        self.size=size

    def recreate(self,collection):
        self.client.recreate_collection(
            collection_name=self.collection_name[collection],
            vectors_config=VectorParams(size=self.size, distance=Distance.COSINE),
        )

    def upsert(self, collection, point_ID, vector, payload):
        self.client.upsert(
            collection_name=self.collection_name[collection],
            wait=True,
            points=[
                PointStruct(id=point_ID, vector=vector,
                            payload=payload),
            ],
        )
        
    def combine_search(self, query_vector,keyword,member_lst,  top_k=[3,3]):
        if member_lst:
            search_result1 = self.client.search(
                collection_name=self.collection_name["exact"],
                query_vector=query_vector,
                limit=top_k[0],
                # score_threshold = 0.9,
                query_filter=Filter(must=[FieldCondition(key="tag",match=MatchAny(any=keyword),),
                                     FieldCondition(key="text",match=MatchText(text=member_lst[0]),)])
                # search_params={"exact": False, "hnsw_ef": 128}
            )
        else:
            search_result1 = self.client.search(
                collection_name=self.collection_name["exact"],
                query_vector=query_vector,
                limit=top_k[0],
                # score_threshold = 0.9,
                query_filter=Filter(must=[FieldCondition(key="tag",match=MatchAny(any=keyword),)]),
                # search_params={"exact": False, "hnsw_ef": 128}
            )
        search_result2 = self.client.search(
            collection_name=self.collection_name["corpus"],
            query_vector=query_vector,
            limit=top_k[1],
            # search_params={"exact": False, "hnsw_ef": 128}
        )

        knowledge = search_result1+search_result2
        return knowledge

    def search(self, collection,query_vector, top_k=3):
        search_result = self.client.search(
            collection_name=self.collection_name[collection],
            query_vector=query_vector,
            limit=top_k,
            # search_params={"exact": False, "hnsw_ef": 128}
        )
        return search_result

    def delete_point(self, collection, point_ID):
        self.client.delete(
            collection_name=self.collection_name[collection],
            points_selector=PointIdsList(
                points=point_ID,
            ),)

    def retrieve(self, collection, point_ID):
        retrieve_result = self.client.retrieve(
            collection_name=self.collection_name[collection],
            point_id=point_ID,) # type: ignore
        return retrieve_result

    def scroll(self, collection, key, value):
        scroll_result = self.client.scroll(collection_name=self.collection_name[collection],
                                           scroll_filter=Filter(must=[
                                               FieldCondition(
                                                   key=key, match=MatchValue(value=value)),
                                           ]
                                           ), limit=1,
                                           with_payload=True,
                                           with_vector=False,
                                           )
        return scroll_result
    def delete_collection(self,collection):
        self.client.delete_collection(collection_name=self.collection_name[collection])

    def info(self,collection):
        return self.client.get_collection(collection_name=self.collection_name[collection])


def server_init(exact_keyword_path=""):
    if exact_keyword_path:
        jieba.set_dictionary(f'{exact_keyword_path}/keyword_dict.txt')
        exact_dict=load_exact_dict(exact_keyword_path)
        qd_client = qdrant_client()
        return qd_client,exact_dict
    else:
        qd_client = qdrant_client()
        return qd_client


if __name__ == "__main__":
    qd_client=server_init()
    if sys.argv[1]=="info":
        print(qd_client.info("exact"))
        print(qd_client.info("corpus"))
    elif sys.argv[1]=="clear":
        qd_client.client.delete_collection(sys.argv[2])
    elif sys.argv[1]=="create":
        qd_client.recreate(sys.argv[2])
        # qd_client.client.recreate_collection("exact_collection")
