import sys
import jieba
import jieba.analyse
from collections import defaultdict
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams
from qdrant_client.http.models import PointStruct, PointIdsList, Filter, FieldCondition, MatchValue,MatchAny,MatchText

def keyword_search(user_input,exact_dict,matchtext_set):
    # jieba.analyse.set_idf_path("/home/aigi/project/ChatBGI/document_ai/chatBGI/userdict.txt")
    keywords=jieba.analyse.extract_tags(user_input,topK=20, withWeight=False)
    print(f'user_input keyword:{keywords}')
    group_choose=[]
    match_text=[]
    for keyword in keywords:
        if keyword in matchtext_set:
            # info(f"match_text {keyword}")
            match_text+=[keyword]
        group_choose+=exact_dict[keyword]
    return keywords,group_choose,match_text

def load_exact_dict(datadir):
    exact_dict=defaultdict(list)
    for group in ["member","product","history","strategy"]:
        with open(f'{datadir}/{group}.txt', 'r') as f:
            group_lst = f.read().splitlines()
            for i in group_lst:
                exact_dict[i]+=[group]
    return exact_dict

def load_matchtext_set(file_path):
    matchtext_set = set()
    with open(file_path, 'r') as f:
        for line in f:
            columns = line.split()
            matchtext_set.add(columns[0])
    return matchtext_set

def filter_knowledge(knowledge1,knowledge2):
        answers1 = [f"- {result.payload['text']}" for result in knowledge1]
        answers2 = [f"- {result.payload['text']}" for result in knowledge2]
        show_knowledge = knowledge1 + knowledge2
        show_answers=[f"({result.payload['tag']} {result.score})《{result.payload['file_name']}》:{result.payload['text']}"
                    for result in show_knowledge]
        #排序去重
        count1,count2=0,0
        while len(' '.join(answers1+answers2)) > 2000 and len(answers1+answers2) > 1:
            if len(answers2) > 1 and len(answers1) -len(answers2) <2:
                count2+=1
                answers2 = answers2[:-1]
            elif len(answers1) > 1:
                count1+=1
                answers1 = answers1[:-1]
            else:
                break
        answers=answers1+answers2
        if count1 or count2:
            print(f">>> cut {count1} exact knowledge and {count2} corpus knowledge\n>>> remain {len(answers1)} + {len(answers2)} answers length:{len(' '.join(answers))}")
        str_knowledge1= "\n".join(answers1[::-1])
        str_knowledge2= "\n".join(answers2[::-1])
        show_str_knowledges = "\n".join(["- " + i for i in show_answers])+"\n"
        return str_knowledge1,str_knowledge2,show_str_knowledges


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

    def combine_search(self, query_vector,keywords,group_choose,match_text, top_k=[3,3]):
        update=True
        print(f">>>> (search mode) ingroup:{group_choose};match_text:{match_text}")
        if group_choose and match_text:
            search_result1 = self.client.search(
                collection_name=self.collection_name["exact"],
                query_vector=query_vector,
                limit=top_k[0],
                # score_threshold = 0.9,
                query_filter=Filter(must=[FieldCondition(key="tag",match=MatchAny(any=group_choose),),],
                                    should=[FieldCondition(key="text",match=MatchText(text=i),) for i in match_text])
                # search_params={"exact": False, "hnsw_ef": 128}
            )
            search_result2 = self.client.search(
                collection_name=self.collection_name["corpus"],
                query_vector=query_vector,
                limit=top_k[1],
                query_filter=Filter(should=[FieldCondition(key="text",match=MatchText(text=i),) for i in match_text])
                # search_params={"exact": False, "hnsw_ef": 128}
            )
        elif group_choose:

            search_result1 = self.client.search(
                collection_name=self.collection_name["exact"],
                query_vector=query_vector,
                limit=top_k[0],
                # score_threshold = 0.9,
                query_filter=Filter(must=[FieldCondition(key="tag",match=MatchAny(any=group_choose),),],
                                    should=[FieldCondition(key="text",match=MatchText(text=i),) for i in keywords])
                # search_params={"exact": False, "hnsw_ef": 128}
            )
            search_result2 = self.client.search(
                collection_name=self.collection_name["corpus"],
                query_vector=query_vector,
                limit=top_k[1],
                query_filter=Filter(should=[FieldCondition(key="text",match=MatchText(text=i),) for i in keywords])
                # search_params={"exact": False, "hnsw_ef": 128}
            )
        elif match_text:
            search_result1 = self.client.search(
                collection_name=self.collection_name["exact"],
                query_vector=query_vector,
                limit=top_k[0],
                query_filter=Filter(should=[FieldCondition(key="text",match=MatchText(text=i),) for i in match_text])
                # score_threshold = 0.9,
                # search_params={"exact": False, "hnsw_ef": 128}
            )
            search_result2 = self.client.search(
                collection_name=self.collection_name["corpus"],
                query_vector=query_vector,
                limit=top_k[1],
                query_filter=Filter(should=[FieldCondition(key="text",match=MatchText(text=i),) for i in match_text])
                # search_params={"exact": False, "hnsw_ef": 128}
            )
        else:
            search_result1 = self.client.search(
                collection_name=self.collection_name["exact"],
                query_vector=query_vector,
                limit=top_k[0],
                query_filter=Filter(should=[FieldCondition(key="text",match=MatchText(text=i),) for i in keywords])
                # score_threshold = 0.9,
                # search_params={"exact": False, "hnsw_ef": 128}
            )
            search_result2 = self.client.search(
                collection_name=self.collection_name["corpus"],
                query_vector=query_vector,
                limit=top_k[1],
                query_filter=Filter(should=[FieldCondition(key="text",match=MatchText(text=i),) for i in keywords])
                # search_params={"exact": False, "hnsw_ef": 128}
            )
            update=False
        str_knowledge1,str_knowledge2,show_str_knowledges = filter_knowledge(search_result1,search_result2)
        return str_knowledge1,str_knowledge2,show_str_knowledges,update

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
        matchtext_set=load_matchtext_set(f'{exact_keyword_path}/keyword_dict.txt')
        exact_dict=load_exact_dict(exact_keyword_path)
        qd_client = qdrant_client()
        return qd_client,exact_dict,matchtext_set
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
