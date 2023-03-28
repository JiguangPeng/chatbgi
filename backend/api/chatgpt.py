import json
import time
from azureChatGPT import Chatbot as cb
import asyncio
from api.config import config
import uuid
from collections import defaultdict
from datetime import datetime,timedelta
from api.qd_client import keyword_search,server_init as qdrant_server_init
from text2vec import SentenceModel, EncoderType

class ChatGPTManager:
    def __init__(self):
        self.api_dict = {}
        self.semaphore_dict = defaultdict(lambda: asyncio.Semaphore(1))
        self.database_system_prompt = config.get('database_system_prompt')
        self.default_system_prompt = config.get('default_system_prompt')
        self.model = SentenceModel("shibing624/text2vec-base-chinese",encoder_type=EncoderType.FIRST_LAST_AVG)
        self.client,self.exact_dict = qdrant_server_init(config.get('database_keyword'))

    def load_api(self, conversation_id: str, conversation_history: str):
        if conversation_id is None:
            conversation_id = str(uuid.uuid4())
        self.api_dict[conversation_id] = cb()
        self.api_dict[conversation_id].load(config.get("azure_yaml"))
        self.load_record(conversation_id, conversation_history)
        self.api_dict[conversation_id].active_time = datetime.utcnow()
        return conversation_id

    def clean_api(self):
        del_api_set = [i for i in self.api_dict.keys() if datetime.utcnow() - self.api_dict[i].active_time > timedelta(seconds=600)]
        for i in del_api_set:
            self.delete_conversation(i)

    def is_busy(self, conversation_id: str):
        return self.semaphore_dict[conversation_id].locked()

    def load_record(self, conversation_id: str, record: str):
        if not record:
            return
        record_dict = json.loads(record)
        conversation_item = {"role": "", "content": ""}
        itemdict = record_dict["mapping"][record_dict["current_node"]]
        conversation_list = [
            {
                "role": itemdict["message"]["author"]["role"],
                "content": itemdict["message"]["content"]["parts"][0],
            }
        ]
        point = itemdict["parent"]
        for _ in range(len(record_dict["mapping"]) - 1):
            itemdict = record_dict["mapping"][point]
            conversation_list.append(
                {
                    "role": itemdict["message"]["author"]["role"],
                    "content": itemdict["message"]["content"]["parts"][0],
                }
            )
            point = itemdict["parent"]
        self.api_dict[conversation_id].conversation["default"] = (
            self.api_dict[conversation_id].conversation["default"][:1]
            + conversation_list[::-1]
        )

    def get_conversation_messages(self, conversation_id: str):
        record_dict = {"mapping": {}, "current_node": ""}
        item = '{"message": {"author": {"role": ""}, "content": {"parts": [""]}}, "children": [], "parent":""}'
        records = self.api_dict[conversation_id].conversation["default"][1:]
        for idx, record in enumerate(records):
            itemdict = json.loads(item)
            itemdict["message"]["author"]["role"] = record["role"]
            itemdict["message"]["content"]["parts"][0] = record["content"]
            if idx > 0:
                itemdict["parent"] = str(idx - 1)
            if idx < len(records) - 1:
                itemdict["children"].append(str(idx + 1))
            if idx == len(records) - 1:
                record_dict["current_node"] = str(idx)
            record_dict["mapping"][str(idx)] = itemdict
        return record_dict

    def get_ask_generator(
        self,
        message,
        use_paid=False,
        conversation_id: str = None,
        parrent_id: str = None,
        conversation_history: str = "",
        timeout=360,
    ):
        if conversation_id is None or self.api_dict.get(conversation_id, True):
            conversation_id = self.load_api(conversation_id, conversation_history)
        if use_paid or "BGI" in message or 'bgi' in message or "华大" in message:
            knowledge=self.get_knowledege(message)
            self.api_dict[conversation_id].conversation["default"][0]["content"] = f"{self.database_system_prompt}\n来源:{knowledge}\n"
            use_paid = True
        else:
            self.api_dict[conversation_id].conversation["default"][0]["content"] = f"{self.default_system_prompt}"
        print(self.api_dict[conversation_id].conversation["default"][0]["content"])
        return (
            use_paid,
            self.api_dict[conversation_id].ask_stream(prompt=message, role="user"),
            conversation_id,
            str(time.time()),
        )
    def get_knowledege(self, message):
        keyword,member_lst = keyword_search(message,self.exact_dict)
        query_vector = self.model.encode(message)
        knowledge = self.client.combine_search(query_vector, keyword,member_lst, top_k=[2,3])
        answers = [f"- {result.payload['text'][:400]}" for result in knowledge]
        answers=list(set(answers))
        answers.sort(key=answers.index)
        return "\n".join(answers) + "\n"

    def delete_conversation(self, conversation_id: str):
        del self.api_dict[conversation_id]
