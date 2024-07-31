"""
Copied with minimal edit from https://gitlab.smart-aging.tech/ds/ai/ai-VoiceFill/-/blob/DEV/keyword_method.py
"""

import json
import re
from pathlib import Path
# 假設中華電信語音轉文字都是正確


# 定位所有字典中提到的關鍵字
class WordLocator:
    def __init__(self, path='vital.json'):
        base_path = Path(__file__).parent
        config_path = (base_path / path).resolve()
        with open(config_path, 'r') as file:
            self.keyword_book = json.load(file)

    def key_word_search(self, key, key_word, word):
        key_word = key_word.split(',')
        los = []
        for kw in key_word:
            key_reobj = re.compile(kw, re.IGNORECASE)
            for match in key_reobj.finditer(word):
                groi = [key, match.start(), match.end(), word[match.start():match.end() + 6]]
                los.append(groi)
        los = sorted(los, key=lambda s: s[1])
        los = self.merge_near_item(los)
        return los
    
    # 關鍵字位置從小到大排序
    def merge_near_item(self, search_list):
        ite = 0
        while ite + 1 < len(search_list):
            if (search_list[ite][2] - search_list[ite + 1][1]) >= (-1):  # intersection or Align
                search_list[ite][2] = max(search_list[ite][2], search_list[ite + 1][2])
                del search_list[ite + 1]
            else:
                ite += 1
        return search_list
    
    def key_key_search(self, key_dict, word):
        result = []
        for sub_key in key_dict.keys():
            result.extend(self.key_word_search(sub_key, key_dict[sub_key], word)) 
        return result
    
    def key_list_search(self, key_list, word):
        result = []
        for sub_item in key_list:
            result.extend(self.key_word_search(list(sub_item.keys())[0], list(sub_item.values())[0], word))
        return result

    def locate_keyword(self, word):
        # word = word.replace(" ","") <--bad call
        result = []
        for key in self.keyword_book.keys():
            if isinstance(self.keyword_book[key], str):
                result.extend(self.key_word_search(key, self.keyword_book[key], word))
            if isinstance(self.keyword_book[key], dict):    
                result.extend(self.key_key_search(self.keyword_book[key], word))
            if isinstance(self.keyword_book[key], list):
                result.extend(self.key_list_search(self.keyword_book[key], word))
        result = sorted(result, key=lambda s: s[1])
        result = self.merge_near_item(result)
        return result


class ValueExtractor(WordLocator):
    # The result (max_search 20) is derived from the validation dataset through search.
    def __init__(self, path='vital.json', max_search=20):
        super().__init__(path)
        self.re_params = {
            "TP": r"(3\d點\d|4[0-5]點\d|3\d\.\d|4[0-5]\.\d|3\d|4[0-5])",  # 30-45
            "HR": r"(3\d|4\d|5\d|6\d|7\d|8\d|9\d|1\d{2}|2[0-2]\d)",  # 30-220
            "RR": r"[0-2]\d|\d",  # 0-29
            "SBP": r"(300|[1-2]\d{2}|[1-9]\d|\d)",
            "DBP": r"\b(1[0-8]\d|[1-9]\d|\d)",
            "SPO2": r"(\b4\d|5\d|6\d|7\d|8\d|9\d|100|1)",
            "FLOW": r"([0-2]?[0-9])",
            "PAIN": r"(10|[1-9])"
        }
        self.max_search = max_search

    def preprocess_word(self, word):
        if '點' in word:
            word = word.replace('點', '.')
        word = word.replace(' ', '')
        for sym in "!#$%&'(/)*+,-:;<=>?@，[\]^、_`{|}~":
            word = word.replace(sym, '')
        return word 

    def chinese_repl(self, data):
        cnNum = ['零', '一', '二', '三', '四', '五', '六', '七', '八', '九', '百']
        enNum = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '00']

        for i in range(len(cnNum)):
            data = data.replace(cnNum[i], enNum[i])

        data = list(data)
        for i, word in enumerate(data):
            # print(data)
            if i == 0 and word == '十':
                data[i] = '10'

            elif i != len(data) - 1 and word == '十':
                if re.search("[1-9]", data[i - 1]) is not None and re.search("[1-9]", data[i + 1]) is not None:
                    data[i] = ''
                elif re.search("[1-9]", data[i - 1]) is not None:
                    data[i] = '0'
                elif re.search("[1-9]", data[i + 1]) is not None:
                    data[i] = '1'
                else:
                    data[i] = '10'
            elif i == len(data) - 1 and word == '十':
                if re.search("[1-9]", data[i - 1]) is not None:
                    data[i] = '0'
                else:
                    data[i] = '10'

        data = ''.join(data)
        return data

    def extract_value(self, word):
        word = self.chinese_repl(self.preprocess_word(word))
        possible_locate = self.locate_keyword(word)
        res = []
        for item in range(len(possible_locate)):
            
            # define search range word
            if item == len(possible_locate) - 1:  # last searching item
                if len(word[possible_locate[item][2]:]) > self.max_search:
                    last = possible_locate[item][2] + self.max_search
                else:
                    last = None
                search_word = word[possible_locate[item][2]:last]
            else:
                if len(word[possible_locate[item][2]:possible_locate[item + 1][1]]) > self.max_search:
                    last = possible_locate[item][2] + self.max_search
                else:
                    last = possible_locate[item + 1][1]
                search_word = word[possible_locate[item][2]:last]

            # case BP
            if possible_locate[item][0] in ['BP', 'SBP', 'DBP']:
                # case like BP 110/90
                if possible_locate[item][0] == 'BP':
                    match = re.search(self.re_params['SBP'], search_word)
                    if match is None:
                        continue
                    match2 = re.search(self.re_params['DBP'], search_word[match.span()[1]:])
                    if match is not None and match2 is not None:
                        res.append({'SBP': match.group()})
                        res.append({'DBP': match2.group()})
                # BP subcase： like SBP 110 DBP 90
                else:
                    match = re.search(self.re_params[possible_locate[item][0]], search_word)
                    if match is None:
                        continue
                    res.append({possible_locate[item][0]: match.group()})

            # other case in quantifiable numbers like tp/pain
            elif possible_locate[item][0] in self.re_params.keys():
                match = re.search(self.re_params[possible_locate[item][0]], search_word)
                if match is None:
                    continue
                res.append({possible_locate[item][0]: match.group()})
            else:
                res.append({possible_locate[item][0]: "1"})

        return res
    
    def extract_note(self, word):
        notes = re.findall(r'(?<=(備註開始|開始備註))(.*?)(?=(備註結束|結束備註))', word)
        notes = [n[1] for n in notes]

        return notes
    